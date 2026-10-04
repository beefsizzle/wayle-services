use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, error, warn};
use wayle_traits::ServiceMonitoring;

use crate::{
    Error,
    ffi::{AudioInput, AudioOutput, Config},
    service::CavaService,
};

impl ServiceMonitoring for CavaService {
    type Error = Error;

    async fn start_monitoring(&self) -> Result<(), Self::Error> {
        let bars_raw = self.bars.get();
        let stereo = self.stereo.get();
        let bars_adjusted = bars_raw.adjusted_for_stereo(stereo);
        if bars_adjusted != bars_raw {
            warn!(
                requested = %bars_raw,
                adjusted = %bars_adjusted,
                "odd bar count rounded up for stereo output"
            );
        }
        let bars = bars_adjusted.value() as usize;
        let autosens = self.autosens.get();
        let noise_reduction = self.noise_reduction.get();
        let monstercat = self.monstercat.get();
        let waves = self.waves.get();
        let framerate = self.framerate.get().value();
        let input = self.input.get();
        let source = self.source.get();
        let low_cutoff = self.low_cutoff.get();
        let high_cutoff = self.high_cutoff.get();
        let samplerate = self.samplerate.get();

        let channels = if stereo { 2 } else { 1 };
        let buffer_size = calculate_cava_buffer_size(samplerate, channels);

        let mut audio_input = AudioInput::new(buffer_size, channels, samplerate)?;
        let mut config = Config::new(
            bars,
            autosens,
            stereo,
            noise_reduction,
            monstercat,
            waves,
            framerate,
            input.into(),
            channels,
            samplerate,
            low_cutoff,
            high_cutoff,
            &source,
        )?;

        let input_fn = audio_input.setup_input(&mut config)?;

        let mut audio_output = AudioOutput::new(bars);
        let plan = audio_output.init(&mut audio_input, &mut config)?;

        audio_input.spawn_input_thread(input_fn);

        let values = self.values.clone();
        let cancellation = {
            let token = self
                .cancellation_token
                .lock()
                .map_err(|_| Error::InitFailed("cannot lock cancellation token".to_string()))?;
            token.child_token()
        };

        tokio::spawn(async move {
            let interval_duration = Duration::from_millis(1000 / framerate as u64);
            let mut interval = tokio::time::interval(interval_duration);
            let mut pacer = TickPacer::new(interval_duration);

            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        debug!("Cava visualization loop cancelled");
                        return;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = audio_input.lock() {
                            error!(error = %e, "cannot lock audio input mutex");
                            continue;
                        }

                        plan.execute(&audio_input, &audio_output);

                        if audio_input.samples_counter() > 0 {
                            audio_input.reset_samples_counter();
                        }

                        if let Err(e) = audio_input.unlock() {
                            error!(error = %e, "cannot unlock audio input mutex");
                        }

                        let new_values = audio_output.values().to_vec();
                        if let Some(period) = pacer.observe(&new_values) {
                            debug!(?period, "cava tick period changed");
                            interval = tokio::time::interval_at(Instant::now() + period, period);
                        }
                        values.set(new_values);
                    }
                }
            }
        });

        Ok(())
    }
}

/// Bar level at or below which a frame counts as silent. Bars are normalised to
/// `0.0..=1.0`, so this is well under one pixel on any bar.
const SILENT_LEVEL: f64 = 1e-3;

/// How long output must stay silent before the loop slows down.
const SILENCE_GRACE: Duration = Duration::from_secs(2);

/// Tick period while silent. Short enough that bars react to resumed audio
/// within a frame or two, and well within the capture buffer's capacity.
const IDLE_PERIOD: Duration = Duration::from_millis(100);

/// Picks the visualization tick period: the configured framerate while there is
/// sound, and [`IDLE_PERIOD`] once output has been silent for [`SILENCE_GRACE`].
///
/// Silent frames produce identical (all-zero) values, so running at the full
/// framerate only burns wakeups and FFT work while nothing changes on screen.
struct TickPacer {
    active: Duration,
    idle: Duration,
    grace_ticks: u32,
    silent_ticks: u32,
}

impl TickPacer {
    fn new(active: Duration) -> Self {
        let grace_ticks = SILENCE_GRACE
            .as_nanos()
            .checked_div(active.as_nanos())
            .and_then(|ticks| u32::try_from(ticks).ok())
            .unwrap_or(u32::MAX)
            .max(1);

        Self {
            active,
            idle: IDLE_PERIOD.max(active),
            grace_ticks,
            silent_ticks: 0,
        }
    }

    fn period(&self) -> Duration {
        if self.silent_ticks >= self.grace_ticks {
            self.idle
        } else {
            self.active
        }
    }

    /// Records one frame and returns the new period if it changed.
    fn observe(&mut self, values: &[f64]) -> Option<Duration> {
        let before = self.period();

        if values.iter().all(|value| value.abs() <= SILENT_LEVEL) {
            self.silent_ticks = self.silent_ticks.saturating_add(1);
        } else {
            self.silent_ticks = 0;
        }

        let after = self.period();
        (after != before).then_some(after)
    }
}

fn calculate_cava_buffer_size(sample_rate: u32, channels: u32) -> usize {
    const BASE_FFT_SIZE: usize = 512;
    const BASS_BUFFER_MULTIPLIER: usize = 2;

    let fft_multiplier = match sample_rate {
        0..=8125 => 1,
        8126..=16250 => 2,
        16251..=32500 => 4,
        32501..=75000 => 8,
        75001..=150000 => 16,
        150001..=300000 => 32,
        _ => 64,
    };

    let fft_size = BASE_FFT_SIZE * fft_multiplier;
    let fft_bass_size = fft_size * BASS_BUFFER_MULTIPLIER;
    fft_bass_size * channels as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE_FFT_SIZE: usize = 512;
    const BASS_BUFFER_MULTIPLIER: usize = 2;

    const TIER_1_MAX: u32 = 8125;
    const TIER_2_MIN: u32 = 8126;
    const TIER_2_MAX: u32 = 16250;
    const TIER_3_MIN: u32 = 16251;
    const TIER_3_MAX: u32 = 32500;
    const TIER_4_MIN: u32 = 32501;
    const TIER_4_MAX: u32 = 75000;
    const TIER_5_MIN: u32 = 75001;
    const TIER_5_MAX: u32 = 150000;
    const TIER_6_MIN: u32 = 150001;
    const TIER_6_MAX: u32 = 300000;
    const TIER_7_MIN: u32 = 300001;

    const MULTIPLIER_TIER_1: usize = 1;
    const MULTIPLIER_TIER_2: usize = 2;
    const MULTIPLIER_TIER_3: usize = 4;
    const MULTIPLIER_TIER_4: usize = 8;
    const MULTIPLIER_TIER_5: usize = 16;
    const MULTIPLIER_TIER_6: usize = 32;
    const MULTIPLIER_TIER_7: usize = 64;

    const MONO_CHANNELS: u32 = 1;
    const STEREO_CHANNELS: u32 = 2;

    const FRAME_60FPS: Duration = Duration::from_millis(16);

    fn feed(pacer: &mut TickPacer, values: &[f64], ticks: u32) -> Vec<Duration> {
        (0..ticks).filter_map(|_| pacer.observe(values)).collect()
    }

    #[test]
    fn pacer_keeps_framerate_while_audible() {
        let mut pacer = TickPacer::new(FRAME_60FPS);

        let changes = feed(&mut pacer, &[0.0, 0.4, 0.1], 1_000);

        assert!(changes.is_empty());
        assert_eq!(pacer.period(), FRAME_60FPS);
    }

    #[test]
    fn pacer_slows_down_after_silence_grace() {
        let mut pacer = TickPacer::new(FRAME_60FPS);
        let grace = pacer.grace_ticks;

        assert!(feed(&mut pacer, &[0.0; 4], grace - 1).is_empty());
        assert_eq!(pacer.observe(&[0.0; 4]), Some(IDLE_PERIOD));
        assert!(feed(&mut pacer, &[0.0; 4], 100).is_empty());
        assert_eq!(pacer.period(), IDLE_PERIOD);
    }

    #[test]
    fn pacer_treats_sub_pixel_levels_as_silence() {
        let mut pacer = TickPacer::new(FRAME_60FPS);
        let grace = pacer.grace_ticks;

        let changes = feed(&mut pacer, &[SILENT_LEVEL / 2.0; 4], grace);

        assert_eq!(changes, vec![IDLE_PERIOD]);
    }

    #[test]
    fn pacer_resumes_framerate_on_first_audible_frame() {
        let mut pacer = TickPacer::new(FRAME_60FPS);
        let grace = pacer.grace_ticks;
        feed(&mut pacer, &[0.0; 4], grace);

        assert_eq!(pacer.observe(&[0.0, 0.2, 0.0, 0.0]), Some(FRAME_60FPS));
        assert_eq!(pacer.period(), FRAME_60FPS);
    }

    #[test]
    fn pacer_silence_counter_restarts_after_sound() {
        let mut pacer = TickPacer::new(FRAME_60FPS);
        let grace = pacer.grace_ticks;

        feed(&mut pacer, &[0.0; 4], grace - 1);
        pacer.observe(&[0.5; 4]);

        assert!(feed(&mut pacer, &[0.0; 4], grace - 1).is_empty());
        assert_eq!(pacer.period(), FRAME_60FPS);
    }

    #[test]
    fn pacer_never_speeds_up_a_slow_framerate() {
        let slow = Duration::from_millis(500);
        let mut pacer = TickPacer::new(slow);

        let changes = feed(&mut pacer, &[0.0; 4], 100);

        assert!(changes.is_empty());
        assert_eq!(pacer.period(), slow);
    }

    #[test]
    fn calculate_cava_buffer_size_with_low_sample_rate_returns_base_multiplier() {
        let sample_rate = TIER_1_MAX;
        let channels = MONO_CHANNELS;

        let result = calculate_cava_buffer_size(sample_rate, channels);

        let expected =
            BASE_FFT_SIZE * MULTIPLIER_TIER_1 * BASS_BUFFER_MULTIPLIER * channels as usize;
        assert_eq!(result, expected);
    }

    #[test]
    fn calculate_cava_buffer_size_with_mid_sample_rate_returns_doubled_multiplier() {
        let sample_rate = TIER_2_MAX;
        let channels = MONO_CHANNELS;

        let result = calculate_cava_buffer_size(sample_rate, channels);

        let expected =
            BASE_FFT_SIZE * MULTIPLIER_TIER_2 * BASS_BUFFER_MULTIPLIER * channels as usize;
        assert_eq!(result, expected);
    }

    #[test]
    fn calculate_cava_buffer_size_with_high_sample_rate_returns_max_multiplier() {
        let sample_rate = TIER_7_MIN * 2;
        let channels = MONO_CHANNELS;

        let result = calculate_cava_buffer_size(sample_rate, channels);

        let expected =
            BASE_FFT_SIZE * MULTIPLIER_TIER_7 * BASS_BUFFER_MULTIPLIER * channels as usize;
        assert_eq!(result, expected);
    }

    #[test]
    fn calculate_cava_buffer_size_multiplies_by_channel_count() {
        let sample_rate = TIER_4_MAX;
        let channels = STEREO_CHANNELS;

        let result = calculate_cava_buffer_size(sample_rate, channels);

        let expected =
            BASE_FFT_SIZE * MULTIPLIER_TIER_4 * BASS_BUFFER_MULTIPLIER * channels as usize;
        assert_eq!(result, expected);
    }

    #[test]
    fn calculate_cava_buffer_size_at_boundary_values() {
        let channels = MONO_CHANNELS;

        assert_eq!(
            calculate_cava_buffer_size(TIER_1_MAX, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_1 * BASS_BUFFER_MULTIPLIER * channels as usize
        );
        assert_eq!(
            calculate_cava_buffer_size(TIER_2_MIN, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_2 * BASS_BUFFER_MULTIPLIER * channels as usize
        );

        assert_eq!(
            calculate_cava_buffer_size(TIER_2_MAX, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_2 * BASS_BUFFER_MULTIPLIER * channels as usize
        );
        assert_eq!(
            calculate_cava_buffer_size(TIER_3_MIN, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_3 * BASS_BUFFER_MULTIPLIER * channels as usize
        );

        assert_eq!(
            calculate_cava_buffer_size(TIER_3_MAX, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_3 * BASS_BUFFER_MULTIPLIER * channels as usize
        );
        assert_eq!(
            calculate_cava_buffer_size(TIER_4_MIN, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_4 * BASS_BUFFER_MULTIPLIER * channels as usize
        );

        assert_eq!(
            calculate_cava_buffer_size(TIER_4_MAX, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_4 * BASS_BUFFER_MULTIPLIER * channels as usize
        );
        assert_eq!(
            calculate_cava_buffer_size(TIER_5_MIN, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_5 * BASS_BUFFER_MULTIPLIER * channels as usize
        );

        assert_eq!(
            calculate_cava_buffer_size(TIER_5_MAX, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_5 * BASS_BUFFER_MULTIPLIER * channels as usize
        );
        assert_eq!(
            calculate_cava_buffer_size(TIER_6_MIN, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_6 * BASS_BUFFER_MULTIPLIER * channels as usize
        );

        assert_eq!(
            calculate_cava_buffer_size(TIER_6_MAX, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_6 * BASS_BUFFER_MULTIPLIER * channels as usize
        );
        assert_eq!(
            calculate_cava_buffer_size(TIER_7_MIN, channels),
            BASE_FFT_SIZE * MULTIPLIER_TIER_7 * BASS_BUFFER_MULTIPLIER * channels as usize
        );
    }

    #[test]
    fn calculate_cava_buffer_size_above_threshold_uses_fallback_multiplier() {
        let sample_rate = TIER_7_MIN * 10;
        let channels = MONO_CHANNELS;

        let result = calculate_cava_buffer_size(sample_rate, channels);

        let expected =
            BASE_FFT_SIZE * MULTIPLIER_TIER_7 * BASS_BUFFER_MULTIPLIER * channels as usize;
        assert_eq!(result, expected);
    }
}
