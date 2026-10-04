mod daemon;
mod transition;

use std::{
    env,
    io::ErrorKind,
    path::Path,
    process::Stdio,
    sync::{
        OnceLock,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

pub(crate) use daemon::spawn_daemon_if_needed;
use tokio::{process::Command, sync::Notify};
use tracing::{debug, instrument};
pub use transition::{
    BezierCurve, Position, TransitionAngle, TransitionConfig, TransitionDuration, TransitionFps,
    TransitionStep, TransitionType, WaveDimensions,
};

use crate::{Error, types::FitMode};

const DAEMON_IDLE: u8 = 0;
const DAEMON_STARTING: u8 = 1;
const DAEMON_SETTLED: u8 = 2;

const DAEMON_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The daemon answers `query` as soon as its socket is up, which can be before it has
/// enumerated the Wayland outputs. Early `img` calls then fail with this message.
const OUTPUTS_NOT_READY: &str = "none of the requested outputs are valid";
const OUTPUTS_RETRY_ATTEMPTS: u32 = 10;
const OUTPUTS_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Lifecycle of the startup thread. `SETTLED` means it finished, whether or not the daemon
/// came up; callers then proceed and surface the real error from the client.
static DAEMON_STATE: AtomicU8 = AtomicU8::new(DAEMON_IDLE);
static DAEMON_SETTLED_NOTIFY: Notify = Notify::const_new();

fn mark_daemon_starting() {
    DAEMON_STATE.store(DAEMON_STARTING, Ordering::Release);
}

fn mark_daemon_settled() {
    DAEMON_STATE.store(DAEMON_SETTLED, Ordering::Release);
    DAEMON_SETTLED_NOTIFY.notify_waiters();
}

/// Waits while the daemon startup thread is running (up to 10s).
///
/// Returns immediately once the thread has finished, or if it was never started. Any number
/// of callers may wait, and late callers see the settled state.
pub(crate) async fn wait_for_daemon() {
    let settled = async {
        loop {
            let notified = DAEMON_SETTLED_NOTIFY.notified();
            tokio::pin!(notified);
            // Register before re-checking so a notify between check and await isn't lost.
            notified.as_mut().enable();

            if DAEMON_STATE.load(Ordering::Acquire) != DAEMON_STARTING {
                return;
            }
            notified.await;
        }
    };

    let _ = tokio::time::timeout(DAEMON_WAIT_TIMEOUT, settled).await;
}

static CLIENT_BINARY: OnceLock<&str> = OnceLock::new();

fn client_binary() -> &'static str {
    CLIENT_BINARY.get_or_init(|| {
        if binary_in_path("awww") {
            "awww"
        } else {
            "swww"
        }
    })
}

fn daemon_binary() -> &'static str {
    if client_binary() == "awww" {
        "awww-daemon"
    } else {
        "swww-daemon"
    }
}

fn binary_in_path(name: &str) -> bool {
    let Some(paths) = env::var_os("PATH") else {
        return false;
    };
    env::split_paths(&paths).any(|dir| dir.join(name).is_file())
}

/// Backend for rendering wallpapers via awww (with swww fallback).
#[derive(Debug, Clone, Copy, Default)]
pub struct AwwwBackend;

impl AwwwBackend {
    const RESIZE_FLAG: &'static str = "--resize";
    const OUTPUTS_FLAG: &'static str = "--outputs";

    /// Applies a wallpaper image to the specified monitor.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the image file
    /// * `fit_mode` - How to scale the image to fit the display
    /// * `monitor` - Monitor connector name (e.g., "DP-1"), or `None` for all monitors
    /// * `transition` - Transition animation configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the awww/swww command fails or the daemon is not running.
    #[instrument(skip(transition), fields(path = %path.display(), monitor))]
    pub async fn apply(
        path: &Path,
        fit_mode: FitMode,
        monitor: Option<&str>,
        transition: &TransitionConfig,
    ) -> Result<(), Error> {
        wait_for_daemon().await;

        let resize_mode = match fit_mode {
            FitMode::Fill => "crop",
            FitMode::Fit => "fit",
            FitMode::Center => "no",
            FitMode::Stretch => "stretch",
        };

        let path_str = path
            .to_str()
            .ok_or_else(|| Error::InvalidImagePath(path.to_path_buf()))?;

        let mut cmd = Command::new(client_binary());
        cmd.arg("img");
        cmd.arg(path_str);
        cmd.args([Self::RESIZE_FLAG, resize_mode]);

        apply_transition_args(&mut cmd, transition);

        if let Some(monitor) = monitor {
            cmd.args([Self::OUTPUTS_FLAG, monitor]);
        }

        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::piped());

        let mut retries = 0;
        let output = loop {
            let output = cmd.output().await.map_err(|err| {
                if err.kind() == ErrorKind::NotFound {
                    Error::AwwwNotInstalled
                } else {
                    Error::Io(err)
                }
            })?;

            let outputs_not_ready = !output.status.success()
                && String::from_utf8_lossy(&output.stderr).contains(OUTPUTS_NOT_READY);

            if !outputs_not_ready || retries >= OUTPUTS_RETRY_ATTEMPTS {
                break output;
            }

            retries += 1;
            debug!(retries, "daemon has not enumerated outputs yet, retrying");
            tokio::time::sleep(OUTPUTS_RETRY_DELAY).await;
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

            if stderr.contains("daemon") || stderr.contains("socket") {
                return Err(Error::AwwwDaemonNotRunning);
            }

            return Err(Error::AwwwCommandFailed { stderr });
        }

        Ok(())
    }
}

fn apply_transition_args(cmd: &mut Command, config: &TransitionConfig) {
    cmd.args([TransitionType::FLAG, config.transition_type.type_name()]);
    cmd.args([TransitionDuration::FLAG, &config.duration.to_string()]);
    cmd.args([TransitionFps::FLAG, &config.fps.to_string()]);

    if let Some(step) = config.step {
        cmd.args([TransitionStep::FLAG, &step.to_string()]);
    }

    for (flag, value) in config.transition_type.cli_args() {
        cmd.args([flag, &value]);
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        sync::{Mutex, MutexGuard},
        time::{Instant, sleep, timeout},
    };

    use super::*;

    /// The readiness state is process-global, so tests that touch it must not interleave.
    static SERIAL: Mutex<()> = Mutex::const_new(());

    async fn serial() -> MutexGuard<'static, ()> {
        SERIAL.lock().await
    }

    #[tokio::test]
    async fn wait_returns_immediately_when_daemon_was_never_started() {
        let _guard = serial().await;
        DAEMON_STATE.store(DAEMON_IDLE, Ordering::Release);

        let started = Instant::now();
        wait_for_daemon().await;

        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn wait_returns_immediately_after_startup_settled() {
        let _guard = serial().await;
        mark_daemon_starting();
        mark_daemon_settled();

        let started = Instant::now();
        wait_for_daemon().await;

        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn every_waiter_is_released_when_startup_settles() {
        let _guard = serial().await;
        mark_daemon_starting();

        let waiters: Vec<_> = (0..3).map(|_| tokio::spawn(wait_for_daemon())).collect();

        sleep(Duration::from_millis(100)).await;
        assert!(waiters.iter().all(|waiter| !waiter.is_finished()));

        mark_daemon_settled();

        for waiter in waiters {
            let released = timeout(Duration::from_secs(2), waiter).await;
            assert!(matches!(released, Ok(Ok(()))));
        }
    }
}
