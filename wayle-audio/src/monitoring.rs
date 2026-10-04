use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::{broadcast::error::RecvError, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use wayle_core::Property;
use wayle_traits::{ModelMonitoring, ServiceMonitoring};

use crate::{
    backend::{
        commands::Command,
        types::{CommandSender, EventSender, Snapshot},
    },
    core::{
        device::{input::InputDevice, output::OutputDevice},
        stream::AudioStream,
    },
    error::Error,
    events::AudioEvent,
    service::AudioService,
    types::{
        device::{Device, DeviceKey, SinkInfo, SourceInfo},
        stream::{StreamInfo, StreamKey, StreamType},
    },
};

impl ServiceMonitoring for AudioService {
    type Error = Error;

    async fn start_monitoring(&self) -> Result<(), Self::Error> {
        let mut event_rx = self.event_tx.subscribe();
        let cancellation_token = self.cancellation_token.clone();
        let mut registry = Registry {
            command_tx: self.command_tx.clone(),
            event_tx: self.event_tx.clone(),
            cancellation_token: self.cancellation_token.clone(),
            outputs: HashMap::new(),
            inputs: HashMap::new(),
            streams: HashMap::new(),
            output_devices: self.output_devices.clone(),
            input_devices: self.input_devices.clone(),
            playback_streams: self.playback_streams.clone(),
            recording_streams: self.recording_streams.clone(),
            default_input: self.default_input.clone(),
            default_output: self.default_output.clone(),
        };

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("AudioMonitoring cancelled, stopping");
                        return;
                    }
                    event = event_rx.recv() => match event {
                        Ok(event) => registry.apply(event).await,
                        Err(RecvError::Lagged(missed)) => {
                            // Skip whatever is still queued: the snapshot taken
                            // next already includes it, and events sent after
                            // resubscribing are applied on top.
                            warn!(missed, "Audio events were dropped, resynchronizing");
                            event_rx = event_rx.resubscribe();
                            registry.resync().await;
                        }
                        Err(RecvError::Closed) => return,
                    },
                }
            }
        });

        Ok(())
    }
}

/// The live handles the service publishes, kept in step with backend events.
struct Registry {
    command_tx: CommandSender,
    event_tx: EventSender,
    cancellation_token: CancellationToken,
    outputs: HashMap<DeviceKey, Arc<OutputDevice>>,
    inputs: HashMap<DeviceKey, Arc<InputDevice>>,
    streams: HashMap<StreamKey, Arc<AudioStream>>,
    output_devices: Property<Vec<Arc<OutputDevice>>>,
    input_devices: Property<Vec<Arc<InputDevice>>>,
    playback_streams: Property<Vec<Arc<AudioStream>>>,
    recording_streams: Property<Vec<Arc<AudioStream>>>,
    default_input: Property<Option<Arc<InputDevice>>>,
    default_output: Property<Option<Arc<OutputDevice>>>,
}

impl Registry {
    async fn apply(&mut self, event: AudioEvent) {
        match event {
            AudioEvent::DeviceAdded(device) | AudioEvent::DeviceChanged(device) => {
                self.upsert_device(device).await;
            }
            AudioEvent::DeviceRemoved(key) => self.remove_device(key),
            AudioEvent::StreamAdded(info) | AudioEvent::StreamChanged(info) => {
                if self.upsert_stream(info).await {
                    self.publish_streams();
                }
            }
            AudioEvent::StreamRemoved(key) => {
                if self.remove_stream(key) {
                    self.publish_streams();
                }
            }
            AudioEvent::DefaultInputChanged(device) => self.set_default_input(device.as_ref()),
            AudioEvent::DefaultOutputChanged(device) => self.set_default_output(device.as_ref()),
        }
    }

    /// Rebuilds the published state from the backend stores after events
    /// were lost, so no removed object lingers and no change is missed.
    async fn resync(&mut self) {
        let snapshot = match self.fetch_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                warn!(error = %err, "Cannot resynchronize audio state");
                return;
            }
        };

        let device_keys: HashSet<DeviceKey> = snapshot.devices.iter().map(Device::key).collect();
        let gone: Vec<DeviceKey> = self
            .outputs
            .keys()
            .chain(self.inputs.keys())
            .filter(|key| !device_keys.contains(key))
            .copied()
            .collect();
        for key in gone {
            self.remove_device(key);
        }
        for device in snapshot.devices {
            self.upsert_device(device).await;
        }

        let stream_keys: HashSet<StreamKey> =
            snapshot.streams.iter().map(StreamInfo::key).collect();
        let gone: Vec<StreamKey> = self
            .streams
            .keys()
            .filter(|key| !stream_keys.contains(key))
            .copied()
            .collect();
        for key in gone {
            self.remove_stream(key);
        }
        for info in snapshot.streams {
            self.upsert_stream(info).await;
        }
        self.publish_streams();

        self.set_default_input(snapshot.default_input.as_ref());
        self.set_default_output(snapshot.default_output.as_ref());
        debug!("Audio state resynchronized");
    }

    async fn fetch_snapshot(&self) -> Result<Snapshot, Error> {
        let (responder, reply) = oneshot::channel();
        self.command_tx
            .send(Command::Snapshot { responder })
            .map_err(|_| Error::CommandChannelDisconnected)?;
        reply.await.map_err(|_| Error::CommandChannelDisconnected)?
    }

    async fn upsert_device(&mut self, device: Device) {
        match device {
            Device::Sink(sink) => self.upsert_output(&sink).await,
            Device::Source(source) => self.upsert_input(&source).await,
        }
    }

    async fn upsert_output(&mut self, sink: &SinkInfo) {
        let key = sink.key();
        if let Some(existing) = self.outputs.get(&key)
            && existing.instance == sink.device.instance
        {
            existing.update_from_sink(sink);
            return;
        }

        let output = Arc::new(OutputDevice::from_sink(
            sink,
            self.command_tx.clone(),
            Some(self.event_tx.clone()),
            Some(self.cancellation_token.child_token()),
        ));
        output.clone().start_monitoring().await.ok();
        if let Some(replaced) = self.outputs.insert(key, Arc::clone(&output)) {
            cancel(replaced.cancellation_token.as_ref());
            if self.default_output.get().is_some_and(|d| d.key == key) {
                self.default_output.set(Some(output));
            }
        }
        self.publish_outputs();
    }

    async fn upsert_input(&mut self, source: &SourceInfo) {
        let key = source.key();
        if let Some(existing) = self.inputs.get(&key)
            && existing.instance == source.device.instance
        {
            existing.update_from_source(source);
            return;
        }

        let input = Arc::new(InputDevice::from_source(
            source,
            self.command_tx.clone(),
            Some(self.event_tx.clone()),
            Some(self.cancellation_token.child_token()),
        ));
        input.clone().start_monitoring().await.ok();
        if let Some(replaced) = self.inputs.insert(key, Arc::clone(&input)) {
            cancel(replaced.cancellation_token.as_ref());
            if self.default_input.get().is_some_and(|d| d.key == key) {
                self.default_input.set(Some(input));
            }
        }
        self.publish_inputs();
    }

    fn remove_device(&mut self, key: DeviceKey) {
        if let Some(device) = self.outputs.remove(&key) {
            cancel(device.cancellation_token.as_ref());
            self.publish_outputs();
        }
        if let Some(device) = self.inputs.remove(&key) {
            cancel(device.cancellation_token.as_ref());
            self.publish_inputs();
        }
    }

    /// Returns whether the set of handles changed.
    async fn upsert_stream(&mut self, info: StreamInfo) -> bool {
        let key = info.key();
        if let Some(existing) = self.streams.get(&key)
            && existing.instance == info.instance
        {
            existing.update_from_info(&info);
            return false;
        }

        let stream = Arc::new(AudioStream::from_info(
            info,
            self.command_tx.clone(),
            Some(self.event_tx.clone()),
            Some(self.cancellation_token.child_token()),
        ));
        stream.clone().start_monitoring().await.ok();
        if let Some(replaced) = self.streams.insert(key, stream) {
            cancel(replaced.cancellation_token.as_ref());
        }
        true
    }

    /// Returns whether a handle was removed.
    fn remove_stream(&mut self, key: StreamKey) -> bool {
        let Some(stream) = self.streams.remove(&key) else {
            return false;
        };
        cancel(stream.cancellation_token.as_ref());
        true
    }

    fn set_default_input(&self, device: Option<&Device>) {
        let handle = match device {
            Some(Device::Source(source)) => self.inputs.get(&source.key()).cloned(),
            _ => None,
        };
        self.default_input.set(handle);
    }

    fn set_default_output(&self, device: Option<&Device>) {
        let handle = match device {
            Some(Device::Sink(sink)) => self.outputs.get(&sink.key()).cloned(),
            _ => None,
        };
        self.default_output.set(handle);
    }

    fn publish_outputs(&self) {
        self.output_devices
            .set(self.outputs.values().cloned().collect());
    }

    fn publish_inputs(&self) {
        self.input_devices
            .set(self.inputs.values().cloned().collect());
    }

    fn publish_streams(&self) {
        update_stream_properties(
            &self.streams,
            &self.playback_streams,
            &self.recording_streams,
        );
    }
}

/// Stops the monitor of a handle that was removed or replaced.
fn cancel(token: Option<&CancellationToken>) {
    if let Some(token) = token {
        token.cancel();
    }
}

fn update_stream_properties(
    streams: &HashMap<StreamKey, Arc<AudioStream>>,
    playback_streams: &Property<Vec<Arc<AudioStream>>>,
    recording_streams: &Property<Vec<Arc<AudioStream>>>,
) {
    let playback: Vec<Arc<AudioStream>> = streams
        .values()
        .filter(|s| s.key.stream_type == StreamType::Playback)
        .cloned()
        .collect();

    let recording: Vec<Arc<AudioStream>> = streams
        .values()
        .filter(|s| s.key.stream_type == StreamType::Record)
        .cloned()
        .collect();

    playback_streams.set(playback);
    recording_streams.set(recording);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::mpsc;
    use wayle_core::Property;

    use super::*;
    use crate::{
        backend::types::CommandSender,
        types::{
            format::{ChannelMap, SampleFormat, SampleSpec},
            stream::{MediaInfo, StreamInfo, StreamKey, StreamState},
        },
        volume::types::Volume,
    };

    fn create_test_stream(index: u32, stream_type: StreamType) -> Arc<AudioStream> {
        let (command_tx, _): (CommandSender, _) = mpsc::unbounded_channel();
        let stream_info = test_stream_info(index, stream_type);
        Arc::new(AudioStream::from_info(stream_info, command_tx, None, None))
    }

    fn test_stream_info(index: u32, stream_type: StreamType) -> StreamInfo {
        StreamInfo {
            index,
            instance: 0,
            stream_type,
            name: format!("test-stream-{}", index),
            application_name: None,
            binary: None,
            pid: None,
            owner_module: None,
            client: None,
            device_index: 0,
            volume: Volume::mono(1.0),
            muted: false,
            corked: false,
            has_volume: true,
            volume_writable: true,
            state: StreamState::Running,
            sample_spec: SampleSpec {
                format: SampleFormat::S16LE,
                rate: 44100,
                channels: 2,
            },
            channel_map: ChannelMap {
                channels: 2,
                positions: vec![],
            },
            properties: HashMap::new(),
            media: MediaInfo {
                title: None,
                artist: None,
                album: None,
                icon_name: None,
            },
            buffer_latency: 0,
            device_latency: 0,
            resample_method: None,
            driver: String::from("test"),
            format: None,
        }
    }

    #[test]
    fn update_stream_properties_filters_playback_streams_correctly() {
        let mut streams = HashMap::new();
        streams.insert(
            StreamKey::new(1, StreamType::Playback),
            create_test_stream(1, StreamType::Playback),
        );
        streams.insert(
            StreamKey::new(2, StreamType::Playback),
            create_test_stream(2, StreamType::Playback),
        );
        streams.insert(
            StreamKey::new(3, StreamType::Record),
            create_test_stream(3, StreamType::Record),
        );

        let playback = Property::new(Vec::new());
        let recording = Property::new(Vec::new());

        update_stream_properties(&streams, &playback, &recording);

        assert_eq!(playback.get().len(), 2);
        assert_eq!(recording.get().len(), 1);
    }

    #[test]
    fn update_stream_properties_filters_recording_streams_correctly() {
        let mut streams = HashMap::new();
        streams.insert(
            StreamKey::new(1, StreamType::Record),
            create_test_stream(1, StreamType::Record),
        );
        streams.insert(
            StreamKey::new(2, StreamType::Record),
            create_test_stream(2, StreamType::Record),
        );
        streams.insert(
            StreamKey::new(3, StreamType::Playback),
            create_test_stream(3, StreamType::Playback),
        );

        let playback = Property::new(Vec::new());
        let recording = Property::new(Vec::new());

        update_stream_properties(&streams, &playback, &recording);

        assert_eq!(playback.get().len(), 1);
        assert_eq!(recording.get().len(), 2);
    }

    #[test]
    fn replaced_handle_with_same_key_is_published() {
        let old = create_test_stream(1, StreamType::Playback);
        let mut info = test_stream_info(1, StreamType::Playback);
        info.instance = old.instance + 1;
        let (command_tx, _): (CommandSender, _) = mpsc::unbounded_channel();
        let new = Arc::new(AudioStream::from_info(info, command_tx, None, None));
        assert_eq!(old.key, new.key);

        let mut streams = HashMap::new();
        streams.insert(old.key, Arc::clone(&old));
        let playback = Property::new(Vec::new());
        let recording = Property::new(Vec::new());
        update_stream_properties(&streams, &playback, &recording);

        streams.insert(new.key, Arc::clone(&new));
        update_stream_properties(&streams, &playback, &recording);

        assert!(Arc::ptr_eq(&playback.get()[0], &new));
    }

    #[test]
    fn update_stream_properties_handles_empty_streams() {
        let streams = HashMap::new();

        let playback = Property::new(Vec::new());
        let recording = Property::new(Vec::new());

        update_stream_properties(&streams, &playback, &recording);

        assert_eq!(playback.get().len(), 0);
        assert_eq!(recording.get().len(), 0);
    }

    #[test]
    fn update_stream_properties_handles_mixed_stream_types() {
        let mut streams = HashMap::new();
        streams.insert(
            StreamKey::new(1, StreamType::Playback),
            create_test_stream(1, StreamType::Playback),
        );
        streams.insert(
            StreamKey::new(2, StreamType::Record),
            create_test_stream(2, StreamType::Record),
        );
        streams.insert(
            StreamKey::new(3, StreamType::Playback),
            create_test_stream(3, StreamType::Playback),
        );
        streams.insert(
            StreamKey::new(4, StreamType::Record),
            create_test_stream(4, StreamType::Record),
        );

        let playback = Property::new(Vec::new());
        let recording = Property::new(Vec::new());

        update_stream_properties(&streams, &playback, &recording);

        assert_eq!(playback.get().len(), 2);
        assert_eq!(recording.get().len(), 2);
    }

    mod lag {
        use std::{
            sync::{Arc, Mutex},
            time::Duration,
        };

        use tokio::sync::broadcast;
        use tokio_util::sync::CancellationToken;
        use wayle_core::Property;
        use wayle_traits::ServiceMonitoring;

        use crate::{
            backend::{
                commands::Command,
                tests::sink,
                types::{CommandSender, EventSender, Snapshot},
            },
            core::device::output::OutputDevice,
            events::AudioEvent,
            service::AudioService,
            types::device::{Device, DeviceKey, DeviceState, DeviceType},
        };

        /// Serves snapshot and device lookups from `devices`, as the
        /// backend's command processor would.
        fn fake_backend(
            devices: Arc<Mutex<Vec<Device>>>,
        ) -> (CommandSender, tokio::task::JoinHandle<()>) {
            let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
            let handle = tokio::spawn(async move {
                while let Some(command) = command_rx.recv().await {
                    let devices = devices.lock().unwrap().clone();
                    match command {
                        Command::Snapshot { responder } => {
                            let _ = responder.send(Ok(Snapshot {
                                devices: devices.clone(),
                                default_output: devices.first().cloned(),
                                ..Snapshot::default()
                            }));
                        }
                        Command::GetDevice {
                            device_key,
                            responder,
                        } => {
                            let found = devices.iter().find(|d| d.key() == device_key).cloned();
                            let _ = responder.send(found.ok_or(crate::Error::DeviceNotFound {
                                index: device_key.index,
                                device_type: device_key.device_type,
                            }));
                        }
                        _ => {}
                    }
                }
            });
            (command_tx, handle)
        }

        fn service(command_tx: CommandSender, event_tx: EventSender) -> AudioService {
            AudioService {
                command_tx,
                event_tx,
                cancellation_token: CancellationToken::new(),
                backend_handle: None,
                _connection: None,
                output_devices: Property::new(Vec::new()),
                input_devices: Property::new(Vec::new()),
                default_output: Property::new(None),
                default_input: Property::new(None),
                playback_streams: Property::new(Vec::new()),
                recording_streams: Property::new(Vec::new()),
            }
        }

        async fn wait_for(what: &str, mut condition: impl FnMut() -> bool) {
            for _ in 0..500 {
                if condition() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("timed out waiting for {what}");
        }

        fn names(devices: &[Arc<OutputDevice>]) -> Vec<String> {
            let mut names: Vec<String> = devices.iter().map(|d| d.name.get()).collect();
            names.sort();
            names
        }

        /// A reconnect burst larger than the channel used to make receivers
        /// skip events: the removal of a device was lost and it stayed
        /// listed forever (and the old receivers stopped updating at all).
        #[tokio::test]
        async fn lagged_monitors_resynchronize_from_the_backend() {
            let mut old = sink(1);
            old.info_mut().instance = 10;
            let mut new = sink(2);
            new.info_mut().instance = 20;

            let (command_tx, backend) = fake_backend(Arc::new(Mutex::new(vec![new.clone()])));
            let (event_tx, _) = broadcast::channel(4);
            let service = service(command_tx, event_tx.clone());
            service.start_monitoring().await.unwrap();

            event_tx.send(AudioEvent::DeviceAdded(old.clone())).unwrap();
            wait_for("old device", || service.output_devices.get().len() == 1).await;
            let old_handle = service.output_devices.get()[0].clone();

            // Sent without yielding, so every receiver lags.
            event_tx
                .send(AudioEvent::DefaultOutputChanged(None))
                .unwrap();
            event_tx.send(AudioEvent::DeviceRemoved(old.key())).unwrap();
            for _ in 0..8 {
                event_tx.send(AudioEvent::DeviceAdded(new.clone())).unwrap();
            }

            wait_for("resync", || {
                names(&service.output_devices.get()) == ["device-2"]
            })
            .await;
            wait_for("default output", || {
                service
                    .default_output
                    .get()
                    .is_some_and(|d| d.name.get() == "device-2")
            })
            .await;
            wait_for("old handle offline", || {
                old_handle.state.get() == DeviceState::Offline
            })
            .await;

            service.cancellation_token.cancel();
            backend.abort();
        }

        #[tokio::test]
        async fn lagged_device_monitor_keeps_tracking_a_device_that_still_exists() {
            let mut device = sink(1);
            device.info_mut().instance = 10;
            let devices = Arc::new(Mutex::new(vec![device.clone()]));

            let (command_tx, backend) = fake_backend(Arc::clone(&devices));
            let (event_tx, _) = broadcast::channel(4);
            let service = service(command_tx, event_tx.clone());
            // A handle with its own monitor and no service registry, so only
            // the per-device lag handling can update it.
            let handle = service
                .output_device_monitored(DeviceKey::new(1, DeviceType::Output))
                .await
                .unwrap();

            let mut updated = device.clone();
            updated.info_mut().description = String::from("updated");
            *devices.lock().unwrap() = vec![updated];
            for _ in 0..8 {
                event_tx
                    .send(AudioEvent::DefaultOutputChanged(None))
                    .unwrap();
            }

            wait_for("refreshed description", || {
                handle.description.get() == "updated"
            })
            .await;
            assert_ne!(handle.state.get(), DeviceState::Offline);

            devices.lock().unwrap().clear();
            for _ in 0..8 {
                event_tx
                    .send(AudioEvent::DefaultOutputChanged(None))
                    .unwrap();
            }
            wait_for("offline after lag", || {
                handle.state.get() == DeviceState::Offline
            })
            .await;

            service.cancellation_token.cancel();
            backend.abort();
        }
    }
}
