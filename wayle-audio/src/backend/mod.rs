pub(crate) mod commands;
pub(crate) mod conversion;
pub(crate) mod dispatcher;
pub(crate) mod events;
pub(crate) mod supervisor;
pub(crate) mod types;

use std::{
    collections::HashMap,
    future::poll_fn,
    mem,
    sync::{Arc, Mutex, PoisonError, RwLock},
    time::Duration,
};

use commands::Command;
use conversion::volume::to_pulse as convert_volume_to_pulse;
use dispatcher::{handle_external_command, handle_internal_command};
use libpulse_binding::context::{Context, FlagSet as ContextFlags, State as ContextState};
use supervisor::{Connector, SessionEnd, SupervisorConfig, supervise};
use tokio::{
    runtime::Handle,
    sync::mpsc::{self, error::SendError},
    task::{JoinHandle, spawn, spawn_blocking},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use types::{
    CommandReceiver, DefaultDevice, DeviceStore, EventSender, ExternalCommand, InternalRefresh,
    Mutation, Responder, StreamStore,
};

use crate::{Error, events::AudioEvent, tokio_mainloop::TokioMain};

/// How long a single connection attempt may take before it is abandoned.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Server state mirrored from PulseAudio.
///
/// Outlives individual connections: it is cleared when a connection is lost
/// and repopulated by the discovery that runs on every new connection.
struct BackendState {
    devices: DeviceStore,
    streams: StreamStore,
    default_input: DefaultDevice,
    default_output: DefaultDevice,
}

impl BackendState {
    fn new() -> Self {
        Self {
            devices: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
            default_input: Arc::new(RwLock::new(None)),
            default_output: Arc::new(RwLock::new(None)),
        }
    }

    /// Forgets every device, stream and default, emitting the matching
    /// removal events so observers drop their live objects.
    fn clear(&self, event_tx: &EventSender) {
        let had_default_output = self
            .default_output
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .is_some();
        if had_default_output {
            let _ = event_tx.send(AudioEvent::DefaultOutputChanged(None));
        }

        let had_default_input = self
            .default_input
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .is_some();
        if had_default_input {
            let _ = event_tx.send(AudioEvent::DefaultInputChanged(None));
        }

        let streams = mem::take(&mut *self.streams.write().unwrap_or_else(PoisonError::into_inner));
        for key in streams.into_keys() {
            let _ = event_tx.send(AudioEvent::StreamRemoved(key));
        }

        let devices = mem::take(&mut *self.devices.write().unwrap_or_else(PoisonError::into_inner));
        for key in devices.into_keys() {
            let _ = event_tx.send(AudioEvent::DeviceRemoved(key));
        }
    }
}

/// One connection to the PulseAudio server.
///
/// Field order matters: the context must be released while the mainloop it
/// was created on is still alive, and fields drop in declaration order.
pub(crate) struct PulseConnection {
    context: Context,
    mainloop: TokioMain,
}

impl Drop for PulseConnection {
    fn drop(&mut self) {
        self.context.disconnect();
    }
}

/// Hands mutations to the connection loop, but only while it is connected.
///
/// Mutations submitted while disconnected are answered with
/// [`Error::NotConnected`] straight away instead of being queued: they would
/// otherwise be applied to whatever server comes up next, or wait for one
/// indefinitely. The flag and the queue are updated under one lock, so a
/// mutation is either queued before the loop marks itself disconnected (and
/// is then rejected by [`reject_pending`]) or rejected here.
#[derive(Clone)]
struct CommandGate {
    connected: Arc<Mutex<bool>>,
    external_tx: mpsc::UnboundedSender<ExternalCommand>,
}

impl CommandGate {
    fn new(external_tx: mpsc::UnboundedSender<ExternalCommand>) -> Self {
        Self {
            connected: Arc::new(Mutex::new(false)),
            external_tx,
        }
    }

    fn submit(&self, mutation: Mutation, responder: Responder) {
        let connected = self
            .connected
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let command = ExternalCommand {
            mutation,
            responder,
        };

        if !*connected {
            command.reply(Err(Error::NotConnected));
            return;
        }
        if let Err(SendError(command)) = self.external_tx.send(command) {
            command.reply(Err(Error::NotConnected));
        }
    }

    fn set_connected(&self, connected: bool) {
        *self
            .connected
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = connected;
    }
}

/// Answers every queued mutation with [`Error::NotConnected`].
fn reject_pending(external_rx: &mut mpsc::UnboundedReceiver<ExternalCommand>) {
    let mut rejected = 0usize;
    while let Ok(command) = external_rx.try_recv() {
        command.reply(Err(Error::NotConnected));
        rejected += 1;
    }
    if rejected > 0 {
        debug!(rejected, "Rejected audio commands, server is not connected");
    }
}

/// PulseAudio backend.
///
/// Owns the state that survives reconnects and hands itself to the
/// [`supervisor`], which keeps it connected for the lifetime of the service.
pub(crate) struct PulseBackend {
    state: BackendState,
    event_tx: EventSender,
    external_rx: mpsc::UnboundedReceiver<ExternalCommand>,
    gate: CommandGate,
    allow_autospawn: bool,
}

impl PulseBackend {
    pub fn start(
        command_rx: CommandReceiver,
        event_tx: EventSender,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<Result<(), Error>> {
        spawn_blocking(move || {
            let runtime = Handle::current();

            runtime.block_on(async move {
                info!("Starting PulseAudio backend");

                let state = BackendState::new();
                let (external_tx, external_rx) = mpsc::unbounded_channel::<ExternalCommand>();
                let gate = CommandGate::new(external_tx);
                let command_token = cancellation_token.child_token();
                let command_handle = Self::spawn_command_processor(
                    &state,
                    command_rx,
                    gate.clone(),
                    command_token.clone(),
                );

                let mut backend = Self {
                    state,
                    event_tx,
                    external_rx,
                    gate,
                    allow_autospawn: true,
                };
                supervise(
                    &mut backend,
                    SupervisorConfig::default(),
                    &cancellation_token,
                )
                .await;

                command_token.cancel();
                let _ = command_handle.await;

                info!("PulseAudio backend stopped");
                Ok(())
            })
        })
    }

    /// Answers lookups from the shared stores and forwards mutations to the
    /// connection loop. Lives for the whole service, across reconnects.
    fn spawn_command_processor(
        state: &BackendState,
        mut command_rx: CommandReceiver,
        gate: CommandGate,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        let devices = state.devices.clone();
        let streams = state.streams.clone();

        spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("PulseBackend command handler cancelled");
                        return;
                    }
                    command = command_rx.recv() => {
                        let Some(command) = command else {
                            info!("Command channel closed");
                            return;
                        };

                        Self::handle_command(command, &devices, &streams, &gate);
                    }
                }
            }
        })
    }

    #[allow(clippy::too_many_lines)]
    fn handle_command(
        command: Command,
        devices: &DeviceStore,
        streams: &StreamStore,
        gate: &CommandGate,
    ) {
        match command {
            Command::GetDevice {
                device_key,
                responder,
            } => {
                let result = if let Ok(devices_guard) = devices.read() {
                    devices_guard
                        .values()
                        .find(|d| d.key() == device_key)
                        .cloned()
                        .ok_or(Error::DeviceNotFound {
                            index: device_key.index,
                            device_type: device_key.device_type,
                        })
                } else {
                    Err(Error::LockPoisoned)
                };
                let _ = responder.send(result);
            }
            Command::GetStream {
                stream_key,
                responder,
            } => {
                let result = if let Ok(streams_guard) = streams.read() {
                    streams_guard
                        .values()
                        .find(|s| s.key() == stream_key)
                        .cloned()
                        .ok_or(Error::StreamNotFound {
                            index: stream_key.index,
                            stream_type: stream_key.stream_type,
                        })
                } else {
                    Err(Error::LockPoisoned)
                };
                let _ = responder.send(result);
            }
            Command::SetVolume {
                device_key,
                volume,
                responder,
            } => gate.submit(
                Mutation::SetDeviceVolume {
                    device_key,
                    volume: convert_volume_to_pulse(&volume),
                },
                responder,
            ),
            Command::SetMute {
                device_key,
                muted,
                responder,
            } => gate.submit(Mutation::SetDeviceMute { device_key, muted }, responder),
            Command::SetStreamVolume {
                stream_key,
                volume,
                responder,
            } => gate.submit(
                Mutation::SetStreamVolume {
                    stream_key,
                    volume: convert_volume_to_pulse(&volume),
                },
                responder,
            ),
            Command::SetStreamMute {
                stream_key,
                muted,
                responder,
            } => gate.submit(Mutation::SetStreamMute { stream_key, muted }, responder),
            Command::SetDefaultInput {
                device_key,
                responder,
            } => gate.submit(Mutation::SetDefaultInput { device_key }, responder),
            Command::SetDefaultOutput {
                device_key,
                responder,
            } => gate.submit(Mutation::SetDefaultOutput { device_key }, responder),
            Command::MoveStream {
                stream_key,
                device_key,
                responder,
            } => gate.submit(
                Mutation::MoveStream {
                    stream_key,
                    device_key,
                },
                responder,
            ),
            Command::SetPort {
                device_key,
                port,
                responder,
            } => gate.submit(Mutation::SetPort { device_key, port }, responder),
        }
    }

    fn start_session_monitoring(
        &self,
        connection: &mut PulseConnection,
        session_token: CancellationToken,
    ) -> Result<(JoinHandle<()>, SessionChannels), Error> {
        let (internal_command_tx, internal_command_rx) =
            mpsc::unbounded_channel::<InternalRefresh>();

        info!("Setting up PulseAudio event subscription");
        let event_handle = events::start_event_processor(
            &mut connection.context,
            self.state.devices.clone(),
            self.state.streams.clone(),
            self.event_tx.clone(),
            internal_command_tx.clone(),
            session_token,
        )?;

        info!("Triggering initial device and stream discovery");
        let _ = internal_command_tx.send(InternalRefresh::Devices);
        let _ = internal_command_tx.send(InternalRefresh::Streams);
        let _ = internal_command_tx.send(InternalRefresh::ServerInfo);

        Ok((event_handle, (internal_command_tx, internal_command_rx)))
    }
}

/// Per-connection refresh queue. Recreated for every connection so that
/// refreshes requested by an old connection never reach a new one.
type SessionChannels = (
    mpsc::UnboundedSender<InternalRefresh>,
    mpsc::UnboundedReceiver<InternalRefresh>,
);

impl Connector for PulseBackend {
    type Session = PulseConnection;

    async fn connect(&mut self) -> Result<PulseConnection, Error> {
        let mainloop = TokioMain::new();
        debug!("Creating PulseAudio context");
        let context = Context::new(&mainloop, "wayle-pulse").ok_or(Error::ContextCreationFailed)?;
        let mut connection = PulseConnection { context, mainloop };

        // Only the very first attempt may autospawn a server, as before
        // reconnects existed. Retries must never start a server the user
        // deliberately stopped.
        let flags = if mem::replace(&mut self.allow_autospawn, false) {
            ContextFlags::NOFLAGS
        } else {
            ContextFlags::NOAUTOSPAWN
        };

        debug!("Connecting to PulseAudio server");
        connection
            .context
            .connect(None, flags, None)
            .map_err(Error::ConnectionFailed)?;

        let ready = tokio::time::timeout(
            CONNECT_TIMEOUT,
            connection.mainloop.wait_for_ready(&connection.context),
        )
        .await;

        let state = match ready {
            Ok(Ok(state)) => state,
            Ok(Err(_retval)) => ContextState::Terminated,
            Err(_elapsed) => connection.context.get_state(),
        };

        if state != ContextState::Ready {
            return Err(Error::ContextStateFailed(state));
        }

        debug!("PulseAudio context is ready");
        Ok(connection)
    }

    async fn run_session(
        &mut self,
        mut connection: PulseConnection,
        cancellation_token: &CancellationToken,
    ) -> SessionEnd {
        reject_pending(&mut self.external_rx);

        let session_token = cancellation_token.child_token();
        let (event_handle, (_internal_tx, mut internal_rx)) =
            match self.start_session_monitoring(&mut connection, session_token.clone()) {
                Ok(monitoring) => monitoring,
                Err(err) => {
                    warn!(error = %err, "Cannot start PulseAudio event monitoring");
                    return SessionEnd::Disconnected;
                }
            };

        info!("PulseAudio backend fully initialized and monitoring");
        self.gate.set_connected(true);

        let end = loop {
            tokio::select! {
                biased;

                _ = cancellation_token.cancelled() => {
                    info!("PulseAudio backend cancelled");
                    break SessionEnd::Cancelled;
                }

                result = poll_fn(|cx| connection.mainloop.tick(cx)) => {
                    if result.is_some() {
                        warn!("PulseAudio mainloop quit requested");
                        break SessionEnd::Disconnected;
                    }

                    let state = connection.context.get_state();
                    if !state.is_good() {
                        warn!(?state, "PulseAudio context is no longer usable");
                        break SessionEnd::Disconnected;
                    }
                }

                Some(cmd) = internal_rx.recv() => {
                    handle_internal_command(
                        &mut connection.context,
                        cmd,
                        &self.state.devices,
                        &self.state.streams,
                        &self.event_tx,
                        &self.state.default_input,
                        &self.state.default_output,
                    );
                }

                Some(command) = self.external_rx.recv() => {
                    handle_external_command(
                        &mut connection.context,
                        command.mutation,
                        &self.state.devices,
                        &self.state.streams,
                    );
                    let _ = command.responder.send(Ok(()));
                }
            }
        };

        self.gate.set_connected(false);
        reject_pending(&mut self.external_rx);

        // Stop this connection's event processor before the supervisor
        // resets state, so no notification from this connection can touch
        // the stores after they are cleared or repopulated by the next one.
        session_token.cancel();
        let _ = event_handle.await;
        drop(connection);

        end
    }

    fn reset_state(&mut self) {
        self.state.clear(&self.event_tx);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use libpulse_binding::time::MicroSeconds;
    use tokio::sync::broadcast;

    use super::*;
    use crate::{
        types::{
            device::{Device, DeviceInfo, DeviceKey, DeviceState, SinkInfo, SourceInfo},
            format::{ChannelMap, SampleFormat, SampleSpec},
            stream::{MediaInfo, StreamInfo, StreamKey, StreamState, StreamType},
        },
        volume::types::Volume,
    };

    fn device_info(index: u32) -> DeviceInfo {
        DeviceInfo {
            index,
            name: format!("device-{index}"),
            description: format!("Device {index}"),
            card_index: None,
            owner_module: None,
            driver: String::from("test"),
            state: DeviceState::Idle,
            volume: Volume::stereo(1.0, 1.0),
            base_volume: Volume::stereo(1.0, 1.0),
            n_volume_steps: 65536,
            muted: false,
            properties: HashMap::new(),
            ports: Vec::new(),
            active_port: None,
            formats: Vec::new(),
            sample_spec: SampleSpec {
                format: SampleFormat::S16LE,
                rate: 48000,
                channels: 2,
            },
            channel_map: ChannelMap {
                channels: 2,
                positions: Vec::new(),
            },
            latency: MicroSeconds(0),
            configured_latency: MicroSeconds(0),
            flags: 0,
        }
    }

    fn sink(index: u32) -> Device {
        Device::Sink(SinkInfo {
            device: device_info(index),
            monitor_source: index + 100,
            monitor_source_name: format!("device-{index}.monitor"),
        })
    }

    fn source(index: u32) -> Device {
        Device::Source(SourceInfo {
            device: device_info(index),
            monitor_of_sink: None,
            monitor_of_sink_name: None,
            is_monitor: false,
        })
    }

    fn stream(index: u32, stream_type: StreamType) -> StreamInfo {
        StreamInfo {
            index,
            stream_type,
            name: format!("stream-{index}"),
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
                rate: 48000,
                channels: 2,
            },
            channel_map: ChannelMap {
                channels: 2,
                positions: Vec::new(),
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

    fn populated_state() -> BackendState {
        let state = BackendState::new();
        {
            let mut devices = state.devices.write().unwrap();
            for device in [sink(1), sink(2), source(1)] {
                devices.insert(device.key(), device);
            }
            let mut streams = state.streams.write().unwrap();
            for info in [
                stream(7, StreamType::Playback),
                stream(8, StreamType::Record),
            ] {
                streams.insert(info.key(), info);
            }
        }
        *state.default_output.write().unwrap() = Some(sink(1));
        *state.default_input.write().unwrap() = Some(source(1));
        state
    }

    fn drain(rx: &mut broadcast::Receiver<AudioEvent>) -> Vec<AudioEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    #[test]
    fn clear_empties_stores_and_emits_removals() {
        let state = populated_state();
        let (event_tx, mut event_rx) = broadcast::channel(100);

        state.clear(&event_tx);
        let events = drain(&mut event_rx);

        assert!(state.devices.read().unwrap().is_empty());
        assert!(state.streams.read().unwrap().is_empty());
        assert!(state.default_output.read().unwrap().is_none());
        assert!(state.default_input.read().unwrap().is_none());

        assert_eq!(events.len(), 7, "{events:?}");
        assert!(matches!(events[0], AudioEvent::DefaultOutputChanged(None)));
        assert!(matches!(events[1], AudioEvent::DefaultInputChanged(None)));

        let removed_streams: HashSet<StreamKey> = events[2..4]
            .iter()
            .map(|event| match event {
                AudioEvent::StreamRemoved(key) => *key,
                other => panic!("expected stream removal, got {other:?}"),
            })
            .collect();
        assert_eq!(
            removed_streams,
            HashSet::from([
                StreamKey::new(7, StreamType::Playback),
                StreamKey::new(8, StreamType::Record),
            ])
        );

        let removed_devices: HashSet<DeviceKey> = events[4..]
            .iter()
            .map(|event| match event {
                AudioEvent::DeviceRemoved(key) => *key,
                other => panic!("expected device removal, got {other:?}"),
            })
            .collect();
        assert_eq!(
            removed_devices,
            HashSet::from([sink(1).key(), sink(2).key(), source(1).key()])
        );
    }

    #[test]
    fn clear_on_empty_state_emits_nothing() {
        let state = populated_state();
        let (event_tx, mut event_rx) = broadcast::channel(100);

        state.clear(&event_tx);
        drain(&mut event_rx);
        state.clear(&event_tx);

        assert!(drain(&mut event_rx).is_empty());
    }

    #[test]
    fn clear_recovers_from_poisoned_lock() {
        let state = populated_state();
        let devices = Arc::clone(&state.devices);
        let _ = std::thread::spawn(move || {
            let _guard = devices.write().unwrap();
            panic!("poison the device store");
        })
        .join();
        assert!(state.devices.is_poisoned());

        let (event_tx, mut event_rx) = broadcast::channel(100);
        state.clear(&event_tx);

        let device_removals = drain(&mut event_rx)
            .into_iter()
            .filter(|event| matches!(event, AudioEvent::DeviceRemoved(_)))
            .count();
        assert_eq!(device_removals, 3);
    }

    fn submit(gate: &CommandGate) -> tokio::sync::oneshot::Receiver<Result<(), Error>> {
        let mutation = Mutation::SetDeviceMute {
            device_key: sink(1).key(),
            muted: true,
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        gate.submit(mutation, tx);
        rx
    }

    #[test]
    fn gate_rejects_commands_while_disconnected() {
        let (external_tx, mut external_rx) = mpsc::unbounded_channel();
        let gate = CommandGate::new(external_tx);

        let mut reply = submit(&gate);

        assert!(matches!(reply.try_recv(), Ok(Err(Error::NotConnected))));
        assert!(external_rx.try_recv().is_err(), "nothing may be queued");
    }

    #[test]
    fn gate_forwards_commands_while_connected() {
        let (external_tx, mut external_rx) = mpsc::unbounded_channel();
        let gate = CommandGate::new(external_tx);
        gate.set_connected(true);

        let mut reply = submit(&gate);

        let command = external_rx.try_recv().unwrap();
        assert!(matches!(
            command.mutation,
            Mutation::SetDeviceMute { muted: true, .. }
        ));
        assert!(
            reply.try_recv().is_err(),
            "reply comes from the connection loop"
        );
        command.reply(Ok(()));
        assert!(matches!(reply.try_recv(), Ok(Ok(()))));
    }

    #[test]
    fn commands_queued_before_disconnect_are_rejected() {
        let (external_tx, mut external_rx) = mpsc::unbounded_channel();
        let gate = CommandGate::new(external_tx);
        gate.set_connected(true);
        let mut first = submit(&gate);
        let mut second = submit(&gate);

        gate.set_connected(false);
        reject_pending(&mut external_rx);

        assert!(matches!(first.try_recv(), Ok(Err(Error::NotConnected))));
        assert!(matches!(second.try_recv(), Ok(Err(Error::NotConnected))));
        let mut late = submit(&gate);
        assert!(matches!(late.try_recv(), Ok(Err(Error::NotConnected))));
    }

    #[test]
    fn gate_rejects_when_connection_loop_is_gone() {
        let (external_tx, external_rx) = mpsc::unbounded_channel();
        let gate = CommandGate::new(external_tx);
        gate.set_connected(true);
        drop(external_rx);

        let mut reply = submit(&gate);

        assert!(matches!(reply.try_recv(), Ok(Err(Error::NotConnected))));
    }
}
