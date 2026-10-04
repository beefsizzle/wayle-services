use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use libpulse_binding::{
    context::subscribe::{Facility, Operation},
    volume::ChannelVolumes,
};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::commands::Command;
use crate::{
    error::Error,
    events::AudioEvent,
    types::{
        device::{Device, DeviceKey, DeviceTarget},
        stream::{StreamInfo, StreamKey, StreamTarget},
    },
};

pub(crate) type DeviceStore = Arc<RwLock<HashMap<DeviceKey, Device>>>;

/// Returns a process-wide unique id for an object entering a store.
///
/// Server indices are not enough to identify objects across reconnects (or
/// on PipeWire, even within one connection), because they are reused.
pub(crate) fn next_instance() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

pub(crate) type StreamStore = Arc<RwLock<HashMap<StreamKey, StreamInfo>>>;

pub(crate) type DefaultDevice = Arc<RwLock<Option<Device>>>;

pub(crate) type EventSender = broadcast::Sender<AudioEvent>;

pub(crate) type CommandSender = mpsc::UnboundedSender<Command>;

pub(crate) type CommandReceiver = mpsc::UnboundedReceiver<Command>;

pub(super) type InternalCommandSender = mpsc::UnboundedSender<InternalRefresh>;

#[derive(Debug, Clone)]
pub(crate) enum ChangeNotification {
    Device {
        facility: Facility,
        operation: Operation,
        index: u32,
    },
    Stream {
        facility: Facility,
        operation: Operation,
        index: u32,
    },
    Server {
        operation: Operation,
    },
}

#[derive(Debug)]
pub(crate) enum InternalRefresh {
    Devices,
    Streams,
    ServerInfo,
    Device {
        device_key: DeviceKey,
        facility: Facility,
    },
    Stream {
        stream_key: StreamKey,
        facility: Facility,
    },
}

/// Reply channel for a mutation.
pub(crate) type Responder = oneshot::Sender<Result<(), Error>>;

/// A mutation for the connection loop, with the channel its caller waits on.
///
/// The connection loop answers it once the request has been handed to
/// libpulse, or with [`Error::NotConnected`] if it is dropped because no
/// connection is available.
#[derive(Debug)]
pub(crate) struct ExternalCommand {
    pub mutation: Mutation,
    pub responder: Responder,
}

impl ExternalCommand {
    pub(crate) fn reply(self, result: Result<(), Error>) {
        let _ = self.responder.send(result);
    }
}

#[derive(Debug)]
pub(crate) enum Mutation {
    SetDeviceVolume {
        device: DeviceTarget,
        volume: ChannelVolumes,
    },
    SetDeviceMute {
        device: DeviceTarget,
        muted: bool,
    },
    SetStreamVolume {
        stream: StreamTarget,
        volume: ChannelVolumes,
    },
    SetStreamMute {
        stream: StreamTarget,
        muted: bool,
    },
    SetDefaultInput {
        device: DeviceTarget,
    },
    SetDefaultOutput {
        device: DeviceTarget,
    },
    MoveStream {
        stream: StreamTarget,
        device_key: DeviceKey,
    },
    SetPort {
        device: DeviceTarget,
        port: String,
    },
}
