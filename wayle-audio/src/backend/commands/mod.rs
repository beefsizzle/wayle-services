pub(crate) mod device;
pub(crate) mod server;
pub(crate) mod stream;

use tokio::sync::oneshot;

use crate::{
    error::Error,
    types::{
        device::{Device, DeviceKey, DeviceTarget},
        stream::{StreamInfo, StreamKey, StreamTarget},
    },
    volume::types::Volume,
};

#[derive(Debug)]
pub(crate) enum Command {
    GetDevice {
        device_key: DeviceKey,
        responder: oneshot::Sender<Result<Device, Error>>,
    },
    GetStream {
        stream_key: StreamKey,
        responder: oneshot::Sender<Result<StreamInfo, Error>>,
    },
    SetVolume {
        device: DeviceTarget,
        volume: Volume,
        responder: oneshot::Sender<Result<(), Error>>,
    },
    SetMute {
        device: DeviceTarget,
        muted: bool,
        responder: oneshot::Sender<Result<(), Error>>,
    },
    SetStreamVolume {
        stream: StreamTarget,
        volume: Volume,
        responder: oneshot::Sender<Result<(), Error>>,
    },
    SetStreamMute {
        stream: StreamTarget,
        muted: bool,
        responder: oneshot::Sender<Result<(), Error>>,
    },
    SetDefaultInput {
        device: DeviceTarget,
        responder: oneshot::Sender<Result<(), Error>>,
    },
    SetDefaultOutput {
        device: DeviceTarget,
        responder: oneshot::Sender<Result<(), Error>>,
    },
    MoveStream {
        stream: StreamTarget,
        device_key: DeviceKey,
        responder: oneshot::Sender<Result<(), Error>>,
    },
    SetPort {
        device: DeviceTarget,
        port: String,
        responder: oneshot::Sender<Result<(), Error>>,
    },
}
