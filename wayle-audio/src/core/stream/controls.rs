use tokio::sync::oneshot;

use crate::{
    backend::{commands::Command, types::CommandSender},
    error::Error,
    types::{device::DeviceKey, stream::StreamTarget},
    volume::types::Volume,
};

pub(crate) struct AudioStreamController;

impl AudioStreamController {
    pub async fn set_volume(
        command_tx: &CommandSender,
        stream: StreamTarget,
        volume: Volume,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        command_tx
            .send(Command::SetStreamVolume {
                stream,
                volume,
                responder: tx,
            })
            .map_err(|_| Error::CommandChannelDisconnected)?;

        rx.await.map_err(|_| Error::CommandChannelDisconnected)?
    }

    pub async fn set_mute(
        command_tx: &CommandSender,
        stream: StreamTarget,
        muted: bool,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        command_tx
            .send(Command::SetStreamMute {
                stream,
                muted,
                responder: tx,
            })
            .map_err(|_| Error::CommandChannelDisconnected)?;

        rx.await.map_err(|_| Error::CommandChannelDisconnected)?
    }

    pub async fn move_to_device(
        command_tx: &CommandSender,
        stream: StreamTarget,
        device_key: DeviceKey,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        command_tx
            .send(Command::MoveStream {
                stream,
                device_key,
                responder: tx,
            })
            .map_err(|_| Error::CommandChannelDisconnected)?;

        rx.await.map_err(|_| Error::CommandChannelDisconnected)?
    }
}
