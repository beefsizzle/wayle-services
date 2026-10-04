use tokio::sync::oneshot;
use tracing::instrument;

use crate::{
    backend::{commands::Command, types::CommandSender},
    error::Error,
    types::device::DeviceTarget,
    volume::types::Volume,
};

pub(crate) struct InputDeviceController;

impl InputDeviceController {
    #[instrument(skip(command_tx), fields(device = ?device.key, volume = ?volume), err)]
    pub async fn set_volume(
        command_tx: &CommandSender,
        device: DeviceTarget,
        volume: Volume,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        command_tx
            .send(Command::SetVolume {
                device,
                volume,
                responder: tx,
            })
            .map_err(|_| Error::CommandChannelDisconnected)?;

        rx.await.map_err(|_| Error::CommandChannelDisconnected)?
    }

    #[instrument(skip(command_tx), fields(device = ?device.key, muted = muted), err)]
    pub async fn set_mute(
        command_tx: &CommandSender,
        device: DeviceTarget,
        muted: bool,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        command_tx
            .send(Command::SetMute {
                device,
                muted,
                responder: tx,
            })
            .map_err(|_| Error::CommandChannelDisconnected)?;

        rx.await.map_err(|_| Error::CommandChannelDisconnected)?
    }

    #[instrument(skip(command_tx), fields(device = ?device.key, port = %port), err)]
    pub async fn set_port(
        command_tx: &CommandSender,
        device: DeviceTarget,
        port: String,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        command_tx
            .send(Command::SetPort {
                device,
                port,
                responder: tx,
            })
            .map_err(|_| Error::CommandChannelDisconnected)?;

        rx.await.map_err(|_| Error::CommandChannelDisconnected)?
    }

    #[instrument(skip(command_tx), fields(device = ?device.key), err)]
    pub async fn set_as_default(
        command_tx: &CommandSender,
        device: DeviceTarget,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        command_tx
            .send(Command::SetDefaultInput {
                device,
                responder: tx,
            })
            .map_err(|_| Error::CommandChannelDisconnected)?;

        rx.await.map_err(|_| Error::CommandChannelDisconnected)?
    }
}
