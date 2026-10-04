use std::sync::Arc;

use tokio::sync::{broadcast::error::RecvError, oneshot};
use tracing::debug;
use wayle_traits::ModelMonitoring;

use crate::{
    backend::{commands::Command, types::CommandSender},
    core::device::input::InputDevice,
    error::{Error, MissingMonitoringComponent},
    events::AudioEvent,
    types::device::{Device, DeviceKey, DeviceState, SourceInfo},
};

impl ModelMonitoring for InputDevice {
    type Error = Error;

    async fn start_monitoring(self: Arc<Self>) -> Result<(), Self::Error> {
        let Some(ref cancellation_token) = self.cancellation_token else {
            return Err(Error::MonitoringNotInitialized(
                MissingMonitoringComponent::CancellationToken,
            ));
        };

        let Some(ref event_tx) = self.event_tx else {
            return Err(Error::MonitoringNotInitialized(
                MissingMonitoringComponent::EventSender,
            ));
        };

        let weak_device = Arc::downgrade(&self);
        let device_key = self.key;
        let cancellation_token = cancellation_token.clone();
        let mut event_rx = event_tx.subscribe();

        tokio::spawn(async move {
            loop {
                let event = tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        debug!("InputDevice monitor cancelled for {:?}", device_key);
                        return;
                    }
                    event = event_rx.recv() => event,
                };

                let Some(device) = weak_device.upgrade() else {
                    return;
                };

                let current = match event {
                    Ok(AudioEvent::DeviceChanged(Device::Source(source)))
                        if source.key() == device_key =>
                    {
                        Some(source)
                    }
                    Ok(AudioEvent::DeviceRemoved(key)) if key == device_key => None,
                    Ok(_) => continue,
                    Err(RecvError::Lagged(missed)) => {
                        // The removal or change of this device may be among
                        // the lost events, so ask the backend for its state.
                        debug!(
                            missed,
                            ?device_key,
                            "InputDevice monitor lagged, refreshing"
                        );
                        event_rx = event_rx.resubscribe();
                        fetch(&device.command_tx, device_key).await
                    }
                    Err(RecvError::Closed) => return,
                };

                match current {
                    Some(source) if source.device.instance == device.instance => {
                        device.update_from_source(&source);
                    }
                    // Gone, or another device took over the index.
                    _ => {
                        device.state.set(DeviceState::Offline);
                        return;
                    }
                }
            }
        });

        Ok(())
    }
}

/// Current state of the input device with `device_key`, if it exists.
async fn fetch(command_tx: &CommandSender, device_key: DeviceKey) -> Option<SourceInfo> {
    let (responder, reply) = oneshot::channel();
    command_tx
        .send(Command::GetDevice {
            device_key,
            responder,
        })
        .ok()?;
    match reply.await {
        Ok(Ok(Device::Source(source))) => Some(source),
        _ => None,
    }
}
