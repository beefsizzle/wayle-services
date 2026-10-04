use std::sync::Arc;

use tokio::sync::{broadcast::error::RecvError, oneshot};
use tracing::debug;
use wayle_traits::ModelMonitoring;

use crate::{
    backend::{commands::Command, types::CommandSender},
    core::stream::AudioStream,
    error::{Error, MissingMonitoringComponent},
    events::AudioEvent,
    types::stream::{StreamInfo, StreamKey, StreamState},
};

impl ModelMonitoring for AudioStream {
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

        let weak_stream = Arc::downgrade(&self);
        let stream_key = self.key;
        let cancellation_token = cancellation_token.clone();
        let mut event_rx = event_tx.subscribe();

        tokio::spawn(async move {
            loop {
                let event = tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        debug!("AudioStream monitor cancelled for {:?}", stream_key);
                        return;
                    }
                    event = event_rx.recv() => event,
                };

                let Some(stream) = weak_stream.upgrade() else {
                    return;
                };

                let current = match event {
                    Ok(AudioEvent::StreamChanged(info)) if info.key() == stream_key => Some(info),
                    Ok(AudioEvent::StreamRemoved(key)) if key == stream_key => None,
                    Ok(_) => continue,
                    Err(RecvError::Lagged(missed)) => {
                        // The removal or change of this stream may be among
                        // the lost events, so ask the backend for its state.
                        debug!(
                            missed,
                            ?stream_key,
                            "AudioStream monitor lagged, refreshing"
                        );
                        event_rx = event_rx.resubscribe();
                        fetch(&stream.command_tx, stream_key).await
                    }
                    Err(RecvError::Closed) => return,
                };

                match current {
                    Some(info) if info.instance == stream.instance => {
                        stream.update_from_info(&info);
                    }
                    // Gone, or another stream took over the index.
                    _ => {
                        stream.state.set(StreamState::Terminated);
                        return;
                    }
                }
            }
        });

        Ok(())
    }
}

/// Current state of the stream with `stream_key`, if it exists.
async fn fetch(command_tx: &CommandSender, stream_key: StreamKey) -> Option<StreamInfo> {
    let (responder, reply) = oneshot::channel();
    command_tx
        .send(Command::GetStream {
            stream_key,
            responder,
        })
        .ok()?;
    reply.await.ok()?.ok()
}
