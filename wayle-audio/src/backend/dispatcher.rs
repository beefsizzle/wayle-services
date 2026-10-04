use libpulse_binding::context::{Context, State as ContextState};

use super::{
    commands::{device, server, stream},
    types::{DefaultDevice, DeviceStore, EventSender, InternalRefresh, Mutation, StreamStore},
};
use crate::error::Error;

/// Fails with [`Error::NotConnected`] unless `context` can take requests.
///
/// libpulse refuses requests on a context that is not ready by returning a
/// null operation, which libpulse-binding turns into a panic, so every
/// request must be guarded by this check. The state only changes while the
/// mainloop dispatches, never between this check and the request.
pub(super) fn ensure_ready(context: &Context) -> Result<(), Error> {
    match context.get_state() {
        ContextState::Ready => Ok(()),
        _ => Err(Error::NotConnected),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_internal_command(
    context: &mut Context,
    command: InternalRefresh,
    devices: &DeviceStore,
    streams: &StreamStore,
    events_tx: &EventSender,
    default_input: &DefaultDevice,
    default_output: &DefaultDevice,
) -> Result<(), Error> {
    ensure_ready(context)?;
    match command {
        InternalRefresh::Devices => {
            device::trigger_discovery(context, devices, events_tx);
        }
        InternalRefresh::Streams => {
            stream::trigger_discovery(context, streams, events_tx);
        }
        InternalRefresh::ServerInfo => {
            server::trigger_info_query(context, devices, events_tx, default_input, default_output);
        }
        InternalRefresh::Device {
            device_key,
            facility,
        } => {
            device::trigger_refresh(context, devices, events_tx, device_key, facility);
        }
        InternalRefresh::Stream {
            stream_key,
            facility,
        } => {
            stream::trigger_refresh(context, streams, events_tx, stream_key, facility);
        }
    }
    Ok(())
}

/// Applies a mutation from a caller.
///
/// The target is validated against the stores at the moment the request is
/// handed to libpulse, on the connection that will carry it, so a handle
/// whose object is gone (or was replaced by another one at the same index)
/// gets [`Error::DeviceNotFound`] or [`Error::StreamNotFound`] instead of
/// changing something else. Fails with [`Error::NotConnected`] if the
/// connection is no longer usable.
pub(super) fn handle_external_command(
    context: &mut Context,
    command: Mutation,
    devices: &DeviceStore,
    streams: &StreamStore,
) -> Result<(), Error> {
    ensure_ready(context)?;
    match command {
        Mutation::SetDeviceVolume { device, volume } => {
            device::set_device_volume(context, devices, device, &volume)
        }
        Mutation::SetDeviceMute { device, muted } => {
            device::set_device_mute(context, devices, device, muted)
        }
        Mutation::SetDefaultInput { device } => server::set_default_input(context, devices, device),
        Mutation::SetDefaultOutput { device } => {
            server::set_default_output(context, devices, device)
        }
        Mutation::SetStreamVolume { stream, volume } => {
            stream::set_stream_volume(context, streams, stream, &volume)
        }
        Mutation::SetStreamMute { stream, muted } => {
            stream::set_stream_mute(context, streams, stream, muted)
        }
        Mutation::MoveStream { stream, device_key } => {
            stream::move_stream(context, streams, devices, stream, device_key)
        }
        Mutation::SetPort { device, port } => {
            device::set_device_port(context, devices, device, &port)
        }
    }
}
