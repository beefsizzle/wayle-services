use libpulse_binding::context::Context;

use super::{
    commands::{device, server, stream},
    types::{DefaultDevice, DeviceStore, EventSender, InternalRefresh, Mutation, StreamStore},
};
use crate::error::Error;

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_internal_command(
    context: &mut Context,
    command: InternalRefresh,
    devices: &DeviceStore,
    streams: &StreamStore,
    events_tx: &EventSender,
    default_input: &DefaultDevice,
    default_output: &DefaultDevice,
) {
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
}

/// Applies a mutation from a caller.
///
/// The target is validated against the stores at the moment the request is
/// handed to libpulse, on the connection that will carry it, so a handle
/// whose object is gone (or was replaced by another one at the same index)
/// gets [`Error::DeviceNotFound`] or [`Error::StreamNotFound`] instead of
/// changing something else.
pub(super) fn handle_external_command(
    context: &mut Context,
    command: Mutation,
    devices: &DeviceStore,
    streams: &StreamStore,
) -> Result<(), Error> {
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
