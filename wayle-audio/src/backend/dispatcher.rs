use libpulse_binding::context::Context;

use super::{
    commands::{device, server, stream},
    types::{DefaultDevice, DeviceStore, EventSender, InternalRefresh, Mutation, StreamStore},
};

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

pub(super) fn handle_external_command(
    context: &mut Context,
    command: Mutation,
    devices: &DeviceStore,
    streams: &StreamStore,
) {
    match command {
        Mutation::SetDeviceVolume { device_key, volume } => {
            device::set_device_volume(context, device_key, volume, devices);
        }
        Mutation::SetDeviceMute { device_key, muted } => {
            device::set_device_mute(context, device_key, muted, devices);
        }
        Mutation::SetDefaultInput { device_key } => {
            server::set_default_input(context, device_key, devices);
        }
        Mutation::SetDefaultOutput { device_key } => {
            server::set_default_output(context, device_key, devices);
        }
        Mutation::SetStreamVolume { stream_key, volume } => {
            stream::set_stream_volume(context, stream_key, volume, streams);
        }
        Mutation::SetStreamMute { stream_key, muted } => {
            stream::set_stream_mute(context, stream_key, muted, streams);
        }
        Mutation::MoveStream {
            stream_key,
            device_key,
        } => {
            stream::move_stream(context, stream_key, device_key, streams);
        }
        Mutation::SetPort { device_key, port } => {
            device::set_device_port(context, device_key, port, devices);
        }
    }
}
