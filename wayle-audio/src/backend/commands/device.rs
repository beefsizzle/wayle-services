use std::sync::Arc;

use libpulse_binding::{
    callbacks::ListResult,
    context::{Context, subscribe::Facility},
    volume::ChannelVolumes,
};

use crate::{
    backend::{
        conversion::device::{from_sink, from_source},
        types::{DeviceStore, EventSender, next_instance},
    },
    error::Error,
    events::AudioEvent,
    types::device::{Device, DeviceKey, DeviceTarget, DeviceType},
};

pub(crate) fn trigger_discovery(context: &Context, devices: &DeviceStore, events_tx: &EventSender) {
    let introspect = context.introspect();

    let devices_clone = Arc::clone(devices);
    let events_tx_clone = events_tx.clone();

    introspect.get_sink_info_list(move |sink_list| {
        if let ListResult::Item(sink) = sink_list {
            let sink_info = from_sink(sink);
            let device_key = sink_info.key();
            let device_data = Device::Sink(sink_info);

            process_device_update(device_key, device_data, &devices_clone, &events_tx_clone);
        }
    });

    let devices_clone = Arc::clone(devices);
    let events_tx_clone = events_tx.clone();

    introspect.get_source_info_list(move |source_list| {
        if let ListResult::Item(source) = source_list {
            let source_info = from_source(source);
            let device_key = source_info.key();
            let device_data = Device::Source(source_info);

            process_device_update(device_key, device_data, &devices_clone, &events_tx_clone);
        }
    });
}

pub(crate) fn trigger_refresh(
    context: &Context,
    devices: &DeviceStore,
    events_tx: &EventSender,
    device_key: DeviceKey,
    facility: Facility,
) {
    let introspect = context.introspect();

    let devices_clone = Arc::clone(devices);
    let events_tx_clone = events_tx.clone();

    match facility {
        Facility::Sink => {
            introspect.get_sink_info_by_index(device_key.index, move |sink_list| {
                if let ListResult::Item(sink) = sink_list {
                    let sink_info = from_sink(sink);
                    let device_data = Device::Sink(sink_info);

                    process_device_update(
                        device_key,
                        device_data,
                        &devices_clone,
                        &events_tx_clone,
                    );
                }
            });
        }
        Facility::Source => {
            introspect.get_source_info_by_index(device_key.index, move |source_list| {
                if let ListResult::Item(source) = source_list {
                    let source_info = from_source(source);
                    let device_data = Device::Source(source_info);

                    process_device_update(
                        device_key,
                        device_data,
                        &devices_clone,
                        &events_tx_clone,
                    );
                }
            });
        }
        _ => {}
    }
}

pub(crate) fn process_device_update(
    device_key: DeviceKey,
    mut device_data: Device,
    devices: &DeviceStore,
    events_tx: &EventSender,
) {
    let Ok(mut devices_guard) = devices.write() else {
        return;
    };

    let existing = devices_guard
        .get(&device_key)
        .map(|device| device.info().instance);
    device_data.info_mut().instance = existing.unwrap_or_else(next_instance);
    devices_guard.insert(device_key, device_data.clone());

    let event = if existing.is_none() {
        AudioEvent::DeviceAdded(device_data)
    } else {
        AudioEvent::DeviceChanged(device_data)
    };

    let _ = events_tx.send(event);
}

/// Returns the server name of the device `target` addresses.
///
/// Fails with [`Error::DeviceNotFound`] when no device has the key, or when
/// the device there is not the one the caller saw (its instance differs):
/// the server reuses indices, so after a restart or a hotplug the same index
/// can name an unrelated device.
pub(crate) fn resolve_device_name(
    devices: &DeviceStore,
    target: DeviceTarget,
) -> Result<String, Error> {
    let devices = devices.read().map_err(|_| Error::LockPoisoned)?;
    devices
        .get(&target.key)
        .map(Device::info)
        .filter(|info| {
            target
                .instance
                .is_none_or(|instance| instance == info.instance)
        })
        .map(|info| info.name.clone())
        .ok_or(Error::DeviceNotFound {
            index: target.key.index,
            device_type: target.key.device_type,
        })
}

// Devices are addressed by name: unlike indices, names are stable for the
// lifetime of a device, so a request can never land on a different device
// even if it reaches the server after the index was reused.

pub(crate) fn set_device_volume(
    context: &Context,
    devices: &DeviceStore,
    target: DeviceTarget,
    volume: &ChannelVolumes,
) -> Result<(), Error> {
    let name = resolve_device_name(devices, target)?;
    let mut introspect = context.introspect();
    match target.key.device_type {
        DeviceType::Output => introspect.set_sink_volume_by_name(&name, volume, None),
        DeviceType::Input => introspect.set_source_volume_by_name(&name, volume, None),
    };
    Ok(())
}

pub(crate) fn set_device_mute(
    context: &Context,
    devices: &DeviceStore,
    target: DeviceTarget,
    muted: bool,
) -> Result<(), Error> {
    let name = resolve_device_name(devices, target)?;
    let mut introspect = context.introspect();
    match target.key.device_type {
        DeviceType::Output => introspect.set_sink_mute_by_name(&name, muted, None),
        DeviceType::Input => introspect.set_source_mute_by_name(&name, muted, None),
    };
    Ok(())
}

pub(crate) fn set_device_port(
    context: &Context,
    devices: &DeviceStore,
    target: DeviceTarget,
    port: &str,
) -> Result<(), Error> {
    let name = resolve_device_name(devices, target)?;
    let mut introspect = context.introspect();
    match target.key.device_type {
        DeviceType::Output => introspect.set_sink_port_by_name(&name, port, None),
        DeviceType::Input => introspect.set_source_port_by_name(&name, port, None),
    };
    Ok(())
}
