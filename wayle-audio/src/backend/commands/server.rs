use std::sync::Arc;

use libpulse_binding::context::Context;
use tracing::warn;

use super::device::resolve_device_name;
use crate::{
    backend::types::{DefaultDevice, DeviceStore, EventSender},
    error::Error,
    events::AudioEvent,
    types::device::{Device, DeviceTarget, DeviceType},
};

pub(crate) fn trigger_info_query(
    context: &Context,
    devices: &DeviceStore,
    events_tx: &EventSender,
    default_input: &DefaultDevice,
    default_output: &DefaultDevice,
) {
    let introspect = context.introspect();

    let default_input_clone = Arc::clone(default_input);
    let default_output_clone = Arc::clone(default_output);
    let events_tx_clone = events_tx.clone();
    let devices_clone = Arc::clone(devices);

    introspect.get_server_info(move |server_info| {
        if let Some(sink_name) = server_info.default_sink_name.as_ref() {
            let sink_name = sink_name.to_string();

            if let Ok(devices_guard) = devices_clone.read() {
                let default_device = devices_guard
                    .values()
                    .find(|device| {
                        if let Device::Sink(sink) = device {
                            return sink.device.name == sink_name;
                        }

                        false
                    })
                    .cloned();

                if let Some(device) = default_device {
                    if let Ok(mut guard) = default_output_clone.write() {
                        *guard = Some(device.clone());
                    }
                    let _ = events_tx_clone.send(AudioEvent::DefaultOutputChanged(Some(device)));
                } else {
                    warn!("Default output device '{sink_name}' not found in store. Available devices: {:?}",
                        devices_guard.keys().collect::<Vec<_>>());
                }
            }
        }

        if let Some(source_name) = server_info.default_source_name.as_ref() {
            let source_name = source_name.to_string();

            if let Ok(devices_guard) = devices_clone.read() {
                let default_device = devices_guard
                    .values()
                    .find(|device| {
                        if let Device::Source(source) = device {
                            source.device.name == source_name
                        } else {
                            false
                        }
                    })
                    .cloned();

                if let Some(device) = default_device {
                    if let Ok(mut guard) = default_input_clone.write() {
                        *guard = Some(device.clone());
                    }
                    let _ = events_tx_clone.send(AudioEvent::DefaultInputChanged(Some(device)));
                } else {
                    warn!("Default input device '{source_name}' not found in store. Available devices: {:?}",
                        devices_guard.keys().collect::<Vec<_>>());
                }
            }
        }
    });
}

pub(crate) fn set_default_input(
    context: &mut Context,
    devices: &DeviceStore,
    target: DeviceTarget,
) -> Result<(), Error> {
    let name = resolve_device_name(devices, target)?;
    if target.key.device_type != DeviceType::Input {
        return Err(Error::DeviceNotFound {
            index: target.key.index,
            device_type: DeviceType::Input,
        });
    }
    context.set_default_source(&name, |_success| {});
    Ok(())
}

pub(crate) fn set_default_output(
    context: &mut Context,
    devices: &DeviceStore,
    target: DeviceTarget,
) -> Result<(), Error> {
    let name = resolve_device_name(devices, target)?;
    if target.key.device_type != DeviceType::Output {
        return Err(Error::DeviceNotFound {
            index: target.key.index,
            device_type: DeviceType::Output,
        });
    }
    context.set_default_sink(&name, |_success| {});
    Ok(())
}
