use std::sync::Arc;

use libpulse_binding::{
    callbacks::ListResult,
    context::{Context, subscribe::Facility},
    volume::ChannelVolumes,
};

use super::device::resolve_device_name;
use crate::{
    backend::{
        conversion::stream::{from_sink_input, from_source_output},
        types::{DeviceStore, EventSender, StreamStore, next_instance},
    },
    error::Error,
    events::AudioEvent,
    types::{
        device::{DeviceKey, DeviceTarget, DeviceType},
        stream::{StreamInfo, StreamKey, StreamTarget, StreamType},
    },
};

pub(crate) fn trigger_discovery(context: &Context, streams: &StreamStore, events_tx: &EventSender) {
    let introspect = context.introspect();

    let streams_clone = Arc::clone(streams);
    let events_tx_clone = events_tx.clone();
    introspect.get_sink_input_info_list(move |sink_input_list| {
        if let ListResult::Item(sink_input) = sink_input_list {
            let stream_info = from_sink_input(sink_input);
            let stream_key = stream_info.key();
            process_stream_update(stream_key, stream_info, &streams_clone, &events_tx_clone);
        }
    });

    let streams_clone = Arc::clone(streams);
    let events_tx_clone = events_tx.clone();
    introspect.get_source_output_info_list(move |source_output_list| {
        if let ListResult::Item(source_output) = source_output_list {
            let stream_info = from_source_output(source_output);
            let stream_key = stream_info.key();
            process_stream_update(stream_key, stream_info, &streams_clone, &events_tx_clone);
        }
    });
}

pub(crate) fn trigger_refresh(
    context: &Context,
    streams: &StreamStore,
    events_tx: &EventSender,
    stream_key: StreamKey,
    facility: Facility,
) {
    let introspect = context.introspect();
    let streams_clone = Arc::clone(streams);
    let events_tx_clone = events_tx.clone();

    match facility {
        Facility::SinkInput => {
            introspect.get_sink_input_info(stream_key.index, move |input_list| {
                if let ListResult::Item(input) = input_list {
                    let stream_info = from_sink_input(input);
                    process_stream_update(
                        stream_key,
                        stream_info,
                        &streams_clone,
                        &events_tx_clone,
                    );
                }
            });
        }
        Facility::SourceOutput => {
            introspect.get_source_output_info(stream_key.index, move |output_list| {
                if let ListResult::Item(output) = output_list {
                    let stream_info = from_source_output(output);
                    process_stream_update(
                        stream_key,
                        stream_info,
                        &streams_clone,
                        &events_tx_clone,
                    );
                }
            });
        }
        _ => {}
    }
}

pub(crate) fn process_stream_update(
    stream_key: StreamKey,
    mut stream_data: StreamInfo,
    streams: &StreamStore,
    events_tx: &EventSender,
) {
    let Ok(mut streams_guard) = streams.write() else {
        return;
    };

    let existing = streams_guard.get(&stream_key).map(|stream| stream.instance);
    stream_data.instance = existing.unwrap_or_else(next_instance);
    streams_guard.insert(stream_key, stream_data.clone());

    let event = if existing.is_none() {
        AudioEvent::StreamAdded(stream_data)
    } else {
        AudioEvent::StreamChanged(stream_data)
    };

    let _ = events_tx.send(event);
}

/// Checks that the stream `target` addresses still exists.
///
/// Streams only have indices, which the server reuses, so the instance the
/// caller saw must match the stored one; otherwise the index now belongs to
/// another stream (for example after a server restart) and the command fails
/// with [`Error::StreamNotFound`].
pub(crate) fn resolve_stream(streams: &StreamStore, target: StreamTarget) -> Result<(), Error> {
    let streams = streams.read().map_err(|_| Error::LockPoisoned)?;
    match streams.get(&target.key) {
        Some(stream) if stream.instance == target.instance => Ok(()),
        _ => Err(Error::StreamNotFound {
            index: target.key.index,
            stream_type: target.key.stream_type,
        }),
    }
}

pub(crate) fn set_stream_volume(
    context: &Context,
    streams: &StreamStore,
    target: StreamTarget,
    volume: &ChannelVolumes,
) -> Result<(), Error> {
    resolve_stream(streams, target)?;
    let mut introspect = context.introspect();
    let index = target.key.index;
    match target.key.stream_type {
        StreamType::Playback => introspect.set_sink_input_volume(index, volume, None),
        StreamType::Record => introspect.set_source_output_volume(index, volume, None),
    };
    Ok(())
}

pub(crate) fn set_stream_mute(
    context: &Context,
    streams: &StreamStore,
    target: StreamTarget,
    muted: bool,
) -> Result<(), Error> {
    resolve_stream(streams, target)?;
    let mut introspect = context.introspect();
    let index = target.key.index;
    match target.key.stream_type {
        StreamType::Playback => introspect.set_sink_input_mute(index, muted, None),
        StreamType::Record => introspect.set_source_output_mute(index, muted, None),
    };
    Ok(())
}

/// Moves a stream to the device with `device_key`, which must be a sink for
/// playback streams and a source for recording streams.
pub(crate) fn move_stream(
    context: &Context,
    streams: &StreamStore,
    devices: &DeviceStore,
    target: StreamTarget,
    device_key: DeviceKey,
) -> Result<(), Error> {
    resolve_stream(streams, target)?;

    let expected_type = match target.key.stream_type {
        StreamType::Playback => DeviceType::Output,
        StreamType::Record => DeviceType::Input,
    };
    if device_key.device_type != expected_type {
        return Err(Error::DeviceNotFound {
            index: device_key.index,
            device_type: expected_type,
        });
    }
    let device_name = resolve_device_name(
        devices,
        DeviceTarget {
            key: device_key,
            instance: None,
        },
    )?;

    let mut introspect = context.introspect();
    let index = target.key.index;
    match target.key.stream_type {
        StreamType::Playback => introspect.move_sink_input_by_name(index, &device_name, None),
        StreamType::Record => introspect.move_source_output_by_name(index, &device_name, None),
    };
    Ok(())
}
