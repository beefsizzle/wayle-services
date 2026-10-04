# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Error::NotConnected`, returned by device and stream commands while no audio server is connected (they used to return `Ok` without doing anything)
- `Error::InvalidChannelCount`, returned for volumes with no channels or more than 32 instead of crashing the backend

### Fixed

- connect in the background, and reconnect with backoff when the audio server is missing at startup or restarts
- a device or stream handle kept across a server restart no longer changes whichever object reused its index; its commands return `DeviceNotFound` / `StreamNotFound`
- device commands address devices by name instead of by index
- notice a failed server connection even when no socket event fires, instead of hanging and panicking on the next command
- monitors that fall behind the event channel resynchronize instead of keeping removed devices or losing the default device
- a server that keeps restarting no longer floods the log

## [0.1.4](https://github.com/wayle-rs/wayle-services/compare/wayle-audio-v0.1.3...wayle-audio-v0.1.4) - 2026-06-03

### Fixed

- broken dbus.md links in docs (pointing to old repo) ([#28](https://github.com/wayle-rs/wayle-services/pull/28))

## [0.1.3](https://github.com/wayle-rs/wayle-services/compare/wayle-audio-v0.1.2...wayle-audio-v0.1.3) - 2026-06-03

### Fixed

- support PulseAudio v13 encoded formats ([#26](https://github.com/wayle-rs/wayle-services/pull/26))

## [0.1.2](https://github.com/wayle-rs/wayle-services/compare/wayle-audio-v0.1.1...wayle-audio-v0.1.2) - 2026-04-03

### Other

- switch to independent crate versioning ([#7](https://github.com/wayle-rs/wayle-services/pull/7))

## [0.1.1](https://github.com/wayle-rs/wayle-services/compare/wayle-audio-v0.1.0...wayle-audio-v0.1.1) - 2026-03-30

### Other

- readme and doc updates (plus willi the whale :])
