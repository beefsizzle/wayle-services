//! Integration tests against a private PipeWire server.
//!
//! These tests are ignored by default because they need `pipewire`,
//! `pipewire-pulse`, `pactl` and `pacat`. Run them with:
//!
//! ```text
//! cargo test -p wayle-audio --test private_server -- --ignored
//! ```
//!
//! Every test re-runs itself in a child process whose environment points
//! libpulse at a server the test starts in a scratch directory under the
//! cargo target dir, so the user's own audio server is never touched.

#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]

use std::{
    env, fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, PoisonError},
    thread,
    time::{Duration, Instant},
};

use wayle_audio::{AudioService, Error, core::stream::AudioStream, volume::types::Volume};

/// Set in the child process to the scratch directory of its private server.
const CHILD_ENV: &str = "WAYLE_AUDIO_PRIVATE_SERVER_DIR";

const TIMEOUT: Duration = Duration::from_secs(30);

const PIPEWIRE_CONF: &str = r#"
context.properties = {
    core.daemon = true
    core.name = pipewire-0
    support.dbus = false
    log.level = 1
}
context.spa-libs = {
    audio.convert.* = audioconvert/libspa-audioconvert
    support.* = support/libspa-support
}
context.modules = [
    { name = libpipewire-module-protocol-native }
    { name = libpipewire-module-metadata }
    { name = libpipewire-module-spa-node-factory }
    { name = libpipewire-module-client-node }
    { name = libpipewire-module-access }
    { name = libpipewire-module-adapter }
    { name = libpipewire-module-link-factory }
]
context.objects = [
    { factory = spa-node-factory
        args = {
            factory.name = support.node.driver
            node.name = Dummy-Driver
            node.group = pipewire.dummy
            priority.driver = 200000
        }
    }
]
"#;

const PIPEWIRE_PULSE_CONF: &str = r#"
context.properties = {
    support.dbus = false
    log.level = 1
}
context.spa-libs = {
    audio.convert.* = audioconvert/libspa-audioconvert
    support.* = support/libspa-support
}
context.modules = [
    { name = libpipewire-module-protocol-native }
    { name = libpipewire-module-client-node }
    { name = libpipewire-module-adapter }
    { name = libpipewire-module-metadata }
    { name = libpipewire-module-protocol-pulse args = { } }
]
pulse.properties = {
    server.address = [ "unix:@SOCKET@" ]
}
"#;

/// Runs `scenario` in a child process with a private audio server.
///
/// In the parent this prepares a scratch directory and re-runs the current
/// test binary, filtered to `test_name`, with an environment that only
/// reaches that directory. In the child it runs the scenario.
///
/// `tag` names the scratch directory; it is kept short because unix socket
/// paths are limited to 108 bytes.
fn run_isolated(test_name: &str, tag: &str, scenario: fn(&PrivateServer)) {
    if let Ok(dir) = env::var(CHILD_ENV) {
        let dir = PathBuf::from(dir);
        let expected = format!("unix:{}", dir.join("native").display());
        assert_eq!(
            env::var("PULSE_SERVER").ok().as_deref(),
            Some(expected.as_str()),
            "refusing to run against anything but the private server"
        );
        let server = PrivateServer::new(dir);
        scenario(&server);
        return;
    }

    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("wa-{tag}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("home")).unwrap();

    let status = Command::new(env::current_exe().unwrap())
        .args([test_name, "--exact", "--ignored", "--nocapture"])
        .env(CHILD_ENV, &dir)
        .env(
            "PULSE_SERVER",
            format!("unix:{}", dir.join("native").display()),
        )
        .env("PIPEWIRE_RUNTIME_DIR", &dir)
        .env("XDG_RUNTIME_DIR", &dir)
        .env("HOME", dir.join("home"))
        .env("XDG_CONFIG_HOME", dir.join("home/.config"))
        .env("XDG_STATE_HOME", dir.join("home/.state"))
        .env(
            "DBUS_SESSION_BUS_ADDRESS",
            format!("unix:path={}", dir.join("no-bus").display()),
        )
        .env_remove("PIPEWIRE_REMOTE")
        .env_remove("DISPLAY")
        .status()
        .unwrap();

    let _ = fs::remove_dir_all(&dir);
    assert!(
        status.success(),
        "isolated test {test_name} failed: {status}"
    );
}

/// A pipewire + pipewire-pulse pair owned by the test.
struct PrivateServer {
    dir: PathBuf,
    processes: Mutex<Vec<Child>>,
}

impl PrivateServer {
    fn new(dir: PathBuf) -> Self {
        fs::write(dir.join("pipewire.conf"), PIPEWIRE_CONF).unwrap();
        let socket = dir.join("native");
        fs::write(
            dir.join("pipewire-pulse.conf"),
            PIPEWIRE_PULSE_CONF.replace("@SOCKET@", &socket.display().to_string()),
        )
        .unwrap();
        Self {
            dir,
            processes: Mutex::new(Vec::new()),
        }
    }

    fn start(&self) {
        assert!(self.processes().is_empty(), "server already running");

        let pipewire = self.spawn("pipewire");
        self.processes().push(pipewire);
        wait_until("pipewire socket", || self.dir.join("pipewire-0").exists());

        let pulse = self.spawn("pipewire-pulse");
        self.processes().push(pulse);
        wait_until("pulse server", || pactl_ok(&["info"]));
    }

    fn processes(&self) -> std::sync::MutexGuard<'_, Vec<Child>> {
        self.processes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts `program` with its config from the scratch dir, logging there.
    fn spawn(&self, program: &str) -> Child {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(format!("{program}.log")))
            .unwrap();
        Command::new(program)
            .arg("-c")
            .arg(self.dir.join(format!("{program}.conf")))
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap()
    }

    /// Stops the server; `graceful` sends SIGTERM, otherwise SIGKILL.
    fn stop(&self, graceful: bool) {
        let mut processes = self.processes();
        for child in processes.iter_mut().rev() {
            if graceful {
                let _ = Command::new("kill")
                    .args(["-TERM", &child.id().to_string()])
                    .status();
            } else {
                let _ = child.kill();
            }
        }
        for mut child in processes.drain(..).rev() {
            let _ = child.wait();
        }
        for name in [
            "native",
            "pipewire-0",
            "pipewire-0.lock",
            "pipewire-0-manager",
            "pipewire-0-manager.lock",
        ] {
            let _ = fs::remove_file(self.dir.join(name));
        }
    }

    fn restart(&self, graceful: bool) {
        self.stop(graceful);
        self.start();
    }
}

impl Drop for PrivateServer {
    fn drop(&mut self) {
        self.stop(false);
    }
}

/// A `pacat` playback stream that is killed when dropped.
struct Player(Child);

impl Player {
    fn start(name: &str) -> Self {
        let child = Command::new("pacat")
            .args([
                "--playback",
                &format!("--client-name={name}"),
                &format!("--stream-name={name}"),
            ])
            .stdin(fs::File::open("/dev/zero").unwrap())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self(child)
    }
}

impl Drop for Player {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn pactl(args: &[&str]) -> String {
    let output = Command::new("pactl").args(args).output().unwrap();
    assert!(
        output.status.success(),
        "pactl {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn pactl_ok(args: &[&str]) -> bool {
    Command::new("pactl")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn load_null_sink(name: &str) {
    pactl(&[
        "load-module",
        "module-null-sink",
        &format!("sink_name={name}"),
    ]);
}

/// Server-side index of the sink called `name`.
fn sink_index(name: &str) -> u32 {
    pactl(&["list", "short", "sinks"])
        .lines()
        .find_map(|line| {
            let mut fields = line.split('\t');
            let index = fields.next()?;
            (fields.next()? == name).then(|| index.parse().ok())?
        })
        .unwrap_or_else(|| panic!("sink {name} not found"))
}

/// First channel's volume of the sink called `name`, in percent.
fn sink_volume_percent(name: &str) -> u32 {
    first_percentage(&pactl(&["get-sink-volume", name]))
}

/// Server-side index of the playback stream called `name`.
fn sink_input_index(name: &str) -> Option<u32> {
    let listing = pactl(&["list", "sink-inputs"]);
    listing.split("Sink Input #").skip(1).find_map(|block| {
        let index = block.lines().next()?.trim().parse().ok()?;
        block
            .contains(&format!("media.name = \"{name}\""))
            .then_some(index)
    })
}

/// First channel's volume of the playback stream at `index`, in percent.
fn sink_input_volume_percent(index: u32) -> u32 {
    let listing = pactl(&["list", "sink-inputs"]);
    let block = listing
        .split("Sink Input #")
        .find(|block| block.lines().next().map(str::trim) == Some(&index.to_string()))
        .unwrap_or_else(|| panic!("sink input {index} not found"));
    let volume_line = block
        .lines()
        .find(|line| line.trim_start().starts_with("Volume:"))
        .unwrap();
    first_percentage(volume_line)
}

fn first_percentage(text: &str) -> u32 {
    text.split_whitespace()
        .find_map(|word| word.strip_suffix('%')?.parse().ok())
        .unwrap_or_else(|| panic!("no percentage in {text:?}"))
}

fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn output_names(service: &AudioService) -> Vec<String> {
    let mut names: Vec<String> = service
        .output_devices
        .get()
        .iter()
        .map(|device| device.name.get())
        .collect();
    names.sort();
    names
}

fn playback_stream(service: &AudioService, name: &str) -> Option<Arc<AudioStream>> {
    service
        .playback_streams
        .get()
        .into_iter()
        .find(|stream| stream.name.get() == name)
}

fn open_fds() -> usize {
    fs::read_dir("/proc/self/fd").map_or(0, Iterator::count)
}

fn threads() -> usize {
    fs::read_dir("/proc/self/task").map_or(0, Iterator::count)
}

/// Regression: after a restart, PipeWire hands a removed sink's index to a
/// different sink. A handle kept from before the restart must not change it.
#[test]
#[ignore = "starts a private pipewire-pulse server"]
fn stale_device_handle_cannot_change_sink_that_reused_its_index() {
    run_isolated(
        "stale_device_handle_cannot_change_sink_that_reused_its_index",
        "device",
        |server| {
            server.start();
            load_null_sink("alpha");
            load_null_sink("beta");
            let alpha_index = sink_index("alpha");

            let rt = runtime();
            let service = rt.block_on(AudioService::new()).unwrap();
            wait_until("alpha and beta", || {
                output_names(&service) == ["alpha", "beta"]
            });
            let old_alpha = service
                .output_devices
                .get()
                .into_iter()
                .find(|device| device.name.get() == "alpha")
                .unwrap();
            assert_eq!(old_alpha.key.index, alpha_index);

            server.restart(false);
            load_null_sink("beta");
            load_null_sink("alpha");
            assert_eq!(
                sink_index("beta"),
                alpha_index,
                "precondition: beta must reuse alpha's old index"
            );
            wait_until("sinks after restart", || {
                output_names(&service) == ["alpha", "beta"]
            });
            let beta_before = sink_volume_percent("beta");
            let alpha_before = sink_volume_percent("alpha");

            let result = rt.block_on(old_alpha.set_volume(Volume::from_percentage(10.0, 2)));
            let mute_result = rt.block_on(old_alpha.set_mute(true));
            thread::sleep(Duration::from_millis(500));

            assert_eq!(sink_volume_percent("beta"), beta_before, "beta was changed");
            assert_eq!(
                sink_volume_percent("alpha"),
                alpha_before,
                "alpha was changed"
            );
            assert!(
                !pactl(&["get-sink-mute", "beta"]).contains("yes"),
                "beta was muted"
            );
            assert!(
                matches!(result, Err(Error::DeviceNotFound { .. })),
                "stale set_volume returned {result:?}"
            );
            assert!(
                matches!(mute_result, Err(Error::DeviceNotFound { .. })),
                "stale set_mute returned {mute_result:?}"
            );

            let new_alpha = service
                .output_devices
                .get()
                .into_iter()
                .find(|device| device.name.get() == "alpha")
                .unwrap();
            rt.block_on(new_alpha.set_volume(Volume::from_percentage(30.0, 2)))
                .unwrap();
            wait_until("fresh handle to set alpha", || {
                sink_volume_percent("alpha") == 30
            });
            assert_eq!(sink_volume_percent("beta"), beta_before);

            drop(service);
            drop(rt);
        },
    );
}

/// Regression: the same as above for playback streams, whose handles are
/// held by per-application volume sliders.
#[test]
#[ignore = "starts a private pipewire-pulse server"]
fn stale_stream_handle_cannot_change_stream_that_reused_its_index() {
    run_isolated(
        "stale_stream_handle_cannot_change_stream_that_reused_its_index",
        "stream",
        |server| {
            server.start();
            load_null_sink("out");
            let player_a = Player::start("stream-a");
            wait_until("stream-a", || sink_input_index("stream-a").is_some());
            let player_b = Player::start("stream-b");
            wait_until("stream-b", || sink_input_index("stream-b").is_some());
            let a_index = sink_input_index("stream-a").unwrap();

            let rt = runtime();
            let service = rt.block_on(AudioService::new()).unwrap();
            wait_until("stream-a in service", || {
                playback_stream(&service, "stream-a").is_some()
            });
            let old_a = playback_stream(&service, "stream-a").unwrap();
            assert_eq!(old_a.key.index, a_index);

            server.restart(false);
            drop(player_a);
            drop(player_b);
            load_null_sink("out");
            let _player_b = Player::start("stream-b");
            wait_until("stream-b after restart", || {
                sink_input_index("stream-b").is_some()
            });
            let b_index = sink_input_index("stream-b").unwrap();
            assert_eq!(
                b_index, a_index,
                "precondition: stream-b must reuse stream-a's old index"
            );
            wait_until("stream-b in service", || {
                playback_stream(&service, "stream-b").is_some()
            });
            let b_before = sink_input_volume_percent(b_index);

            let result = rt.block_on(old_a.set_volume(Volume::from_percentage(10.0, 2)));
            thread::sleep(Duration::from_millis(500));

            assert_eq!(
                sink_input_volume_percent(b_index),
                b_before,
                "stream-b was changed"
            );
            assert!(
                matches!(result, Err(Error::StreamNotFound { .. })),
                "stale set_volume returned {result:?}"
            );

            drop(service);
            drop(rt);
        },
    );
}

/// Starts the service before the server exists, then restarts the server
/// repeatedly (alternating SIGKILL and SIGTERM, swapping the sink creation
/// order each time) and checks that volume changes always land on the device
/// the handle names, and that the client does not leak fds or threads.
#[test]
#[ignore = "starts a private pipewire-pulse server"]
fn volume_lands_on_the_right_device_across_restarts() {
    run_isolated(
        "volume_lands_on_the_right_device_across_restarts",
        "soak",
        |server| {
            const CYCLES: u32 = 10;

            let rt = runtime();
            let service = rt.block_on(AudioService::new()).unwrap();
            assert!(service.output_devices.get().is_empty());

            let mut baseline = None;
            let mut last_alpha = None;
            for cycle in 0..CYCLES {
                if cycle == 0 {
                    server.start();
                } else {
                    server.restart(cycle % 2 == 0);
                }
                let order: [&str; 2] = if cycle % 2 == 0 {
                    ["alpha", "beta"]
                } else {
                    ["beta", "alpha"]
                };
                for name in order {
                    load_null_sink(name);
                }
                wait_until("sinks", || output_names(&service) == ["alpha", "beta"]);

                let alpha = service
                    .output_devices
                    .get()
                    .into_iter()
                    .find(|device| device.name.get() == "alpha")
                    .unwrap();
                let beta_before = sink_volume_percent("beta");
                let target = 20 + cycle;
                rt.block_on(alpha.set_volume(Volume::from_percentage(f64::from(target), 2)))
                    .unwrap();
                wait_until("alpha volume", || sink_volume_percent("alpha") == target);
                assert_eq!(sink_volume_percent("beta"), beta_before, "cycle {cycle}");

                last_alpha = Some(alpha);

                let counts = (open_fds(), threads());
                eprintln!("cycle {cycle}: fds={} threads={}", counts.0, counts.1);
                match baseline {
                    None => baseline = Some(counts),
                    Some((fds, threads)) => {
                        assert!(counts.0 <= fds + 4, "fd leak: {fds} -> {}", counts.0);
                        assert!(
                            counts.1 <= threads + 4,
                            "thread leak: {threads} -> {}",
                            counts.1
                        );
                    }
                }
            }

            server.stop(false);
            wait_until("devices cleared", || {
                service.output_devices.get().is_empty()
            });
            let result = rt.block_on(last_alpha.unwrap().set_mute(true));
            assert!(matches!(result, Err(Error::NotConnected)), "{result:?}");

            drop(service);
            drop(rt);
        },
    );
}
