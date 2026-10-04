//! Wallpaper daemon lifecycle management.

use std::{
    io,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use tracing::{debug, info, warn};

const READY_POLL_INTERVAL: Duration = Duration::from_millis(50);
const READY_TIMEOUT: Duration = Duration::from_secs(5);

/// Starts a background thread that spawns the wallpaper daemon if it is not already running.
///
/// Prefers `awww-daemon`, falling back to `swww-daemon` if awww is not
/// installed. The thread polls until the daemon is accepting commands, then marks startup as
/// settled so [`AwwwBackend::apply`](super::AwwwBackend::apply) callers proceed. Does nothing
/// if a startup thread is already in flight.
pub fn spawn_daemon_if_needed() {
    if !super::mark_daemon_starting() {
        return;
    }
    thread::spawn(ensure_daemon_ready);
}

/// Marks startup as settled when dropped, so a panic cannot leave callers waiting.
struct SettleOnDrop;

impl Drop for SettleOnDrop {
    fn drop(&mut self) {
        super::mark_daemon_settled();
    }
}

fn ensure_daemon_ready() {
    let _settle = SettleOnDrop;
    let daemon = super::daemon_binary();

    if should_wait_for_ready(daemon) {
        wait_until_ready(daemon);
    }
}

#[allow(clippy::cognitive_complexity)]
fn should_wait_for_ready(daemon: &str) -> bool {
    match is_daemon_running() {
        Ok(true) => {
            debug!("{daemon} already running");
            false
        }
        Ok(false) => {
            info!("Starting {daemon}");
            if let Err(error) = start_daemon() {
                warn!(error = %error, "cannot start {daemon}");
                return false;
            }
            true
        }
        Err(error) => {
            warn!(error = %error, "cannot check {daemon} status");
            false
        }
    }
}

fn wait_until_ready(daemon: &str) {
    let start = Instant::now();

    while start.elapsed() < READY_TIMEOUT {
        thread::sleep(READY_POLL_INTERVAL);

        if let Ok(true) = daemon_lists_outputs() {
            debug!(elapsed_ms = start.elapsed().as_millis(), "{daemon} ready");
            return;
        }
    }

    warn!("{daemon} not ready after {}s", READY_TIMEOUT.as_secs());
}

fn is_daemon_running() -> Result<bool, io::Error> {
    let output = Command::new(super::client_binary())
        .arg("query")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;

    Ok(output.success())
}

/// A freshly started daemon answers `query` before it has enumerated outputs, with an empty
/// listing. Requests sent in that window fail, and the daemon can abort on them, so only treat
/// it as ready once at least one output is listed.
fn daemon_lists_outputs() -> Result<bool, io::Error> {
    let output = Command::new(super::client_binary())
        .arg("query")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()?;

    Ok(output.status.success() && !output.stdout.trim_ascii().is_empty())
}

fn start_daemon() -> Result<(), io::Error> {
    Command::new(super::daemon_binary())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    Ok(())
}
