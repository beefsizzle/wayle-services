//! Connection supervision for the audio backend.
//!
//! The audio server (PulseAudio, or pipewire-pulse) may not be running yet
//! when the service starts, and it may restart at any time. [`supervise`]
//! keeps a connection alive across both cases:
//!
//! ```text
//!            +-----------+  ok   +---------+
//!   start -->| Connecting|------>| Running |
//!            +-----------+       +---------+
//!               ^     | err          | connection lost
//!               |     v              v
//!            +-----------+    reset_state() (clear + emit removals)
//!            |  Backoff  |<----------+
//!            +-----------+
//! ```
//!
//! Cancellation is honoured in every state, and at most one session (and so
//! one server connection and one event subscription) exists at any time.

use std::{future::Future, time::Duration};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::Error;

/// Delay before the first reconnect attempt.
const INITIAL_BACKOFF: Duration = Duration::from_millis(250);

/// Upper bound for the delay between reconnect attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// A session that lasted at least this long resets the backoff schedule.
const STABLE_SESSION: Duration = Duration::from_secs(5);

/// Why a running session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionEnd {
    /// Shutdown was requested; the supervisor stops.
    Cancelled,
    /// The server connection was lost; the supervisor reconnects.
    Disconnected,
}

/// A backend that the supervisor can (re)connect.
///
/// Implementations own all state that outlives a single connection (device
/// stores, event channels, command queues). A session owns everything tied
/// to one server connection and must release it when dropped.
pub(crate) trait Connector {
    /// Everything tied to one live server connection.
    type Session;

    /// Opens a new connection and waits until it is ready.
    ///
    /// The supervisor may drop the returned future before it completes (on
    /// shutdown), so any partially opened connection must be released on drop.
    fn connect(&mut self) -> impl Future<Output = Result<Self::Session, Error>>;

    /// Serves a ready session until the connection is lost or `token` is
    /// cancelled. The session must be fully torn down before this returns.
    fn run_session(
        &mut self,
        session: Self::Session,
        token: &CancellationToken,
    ) -> impl Future<Output = SessionEnd>;

    /// Forgets all server state after a connection was lost, notifying
    /// observers that every known object is gone.
    fn reset_state(&mut self);
}

/// Exponential backoff schedule between reconnect attempts.
#[derive(Debug, Clone)]
pub(crate) struct Backoff {
    initial: Duration,
    max: Duration,
    next: Duration,
}

impl Backoff {
    pub(crate) fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            next: initial,
        }
    }

    /// Returns the delay to wait now and advances the schedule.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.max);
        delay
    }

    /// Restarts the schedule from the initial delay.
    pub(crate) fn reset(&mut self) {
        self.next = self.initial;
    }
}

/// Tunables for [`supervise`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct SupervisorConfig {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub stable_session: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            initial_backoff: INITIAL_BACKOFF,
            max_backoff: MAX_BACKOFF,
            stable_session: STABLE_SESSION,
        }
    }
}

/// Keeps `connector` connected until `token` is cancelled.
///
/// Failed connection attempts are retried with exponential backoff. When a
/// running session loses its connection, [`Connector::reset_state`] runs
/// before the next attempt so observers see every object removed and then
/// re-added.
///
/// A successful connection restarts the backoff schedule, so a server that
/// took a while to appear is reconnected quickly if it later restarts. The
/// exception is a server that keeps dropping connections: a session shorter
/// than [`SupervisorConfig::stable_session`] counts as a failure, and the
/// schedule keeps growing until a session stays up that long.
///
/// Connection changes are logged at info/warn level the first time; while
/// the server keeps dropping connections, the repeats are logged at debug
/// level so a flapping server cannot flood the log.
pub(crate) async fn supervise<C: Connector>(
    connector: &mut C,
    config: SupervisorConfig,
    token: &CancellationToken,
) {
    let mut backoff = Backoff::new(config.initial_backoff, config.max_backoff);
    let mut failed_attempts: u32 = 0;
    let mut flapping = false;

    loop {
        let attempt = tokio::select! {
            biased;
            () = token.cancelled() => return,
            attempt = connector.connect() => attempt,
        };

        match attempt {
            Ok(session) => {
                if flapping {
                    debug!(failed_attempts, "Reconnected to audio server");
                } else {
                    info!(failed_attempts, "Connected to audio server");
                }
                failed_attempts = 0;
                if !flapping {
                    backoff.reset();
                }

                let started = Instant::now();
                match connector.run_session(session, token).await {
                    SessionEnd::Cancelled => return,
                    SessionEnd::Disconnected => {
                        let short = started.elapsed() < config.stable_session;
                        if flapping && short {
                            debug!("Audio server connection lost again, reconnecting");
                        } else {
                            warn!("Audio server connection lost, reconnecting");
                        }
                        connector.reset_state();
                        flapping = short;
                        if !flapping {
                            backoff.reset();
                        }
                    }
                }
            }
            Err(err) => {
                failed_attempts = failed_attempts.saturating_add(1);
                if failed_attempts == 1 && !flapping {
                    warn!(error = %err, "Cannot connect to audio server, will keep retrying");
                } else {
                    debug!(error = %err, failed_attempts, "Audio server connection attempt failed");
                }
            }
        }

        let delay = backoff.next_delay();
        debug!(?delay, "Waiting before reconnecting to audio server");
        tokio::select! {
            biased;
            () = token.cancelled() => return,
            () = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::VecDeque, future::pending, rc::Rc, time::Duration};

    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn config() -> SupervisorConfig {
        SupervisorConfig {
            initial_backoff: 250 * MS,
            max_backoff: 5000 * MS,
            stable_session: 5000 * MS,
        }
    }

    /// Scripted outcome of one connection attempt.
    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// The attempt fails.
        Fail,
        /// The attempt never completes.
        Hang,
        /// The attempt succeeds and the session drops after this long.
        Session(Duration),
        /// The attempt succeeds and the session runs until cancelled.
        SessionForever,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum Log {
        Connect(Duration),
        SessionStart,
        SessionEnd(SessionEnd),
        Reset,
    }

    /// Counts live sessions the way a real backend would count server
    /// connections and event subscriptions.
    #[derive(Default)]
    struct Tracker {
        live: usize,
        max_live: usize,
        created: usize,
    }

    struct FakeSession {
        tracker: Rc<RefCell<Tracker>>,
        lifetime: Option<Duration>,
    }

    impl Drop for FakeSession {
        fn drop(&mut self) {
            self.tracker.borrow_mut().live -= 1;
        }
    }

    struct FakeConnector {
        steps: VecDeque<Step>,
        start: Instant,
        log: Vec<Log>,
        tracker: Rc<RefCell<Tracker>>,
        on_exhausted: Option<CancellationToken>,
    }

    impl FakeConnector {
        fn new(steps: impl IntoIterator<Item = Step>) -> Self {
            Self {
                steps: steps.into_iter().collect(),
                start: Instant::now(),
                log: Vec::new(),
                tracker: Rc::default(),
                on_exhausted: None,
            }
        }

        /// Cancels `token` once every scripted step has been used.
        fn cancel_when_done(mut self, token: &CancellationToken) -> Self {
            self.on_exhausted = Some(token.clone());
            self
        }

        fn connect_times(&self) -> Vec<Duration> {
            self.log
                .iter()
                .filter_map(|entry| match entry {
                    Log::Connect(at) => Some(*at),
                    _ => None,
                })
                .collect()
        }

        fn count(&self, wanted: &Log) -> usize {
            self.log.iter().filter(|entry| *entry == wanted).count()
        }
    }

    impl Connector for FakeConnector {
        type Session = FakeSession;

        async fn connect(&mut self) -> Result<FakeSession, Error> {
            self.log.push(Log::Connect(self.start.elapsed()));
            let Some(step) = self.steps.pop_front() else {
                if let Some(token) = &self.on_exhausted {
                    token.cancel();
                }
                return Err(Error::ContextCreationFailed);
            };

            let lifetime = match step {
                Step::Fail => return Err(Error::ContextCreationFailed),
                Step::Hang => pending().await,
                Step::Session(lifetime) => Some(lifetime),
                Step::SessionForever => None,
            };

            let mut tracker = self.tracker.borrow_mut();
            tracker.live += 1;
            tracker.created += 1;
            tracker.max_live = tracker.max_live.max(tracker.live);
            drop(tracker);

            Ok(FakeSession {
                tracker: Rc::clone(&self.tracker),
                lifetime,
            })
        }

        async fn run_session(
            &mut self,
            session: FakeSession,
            token: &CancellationToken,
        ) -> SessionEnd {
            self.log.push(Log::SessionStart);
            let end = match session.lifetime {
                Some(lifetime) => tokio::select! {
                    () = token.cancelled() => SessionEnd::Cancelled,
                    () = tokio::time::sleep(lifetime) => SessionEnd::Disconnected,
                },
                None => {
                    token.cancelled().await;
                    SessionEnd::Cancelled
                }
            };
            drop(session);
            self.log.push(Log::SessionEnd(end));
            end
        }

        fn reset_state(&mut self) {
            assert_eq!(
                self.tracker.borrow().live,
                0,
                "state must be reset only after the session is torn down"
            );
            self.log.push(Log::Reset);
        }
    }

    #[test]
    fn backoff_doubles_up_to_cap() {
        let mut backoff = Backoff::new(250 * MS, 5000 * MS);
        let delays: Vec<_> = (0..8).map(|_| backoff.next_delay().as_millis()).collect();
        assert_eq!(delays, [250, 500, 1000, 2000, 4000, 5000, 5000, 5000]);
    }

    #[test]
    fn backoff_reset_restarts_schedule() {
        let mut backoff = Backoff::new(250 * MS, 5000 * MS);
        for _ in 0..5 {
            backoff.next_delay();
        }
        backoff.reset();
        assert_eq!(backoff.next_delay(), 250 * MS);
        assert_eq!(backoff.next_delay(), 500 * MS);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_failed_connects_with_exponential_backoff() {
        let token = CancellationToken::new();
        let mut connector =
            FakeConnector::new([Step::Fail; 7].into_iter().chain([Step::SessionForever]));

        let canceller = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            canceller.cancel();
        });
        supervise(&mut connector, config(), &token).await;

        let times: Vec<_> = connector
            .connect_times()
            .iter()
            .map(Duration::as_millis)
            .collect();
        // Delays: 250, 500, 1000, 2000, 4000, 5000 (capped), 5000.
        assert_eq!(times, [0, 250, 750, 1750, 3750, 7750, 12750, 17750]);
        assert_eq!(connector.count(&Log::SessionStart), 1);
        assert_eq!(connector.count(&Log::Reset), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn lost_session_resets_state_then_reconnects() {
        let token = CancellationToken::new();
        let mut connector = FakeConnector::new([
            Step::Session(10_000 * MS),
            Step::Fail,
            Step::Session(10_000 * MS),
        ])
        .cancel_when_done(&token);

        supervise(&mut connector, config(), &token).await;

        assert_eq!(
            connector.log,
            [
                Log::Connect(Duration::ZERO),
                Log::SessionStart,
                Log::SessionEnd(SessionEnd::Disconnected),
                Log::Reset,
                Log::Connect(10_250 * MS),
                Log::Connect(10_750 * MS),
                Log::SessionStart,
                Log::SessionEnd(SessionEnd::Disconnected),
                Log::Reset,
                Log::Connect(20_750 * MS + 250 * MS),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn many_reconnects_never_overlap_sessions() {
        const CYCLES: usize = 50;
        let token = CancellationToken::new();
        let mut connector =
            FakeConnector::new([Step::Session(100 * MS); CYCLES]).cancel_when_done(&token);

        supervise(&mut connector, config(), &token).await;

        let tracker = connector.tracker.borrow();
        assert_eq!(tracker.created, CYCLES);
        assert_eq!(tracker.max_live, 1, "sessions must never overlap");
        assert_eq!(tracker.live, 0, "every session must be torn down");
        assert_eq!(connector.count(&Log::Reset), CYCLES);
    }

    #[tokio::test(start_paused = true)]
    async fn short_sessions_keep_backing_off() {
        let token = CancellationToken::new();
        let mut connector = FakeConnector::new([Step::Session(MS); 5]).cancel_when_done(&token);

        supervise(&mut connector, config(), &token).await;

        let times = connector.connect_times();
        let gaps: Vec<_> = times
            .windows(2)
            .map(|pair| (pair[1] - pair[0] - MS).as_millis())
            .collect();
        // A flapping server must not cause a tight reconnect loop.
        assert_eq!(gaps, [250, 500, 1000, 2000, 4000]);
    }

    #[tokio::test(start_paused = true)]
    async fn stable_session_resets_backoff() {
        let token = CancellationToken::new();
        let mut connector = FakeConnector::new([
            Step::Fail,
            Step::Fail,
            Step::Fail,
            Step::Session(6000 * MS),
            Step::Fail,
        ])
        .cancel_when_done(&token);

        supervise(&mut connector, config(), &token).await;

        let times: Vec<_> = connector
            .connect_times()
            .iter()
            .map(Duration::as_millis)
            .collect();
        // 3 failures (250, 500, 1000), connect at 1750, session lasts 6000,
        // then the schedule restarts at 250 instead of continuing at 2000.
        assert_eq!(times, [0, 250, 750, 1750, 8000, 8500]);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_before_first_connect_do_not_slow_later_reconnect() {
        let token = CancellationToken::new();
        let mut connector = FakeConnector::new(
            [Step::Fail; 5]
                .into_iter()
                .chain([Step::Session(1000 * MS), Step::Fail]),
        )
        .cancel_when_done(&token);

        supervise(&mut connector, config(), &token).await;

        let times: Vec<_> = connector
            .connect_times()
            .iter()
            .map(Duration::as_millis)
            .collect();
        // The server appears at the 6th attempt (7750 ms). When that session
        // drops after 1 s, the retry comes after 250 ms, not after the 5 s
        // the startup failures had ramped the schedule up to.
        assert_eq!(times, [0, 250, 750, 1750, 3750, 7750, 9000, 9500]);
    }

    #[tokio::test(start_paused = true)]
    async fn flapping_server_keeps_backing_off_across_failures() {
        let token = CancellationToken::new();
        let mut connector =
            FakeConnector::new([Step::Session(MS), Step::Fail, Step::Session(MS), Step::Fail])
                .cancel_when_done(&token);

        supervise(&mut connector, config(), &token).await;

        let times: Vec<_> = connector
            .connect_times()
            .iter()
            .map(Duration::as_millis)
            .collect();
        assert_eq!(times, [0, 251, 751, 1752, 3752]);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_while_waiting_to_reconnect_returns_promptly() {
        let token = CancellationToken::new();
        let mut connector = FakeConnector::new([Step::Fail; 100]);

        let canceller = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(30_000 * MS).await;
            canceller.cancel();
        });

        let start = Instant::now();
        supervise(&mut connector, config(), &token).await;

        assert_eq!(start.elapsed(), 30_000 * MS);
        assert_eq!(connector.count(&Log::Reset), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_hung_connect_returns() {
        let token = CancellationToken::new();
        let mut connector = FakeConnector::new([Step::Hang]);

        let canceller = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(1000 * MS).await;
            canceller.cancel();
        });

        supervise(&mut connector, config(), &token).await;

        assert_eq!(connector.connect_times(), [Duration::ZERO]);
        assert_eq!(connector.tracker.borrow().created, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_session_does_not_reset_state() {
        let token = CancellationToken::new();
        let mut connector = FakeConnector::new([Step::SessionForever]);

        let canceller = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(1000 * MS).await;
            canceller.cancel();
        });

        supervise(&mut connector, config(), &token).await;

        assert_eq!(
            connector.log,
            [
                Log::Connect(Duration::ZERO),
                Log::SessionStart,
                Log::SessionEnd(SessionEnd::Cancelled),
            ]
        );
        assert_eq!(connector.tracker.borrow().live, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn already_cancelled_token_never_connects() {
        let token = CancellationToken::new();
        token.cancel();
        let mut connector = FakeConnector::new([Step::SessionForever]);

        supervise(&mut connector, config(), &token).await;

        assert!(connector.log.is_empty());
    }
}
