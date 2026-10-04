//! End-to-end tests of the tray service against real StatusNotifierItem peers.
//!
//! They need a session bus of their own and are therefore `#[ignore]`d by default. Run
//! them inside a throwaway bus, one at a time (they share one tray service, which owns
//! the watcher name for the whole run):
//!
//! ```sh
//! dbus-run-session -- cargo test -p wayle-systray --test private_bus -- --ignored --test-threads=1
//! ```
//!
//! Every test refuses to run unless `DBUS_SESSION_BUS_ADDRESS` points at a private
//! `/tmp/dbus-*` socket, so they never touch a real desktop session (where they would
//! fight the running tray for `org.kde.StatusNotifierWatcher`).

// Inside `cfg(test)` so the workspace lints treat this as test code, as they do the
// crate's unit tests.
#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        process,
        sync::{
            Arc, Mutex, OnceLock, PoisonError,
            atomic::{AtomicBool, AtomicU32, Ordering},
        },
        time::{Duration, Instant},
    };

    use futures::StreamExt;
    use tokio::{runtime::Runtime, sync::OnceCell};
    use wayle_systray::{
        SystemTrayService,
        core::item::TrayItem,
        types::{Coordinates, TrayMode, item::Status},
    };
    use zbus::{
        Connection, MessageStream,
        fdo::DBusProxy,
        names::{BusName, WellKnownName},
        zvariant::OwnedObjectPath,
    };

    const HOW_TO_RUN: &str = "these tests need a private session bus; run them with \
        `dbus-run-session -- cargo test -p wayle-systray --test private_bus -- --ignored \
        --test-threads=1`";

    const WATCHER: &str = "org.kde.StatusNotifierWatcher";
    const WATCHER_PATH: &str = "/StatusNotifierWatcher";
    const ITEM_PATH: &str = "/StatusNotifierItem";
    const ITEM_INTERFACE: &str = "org.kde.StatusNotifierItem";
    const WAYLE_DAEMON: &str = "com.wayle.SystemTray1";

    /// The service's per-call D-Bus timeout (`METHOD_TIMEOUT`), plus scheduling slack.
    const CALL_BOUND: Duration = Duration::from_secs(5 + 2);

    /// Tests share the bus and one tray service, so they never overlap even when the
    /// harness runs them on several threads.
    static SERIAL: Mutex<()> = Mutex::new(());

    /// The runtime the shared tray service lives on, reused by every test.
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();

    /// One service for the whole run: it owns the watcher name, and with the CLI daemon
    /// registered it is never dropped (the daemon object holds the service).
    static TRAY: OnceCell<Arc<SystemTrayService>> = OnceCell::const_new();

    fn on_private_bus() -> bool {
        let address = std::env::var("DBUS_SESSION_BUS_ADDRESS").unwrap_or_default();
        address.starts_with("unix:path=/tmp/dbus-")
            || address.starts_with("unix:abstract=/tmp/dbus-")
    }

    fn run<F: Future<Output = ()>>(test: F) {
        assert!(on_private_bus(), "{HOW_TO_RUN}");
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        RUNTIME
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            })
            .block_on(test);
    }

    /// Polls `condition` every 10 ms until it holds or `within` passes.
    async fn eventually(within: Duration, mut condition: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + within;
        loop {
            if condition() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn start_tray() -> Arc<SystemTrayService> {
        let tray = TRAY
            .get_or_init(|| async {
                SystemTrayService::builder()
                    .with_daemon()
                    .mode(TrayMode::Watcher)
                    .build()
                    .await
                    .unwrap()
            })
            .await;
        Arc::clone(tray)
    }

    fn shown(tray: &SystemTrayService, service: &str) -> Option<Arc<TrayItem>> {
        tray.items
            .get()
            .iter()
            .find(|item| item.bus_name.get() == service)
            .cloned()
    }

    fn count_shown(tray: &SystemTrayService, service: &str) -> usize {
        tray.items
            .get()
            .iter()
            .filter(|item| item.bus_name.get() == service)
            .count()
    }

    fn unique_item_name() -> String {
        static NEXT: AtomicU32 = AtomicU32::new(1);
        format!(
            "org.kde.StatusNotifierItem-{}-{}",
            process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Mutable state of a fake app's tray item.
    #[derive(Clone)]
    struct ItemState {
        props: Arc<Mutex<Props>>,
        /// While set, every call to the item (property reads and methods) goes unanswered.
        hung: Arc<AtomicBool>,
    }

    #[derive(Clone)]
    struct Props {
        id: String,
        title: String,
        icon: String,
        tooltip: String,
        status: String,
    }

    impl ItemState {
        fn new(id: &str) -> Self {
            Self {
                props: Arc::new(Mutex::new(Props {
                    id: id.to_string(),
                    title: format!("{id} title"),
                    icon: format!("{id}-icon"),
                    tooltip: String::new(),
                    status: String::from("Active"),
                })),
                hung: Arc::new(AtomicBool::new(false)),
            }
        }

        fn edit(&self, change: impl FnOnce(&mut Props)) {
            change(&mut self.props.lock().unwrap());
        }

        async fn read(&self) -> Props {
            while self.hung.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            self.props.lock().unwrap().clone()
        }
    }

    struct FakeItem(ItemState);

    #[zbus::interface(name = "org.kde.StatusNotifierItem")]
    impl FakeItem {
        #[zbus(property)]
        async fn id(&self) -> String {
            self.0.read().await.id
        }

        #[zbus(property)]
        async fn title(&self) -> String {
            self.0.read().await.title
        }

        #[zbus(property)]
        async fn category(&self) -> String {
            String::from("ApplicationStatus")
        }

        #[zbus(property)]
        async fn status(&self) -> String {
            self.0.read().await.status
        }

        #[zbus(property)]
        async fn icon_name(&self) -> String {
            self.0.read().await.icon
        }

        #[zbus(property)]
        #[allow(clippy::type_complexity)]
        async fn tool_tip(&self) -> (String, Vec<(i32, i32, Vec<u8>)>, String, String) {
            let tooltip = self.0.read().await.tooltip;
            (String::new(), Vec::new(), tooltip, String::new())
        }

        #[zbus(property)]
        async fn menu(&self) -> OwnedObjectPath {
            OwnedObjectPath::try_from("/NO_DBUSMENU").unwrap()
        }

        #[zbus(property)]
        async fn item_is_menu(&self) -> bool {
            false
        }

        async fn activate(&self, _x: i32, _y: i32) {
            self.0.read().await;
        }

        async fn secondary_activate(&self, _x: i32, _y: i32) {}

        async fn context_menu(&self, _x: i32, _y: i32) {}

        async fn scroll(&self, _delta: i32, _orientation: String) {}
    }

    /// A fake app: one connection serving a `StatusNotifierItem`, optionally under a
    /// well-known name.
    struct FakeApp {
        conn: Connection,
        state: ItemState,
    }

    impl FakeApp {
        async fn new(id: &str, name: Option<&str>) -> Self {
            let state = ItemState::new(id);
            let mut builder = zbus::connection::Builder::session()
                .unwrap()
                .serve_at(ITEM_PATH, FakeItem(state.clone()))
                .unwrap();
            if let Some(name) = name {
                builder = builder.name(name.to_string()).unwrap();
            }
            Self {
                conn: builder.build().await.unwrap(),
                state,
            }
        }

        fn unique_name(&self) -> String {
            self.conn.unique_name().unwrap().to_string()
        }

        async fn emit(&self, member: &str) {
            self.conn
                .emit_signal(None::<BusName<'_>>, ITEM_PATH, ITEM_INTERFACE, member, &())
                .await
                .unwrap();
        }
    }

    async fn register(from: &Connection, service: &str) {
        from.call_method(
            Some(WATCHER),
            WATCHER_PATH,
            Some(WATCHER),
            "RegisterStatusNotifierItem",
            &(service,),
        )
        .await
        .unwrap();
    }

    /// Counts the messages the tray's item connection (the owner of the CLI daemon name)
    /// sends while `during` runs and for `settle` afterwards, using a bus monitor.
    ///
    /// Bus-side match rule statistics would not show the cost of rebuilding an item:
    /// zbus shares one bus rule among identical subscriptions, but every rebuild still
    /// costs its round trips.
    async fn messages_sent_by_tray(during: impl Future<Output = ()>, settle: Duration) -> usize {
        let bus = Connection::session().await.unwrap();
        let tray = DBusProxy::new(&bus)
            .await
            .unwrap()
            .get_name_owner(BusName::try_from(WAYLE_DAEMON).unwrap())
            .await
            .unwrap();

        let monitor = Connection::session().await.unwrap();
        let mut stream = MessageStream::from(&monitor);
        let rules = vec![format!("sender='{tray}'")];
        monitor
            .call_method(
                Some("org.freedesktop.DBus"),
                "/org/freedesktop/DBus",
                Some("org.freedesktop.DBus.Monitoring"),
                "BecomeMonitor",
                &(rules, 0u32),
            )
            .await
            .unwrap();

        let counted = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&counted);
        let sender = tray.to_string();
        let counting = tokio::spawn(async move {
            while let Some(Ok(message)) = stream.next().await {
                if message.header().sender().map(|name| name.as_str()) == Some(sender.as_str()) {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        during.await;
        tokio::time::sleep(settle).await;
        counting.abort();
        counted.load(Ordering::Relaxed) as usize
    }

    #[test]
    #[ignore = "needs a private session bus, see the module docs"]
    fn icon_storm_does_not_wedge_the_connection() {
        run(async {
            let tray = start_tray().await;

            let name = unique_item_name();
            let stormy = FakeApp::new("stormy", Some(&name)).await;
            register(&stormy.conn, &name).await;
            // An app that animates its icon: far more signals than zbus queues for one
            // unpolled stream (64), first while the tray is still setting the item up
            // (subscribing to its signals one round trip at a time), then once it is shown.
            let mut frame = 0;
            let deadline = Instant::now() + Duration::from_secs(3);
            while shown(&tray, &name).is_none() && Instant::now() < deadline {
                for _ in 0..100 {
                    frame += 1;
                    stormy
                        .state
                        .edit(|props| props.icon = format!("frame-{frame}"));
                    stormy.emit("NewIcon").await;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert!(shown(&tray, &name).is_some());
            tokio::time::sleep(Duration::from_millis(200)).await;
            for _ in 0..200 {
                frame += 1;
                stormy
                    .state
                    .edit(|props| props.icon = format!("frame-{frame}"));
                stormy.emit("NewIcon").await;
            }
            let last_frame = format!("frame-{frame}");

            let calm_name = unique_item_name();
            let calm = FakeApp::new("calm", Some(&calm_name)).await;
            register(&calm.conn, &calm_name).await;
            assert!(
                eventually(Duration::from_secs(2), || shown(&tray, &calm_name)
                    .is_some())
                .await,
                "a second item did not appear: the tray's connection is wedged"
            );

            let Some(item) = shown(&tray, &name) else {
                unreachable!("checked above");
            };
            assert!(
                eventually(Duration::from_secs(2), || {
                    item.icon_name.get().as_deref() == Some(last_frame.as_str())
                })
                .await,
                "icon after the storm: {:?}",
                item.icon_name.get()
            );
        });
    }

    #[test]
    #[ignore = "needs a private session bus, see the module docs"]
    fn kstatusnotifieritem_registration_follows_the_item_connection() {
        run(async {
            let tray = start_tray().await;

            // KStatusNotifierItem serves each tray item on a dedicated connection and
            // registers that connection's unique name from the app's main connection, which
            // outlives the item when the app recreates its icon.
            let main = Connection::session().await.unwrap();
            let first = FakeApp::new("ksni", None).await;
            let first_service = first.unique_name();
            register(&main, &first_service).await;
            assert!(
                eventually(Duration::from_secs(2), || shown(&tray, &first_service)
                    .is_some())
                .await
            );

            drop(first);
            assert!(
                eventually(Duration::from_secs(2), || shown(&tray, &first_service)
                    .is_none())
                .await,
                "the item stayed after its connection closed (main connection still alive)"
            );

            let second = FakeApp::new("ksni", None).await;
            let second_service = second.unique_name();
            register(&main, &second_service).await;
            assert!(
                eventually(Duration::from_secs(2), || shown(&tray, &second_service)
                    .is_some())
                .await,
                "the recreated item did not appear"
            );
            assert!(shown(&tray, &first_service).is_none());
        });
    }

    #[test]
    #[ignore = "needs a private session bus, see the module docs"]
    fn releasing_the_item_name_removes_the_item() {
        run(async {
            let tray = start_tray().await;

            let name = unique_item_name();
            let app = FakeApp::new("qt", Some(&name)).await;
            register(&app.conn, &name).await;
            assert!(eventually(Duration::from_secs(2), || shown(&tray, &name).is_some()).await);

            // QSystemTrayIcon::hide(): the name goes, the connection stays.
            app.conn
                .release_name(WellKnownName::try_from(name.as_str()).unwrap())
                .await
                .unwrap();
            assert!(
                eventually(Duration::from_secs(2), || shown(&tray, &name).is_none()).await,
                "the item stayed after its name was released"
            );
        });
    }

    #[test]
    #[ignore = "needs a private session bus, see the module docs"]
    fn repeated_registration_is_cheap_and_resyncs_the_item() {
        run(async {
            let tray = start_tray().await;

            let name = unique_item_name();
            let app = FakeApp::new("looper", Some(&name)).await;
            register(&app.conn, &name).await;
            assert!(eventually(Duration::from_secs(2), || shown(&tray, &name).is_some()).await);
            // Let the item's initial resync and subscriptions settle.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let tasks_before = tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks();

            // Changed without a signal: only a re-read can pick this up.
            app.state
                .edit(|props| props.title = String::from("renamed"));
            let repeats = 20;
            let sent = messages_sent_by_tray(
                async {
                    for _ in 0..repeats {
                        register(&app.conn, &name).await;
                    }
                },
                Duration::from_millis(500),
            )
            .await;

            assert_eq!(count_shown(&tray, &name), 1);
            let Some(item) = shown(&tray, &name) else {
                unreachable!("checked above");
            };
            assert!(
                eventually(Duration::from_secs(2), || item.title.get() == "renamed").await,
                "a repeated registration did not refresh the item: title {:?}",
                item.title.get()
            );

            // Resolving the name and resyncing take three calls per repeat at most;
            // building the item again takes about 25 round trips.
            assert!(
                sent <= repeats * 4,
                "{repeats} repeated registrations made the tray send {sent} messages"
            );
            // Give the monitor's connections time to shut their tasks down.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let tasks_after = tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks();
            assert!(
                tasks_after <= tasks_before + 2,
                "20 repeats left {tasks_after} tasks alive, {tasks_before} before"
            );
        });
    }

    #[test]
    #[ignore = "needs a private session bus, see the module docs"]
    fn title_tooltip_and_status_follow_their_signals() {
        run(async {
            let tray = start_tray().await;

            let name = unique_item_name();
            let app = FakeApp::new("mail", Some(&name)).await;
            register(&app.conn, &name).await;
            assert!(eventually(Duration::from_secs(2), || shown(&tray, &name).is_some()).await);
            let Some(item) = shown(&tray, &name) else {
                unreachable!("checked above");
            };
            tokio::time::sleep(Duration::from_millis(300)).await;

            app.state.edit(|props| {
                props.title = String::from("Mail (3)");
                props.tooltip = String::from("3 unread");
                props.status = String::from("NeedsAttention");
            });
            app.emit("NewTitle").await;
            app.emit("NewToolTip").await;
            app.conn
                .emit_signal(
                    None::<BusName<'_>>,
                    ITEM_PATH,
                    ITEM_INTERFACE,
                    "NewStatus",
                    &("NeedsAttention",),
                )
                .await
                .unwrap();

            assert!(
                eventually(Duration::from_secs(2), || {
                    item.title.get() == "Mail (3)"
                        && item.tooltip.get().title == "3 unread"
                        && item.status.get() == Status::NeedsAttention
                })
                .await,
                "title {:?}, tooltip {:?}, status {:?}",
                item.title.get(),
                item.tooltip.get().title,
                item.status.get()
            );
        });
    }

    #[test]
    #[ignore = "needs a private session bus, see the module docs"]
    fn unresponsive_item_is_bounded_by_the_call_timeout() {
        run(async {
            let tray = start_tray().await;

            let name = unique_item_name();
            let frozen = FakeApp::new("frozen", Some(&name)).await;
            register(&frozen.conn, &name).await;
            assert!(eventually(Duration::from_secs(2), || shown(&tray, &name).is_some()).await);
            let Some(item) = shown(&tray, &name) else {
                unreachable!("checked above");
            };
            tokio::time::sleep(Duration::from_millis(300)).await;

            // The app stops answering, with a refresh pending and a click in flight.
            frozen.state.hung.store(true, Ordering::Release);
            frozen
                .state
                .edit(|props| props.icon = String::from("thawed"));
            frozen.emit("NewIcon").await;
            let started = Instant::now();
            let click =
                tokio::time::timeout(CALL_BOUND, item.activate(Coordinates::new(0, 0))).await;
            assert!(
                matches!(click, Ok(Err(_))),
                "a click on an unresponsive item did not fail within {CALL_BOUND:?}"
            );
            assert!(started.elapsed() < CALL_BOUND);

            // Everyone else is unaffected meanwhile.
            let healthy_name = unique_item_name();
            let healthy = FakeApp::new("healthy", Some(&healthy_name)).await;
            register(&healthy.conn, &healthy_name).await;
            assert!(
                eventually(Duration::from_secs(2), || shown(&tray, &healthy_name)
                    .is_some())
                .await
            );
            let Some(healthy_item) = shown(&tray, &healthy_name) else {
                unreachable!("checked above");
            };
            let click = tokio::time::timeout(
                Duration::from_secs(1),
                healthy_item.activate(Coordinates::new(0, 0)),
            )
            .await;
            assert!(matches!(click, Ok(Ok(()))), "{click:?}");

            // Once the app answers again, its item recovers on the next signal: it was never
            // removed, and its worker gave up on the stalled read.
            frozen.state.hung.store(false, Ordering::Release);
            frozen.emit("NewIcon").await;
            assert!(shown(&tray, &name).is_some());
            assert!(
                eventually(CALL_BOUND, || item.icon_name.get().as_deref()
                    == Some("thawed"))
                .await,
                "icon {:?}",
                item.icon_name.get()
            );
        });
    }
}
