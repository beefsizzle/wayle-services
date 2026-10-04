//! Central registrar: the single owner of tray-item state.
//!
//! All tray-item mutations flow through one actor task ([`run_registrar`]) that owns
//! the authoritative ordered list of items. The reactive [`SystemTrayService::items`]
//! property and the `org.kde.StatusNotifierWatcher` D-Bus surface are pure derivations
//! of that list, so the two-sources-of-truth divergence of the old design is gone.
//!
//! Removal is fully reactive: each item runs a lifecycle task that watches an
//! `arg0`-filtered `NameOwnerChanged` for exactly its owner connection (and, when it
//! registered a well-known name, for that name too) and tears the item down when the
//! owner disappears or the name is released. There is no periodic reconciliation and no
//! polling — the only non-listener query is a single liveness check per item, armed
//! *after* the death watch so the "owner already gone" gap is closed by construction.
//!
//! [`SystemTrayService::items`]: crate::service::SystemTrayService::items

use std::sync::Arc;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use wayle_core::Property;
use wayle_traits::Reactive;
use zbus::{
    Connection,
    fdo::{DBusProxy, NameOwnerChanged},
    names::{BusName, OwnedUniqueName},
};

use crate::{
    core::item::{LiveTrayItemParams, TrayItem},
    types::{ITEM_OBJECT_PATH, WATCHER_INTERFACE, WATCHER_OBJECT_PATH},
};

/// Stable identity of a tray item: the unique owner connection plus the object path
/// its `StatusNotifierItem` lives at.
///
/// The same connection can register under three service-string shapes — a well-known
/// name (`org.kde.StatusNotifierItem-PID-N`), the object-path form
/// (`{unique}/StatusNotifierItem`), or a bare unique name (`:1.42`, produced by startup
/// discovery). All three resolve to the same `(owner, path)`, so one app is exactly one
/// entry and duplicate icons cannot occur regardless of which shape is used. When the
/// same object is registered again under a different service string, the newer string
/// replaces the older one (see [`plan_insert`]).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ItemKey {
    owner: OwnedUniqueName,
    path: String,
}

struct Entry {
    /// Identifies the lifecycle that produced this entry, so a stale lifecycle can never
    /// remove an entry that has since been replaced under the same key.
    id: u64,
    key: ItemKey,
    /// The callable service string (becomes `TrayItem::bus_name`) and the value exposed
    /// via the watcher's `RegisteredStatusNotifierItems` property / signals.
    service: String,
    item: Arc<TrayItem>,
    /// Cancels this item's lifecycle task and its property monitor.
    cancel: CancellationToken,
}

/// Messages accepted by the registrar actor.
pub(crate) enum RegistrarMsg {
    /// A registration request. `sender` is `Some` for a live method call or startup
    /// discovery; `None` for a host-mode external `Registered` signal. The sender only
    /// determines the owner for the object-path form; a bus-name service is resolved to
    /// the connection that owns the name.
    Register {
        service: String,
        sender: Option<OwnedUniqueName>,
    },
    /// Host-mode external `StatusNotifierItemUnregistered`: correlate by exact string,
    /// since the external watcher emits the raw string it stored (after owner death, so
    /// the owner is no longer resolvable).
    RemoveByService { service: String },
    /// A prepared item is ready to publish (sent by a lifecycle task).
    Insert {
        id: u64,
        key: ItemKey,
        service: String,
        item: Arc<TrayItem>,
        cancel: CancellationToken,
    },
    /// Tear down the entry produced by lifecycle `id` (its owner disappeared or its
    /// well-known name was released).
    Remove { id: u64 },
    /// Reply with the current registered service strings (for the D-Bus property).
    Query { reply: oneshot::Sender<Vec<String>> },
}

/// Cloneable handle used by producers (the watcher interface, discovery, host-mode
/// signal listeners) to talk to the registrar actor.
#[derive(Clone)]
pub(crate) struct RegistrarHandle {
    tx: mpsc::UnboundedSender<RegistrarMsg>,
}

impl RegistrarHandle {
    /// Requests registration of `service`. `sender` is the resolved owner when known.
    pub(crate) fn register(&self, service: String, sender: Option<OwnedUniqueName>) {
        let _ = self.tx.send(RegistrarMsg::Register { service, sender });
    }

    /// Removes an item correlated by its exact registered string (host mode).
    pub(crate) fn remove_by_service(&self, service: String) {
        let _ = self.tx.send(RegistrarMsg::RemoveByService { service });
    }

    /// Returns the current registered service strings.
    pub(crate) async fn registered_services(&self) -> Vec<String> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(RegistrarMsg::Query { reply }).is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    fn send(&self, msg: RegistrarMsg) {
        let _ = self.tx.send(msg);
    }
}

/// Spawns the registrar actor and returns a handle to it.
///
/// `items` is the reactive property kept in sync (single source of truth). `watcher_conn`
/// is `Some` in watcher mode — the connection owning `org.kde.StatusNotifierWatcher`, used
/// to emit `StatusNotifierItem(Un)Registered`; `None` in host mode.
pub(crate) fn spawn_registrar(
    conn: Connection,
    watcher_conn: Option<Connection>,
    items: Property<Vec<Arc<TrayItem>>>,
    cancellation_token: CancellationToken,
) -> RegistrarHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = RegistrarHandle { tx };
    tokio::spawn(run_registrar(
        rx,
        handle.clone(),
        conn,
        watcher_conn,
        items,
        cancellation_token,
    ));
    handle
}

async fn run_registrar(
    mut rx: mpsc::UnboundedReceiver<RegistrarMsg>,
    handle: RegistrarHandle,
    conn: Connection,
    watcher_conn: Option<Connection>,
    items: Property<Vec<Arc<TrayItem>>>,
    cancellation_token: CancellationToken,
) {
    // Authoritative, insertion-ordered item list (small N; linear lookup is fine, and
    // the stable order keeps the tray from reshuffling on every change).
    let mut entries: Vec<Entry> = Vec::new();
    let mut next_id: u64 = 0;

    loop {
        tokio::select! {
            () = cancellation_token.cancelled() => {
                for entry in entries.drain(..) {
                    entry.cancel.cancel();
                }
                return;
            }
            msg = rx.recv() => {
                let Some(msg) = msg else { return };
                match msg {
                    RegistrarMsg::Register { service, sender } => {
                        next_id += 1;
                        handle_register(&handle, &conn, &cancellation_token, &entries, next_id, service, sender);
                    }
                    RegistrarMsg::Insert { id, key, service, item, cancel } => {
                        let new_entry = Entry { id, key, service: service.clone(), item, cancel };
                        match plan_insert(&entries, &new_entry.key, &service) {
                            InsertPlan::Discard => {
                                // Already present under this string (or a concurrent register
                                // for it won the race), or a weaker bare-unique-name shape.
                                new_entry.cancel.cancel();
                            }
                            InsertPlan::Push => {
                                entries.push(new_entry);
                                publish(&items, &entries);
                                emit_signal(&watcher_conn, "StatusNotifierItemRegistered", &service).await;
                            }
                            InsertPlan::Replace(pos) => {
                                // The same object was registered again under a new service
                                // string (e.g. an app recreated its icon under a new name on
                                // the same connection). Keep the position so the tray does not
                                // reshuffle; the newer registration is the one that works.
                                let old = std::mem::replace(&mut entries[pos], new_entry);
                                old.cancel.cancel();
                                publish(&items, &entries);
                                emit_signal(&watcher_conn, "StatusNotifierItemUnregistered", &old.service).await;
                                emit_signal(&watcher_conn, "StatusNotifierItemRegistered", &service).await;
                            }
                        }
                    }
                    RegistrarMsg::Remove { id } => {
                        if let Some(pos) = entries.iter().position(|e| e.id == id) {
                            let entry = entries.remove(pos);
                            entry.cancel.cancel();
                            publish(&items, &entries);
                            emit_signal(&watcher_conn, "StatusNotifierItemUnregistered", &entry.service).await;
                        }
                    }
                    RegistrarMsg::RemoveByService { service } => {
                        if let Some(pos) = entries.iter().position(|e| e.service == service) {
                            let entry = entries.remove(pos);
                            entry.cancel.cancel();
                            publish(&items, &entries);
                            // Host mode: the external watcher owns the signals; we don't emit.
                        }
                    }
                    RegistrarMsg::Query { reply } => {
                        let _ = reply.send(entries.iter().map(|e| e.service.clone()).collect());
                    }
                }
            }
        }
    }
}

/// What to do with a freshly prepared item whose key may already be present.
#[derive(Debug, PartialEq, Eq)]
enum InsertPlan {
    /// New object: append it.
    Push,
    /// Same object already present under a different service string: replace that entry.
    Replace(usize),
    /// Same object already present under this string, or the new string is the weaker
    /// bare-unique-name shape (startup discovery): keep the existing entry.
    Discard,
}

fn plan_insert(entries: &[Entry], key: &ItemKey, service: &str) -> InsertPlan {
    plan_insert_by(
        entries.iter().map(|e| (&e.key, e.service.as_str())),
        key,
        service,
    )
}

fn plan_insert_by<'a>(
    existing: impl Iterator<Item = (&'a ItemKey, &'a str)>,
    key: &ItemKey,
    service: &str,
) -> InsertPlan {
    for (pos, (existing_key, existing_service)) in existing.enumerate() {
        if existing_key != key {
            continue;
        }
        if existing_service == service || is_bare_unique_name(service) {
            return InsertPlan::Discard;
        }
        return InsertPlan::Replace(pos);
    }
    InsertPlan::Push
}

/// `true` for a service string that is just a unique connection name (`:1.42`), the
/// shape startup discovery (and KStatusNotifierItem) produce. It carries no more
/// information than the key itself.
fn is_bare_unique_name(service: &str) -> bool {
    service.starts_with(':') && !service.contains('/')
}

/// The well-known bus name a service string depends on, if any.
///
/// `org.kde.StatusNotifierItem-1-1` (or `org.example/Path`) -> `Some(name)`; unique-name
/// forms (`:1.42`, `:1.42/StatusNotifierItem`) -> `None`, since a unique name only goes
/// away with its connection, which the owner watch already covers.
fn well_known_name(service: &str) -> Option<&str> {
    let (name, _) = split_stored(service);
    (!name.starts_with(':') && !name.is_empty()).then_some(name)
}

fn publish(items: &Property<Vec<Arc<TrayItem>>>, entries: &[Entry]) {
    items.set(entries.iter().map(|e| e.item.clone()).collect());
}

async fn emit_signal(watcher_conn: &Option<Connection>, member: &str, service: &str) {
    let Some(conn) = watcher_conn else { return };
    conn.emit_signal(
        None::<()>,
        WATCHER_OBJECT_PATH,
        WATCHER_INTERFACE,
        member,
        &service,
    )
    .await
    .unwrap_or_else(|error| {
        warn!(error = %error, member = %member, service = %service, "cannot emit watcher signal");
    });
}

/// Dispatches a registration request onto a per-item lifecycle task.
///
/// Dedup is intentionally done only against live `entries` here and again at `Insert`
/// time (a duplicate loses the race and is cancelled). There is deliberately no
/// "pending" reservation: a lifecycle that aborts (e.g. a transient `get_live` failure)
/// simply leaves nothing behind, so a re-registration always spawns a fresh, self-healing
/// lifecycle rather than being swallowed by a slot that is about to be released.
fn handle_register(
    handle: &RegistrarHandle,
    conn: &Connection,
    cancellation_token: &CancellationToken,
    entries: &[Entry],
    id: u64,
    service: String,
    sender: Option<OwnedUniqueName>,
) {
    match sender {
        // Object-path form: the object lives on the registering connection itself, so
        // the owner is known synchronously.
        Some(owner) if service.starts_with('/') => {
            let (callable, path) = split_registered(&service, &owner);
            let key = ItemKey {
                owner: owner.clone(),
                path,
            };
            if plan_insert(entries, &key, &callable) == InsertPlan::Discard {
                return;
            }
            tokio::spawn(item_lifecycle(
                handle.clone(),
                conn.clone(),
                cancellation_token.child_token(),
                id,
                callable,
                owner,
                key,
            ));
        }
        // Bus-name form, or a host-mode external registration: the item lives on
        // whichever connection owns that name, which need not be the sender.
        // KStatusNotifierItem, for one, registers the unique name of a dedicated
        // per-item connection from the app's main connection, and recreates that
        // connection (new unique name) whenever the app recreates its tray icon.
        _ => {
            tokio::spawn(resolve_then_run(
                handle.clone(),
                conn.clone(),
                cancellation_token.child_token(),
                id,
                service,
            ));
        }
    }
}

/// Resolves the owner of a bus-name service string, then runs its lifecycle.
async fn resolve_then_run(
    handle: RegistrarHandle,
    conn: Connection,
    cancel: CancellationToken,
    id: u64,
    service: String,
) {
    let (name_part, path) = split_stored(&service);

    let owner = {
        let Ok(dbus) = DBusProxy::new(&conn).await else {
            return;
        };
        let Ok(bus_name) = BusName::try_from(name_part) else {
            return;
        };
        // Owner-less / unresolvable: drop (fail-closed) — these paths have no protocol
        // ordering guarantee, so a dead name must not be registered.
        match dbus.get_name_owner(bus_name).await {
            Ok(owner) => owner,
            Err(_) => return,
        }
        // `dbus` is dropped here so we don't hold an idle proxy for the item's life.
    };

    let key = ItemKey {
        owner: owner.clone(),
        path: path.to_string(),
    };
    item_lifecycle(handle, conn, cancel, id, service, owner, key).await;
}

/// Owns one tray item for its whole life: arm the owner-death watch (and the name watch
/// for a well-known service name), verify liveness, build the live item, publish it,
/// then tear it down when the owner disappears or the name is released.
async fn item_lifecycle(
    handle: RegistrarHandle,
    conn: Connection,
    cancel: CancellationToken,
    id: u64,
    service: String,
    owner: OwnedUniqueName,
    key: ItemKey,
) {
    let Ok(dbus) = DBusProxy::new(&conn).await else {
        return;
    };

    // Arm the death watch BEFORE the liveness check: any owner loss between the check
    // and arming would otherwise be missed. A unique name only ever loses ownership by
    // disconnecting, so this stream firing == this item is gone.
    let mut death = match dbus
        .receive_name_owner_changed_with_args(&[(0, owner.as_str())])
        .await
    {
        Ok(stream) => stream,
        Err(error) => {
            warn!(error = %error, owner = %owner, "cannot watch owner liveness");
            return;
        }
    };

    // A well-known name can be released while its connection stays up (Qt and Chromium
    // do this when an app hides or recreates its tray icon). Watch it too, armed before
    // the ownership check below for the same reason as the owner watch.
    let well_known = well_known_name(&service).map(str::to_string);
    let mut name_lost = match &well_known {
        Some(name) => match dbus
            .receive_name_owner_changed_with_args(&[(0, name.as_str())])
            .await
        {
            Ok(stream) => Some(stream),
            Err(error) => {
                warn!(error = %error, name = %name, "cannot watch item name ownership");
                return;
            }
        },
        None => None,
    };

    // One-shot liveness check (not polling): closes the "owner already dead before the
    // watch was armed" register-after-death race by construction.
    let alive = match BusName::try_from(owner.as_str()) {
        Ok(bus_name) => matches!(dbus.name_has_owner(bus_name).await, Ok(true)),
        Err(_) => true,
    };
    if !alive {
        return;
    }

    // The name must still belong to `owner` now that the watch is armed; if it was
    // released or changed hands since the owner was resolved, there is nothing to show.
    if let Some(name) = &well_known {
        let Ok(bus_name) = BusName::try_from(name.as_str()) else {
            return;
        };
        match dbus.get_name_owner(bus_name).await {
            Ok(name_owner) if name_owner.as_str() == owner.as_str() => {}
            _ => return,
        }
    }

    let params = LiveTrayItemParams {
        connection: &conn,
        service: service.clone(),
        cancellation_token: &cancel,
    };
    let item = tokio::select! {
        result = TrayItem::get_live(params) => match result {
            Ok(item) => item,
            Err(error) => {
                debug!(error = %error, service = %service, "cannot load tray item");
                return;
            }
        },
        () = wait_item_gone(&mut death, name_lost.as_mut(), &owner) => return,
        () = cancel.cancelled() => return,
    };

    handle.send(RegistrarMsg::Insert {
        id,
        key,
        service,
        item,
        cancel: cancel.clone(),
    });

    // Item is live: wait for its owner to vanish or its name to be released (or for a
    // shutdown/dedup/replace cancel), then ask the registrar to remove it.
    tokio::select! {
        () = wait_item_gone(&mut death, name_lost.as_mut(), &owner) => {
            handle.send(RegistrarMsg::Remove { id });
        }
        () = cancel.cancelled() => {}
    }
}

/// Resolves once the item is gone: its owner connection disconnected, or (when it
/// registered a well-known name) that name no longer belongs to `owner`.
async fn wait_item_gone<S>(death: &mut S, name_lost: Option<&mut S>, owner: &OwnedUniqueName)
where
    S: Stream<Item = NameOwnerChanged> + Unpin,
{
    let name_released = async {
        match name_lost {
            Some(stream) => wait_owner_changed_from(stream, owner.as_str()).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        () = wait_owner_gone(death) => {}
        () = name_released => {}
    }
}

/// Resolves once the watched owner loses its bus ownership.
///
/// The stream is `arg0`-filtered to a single unique name, which can only lose ownership
/// by disconnecting, so any `new_owner == None` signal means this item is gone.
async fn wait_owner_gone<S>(stream: &mut S)
where
    S: Stream<Item = NameOwnerChanged> + Unpin,
{
    while let Some(signal) = stream.next().await {
        if let Ok(args) = signal.args()
            && args.new_owner.is_none()
        {
            return;
        }
    }
}

/// Resolves once an `arg0`-filtered well-known name stops being owned by `owner`
/// (released, or taken over by another connection).
async fn wait_owner_changed_from<S>(stream: &mut S, owner: &str)
where
    S: Stream<Item = NameOwnerChanged> + Unpin,
{
    while let Some(signal) = stream.next().await {
        if let Ok(args) = signal.args()
            && owner_lost(args.new_owner.as_deref(), owner)
        {
            return;
        }
    }
}

/// `true` when a `NameOwnerChanged` reporting `new_owner` means `owner` no longer has
/// the name.
fn owner_lost(new_owner: Option<&str>, owner: &str) -> bool {
    new_owner != Some(owner)
}

/// Splits the string an app passes to `RegisterStatusNotifierItem` into the callable
/// service string (`TrayItem::bus_name`) and object path, given its resolved owner.
///
/// - `"/StatusNotifierItem"` (object-path form) -> callable `"{owner}/StatusNotifierItem"`.
/// - `"org.kde.StatusNotifierItem-1-1"` (bus-name form) -> callable unchanged, default path.
fn split_registered(service: &str, owner: &OwnedUniqueName) -> (String, String) {
    if service.starts_with('/') {
        (format!("{owner}{service}"), service.to_string())
    } else {
        (service.to_string(), ITEM_OBJECT_PATH.to_string())
    }
}

/// Splits an already-callable stored service string into its owning bus name and path,
/// i.e. the inverse of what is stored (`"{name}/path"` or a bare bus name).
fn split_stored(service: &str) -> (&str, &str) {
    match service.find('/') {
        Some(idx) => (&service[..idx], &service[idx..]),
        None => (service, ITEM_OBJECT_PATH),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(owner: &str, path: &str) -> ItemKey {
        let Ok(owner) = OwnedUniqueName::try_from(owner) else {
            unreachable!("test owner names are valid");
        };
        ItemKey {
            owner,
            path: path.to_string(),
        }
    }

    fn plan(existing: &[(ItemKey, &str)], new_key: &ItemKey, service: &str) -> InsertPlan {
        plan_insert_by(existing.iter().map(|(k, s)| (k, *s)), new_key, service)
    }

    #[test]
    fn new_object_is_pushed() {
        let existing = [(
            key(":1.1", ITEM_OBJECT_PATH),
            "org.kde.StatusNotifierItem-1-1",
        )];

        let result = plan(
            &existing,
            &key(":1.2", ITEM_OBJECT_PATH),
            "org.kde.StatusNotifierItem-2-1",
        );

        assert_eq!(result, InsertPlan::Push);
    }

    #[test]
    fn same_object_same_service_is_discarded() {
        let existing = [(
            key(":1.1", ITEM_OBJECT_PATH),
            "org.kde.StatusNotifierItem-1-1",
        )];

        let result = plan(
            &existing,
            &key(":1.1", ITEM_OBJECT_PATH),
            "org.kde.StatusNotifierItem-1-1",
        );

        assert_eq!(result, InsertPlan::Discard);
    }

    #[test]
    fn same_object_new_name_replaces_old_entry() {
        // Chromium/Electron recreate a tray icon as `-PID-2` on the same connection
        // and path after releasing `-PID-1`; the new registration must win.
        let existing = [
            (
                key(":1.9", ITEM_OBJECT_PATH),
                "org.kde.StatusNotifierItem-9-1",
            ),
            (
                key(":1.1", ITEM_OBJECT_PATH),
                "org.kde.StatusNotifierItem-1-1",
            ),
        ];

        let result = plan(
            &existing,
            &key(":1.1", ITEM_OBJECT_PATH),
            "org.kde.StatusNotifierItem-1-2",
        );

        assert_eq!(result, InsertPlan::Replace(1));
    }

    #[test]
    fn bare_unique_name_never_replaces_a_registered_name() {
        let existing = [(
            key(":1.1", ITEM_OBJECT_PATH),
            "org.kde.StatusNotifierItem-1-1",
        )];

        let result = plan(&existing, &key(":1.1", ITEM_OBJECT_PATH), ":1.1");

        assert_eq!(result, InsertPlan::Discard);
    }

    #[test]
    fn registered_name_replaces_discovered_bare_unique_name() {
        let existing = [(key(":1.1", ITEM_OBJECT_PATH), ":1.1")];

        let result = plan(
            &existing,
            &key(":1.1", ITEM_OBJECT_PATH),
            "org.kde.StatusNotifierItem-1-1",
        );

        assert_eq!(result, InsertPlan::Replace(0));
    }

    #[test]
    fn well_known_name_extracts_only_well_known_forms() {
        assert_eq!(
            well_known_name("org.kde.StatusNotifierItem-1-1"),
            Some("org.kde.StatusNotifierItem-1-1")
        );
        assert_eq!(
            well_known_name("org.example.App/Item"),
            Some("org.example.App")
        );
        assert_eq!(well_known_name(":1.42"), None);
        assert_eq!(well_known_name(":1.42/StatusNotifierItem"), None);
    }

    #[test]
    fn is_bare_unique_name_matches_only_unique_without_path() {
        assert!(is_bare_unique_name(":1.42"));
        assert!(!is_bare_unique_name(":1.42/StatusNotifierItem"));
        assert!(!is_bare_unique_name("org.kde.StatusNotifierItem-1-1"));
    }

    #[test]
    fn owner_lost_on_release_or_takeover_only() {
        assert!(owner_lost(None, ":1.1"));
        assert!(owner_lost(Some(":1.7"), ":1.1"));
        assert!(!owner_lost(Some(":1.1"), ":1.1"));
    }
}
