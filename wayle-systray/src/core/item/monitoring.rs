use std::sync::{
    Arc, Weak,
    atomic::{AtomicU8, Ordering},
};

use futures::{Stream, StreamExt};
use tokio::{
    sync::Notify,
    time::{Duration, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument};
use wayle_traits::ModelMonitoring;
use zbus::proxy::CacheProperties;

use super::TrayItem;
use crate::{
    error::Error,
    proxy::{dbusmenu::DBusMenuProxy, status_notifier_item::StatusNotifierItemProxy},
    types::{
        item::{Category, IconPixmap, Status, Tooltip},
        menu::MenuItem,
    },
};

const MENU_DEBOUNCE: Duration = Duration::from_millis(100);

const UNKNOWN_PROPERTY_ERROR: &str = "org.freedesktop.DBus.Error.UnknownProperty";

fn is_unknown_property_error(error: &zbus::Error) -> bool {
    matches!(error, zbus::Error::MethodError(name, _, _) if name.as_str() == UNKNOWN_PROPERTY_ERROR)
}

impl ModelMonitoring for TrayItem {
    type Error = Error;

    #[instrument(skip(self), fields(bus_name = %self.bus_name.get()), err)]
    async fn start_monitoring(self: Arc<Self>) -> Result<(), Self::Error> {
        let Some(ref cancellation_token) = self.cancellation_token else {
            return Err(Error::ServiceInitialization(String::from(
                "cancellation token not found",
            )));
        };

        let bus_name = self.bus_name.get();
        let id = Self::parse_service_identifier(&bus_name);
        let service = id.service.to_string();
        let path = id.path.to_string();

        let item_proxy = StatusNotifierItemProxy::builder(&self.zbus_connection)
            .destination(service.clone())?
            .path(path.clone())?
            .cache_properties(CacheProperties::No)
            .build()
            .await?;

        let menu_path = self.menu_path.get().as_str().to_string();
        let menu_proxy = DBusMenuProxy::builder(&self.zbus_connection)
            .destination(service)?
            .path(menu_path)?
            .cache_properties(CacheProperties::No)
            .build()
            .await?;

        let cancel_token = cancellation_token.clone();
        let weak_self = Arc::downgrade(&self);

        tokio::spawn(async move {
            monitor_properties(&bus_name, weak_self, item_proxy, menu_proxy, cancel_token).await;
        });

        Ok(())
    }
}

/// Refresh work owed to one tray item, coalesced into a bit set.
///
/// The signal pumps only record *that* something changed; the refresh worker does the
/// D-Bus round trips. A storm of N identical signals therefore costs one fetch, not N.
#[derive(Default)]
struct PendingRefresh {
    bits: AtomicU8,
    notify: Notify,
}

impl PendingRefresh {
    /// Re-read every property and the menu (initial post-subscription sync).
    const ALL: u8 = 1 << 0;
    const ICON: u8 = 1 << 1;
    const ATTENTION_ICON: u8 = 1 << 2;
    const OVERLAY_ICON: u8 = 1 << 3;
    const MENU: u8 = 1 << 4;

    fn request(&self, bits: u8) {
        self.bits.fetch_or(bits, Ordering::AcqRel);
        self.notify.notify_one();
    }

    fn take(&self) -> u8 {
        self.bits.swap(0, Ordering::AcqRel)
    }
}

/// Subscribes to the item's signals and turns each one into a refresh request.
///
/// Nothing that holds one of these signal streams may stop polling it to await a D-Bus
/// round trip. zbus delivers every incoming message from a single socket-reader task
/// into bounded per-stream queues (64 messages), and that reader *waits* when any live
/// stream's queue is full. If a stream sat unpolled while its item kept signalling (a
/// burst of `NewIcon`, say) and the holder was waiting for a reply queued behind those
/// signals, the reader would stall for good and every caller on the shared connection
/// would hang: no new items, no removals, no clicks, and no error.
///
/// So each stream gets its own pump task the moment it exists (setting up the next
/// subscription is itself a round trip), pumps only flag work, and [`refresh_worker`]
/// does all the fetching.
#[instrument(skip_all, fields(bus_name = %bus_name))]
async fn monitor_properties(
    bus_name: &str,
    weak_item: Weak<TrayItem>,
    item_proxy: StatusNotifierItemProxy<'static>,
    menu_proxy: DBusMenuProxy<'static>,
    cancellation_token: CancellationToken,
) {
    // Stops the worker, the debouncer and every pump together.
    let token = cancellation_token.child_token();
    let pending = Arc::new(PendingRefresh::default());
    let menu_signal = Arc::new(Notify::new());

    tokio::spawn(refresh_worker(
        weak_item,
        item_proxy.clone(),
        menu_proxy.clone(),
        Arc::clone(&pending),
        token.clone(),
    ));
    tokio::spawn(debounce_menu(
        Arc::clone(&menu_signal),
        Arc::clone(&pending),
        token.clone(),
    ));

    let request = |bits: u8| {
        let pending = Arc::clone(&pending);
        move || pending.request(bits)
    };
    let menu_changed = || {
        let menu_signal = Arc::clone(&menu_signal);
        move || menu_signal.notify_one()
    };

    match item_proxy.receive_new_icon().await {
        Ok(stream) => spawn_pump(stream, token.clone(), request(PendingRefresh::ICON)),
        Err(error) => return subscription_failed(&token, &error, "NewIcon"),
    }
    match item_proxy.receive_new_attention_icon().await {
        Ok(stream) => spawn_pump(
            stream,
            token.clone(),
            request(PendingRefresh::ATTENTION_ICON),
        ),
        Err(error) => return subscription_failed(&token, &error, "NewAttentionIcon"),
    }
    match item_proxy.receive_new_overlay_icon().await {
        Ok(stream) => spawn_pump(stream, token.clone(), request(PendingRefresh::OVERLAY_ICON)),
        Err(error) => return subscription_failed(&token, &error, "NewOverlayIcon"),
    }
    match item_proxy.receive_new_menu().await {
        Ok(stream) => spawn_pump(stream, token.clone(), menu_changed()),
        Err(error) => return subscription_failed(&token, &error, "NewMenu"),
    }
    match menu_proxy.receive_layout_updated().await {
        Ok(stream) => spawn_pump(stream, token.clone(), menu_changed()),
        Err(error) => return subscription_failed(&token, &error, "LayoutUpdated"),
    }
    match menu_proxy.receive_items_properties_updated().await {
        Ok(stream) => spawn_pump(stream, token.clone(), menu_changed()),
        Err(error) => return subscription_failed(&token, &error, "ItemsPropertiesUpdated"),
    }

    // `get_live` fetched the properties *before* the subscriptions above existed, so a
    // value an app set in that window (common for apps that register first and populate
    // afterwards) was missed and no signal will replay it. Re-read everything once now.
    pending.request(PendingRefresh::ALL);
    debug!("Tray item '{bus_name}' monitoring started");
}

fn subscription_failed(token: &CancellationToken, error: &zbus::Error, signal: &str) {
    error!(error = %error, signal, "cannot subscribe to tray item signal");
    token.cancel();
}

/// Drains `stream` for as long as it lives, calling `on_signal` (which must not block or
/// await) for every message, until `token` is cancelled or the stream ends.
fn spawn_pump<S>(stream: S, token: CancellationToken, on_signal: impl Fn() + Send + 'static)
where
    S: Stream + Send + 'static,
{
    tokio::spawn(async move {
        tokio::pin!(stream);
        loop {
            tokio::select! {
                () = token.cancelled() => return,
                next = stream.next() => match next {
                    Some(_) => on_signal(),
                    None => return,
                },
            }
        }
    });
}

/// Turns bursts of menu signals into one layout fetch once they go quiet for
/// [`MENU_DEBOUNCE`].
async fn debounce_menu(
    menu_signal: Arc<Notify>,
    pending: Arc<PendingRefresh>,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            () = token.cancelled() => return,
            () = menu_signal.notified() => {}
        }
        loop {
            tokio::select! {
                () = token.cancelled() => return,
                () = sleep(MENU_DEBOUNCE) => break,
                () = menu_signal.notified() => {}
            }
        }
        debug!("menu signals settled, scheduling layout fetch");
        pending.request(PendingRefresh::MENU);
    }
}

/// Performs the D-Bus fetches requested through `pending`, one batch at a time.
///
/// Runs on its own task so a slow or unresponsive item only delays its own refreshes;
/// the signal pumps keep draining regardless. Each batch is raced against cancellation,
/// so a peer that never answers cannot keep this task alive after the item is removed.
async fn refresh_worker(
    weak_item: Weak<TrayItem>,
    item_proxy: StatusNotifierItemProxy<'static>,
    menu_proxy: DBusMenuProxy<'static>,
    pending: Arc<PendingRefresh>,
    cancellation_token: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancellation_token.cancelled() => return,
            () = pending.notify.notified() => {}
        }

        let bits = pending.take();
        if bits == 0 {
            continue;
        }
        let Some(tray_item) = weak_item.upgrade() else {
            // The item is gone; stop the pumps and the debouncer too.
            cancellation_token.cancel();
            return;
        };

        let batch = run_refresh(bits, &item_proxy, &menu_proxy, &tray_item);
        tokio::select! {
            () = cancellation_token.cancelled() => return,
            () = batch => {}
        }
    }
}

async fn run_refresh(
    bits: u8,
    item_proxy: &StatusNotifierItemProxy<'static>,
    menu_proxy: &DBusMenuProxy<'static>,
    tray_item: &TrayItem,
) {
    if bits & PendingRefresh::ALL != 0 {
        resync_properties(item_proxy, menu_proxy, tray_item).await;
        return;
    }

    if bits & PendingRefresh::ICON != 0 {
        refresh_icon(item_proxy, tray_item).await;
    }

    if bits & PendingRefresh::ATTENTION_ICON != 0 {
        if let Ok(name) = item_proxy.attention_icon_name().await {
            tray_item.attention_icon_name.set(filter_empty(name));
        }
        if let Ok(pixmaps) = item_proxy.attention_icon_pixmap().await {
            let pixmaps: Vec<IconPixmap> = pixmaps.into_iter().map(Into::into).collect();
            tray_item.attention_icon_pixmap.set(pixmaps);
        }
    }

    if bits & PendingRefresh::OVERLAY_ICON != 0 {
        if let Ok(name) = item_proxy.overlay_icon_name().await {
            tray_item.overlay_icon_name.set(filter_empty(name));
        }
        if let Ok(pixmaps) = item_proxy.overlay_icon_pixmap().await {
            let pixmaps: Vec<IconPixmap> = pixmaps.into_iter().map(Into::into).collect();
            tray_item.overlay_icon_pixmap.set(pixmaps);
        }
    }

    if bits & PendingRefresh::MENU != 0 {
        match menu_proxy.get_layout(0, -1, vec![]).await {
            Ok(layout) => tray_item.menu.set(Some(MenuItem::from(layout))),
            Err(error) => {
                tray_item.menu.set(None);
                error!(error = %error, "cannot update menu layout");
            }
        }
    }
}

fn filter_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

/// Re-reads every property once, right after the signal subscriptions are established.
/// `get_live` fetched these *before* this task subscribed, so a value an app set in that
/// window was missed and no `*Changed`/`New*` signal will replay it. Each property is
/// written only when its read succeeds, so a transient error never clobbers a value that
/// was fetched correctly by `get_live`; if the connection has since died, every read
/// fails and nothing is overwritten (the item is removed via NameOwnerChanged instead).
#[allow(clippy::cognitive_complexity)]
async fn resync_properties(
    item_proxy: &StatusNotifierItemProxy<'static>,
    menu_proxy: &DBusMenuProxy<'static>,
    tray_item: &TrayItem,
) {
    if let Ok(value) = item_proxy.id().await {
        tray_item.id.set(value);
    }
    if let Ok(value) = item_proxy.title().await {
        tray_item.title.set(value);
    }
    if let Ok(value) = item_proxy.category().await {
        tray_item.category.set(Category::from(value.as_str()));
    }
    if let Ok(value) = item_proxy.status().await {
        tray_item.status.set(Status::from(value.as_str()));
    }
    if let Ok(value) = item_proxy.window_id().await {
        tray_item.window_id.set(value);
    }
    if let Ok(value) = item_proxy.item_is_menu().await {
        tray_item.item_is_menu.set(value);
    }
    if let Ok(value) = item_proxy.icon_theme_path().await {
        tray_item.icon_theme_path.set(filter_empty(value));
    }
    if let Ok(value) = item_proxy.tool_tip().await {
        tray_item.tooltip.set(Tooltip::from(value));
    }
    if let Ok(value) = item_proxy.menu().await {
        tray_item.menu_path.set(value);
    }
    if let Ok(value) = item_proxy.overlay_icon_name().await {
        tray_item.overlay_icon_name.set(filter_empty(value));
    }
    if let Ok(pixmaps) = item_proxy.overlay_icon_pixmap().await {
        let pixmaps: Vec<IconPixmap> = pixmaps.into_iter().map(Into::into).collect();
        tray_item.overlay_icon_pixmap.set(pixmaps);
    }
    if let Ok(value) = item_proxy.attention_icon_name().await {
        tray_item.attention_icon_name.set(filter_empty(value));
    }
    if let Ok(pixmaps) = item_proxy.attention_icon_pixmap().await {
        let pixmaps: Vec<IconPixmap> = pixmaps.into_iter().map(Into::into).collect();
        tray_item.attention_icon_pixmap.set(pixmaps);
    }
    if let Ok(value) = item_proxy.attention_movie_name().await {
        tray_item.attention_movie_name.set(filter_empty(value));
    }

    refresh_icon(item_proxy, tray_item).await;

    if let Ok(layout) = menu_proxy.get_layout(0, -1, vec![]).await {
        tray_item.menu.set(Some(MenuItem::from(layout)));
    }
}

/// Reads the item's current icon (name and pixmap) and updates the reactive properties.
/// Used both for the initial post-subscription sync and in response to `NewIcon` signals.
///
/// The read goes through a *fresh* proxy that eagerly caches via a single `GetAll`, rather
/// than the caller's non-caching proxy. `GetAll` is the only way to read the icon for apps
/// (e.g. libappindicator-based ones) that publish `IconName`/`IconPixmap` through `GetAll`
/// yet answer an individual property `Get` with `UnknownProperty` — reading those
/// individually would wrongly clear the icon. Rebuilding on each call re-runs `GetAll`, so
/// the values stay current after a `NewIcon`. A read that fails with `UnknownProperty`
/// means the app genuinely has no such property (fall back to the pixmap); any other error
/// (a vanished connection) leaves the existing value untouched rather than clobbering it.
async fn refresh_icon(item_proxy: &StatusNotifierItemProxy<'static>, tray_item: &TrayItem) {
    let inner = item_proxy.inner();
    let Ok(builder) = StatusNotifierItemProxy::builder(inner.connection())
        .destination(inner.destination().clone())
    else {
        return;
    };
    let Ok(builder) = builder.path(inner.path().clone()) else {
        return;
    };
    let Ok(proxy) = builder.cache_properties(CacheProperties::Yes).build().await else {
        return;
    };

    match proxy.icon_name().await {
        Ok(name) => tray_item.icon_name.set(filter_empty(name)),
        Err(error) if is_unknown_property_error(&error) => tray_item.icon_name.set(None),
        Err(_) => {}
    }

    match proxy.icon_pixmap().await {
        Ok(pixmaps) => {
            let pixmaps: Vec<IconPixmap> = pixmaps.into_iter().map(Into::into).collect();
            tray_item.icon_pixmap.set(pixmaps);
        }
        Err(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn pending_refresh_coalesces_repeated_requests() {
        let pending = PendingRefresh::default();

        for _ in 0..300 {
            pending.request(PendingRefresh::ICON);
        }
        pending.request(PendingRefresh::MENU);

        assert_eq!(pending.take(), PendingRefresh::ICON | PendingRefresh::MENU);
        assert_eq!(pending.take(), 0);
    }

    #[tokio::test]
    async fn pending_refresh_wakes_worker_requested_before_it_waits() {
        let pending = PendingRefresh::default();

        // A request that lands while the worker is busy must not be lost: the
        // permit is stored and the next `notified()` resolves immediately.
        pending.request(PendingRefresh::ICON);
        let woke = tokio::time::timeout(Duration::from_secs(1), pending.notify.notified()).await;

        assert!(woke.is_ok());
        assert_eq!(pending.take(), PendingRefresh::ICON);
    }

    #[tokio::test]
    async fn pending_refresh_storm_yields_single_wakeup() {
        let pending = PendingRefresh::default();

        for _ in 0..300 {
            pending.request(PendingRefresh::ICON);
        }
        pending.notify.notified().await;
        let _ = pending.take();
        let second =
            tokio::time::timeout(Duration::from_millis(50), pending.notify.notified()).await;

        assert!(second.is_err());
    }
}
