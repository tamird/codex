//! Agent-tree-scoped ownership for reusable MCP connections.

use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::McpProtocolMode;
use crate::request_router::McpConnectionRequestRouter;
use crate::request_router::McpRouteAcquireError;
use crate::request_router::McpSessionRoute;
use crate::rmcp_client::AsyncManagedClient;
use crate::rmcp_client::ManagedClient;
pub(crate) use crate::server::McpServerConnectionIdentity as McpConnectionIdentity;
use crate::tools::ToolInfo;
use anyhow::Result;
use anyhow::anyhow;
use codex_diagnostics::Gauge;
use codex_diagnostics::GaugeGuard;

/// Whether manager construction may reuse or replace the compatible tree connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum McpConnectionPoolMode {
    #[default]
    Reuse,
    /// Atomically replaces the connection followed by every compatible tree member.
    Replace,
}

/// Startup-only inputs that cease to matter once a pooled connection is ready.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct McpConnectionStartupIdentity {
    pub(crate) protocol_mode: Option<McpProtocolMode>,
    pub(crate) timeout: Duration,
}

type ConnectionFactory =
    Arc<dyn Fn(McpConnectionRequestRouter) -> AsyncManagedClient + Send + Sync>;

const RETIREMENT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
static LIVE_CONNECTIONS: Gauge = Gauge::new("mcp.connections.live");

async fn shutdown_retired_connection(client: Arc<AsyncManagedClient>) {
    if tokio::time::timeout(RETIREMENT_SHUTDOWN_TIMEOUT, client.shutdown())
        .await
        .is_err()
    {
        tracing::warn!("timed out shutting down retired MCP connection");
    }
}

struct SharedConnection {
    /// Process-local identity used to bind cached catalogs to this exact generation.
    id: u64,
    client: Arc<AsyncManagedClient>,
    startup_trigger: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
    startup_identity: Option<McpConnectionStartupIdentity>,
    superseded: tokio_util::sync::CancellationToken,
    _diagnostics_guard: GaugeGuard,
}

impl SharedConnection {
    fn new(
        client: AsyncManagedClient,
        startup_identity: Option<McpConnectionStartupIdentity>,
    ) -> Self {
        Self {
            id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            client: Arc::new(client),
            startup_trigger: Mutex::new(None),
            startup_identity,
            superseded: tokio_util::sync::CancellationToken::new(),
            _diagnostics_guard: LIVE_CONNECTIONS.track(),
        }
    }
}

impl Drop for SharedConnection {
    fn drop(&mut self) {
        self.client.request_router.close();
        self.client.cancel_token.cancel();
    }
}

struct ConnectionSlotState {
    generation: u64,
    connection: Arc<SharedConnection>,
    identity: Option<McpConnectionIdentity>,
    factory: ConnectionFactory,
}

struct McpConnectionKey {
    server_name: String,
    identity: McpConnectionIdentity,
}

struct ConnectionSlot {
    /// Present for pooled connections and absent for test-only standalone leases.
    key: Option<McpConnectionKey>,
    state: Mutex<ConnectionSlotState>,
    routes: Mutex<Vec<Weak<McpSessionRoute>>>,
    active_leases: AtomicUsize,
}

impl ConnectionSlot {
    fn new(
        key: McpConnectionKey,
        factory: ConnectionFactory,
        startup_identity: Option<McpConnectionStartupIdentity>,
        route: &Arc<McpSessionRoute>,
    ) -> Arc<Self> {
        let request_router = McpConnectionRequestRouter::default();
        request_router.register(route);
        let connection = Arc::new(SharedConnection::new(
            factory(request_router),
            startup_identity,
        ));
        let identity = key.identity.clone();
        Arc::new(Self {
            key: Some(key),
            state: Mutex::new(ConnectionSlotState {
                generation: 0,
                connection,
                identity: Some(identity),
                factory,
            }),
            routes: Mutex::new(vec![Arc::downgrade(route)]),
            active_leases: AtomicUsize::new(1),
        })
    }

    fn try_acquire(&self, route: &Arc<McpSessionRoute>) -> bool {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut active = self.active_leases.load(Ordering::Acquire);
        loop {
            if active == 0 {
                return false;
            }
            let Some(incremented) = active.checked_add(1) else {
                return false;
            };
            match self.active_leases.compare_exchange_weak(
                active,
                incremented,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.register_route_locked(&mut routes, route);
                    return true;
                }
                Err(current) => active = current,
            }
        }
    }

    fn register_route(&self, route: &Arc<McpSessionRoute>) {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.register_route_locked(&mut routes, route);
    }

    fn register_route_locked(
        &self,
        routes: &mut Vec<Weak<McpSessionRoute>>,
        route: &Arc<McpSessionRoute>,
    ) {
        routes.retain(|existing| existing.upgrade().is_some());
        if !routes
            .iter()
            .filter_map(Weak::upgrade)
            .any(|existing| Arc::ptr_eq(&existing, route))
        {
            routes.push(Arc::downgrade(route));
        }
        self.current().client.register_route(route);
    }

    fn unregister_route(&self, route: &Arc<McpSessionRoute>) {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|existing| {
            existing
                .upgrade()
                .is_some_and(|existing| !Arc::ptr_eq(&existing, route))
        });
        let connection = self.current();
        connection.client.unregister_route(route);
        // Startup observers retain leases, but only live session routes may keep pending work.
        // Hold the route lock until cancellation so a concurrent acquisition observes it.
        if !routes
            .iter()
            .filter_map(Weak::upgrade)
            .any(|route| !route.is_closed())
            && !connection.client.startup_complete.load(Ordering::Acquire)
        {
            connection.client.cancel_token.cancel();
        }
    }

    fn has_live_route(&self) -> bool {
        self.first_live_route().is_some()
    }

    fn has_live_route_other_than(&self, excluded: &Arc<McpSessionRoute>) -> bool {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|route| route.upgrade().is_some());
        routes
            .iter()
            .filter_map(Weak::upgrade)
            .any(|route| !route.is_closed() && !Arc::ptr_eq(&route, excluded))
    }

    fn first_live_route(&self) -> Option<Arc<McpSessionRoute>> {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|route| route.upgrade().is_some());
        routes
            .iter()
            .filter_map(Weak::upgrade)
            .find(|route| !route.is_closed())
    }

    fn current(&self) -> Arc<SharedConnection> {
        Arc::clone(
            &self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .connection,
        )
    }

    async fn is_reusable_connection(
        &self,
        desired: &McpConnectionIdentity,
        startup_identity: Option<McpConnectionStartupIdentity>,
    ) -> bool {
        let (connection, current_identity) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (Arc::clone(&state.connection), state.identity.clone())
        };
        if current_identity
            .as_ref()
            .is_some_and(|identity| !identity.has_same_connection_config(desired))
        {
            return false;
        }
        if !connection.client.startup_complete.load(Ordering::Acquire) {
            return current_identity
                .as_ref()
                .is_none_or(|identity| identity == desired)
                && startup_identity.is_some_and(|desired_startup| {
                    desired_startup.protocol_mode.is_some()
                        && connection.startup_identity == Some(desired_startup)
                        && !connection.client.cancel_token.is_cancelled()
                });
        }
        let Ok(client) = connection.client.client().await else {
            return false;
        };
        if client.client.is_closed().await {
            return false;
        }
        let reusable = if current_identity.as_ref() == Some(desired) {
            if matches!(desired.oauth_credentials(), Ok(None))
                && tokio::time::timeout(Duration::ZERO, client.client.managed_oauth_credentials())
                    .await
                    .is_ok_and(|credentials| matches!(credentials, Some(Some(_))))
            {
                return false;
            }
            true
        } else {
            let Ok(desired_credentials) = desired.oauth_credentials() else {
                return true;
            };
            match client.client.managed_oauth_credentials().await {
                Some(live_credentials) => live_credentials.as_ref() == desired_credentials,
                None => current_identity
                    .as_ref()
                    .and_then(|identity| identity.oauth_credentials().ok())
                    .is_some_and(|startup_credentials| startup_credentials == desired_credentials),
            }
        };

        if reusable && desired.oauth_credentials().is_ok() {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !Arc::ptr_eq(&state.connection, &connection) {
                return false;
            }
            state.identity = Some(desired.clone());
        }
        reusable
    }

    fn replace(
        &self,
        identity: McpConnectionIdentity,
        factory: ConnectionFactory,
        startup_identity: Option<McpConnectionStartupIdentity>,
    ) -> Arc<SharedConnection> {
        let previous = self.replace_with(identity, factory, startup_identity);
        previous.superseded.cancel();
        previous
    }

    fn replace_if_current(
        &self,
        current: &Arc<SharedConnection>,
        preferred_route: Option<&Arc<McpSessionRoute>>,
    ) -> Option<(Arc<SharedConnection>, Arc<McpSessionRoute>)> {
        self.key.as_ref()?;
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|route| route.upgrade().is_some());
        let live_routes = routes
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|route| !route.is_closed())
            .collect::<Vec<_>>();
        if live_routes.is_empty() || self.active_leases.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !Arc::ptr_eq(&state.connection, current) {
            return None;
        }
        let replacement = Self::make_connection_for_routes(
            Arc::clone(&state.factory),
            &live_routes,
            current.startup_identity,
        );
        let startup_route = preferred_route
            .filter(|preferred| {
                live_routes
                    .iter()
                    .any(|route| Arc::ptr_eq(route, preferred))
            })
            .cloned()
            .unwrap_or_else(|| Arc::clone(&live_routes[0]));
        state.generation = state.generation.wrapping_add(1);
        state.connection = Arc::clone(&replacement);
        current.client.request_router.close();
        current.client.cancel_token.cancel();
        Some((replacement, startup_route))
    }

    fn replace_with(
        &self,
        identity: McpConnectionIdentity,
        factory: ConnectionFactory,
        startup_identity: Option<McpConnectionStartupIdentity>,
    ) -> Arc<SharedConnection> {
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes.retain(|route| route.upgrade().is_some());
        let live_routes = routes
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|route| !route.is_closed())
            .collect::<Vec<_>>();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let replacement =
            Self::make_connection_for_routes(Arc::clone(&factory), &live_routes, startup_identity);
        state.generation = state.generation.wrapping_add(1);
        state.identity = Some(identity);
        state.factory = factory;
        std::mem::replace(&mut state.connection, replacement)
    }

    fn make_connection_for_routes(
        factory: ConnectionFactory,
        routes: &[Arc<McpSessionRoute>],
        startup_identity: Option<McpConnectionStartupIdentity>,
    ) -> Arc<SharedConnection> {
        let request_router = McpConnectionRequestRouter::default();
        for route in routes {
            request_router.register(route);
        }
        Arc::new(SharedConnection::new(
            factory(request_router),
            startup_identity,
        ))
    }

    fn release(&self) -> bool {
        let _routes = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.active_leases.fetch_sub(1, Ordering::AcqRel) == 1
    }

    #[cfg(test)]
    fn from_client(client: AsyncManagedClient) -> Arc<Self> {
        Arc::new(Self {
            key: None,
            state: Mutex::new(ConnectionSlotState {
                generation: 0,
                connection: Arc::new(SharedConnection::new(
                    client, /*startup_identity*/ None,
                )),
                identity: None,
                factory: Arc::new(|_| {
                    panic!("test-only standalone MCP lease cannot replace its connection")
                }),
            }),
            routes: Mutex::new(Vec::new()),
            active_leases: AtomicUsize::new(1),
        })
    }
}

struct PoolEntry {
    slot: Weak<ConnectionSlot>,
}

struct LeaseInner {
    slot: Arc<ConnectionSlot>,
    released: AtomicBool,
}

/// One session's ownership of a shared MCP connection slot.
///
/// Cloning a lease for an in-flight operation does not create another session lease. Every tree
/// member follows the slot's healthy generation, so retiring one physical connection does not
/// strand siblings on a cancelled client.
#[derive(Clone)]
pub(crate) struct McpConnectionLease {
    inner: Arc<LeaseInner>,
}

/// Unregisters a startup observer's route without retaining its logical connection lease.
pub(crate) struct McpConnectionRouteCleanup {
    slot: Weak<ConnectionSlot>,
    route: Arc<McpSessionRoute>,
}

impl Drop for McpConnectionRouteCleanup {
    fn drop(&mut self) {
        match self.slot.upgrade() {
            Some(slot) => slot.unregister_route(&self.route),
            None => self.route.close(),
        }
    }
}

/// Stable-binding readiness for the physical connection currently preferred by a lease.
///
/// Callers use the connection id to distinguish replacements that keep the same tool catalog.
pub(crate) enum StableMcpConnectionState {
    Ready(u64),
    TerminalFailure,
    PendingOrClosed,
}

#[derive(Clone)]
pub(crate) struct McpPooledClient {
    connection: Arc<SharedConnection>,
    slot: Weak<ConnectionSlot>,
    route: Arc<McpSessionRoute>,
}

/// The exact physical connection generation and ready client captured for one model step.
///
/// A pool replacement may publish a new generation for later steps. Keeping this guard alive
/// prevents the advertised generation from shutting down until every bound tool and resource
/// operation using it has finished.
#[derive(Clone)]
pub(crate) struct McpPooledBindingClient {
    connection: Arc<SharedConnection>,
    lease: McpConnectionLease,
    route: Arc<McpSessionRoute>,
    managed: Arc<ManagedClient>,
}

impl Deref for McpPooledBindingClient {
    type Target = ManagedClient;

    fn deref(&self) -> &Self::Target {
        &self.managed
    }
}

impl McpPooledBindingClient {
    #[cfg(test)]
    pub(crate) fn for_test(managed: Arc<ManagedClient>) -> Self {
        use futures::FutureExt;

        let route = Arc::new(McpSessionRoute::new(
            "binding-test".to_string(),
            crate::elicitation::ElicitationRequestManager::default(),
            /*tx_event*/ None,
        ));
        let request_router = McpConnectionRequestRouter::default();
        request_router.register(&route);
        let client = AsyncManagedClient {
            client: futures::future::ready(Ok((*managed).clone()))
                .boxed()
                .shared(),
            is_codex_apps_mcp_server: false,
            cached_server_info: None,
            codex_apps_tools_cache_context: None,
            tool_catalog_cache_context: None,
            startup_complete: Arc::new(AtomicBool::new(true)),
            startup_reconnect: None,
            cancel_token: tokio_util::sync::CancellationToken::new(),
            request_router,
        };
        let lease = McpConnectionLease::from(client);
        Self {
            connection: lease.current().expect("test lease should be active"),
            lease,
            route,
            managed,
        }
    }

    pub(crate) async fn run<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<ManagedClient>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let active_route = self
            .connection
            .client
            .request_router
            .acquire(Arc::clone(&self.route))
            .await
            .map_err(|error| anyhow!("MCP binding route unavailable: {error:?}"))?;
        let pooled_client = McpPooledClient {
            connection: Arc::clone(&self.connection),
            slot: Arc::downgrade(&self.lease.inner.slot),
            route: Arc::clone(&self.route),
        };
        let route = Arc::clone(&self.route);
        let managed = Arc::clone(&self.managed);
        let cancelled = self.connection.client.cancel_token.clone();
        let task_client = pooled_client.clone();
        let operation_task = tokio::spawn(async move {
            let _active_route = active_route;
            tokio::select! {
                result = operation(managed) => {
                    if result
                        .as_ref()
                        .is_err_and(codex_rmcp_client::is_connection_unusable)
                    {
                        task_client.retire_after_failure();
                    }
                    result
                },
                () = route.closed() => {
                    task_client.retire_after_route_close_in_background();
                    Err(anyhow!("MCP session route closed"))
                },
                () = cancelled.cancelled() => Err(anyhow!("shared MCP connection closed")),
            }
        });
        let mut retire_on_drop =
            RetireConnectionOnDrop::new(pooled_client, AbandonedOperationAction::RetireConnection);
        let result = operation_task
            .await
            .map_err(|error| anyhow!("shared MCP binding operation task failed: {error}"));
        retire_on_drop.disarm();
        result?
    }

    #[cfg(test)]
    pub(crate) fn tool_timeout(&self) -> Option<Duration> {
        self.managed.tool_timeout
    }
}

impl Deref for McpPooledClient {
    type Target = AsyncManagedClient;

    fn deref(&self) -> &Self::Target {
        &self.connection.client
    }
}

impl McpPooledClient {
    pub(crate) fn connection_id(&self) -> u64 {
        self.connection.id
    }

    fn is_current(&self) -> bool {
        self.slot
            .upgrade()
            .is_some_and(|slot| Arc::ptr_eq(&slot.current(), &self.connection))
    }

    fn retire_after_failure(&self) {
        if let Some(slot) = self.slot.upgrade()
            && let Some((replacement, route)) =
                slot.replace_if_current(&self.connection, Some(&self.route))
        {
            Self {
                connection: replacement,
                slot: Arc::downgrade(&slot),
                route,
            }
            .start_in_background();
        }
        let client = Arc::clone(&self.connection.client);
        tokio::spawn(async move {
            shutdown_retired_connection(client).await;
        });
    }

    fn prepare_retirement_after_route_close(&self) -> Arc<AsyncManagedClient> {
        if let Some(slot) = self.slot.upgrade()
            && slot.has_live_route()
            && let Some((replacement, route)) =
                slot.replace_if_current(&self.connection, /*preferred_route*/ None)
        {
            Self {
                connection: replacement,
                slot: Arc::downgrade(&slot),
                route,
            }
            .start_in_background();
        }
        Arc::clone(&self.connection.client)
    }

    fn retire_after_route_close_in_background(&self) {
        let client = self.prepare_retirement_after_route_close();
        tokio::spawn(async move {
            shutdown_retired_connection(client).await;
        });
    }

    async fn retire_after_route_close(&self) {
        shutdown_retired_connection(self.prepare_retirement_after_route_close()).await;
    }

    fn start_in_background(self) {
        self.start_in_background_inner(
            #[cfg(test)]
            /*after_check*/
            None,
        );
    }

    #[cfg(test)]
    fn start_in_background_after_check(self, hook: impl Future<Output = ()> + Send + 'static) {
        self.start_in_background_inner(Some(Box::pin(hook)));
    }

    fn start_in_background_inner(
        mut self,
        #[cfg(test)] after_check: Option<
            std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
        >,
    ) {
        tokio::spawn(async move {
            let Some(slot) = self.slot.upgrade() else {
                return;
            };
            if self.connection.superseded.is_cancelled()
                || !Arc::ptr_eq(&slot.current(), &self.connection)
            {
                return;
            }
            let active_route = loop {
                match self
                    .connection
                    .client
                    .request_router
                    .acquire(Arc::clone(&self.route))
                    .await
                {
                    Ok(active_route) => break active_route,
                    Err(McpRouteAcquireError::ConnectionClosed) => return,
                    Err(McpRouteAcquireError::RouteClosed) => {
                        let Some(slot) = self.slot.upgrade() else {
                            return;
                        };
                        if !Arc::ptr_eq(&slot.current(), &self.connection) {
                            return;
                        }
                        let Some(route) = slot.first_live_route() else {
                            return;
                        };
                        self.route = route;
                    }
                }
            };
            let _active_route = active_route;
            if self.connection.superseded.is_cancelled()
                || !Arc::ptr_eq(&slot.current(), &self.connection)
            {
                return;
            }
            #[cfg(test)]
            if let Some(after_check) = after_check {
                after_check.await;
            }
            tokio::select! {
                biased;
                () = self.connection.superseded.cancelled() => {}
                () = self.route.closed() => self.retire_after_route_close().await,
                _ = self.connection.client.client() => {}
            }
        });
    }
}

/// Retires a physical connection when its dispatched operation outlives the caller.
///
/// Dropping a `JoinHandle` detaches its task. An MCP request that is waiting for a server request
/// such as elicitation could otherwise keep the session route active forever and block sibling
/// sessions that share the connection.
struct RetireConnectionOnDrop {
    client: Option<McpPooledClient>,
    action: AbandonedOperationAction,
}

#[derive(Clone, Copy)]
enum AbandonedOperationAction {
    RetireConnection,
    CancelIfSoleRoute,
    KeepRunning,
}

#[derive(Clone, Copy)]
enum SupersededOperationAction {
    Drain,
    Retry,
}

impl RetireConnectionOnDrop {
    fn new(client: McpPooledClient, action: AbandonedOperationAction) -> Self {
        Self {
            client: Some(client),
            action,
        }
    }

    fn disarm(&mut self) {
        self.client = None;
    }
}

impl Drop for RetireConnectionOnDrop {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        let action = self.action;
        tokio::spawn(async move {
            match action {
                AbandonedOperationAction::RetireConnection => {
                    client.retire_after_failure();
                }
                AbandonedOperationAction::CancelIfSoleRoute => {
                    let Some(slot) = client.slot.upgrade() else {
                        return;
                    };
                    if Arc::ptr_eq(&slot.current(), &client.connection)
                        && !slot.has_live_route_other_than(&client.route)
                    {
                        client.connection.client.request_router.close();
                        client.connection.client.cancel_token.cancel();
                        shutdown_retired_connection(Arc::clone(&client.connection.client)).await;
                    }
                }
                AbandonedOperationAction::KeepRunning => {}
            }
        });
    }
}

impl McpConnectionLease {
    pub(crate) fn route_cleanup(&self, route: Arc<McpSessionRoute>) -> McpConnectionRouteCleanup {
        McpConnectionRouteCleanup {
            slot: Arc::downgrade(&self.inner.slot),
            route,
        }
    }

    fn new(slot: Arc<ConnectionSlot>) -> Self {
        Self {
            inner: Arc::new(LeaseInner {
                slot,
                released: AtomicBool::new(false),
            }),
        }
    }

    fn current(&self) -> Result<Arc<SharedConnection>> {
        if self.inner.released.load(Ordering::Acquire) {
            return Err(anyhow!("MCP connection lease released"));
        }
        Ok(self.inner.slot.current())
    }

    pub(crate) async fn run<T, F, Fut>(
        &self,
        route: Arc<McpSessionRoute>,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(McpPooledClient) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.run_with_abandoned_operation_action(
            route,
            AbandonedOperationAction::RetireConnection,
            SupersededOperationAction::Drain,
            operation,
        )
        .await
    }

    async fn run_with_abandoned_operation_action<T, F, Fut>(
        &self,
        route: Arc<McpSessionRoute>,
        abandoned_operation_action: AbandonedOperationAction,
        superseded_operation_action: SupersededOperationAction,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(McpPooledClient) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let mut operation = Some(operation);
        loop {
            let connection = self.current()?;
            if let Some(startup_trigger) = connection
                .startup_trigger
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
            {
                startup_trigger.send_replace(true);
            }
            self.inner.slot.register_route(&route);
            let active_route = match connection
                .client
                .request_router
                .acquire(Arc::clone(&route))
                .await
            {
                Ok(active_route) => active_route,
                Err(McpRouteAcquireError::ConnectionClosed) => {
                    if Arc::ptr_eq(&self.inner.slot.current(), &connection) {
                        return Err(anyhow!("shared MCP connection closed"));
                    }
                    continue;
                }
                Err(McpRouteAcquireError::RouteClosed) => {
                    return Err(anyhow!("MCP session route closed"));
                }
            };
            if !Arc::ptr_eq(&self.inner.slot.current(), &connection) {
                drop(active_route);
                continue;
            }
            let pooled_client = McpPooledClient {
                connection: Arc::clone(&connection),
                slot: Arc::downgrade(&self.inner.slot),
                route: Arc::clone(&route),
            };
            let Some(operation) = operation.take() else {
                return Err(anyhow!("shared MCP operation already started"));
            };
            let route_for_task = Arc::clone(&route);
            let task_client = pooled_client.clone();
            let connection_cancelled = task_client.connection.client.cancel_token.clone();
            let connection_superseded = task_client.connection.superseded.clone();
            let operation_task = tokio::spawn(async move {
                let _active_route = active_route;
                let superseded = async move {
                    match superseded_operation_action {
                        SupersededOperationAction::Drain => std::future::pending().await,
                        SupersededOperationAction::Retry => connection_superseded.cancelled().await,
                    }
                };
                tokio::select! {
                    value = operation(task_client.clone()) => Ok(value),
                    () = superseded => Err(anyhow!("shared MCP connection superseded")),
                    () = connection_cancelled.cancelled() => {
                        Err(anyhow!("shared MCP connection closed"))
                    }
                    () = route_for_task.closed() => {
                        task_client.retire_after_route_close_in_background();
                        Err(anyhow!("MCP session route closed"))
                    }
                }
            });
            let mut retire_on_drop =
                RetireConnectionOnDrop::new(pooled_client, abandoned_operation_action);
            let result = operation_task
                .await
                .map_err(|error| anyhow!("shared MCP operation task failed: {error}"));
            retire_on_drop.disarm();
            return result?;
        }
    }

    pub(crate) async fn run_mcp_request<T, F, Fut>(
        &self,
        route: Arc<McpSessionRoute>,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(McpPooledClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.run(route, move |client| async move {
            let result = operation(client.clone()).await;
            if result
                .as_ref()
                .is_err_and(codex_rmcp_client::is_connection_unusable)
            {
                client.retire_after_failure();
            }
            result
        })
        .await?
    }

    pub(crate) fn capture_ready_client(
        &self,
        route: Arc<McpSessionRoute>,
    ) -> Option<McpPooledBindingClient> {
        let connection = self.current().ok()?;
        let managed = Arc::new(connection.client.ready_client()?);
        Some(McpPooledBindingClient {
            connection,
            lease: self.clone(),
            route,
            managed,
        })
    }

    pub(crate) async fn capture_ready_client_and_tools(
        &self,
        route: Arc<McpSessionRoute>,
        catalog_override: Option<(u64, Vec<ToolInfo>)>,
        tool_timeout: Option<Duration>,
    ) -> Option<(McpPooledBindingClient, Vec<ToolInfo>)> {
        // A recoverable Apps startup has no ready client, and an active reconnect owns its
        // original session route. Do not queue a new publication merely to rediscover the known
        // failed initial client.
        if self.has_recoverable_failed_startup() {
            return None;
        }
        if let Ok(connection) = self.current()
            && !route.is_closed()
            && !connection.client.cancel_token.is_cancelled()
            && connection.client.startup_complete.load(Ordering::Acquire)
            && let Some(Ok(client)) = connection.client.client.peek()
            && Arc::ptr_eq(&connection, &self.inner.slot.current())
        {
            let mut managed = client.clone();
            managed.tool_timeout = tool_timeout;
            let tools = catalog_override
                .and_then(|(connection_id, tools)| {
                    (connection_id == connection.id).then_some(tools)
                })
                .unwrap_or_else(|| managed.tools.clone());
            return Some((
                McpPooledBindingClient {
                    connection,
                    lease: self.clone(),
                    route,
                    managed: Arc::new(managed),
                },
                tools,
            ));
        }
        let binding_lease = self.clone();
        self.run(route.clone(), move |client| async move {
            let catalog_override = catalog_override.and_then(|(connection_id, tools)| {
                (connection_id == client.connection_id()).then_some(tools)
            });
            let (managed, tools) = client
                .capture_ready_client_and_tools(catalog_override, Arc::clone(&route), tool_timeout)
                .await?;
            Some((
                McpPooledBindingClient {
                    connection: Arc::clone(&client.connection),
                    lease: binding_lease,
                    route,
                    managed,
                },
                tools,
            ))
        })
        .await
        .ok()
        .flatten()
    }

    /// Waits for startup of the slot's current physical connection.
    ///
    /// Refresh may replace a connection while its startup future is still running. Startup status
    /// must describe the replacement, not a superseded generation that happened to finish later.
    pub(crate) async fn await_current_startup(
        &self,
        route: Arc<McpSessionRoute>,
    ) -> std::result::Result<
        crate::rmcp_client::ManagedClient,
        crate::rmcp_client::StartupOutcomeError,
    > {
        self.await_current_startup_with_abandoned_operation_action(
            route,
            AbandonedOperationAction::CancelIfSoleRoute,
        )
        .await
    }

    /// Waits for startup without cancelling a pending optional server when the caller stops
    /// waiting after its shared startup grace.
    pub(crate) async fn await_current_startup_preserving_connection(
        &self,
        route: Arc<McpSessionRoute>,
    ) -> std::result::Result<
        crate::rmcp_client::ManagedClient,
        crate::rmcp_client::StartupOutcomeError,
    > {
        self.await_current_startup_with_abandoned_operation_action(
            route,
            AbandonedOperationAction::KeepRunning,
        )
        .await
    }

    async fn await_current_startup_with_abandoned_operation_action(
        &self,
        route: Arc<McpSessionRoute>,
        abandoned_operation_action: AbandonedOperationAction,
    ) -> std::result::Result<
        crate::rmcp_client::ManagedClient,
        crate::rmcp_client::StartupOutcomeError,
    > {
        loop {
            let observed_connection = self.current();
            if let Ok(connection) = observed_connection.as_ref()
                && !route.is_closed()
                && !connection.client.cancel_token.is_cancelled()
                && connection.client.startup_complete.load(Ordering::Acquire)
                && let Some(Ok(client)) = connection.client.client.peek()
                && Arc::ptr_eq(connection, &self.inner.slot.current())
            {
                return Ok(client.clone());
            }
            let attempt = self
                .run_with_abandoned_operation_action(
                    Arc::clone(&route),
                    abandoned_operation_action,
                    SupersededOperationAction::Retry,
                    |client| async move {
                        let outcome = client.client().await;
                        (client, outcome)
                    },
                )
                .await;
            match attempt {
                Ok((client, outcome)) if client.is_current() => return outcome,
                Ok(_) => continue,
                Err(_)
                    if observed_connection.is_ok_and(|observed| {
                        !Arc::ptr_eq(&observed, &self.inner.slot.current())
                    }) =>
                {
                    continue;
                }
                Err(_) => return Err(crate::rmcp_client::StartupOutcomeError::Cancelled),
            }
        }
    }

    /// Reads a raw cached catalog without acquiring a session request route.
    ///
    /// A fallback belongs to one physical generation and still obeys cache opt-out. Plugin
    /// provenance is applied once by the caller after capturing all server catalogs.
    pub(crate) fn cached_tools_or(
        &self,
        fallback: Option<(u64, Vec<ToolInfo>)>,
    ) -> Option<(u64, Vec<ToolInfo>)> {
        let connection = self.current().ok()?;
        if connection.superseded.is_cancelled() || connection.client.cancel_token.is_cancelled() {
            return None;
        }
        let fallback = fallback
            .and_then(|(connection_id, tools)| (connection_id == connection.id).then_some(tools));
        let tools = connection.client.cached_tools_or(fallback)?;
        if !connection.client.is_codex_apps_mcp_server && tools.is_empty() {
            return None;
        }
        Some((connection.id, tools))
    }

    pub(crate) fn has_cached_tools(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.has_cached_tools())
    }

    /// Observes the current physical connection without acquiring a route or starting it.
    pub(crate) async fn connection_status(&self) -> codex_protocol::mcp::McpServerConnectionStatus {
        let Ok(connection) = self.current() else {
            return codex_protocol::mcp::McpServerConnectionStatus::Cancelled;
        };
        use codex_protocol::mcp::McpServerConnectionStatus as Status;

        let status = connection.client.connection_status().await;
        // A session view may still describe an older generation's deferred startup.
        let dormant = connection
            .startup_trigger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|trigger| !*trigger.borrow());
        if status == Status::Starting && dormant {
            Status::NotStarted
        } else {
            status
        }
    }

    pub(crate) fn startup_complete(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.startup_complete.load(Ordering::Acquire))
    }

    pub(crate) fn set_startup_trigger(
        &self,
        trigger: tokio::sync::watch::Sender<bool>,
    ) -> (
        tokio::sync::watch::Sender<bool>,
        tokio::sync::watch::Receiver<bool>,
    ) {
        let connection = self.inner.slot.current();
        let mut startup_trigger = connection
            .startup_trigger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let startup_trigger = startup_trigger.get_or_insert(trigger);
        (startup_trigger.clone(), startup_trigger.subscribe())
    }

    pub(crate) fn has_recoverable_failed_startup(&self) -> bool {
        self.current()
            .is_ok_and(|connection| connection.client.has_recoverable_failed_startup())
    }

    pub(crate) fn current_connection_id(&self) -> Option<u64> {
        self.current().ok().map(|connection| connection.id)
    }

    pub(crate) async fn stable_connection_state(&self) -> StableMcpConnectionState {
        let Ok(connection) = self.current() else {
            return StableMcpConnectionState::PendingOrClosed;
        };
        if !connection.client.startup_complete.load(Ordering::Acquire) {
            return StableMcpConnectionState::PendingOrClosed;
        }
        let Some(client) = connection.client.ready_transport() else {
            return if matches!(connection.client.client.peek(), Some(Err(_))) {
                StableMcpConnectionState::TerminalFailure
            } else {
                StableMcpConnectionState::PendingOrClosed
            };
        };
        if client.is_closed().await {
            return StableMcpConnectionState::PendingOrClosed;
        }
        StableMcpConnectionState::Ready(connection.id)
    }

    pub(crate) fn optional_startup_deadline(
        &self,
        default: tokio::time::Instant,
        startup_grace: Duration,
    ) -> tokio::time::Instant {
        self.current()
            .ok()
            .and_then(|connection| {
                connection
                    .client
                    .tool_catalog_cache_context
                    .as_ref()
                    .map(|cache| cache.optional_startup_deadline(default, startup_grace))
            })
            .unwrap_or(default)
    }

    pub(crate) async fn is_reusable_connection(
        &self,
        desired: &McpConnectionIdentity,
        startup_identity: Option<McpConnectionStartupIdentity>,
    ) -> bool {
        self.inner
            .slot
            .is_reusable_connection(desired, startup_identity)
            .await
    }

    pub(crate) async fn authentication_failed(&self) -> bool {
        let Ok(connection) = self.current() else {
            return false;
        };
        if !connection.client.startup_complete.load(Ordering::Acquire) {
            return false;
        }
        connection
            .client
            .client
            .clone()
            .await
            .is_err_and(|error| error.is_authentication_required())
    }

    pub(crate) fn connection_identity(&self) -> Option<McpConnectionIdentity> {
        self.inner
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .identity
            .clone()
    }

    pub(crate) fn has_same_connection_identity(&self, desired: &McpConnectionIdentity) -> bool {
        self.connection_identity().as_ref() == Some(desired)
    }

    pub(crate) fn cached_server_info(&self) -> Option<codex_protocol::mcp::McpServerInfo> {
        self.current()
            .ok()
            .and_then(|connection| connection.client.cached_server_info.clone())
    }

    pub(crate) async fn reconnect_failed_startup(&self, route: Arc<McpSessionRoute>) {
        if let Ok(connection) = self.current()
            && !route.is_closed()
            && !connection.client.cancel_token.is_cancelled()
            && connection.client.startup_complete.load(Ordering::Acquire)
            && matches!(connection.client.client.peek(), Some(Ok(_)))
            && Arc::ptr_eq(&connection, &self.inner.slot.current())
        {
            return;
        }
        let lease = self.clone();
        tokio::spawn(async move {
            let reconnect_route = Arc::clone(&route);
            let _ = lease
                .run(route, move |client| async move {
                    client.reconnect_failed_startup(reconnect_route).await;
                })
                .await;
        });
    }

    pub(crate) fn unregister_route(&self, route: &Arc<McpSessionRoute>) {
        self.inner.slot.unregister_route(route);
    }

    /// Releases this session's logical ownership, returning whether it was the final lease.
    pub(crate) fn release(&self) -> bool {
        if self.inner.released.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.inner.slot.release()
    }

    pub(crate) async fn shutdown(&self) {
        self.inner.slot.current().client.shutdown().await;
    }

    #[cfg(test)]
    pub(crate) fn is_exclusive(&self) -> bool {
        self.inner.slot.active_leases.load(Ordering::Acquire) == 1
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner.slot, &other.inner.slot)
    }

    #[cfg(test)]
    pub(crate) fn is_connection_cancelled(&self) -> bool {
        self.inner.slot.current().client.cancel_token.is_cancelled()
    }

    #[cfg(test)]
    pub(crate) fn connection_cancel_token(&self) -> tokio_util::sync::CancellationToken {
        self.inner.slot.current().client.cancel_token.clone()
    }

    #[cfg(test)]
    pub(crate) fn connection_id(&self) -> u64 {
        self.inner.slot.current().id
    }

    #[cfg(test)]
    pub(crate) fn replace_connection_for_test(&self, client: AsyncManagedClient) {
        let mut state = self
            .inner
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.connection.superseded.cancel();
        state.generation = state.generation.wrapping_add(1);
        state.connection = Arc::new(SharedConnection::new(
            client,
            state.connection.startup_identity,
        ));
    }
}

#[cfg(test)]
impl From<AsyncManagedClient> for McpConnectionLease {
    fn from(client: AsyncManagedClient) -> Self {
        Self::new(ConnectionSlot::from_client(client))
    }
}

impl Drop for LeaseInner {
    fn drop(&mut self) {
        if !self.released.swap(true, Ordering::AcqRel) && self.slot.release() {
            self.slot.current().client.request_router.close();
            self.slot.current().client.cancel_token.cancel();
        }
    }
}

/// Reuses compatible MCP connections within one root agent and its descendants.
#[derive(Clone, Default)]
pub struct McpConnectionPool {
    entries: Arc<Mutex<Vec<PoolEntry>>>,
}

impl McpConnectionPool {
    pub(crate) async fn preferred_connection_is_reusable(
        &self,
        server_name: &str,
        identity: &McpConnectionIdentity,
        startup_identity: Option<McpConnectionStartupIdentity>,
    ) -> Option<bool> {
        let slot = {
            let entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entries
                .iter()
                .rev()
                .filter_map(|entry| entry.slot.upgrade())
                .find(|slot| {
                    slot.key.as_ref().is_some_and(|key| {
                        key.server_name == server_name
                            && key.identity.has_same_connection_config(identity)
                    })
                })
        }?;
        Some(
            slot.is_reusable_connection(identity, startup_identity)
                .await,
        )
    }

    #[cfg(test)]
    pub(crate) fn acquire(
        &self,
        identity: McpConnectionIdentity,
        mode: McpConnectionPoolMode,
        route: &Arc<McpSessionRoute>,
        create: impl Fn(McpConnectionRequestRouter) -> AsyncManagedClient + Send + Sync + 'static,
    ) -> McpConnectionLease {
        self.acquire_named("test".to_string(), identity, mode, route, create)
    }

    #[cfg(test)]
    pub(crate) fn acquire_named(
        &self,
        server_name: String,
        identity: McpConnectionIdentity,
        mode: McpConnectionPoolMode,
        route: &Arc<McpSessionRoute>,
        create: impl Fn(McpConnectionRequestRouter) -> AsyncManagedClient + Send + Sync + 'static,
    ) -> McpConnectionLease {
        self.acquire_named_with_startup_identity(
            server_name,
            identity,
            mode,
            /*startup_identity*/ None,
            route,
            create,
        )
    }

    pub(crate) fn acquire_named_with_startup_identity(
        &self,
        server_name: String,
        identity: McpConnectionIdentity,
        mode: McpConnectionPoolMode,
        startup_identity: Option<McpConnectionStartupIdentity>,
        route: &Arc<McpSessionRoute>,
        create: impl Fn(McpConnectionRequestRouter) -> AsyncManagedClient + Send + Sync + 'static,
    ) -> McpConnectionLease {
        let factory: ConnectionFactory = Arc::new(create);
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|entry| {
            entry
                .slot
                .upgrade()
                .is_some_and(|slot| slot.active_leases.load(Ordering::Acquire) > 0)
        });
        if let Some(slot) = entries
            .iter()
            .rev()
            .filter_map(|entry| entry.slot.upgrade())
            .find(|slot| {
                slot.key.as_ref().is_some_and(|key| {
                    key.server_name == server_name
                        && key.identity.has_same_connection_config(&identity)
                })
            })
            .filter(|slot| slot.try_acquire(route))
        {
            // Another session may have replaced the generation since the caller checked reuse.
            // The pool lock excludes competing acquisitions while we validate its startup inputs.
            let (current, current_identity) = {
                let state = slot
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (Arc::clone(&state.connection), state.identity.clone())
            };
            let incompatible_pending_startup =
                !current.client.startup_complete.load(Ordering::Acquire)
                    && (current_identity
                        .as_ref()
                        .is_some_and(|current| current != &identity)
                        || startup_identity.is_some_and(|desired_startup| {
                            desired_startup.protocol_mode.is_none()
                                || current.startup_identity != Some(desired_startup)
                                || current.client.cancel_token.is_cancelled()
                        }));
            let incompatible_identity = current_identity
                .as_ref()
                .is_some_and(|current| current != &identity)
                && identity.oauth_credentials().is_ok();
            if mode == McpConnectionPoolMode::Replace
                || incompatible_pending_startup
                || incompatible_identity
            {
                drop(slot.replace(identity, factory, startup_identity));
            } else if current_identity.as_ref() == Some(&identity) {
                let mut state = slot
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.identity.as_ref() == Some(&identity) {
                    state.identity = Some(identity);
                }
            }
            return McpConnectionLease::new(slot);
        }

        let slot = ConnectionSlot::new(
            McpConnectionKey {
                server_name,
                identity,
            },
            factory,
            startup_identity,
            route,
        );
        entries.push(PoolEntry {
            slot: Arc::downgrade(&slot),
        });
        McpConnectionLease::new(slot)
    }
}

#[cfg(test)]
#[path = "connection_pool_tests.rs"]
mod tests;
