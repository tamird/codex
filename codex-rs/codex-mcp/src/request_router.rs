//! Routes requests made through one shared MCP connection back to the initiating session.

use std::collections::VecDeque;
#[cfg(test)]
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::Weak;

use crate::elicitation::ElicitationRequestManager;
use crate::elicitation::SendEvent;
use anyhow::anyhow;
use async_channel::Sender;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::McpStartupStatus;
use codex_protocol::protocol::McpStartupUpdateEvent;
use codex_rmcp_client::SendElicitation;
use futures::FutureExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Session-specific state that must not be owned by a shared MCP connection.
pub(crate) struct McpSessionRoute {
    submit_id: String,
    elicitation_requests: ElicitationRequestManager,
    tx_event: Option<Sender<Event>>,
    closed: CancellationToken,
    event_dispatch: StdMutex<()>,
    #[cfg(test)]
    before_event_dispatch: StdMutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl McpSessionRoute {
    pub(crate) fn new(
        submit_id: String,
        elicitation_requests: ElicitationRequestManager,
        tx_event: Option<Sender<Event>>,
    ) -> Self {
        Self {
            submit_id,
            elicitation_requests,
            tx_event,
            closed: CancellationToken::new(),
            event_dispatch: StdMutex::new(()),
            #[cfg(test)]
            before_event_dispatch: StdMutex::new(None),
        }
    }

    pub(crate) fn close(&self) {
        // Event enqueueing and closure share this lock. An event either linearizes before close(),
        // or observes the closed route and is rejected; it cannot be enqueued after close returns.
        let _event_dispatch = self
            .event_dispatch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.closed.cancel();
    }

    pub(crate) async fn closed(&self) {
        self.closed.cancelled().await;
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
    }

    fn dispatch_event(&self, event: Event) -> anyhow::Result<()> {
        let _event_dispatch = self
            .event_dispatch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(test)]
        if let Some(hook) = self
            .before_event_dispatch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            hook();
        }
        if self.is_closed() {
            return Err(anyhow!("MCP session route closed"));
        }
        let Some(tx_event) = self.tx_event.as_ref() else {
            return Err(anyhow!("MCP session route has no event channel"));
        };
        tx_event
            .try_send(event)
            .map_err(|error| anyhow!("failed to deliver MCP event: {error}"))
    }

    #[cfg(test)]
    fn set_before_event_dispatch(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *self
            .before_event_dispatch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = hook;
    }
}

#[derive(Default)]
struct RouteState {
    closed: bool,
    active: Option<ActiveRouteState>,
    live: Vec<Weak<McpSessionRoute>>,
    waiters: VecDeque<RouteWaiterState>,
    next_waiter_id: u64,
}

struct ActiveRouteState {
    route: Arc<McpSessionRoute>,
    operations: usize,
}

struct RouteWaiterState {
    id: u64,
    route: Weak<McpSessionRoute>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum McpRouteAcquireError {
    #[error("MCP session route closed")]
    RouteClosed,
    #[error("shared MCP connection closed")]
    ConnectionClosed,
}

/// Serializes operations for which MCP cannot identify the initiating client request.
///
/// MCP defines elicitation as nested inside another MCP interaction, but server-to-client requests
/// do not carry a required parent request identifier. While an operation is active, its route is
/// therefore authoritative. With multiple live routes, requests outside an active operation fail
/// closed rather than borrowing another session's approval or permission policy.
#[derive(Clone, Default)]
pub(crate) struct McpConnectionRequestRouter {
    state: Arc<StdMutex<RouteState>>,
    route_available: Arc<Notify>,
    closed: CancellationToken,
}

impl McpConnectionRequestRouter {
    pub(crate) fn register(&self, route: &Arc<McpSessionRoute>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || route.is_closed() {
            return;
        }
        state.live.retain(|existing| existing.upgrade().is_some());
        if !state
            .live
            .iter()
            .filter_map(Weak::upgrade)
            .any(|existing| Arc::ptr_eq(&existing, route))
        {
            state.live.push(Arc::downgrade(route));
        }
    }

    pub(crate) fn unregister(&self, route: &Arc<McpSessionRoute>) {
        route.close();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.live.retain(|existing| {
            existing
                .upgrade()
                .is_some_and(|existing| !Arc::ptr_eq(&existing, route))
        });
        state.waiters.retain(|waiter| {
            waiter
                .route
                .upgrade()
                .is_some_and(|waiting| !Arc::ptr_eq(&waiting, route))
        });
        drop(state);
        self.route_available.notify_waiters();
    }

    pub(crate) fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        state.live.clear();
        state.waiters.clear();
        drop(state);
        self.closed.cancel();
        self.route_available.notify_waiters();
    }

    pub(crate) fn make_sender(&self, server_name: String) -> SendElicitation {
        let router = self.clone();
        Box::new(move |id, elicitation| {
            let router = router.clone();
            let server_name = server_name.clone();
            async move {
                let route = router
                    .current_route()
                    .ok_or_else(|| anyhow!("no live session can handle MCP elicitation"))?;
                let send_event: Option<SendEvent> = route.tx_event.as_ref().map(|_| {
                    let route = Arc::clone(&route);
                    let router = router.clone();
                    Arc::new(move |event| {
                        let result = router.dispatch_event(&route, event);
                        async move { result }.boxed()
                    }) as SendEvent
                });
                let sender = route
                    .elicitation_requests
                    .make_sender_with_event_dispatch(server_name, send_event);
                tokio::select! {
                    biased;
                    () = route.closed() => Err(anyhow!("MCP session route closed")),
                    result = sender(id, elicitation) => result,
                }
            }
            .boxed()
        })
    }

    /// Reports connection-level recovery to every session that currently uses the connection.
    pub(crate) async fn emit_startup_ready(&self, server_name: String) {
        for route in self.live_routes() {
            let event = Event {
                id: route.submit_id.clone(),
                msg: EventMsg::McpStartupUpdate(McpStartupUpdateEvent {
                    server: server_name.clone(),
                    status: McpStartupStatus::Ready,
                }),
            };
            let _ = self.dispatch_event(&route, event);
        }
    }

    fn dispatch_event(&self, route: &Arc<McpSessionRoute>, event: Event) -> anyhow::Result<()> {
        // Keep the generation alive through the synchronous event enqueue. This lock makes a
        // callback selected before close() either finish before close(), or observe the closed
        // generation and fail without borrowing the replacement generation's live route.
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(anyhow!("shared MCP connection closed"));
        }
        route.dispatch_event(event)
    }

    /// Acquires attribution before an MCP request starts.
    ///
    /// Waiting remains in the caller's task, so cancelling a queued caller removes its waiter and
    /// cannot start side effects later. Once this returns, the caller may detach only the started
    /// protocol request while retaining the returned guard.
    pub(crate) async fn acquire(
        &self,
        route: Arc<McpSessionRoute>,
    ) -> std::result::Result<ActiveRoute, McpRouteAcquireError> {
        let mut waiter = None;
        loop {
            if route.is_closed() {
                return Err(McpRouteAcquireError::RouteClosed);
            }

            let route_available = self.route_available.notified();
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.closed {
                    return Err(McpRouteAcquireError::ConnectionClosed);
                }
                state.live.retain(|live| live.upgrade().is_some());
                if !state
                    .live
                    .iter()
                    .filter_map(Weak::upgrade)
                    .any(|live| Arc::ptr_eq(&live, &route))
                {
                    return Err(McpRouteAcquireError::RouteClosed);
                }
                state
                    .waiters
                    .retain(|waiting| waiting.route.upgrade().is_some());

                let waiter_is_first = waiter.is_some_and(|id| {
                    state
                        .waiters
                        .front()
                        .is_some_and(|waiting| waiting.id == id)
                });
                let can_start = match state.active.as_ref() {
                    None => state.waiters.is_empty() || waiter_is_first,
                    Some(active) if Arc::ptr_eq(&active.route, &route) => {
                        state.waiters.is_empty() || waiter_is_first
                    }
                    Some(_) => false,
                };
                if can_start {
                    if waiter_is_first {
                        state.waiters.pop_front();
                    }
                    match state.active.as_mut() {
                        Some(active) => active.operations += 1,
                        None => {
                            state.active = Some(ActiveRouteState {
                                route: Arc::clone(&route),
                                operations: 1,
                            });
                        }
                    }
                    return Ok(ActiveRoute {
                        route,
                        state: Arc::clone(&self.state),
                        route_available: Arc::clone(&self.route_available),
                    });
                }

                if waiter.is_none() {
                    let id = state.next_waiter_id;
                    state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
                    state.waiters.push_back(RouteWaiterState {
                        id,
                        route: Arc::downgrade(&route),
                    });
                    waiter = Some(id);
                }
            }

            let Some(waiter_id) = waiter else {
                continue;
            };
            let mut waiter_guard = WaitingRoute {
                id: waiter_id,
                state: Arc::clone(&self.state),
                route_available: Arc::clone(&self.route_available),
                armed: true,
            };
            tokio::select! {
                () = route_available => {}
                () = route.closed() => return Err(McpRouteAcquireError::RouteClosed),
                () = self.closed.cancelled() => {
                    return Err(McpRouteAcquireError::ConnectionClosed);
                }
            }
            waiter_guard.armed = false;
        }
    }

    fn current_route(&self) -> Option<Arc<McpSessionRoute>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return None;
        }
        if let Some(active) = state.active.as_ref() {
            return (!active.route.is_closed()).then(|| Arc::clone(&active.route));
        }
        state
            .live
            .retain(|route| route.upgrade().is_some_and(|route| !route.is_closed()));
        let mut live_routes = state.live.iter().filter_map(Weak::upgrade);
        let route = live_routes.next()?;
        live_routes.next().is_none().then_some(route)
    }

    fn live_routes(&self) -> Vec<Arc<McpSessionRoute>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Vec::new();
        }
        state
            .live
            .retain(|route| route.upgrade().is_some_and(|route| !route.is_closed()));
        state.live.iter().filter_map(Weak::upgrade).collect()
    }

    #[cfg(test)]
    pub(crate) async fn run<T, F>(
        &self,
        route: Arc<McpSessionRoute>,
        operation: F,
    ) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let active_route = self.acquire(route).await?;
        tokio::spawn(async move {
            let _active_route = active_route;
            operation.await
        })
        .await
        .map_err(|error| anyhow!("shared MCP operation task failed: {error}"))
    }
}

pub(crate) struct ActiveRoute {
    route: Arc<McpSessionRoute>,
    state: Arc<StdMutex<RouteState>>,
    route_available: Arc<Notify>,
}

impl Drop for ActiveRoute {
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(active) = state.active.as_mut() else {
            return;
        };
        if !Arc::ptr_eq(&active.route, &self.route) {
            return;
        }
        active.operations -= 1;
        if active.operations == 0 {
            state.active = None;
            drop(state);
            self.route_available.notify_waiters();
        }
    }
}

struct WaitingRoute {
    id: u64,
    state: Arc<StdMutex<RouteState>>,
    route_available: Arc<Notify>,
    armed: bool,
}

impl Drop for WaitingRoute {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.waiters.retain(|waiter| waiter.id != self.id);
        drop(state);
        self.route_available.notify_waiters();
    }
}

#[cfg(test)]
#[path = "request_router_tests.rs"]
mod tests;
