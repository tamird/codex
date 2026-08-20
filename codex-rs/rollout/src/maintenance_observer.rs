//! Request-local ownership of observable maintenance transitions.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use crate::maintenance_status::RolloutMaintenanceRequestStatus;

/// A short, non-reentrant callback. Transition and callback delivery are serialized together.
pub type RolloutMaintenanceObserver = Arc<dyn Fn(RolloutMaintenanceRequestStatus) + Send + Sync>;

struct ObserverContext {
    callback: RolloutMaintenanceObserver,
    current: Mutex<Option<Weak<()>>>,
    root: Arc<()>,
}

impl ObserverContext {
    fn publish(&self, token: &Arc<()>, status: RolloutMaintenanceRequestStatus) {
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current.is_some() {
            *current = Some(Arc::downgrade(token));
            (self.callback)(status);
        }
    }
}

tokio::task_local! {
    static OBSERVER: Arc<ObserverContext>;
}

/// Observe only the supplied future. Ending or canceling it clears its request status.
pub async fn with_rollout_maintenance_observer<F: Future>(
    observer: RolloutMaintenanceObserver,
    future: F,
) -> F::Output {
    struct ClearOnDrop(Arc<ObserverContext>);
    impl Drop for ClearOnDrop {
        fn drop(&mut self) {
            let mut current = self
                .0
                .current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if current.take().is_some() {
                (self.0.callback)(RolloutMaintenanceRequestStatus::Idle);
            }
        }
    }
    let root = Arc::new(());
    let context = Arc::new(ObserverContext {
        callback: Arc::clone(&observer),
        current: Mutex::new(Some(Arc::downgrade(&root))),
        root,
    });
    let _clear = ClearOnDrop(Arc::clone(&context));
    OBSERVER.scope(context, future).await
}

/// Forward a storage transition to the current request, if it has an observer.
pub fn report_rollout_maintenance_status(status: RolloutMaintenanceRequestStatus) {
    let _ = OBSERVER.try_with(|context| context.publish(&context.root, status));
}

struct Transition {
    context: Arc<ObserverContext>,
    token: Arc<()>,
}

impl Drop for Transition {
    fn drop(&mut self) {
        let mut current = self
            .context
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current
            .as_ref()
            .is_some_and(|current| current.ptr_eq(&Arc::downgrade(&self.token)))
        {
            *current = Some(Weak::new());
            (self.context.callback)(RolloutMaintenanceRequestStatus::Idle);
        }
    }
}

/// Clears a wait or activity on drop, only if it still owns the displayed transition.
#[derive(Clone, Default)]
pub struct RolloutMaintenanceRequestScope(Option<Arc<Transition>>);

impl RolloutMaintenanceRequestScope {
    pub fn new(status: RolloutMaintenanceRequestStatus) -> Self {
        let scope = Self(
            OBSERVER
                .try_with(|context| {
                    Arc::new(Transition {
                        context: Arc::clone(context),
                        token: Arc::new(()),
                    })
                })
                .ok(),
        );
        scope.update(status);
        scope
    }

    pub fn update(&self, status: RolloutMaintenanceRequestStatus) {
        if let Some(transition) = &self.0 {
            transition.context.publish(&transition.token, status);
        }
    }
}
