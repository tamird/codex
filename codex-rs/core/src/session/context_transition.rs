//! Publishes rebuilt contexts only to the task and settings that prepared them.

use std::collections::HashSet;
use std::sync::Arc;

use codex_models_manager::ModelRoutingCandidate;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::GitEnrichmentPolicy;
use super::model_routing::ModelRoutingOwner;
use super::session::Session;
use super::step_settings::ResolvedStepSettings;
use super::turn_context::TurnContext;
use crate::state::RunningTask;

pub(super) struct ContextTransitionTarget {
    done: Arc<Notify>,
    context: Arc<TurnContext>,
    settings: Arc<ResolvedStepSettings>,
    cancellation_token: CancellationToken,
}

#[derive(Debug)]
pub(super) enum ContextTransitionError {
    SettingsChanged(Arc<ResolvedStepSettings>),
    Unavailable,
}

enum ContextTransitionKind {
    Workspace,
    Routing,
}

impl ContextTransitionTarget {
    pub(super) fn check(&self, task: Option<&RunningTask>) -> Result<(), ContextTransitionError> {
        let task = task.ok_or(ContextTransitionError::Unavailable)?;
        if self.cancellation_token.is_cancelled()
            || task.cancellation_token.is_cancelled()
            || !Arc::ptr_eq(&task.done, &self.done)
            || !Arc::ptr_eq(&task.turn_context, &self.context)
        {
            return Err(ContextTransitionError::Unavailable);
        }
        let settings = task.turn_context.current_settings.load_full();
        if !Arc::ptr_eq(&settings, &self.settings) {
            return Err(ContextTransitionError::SettingsChanged(settings));
        }
        Ok(())
    }
}

impl Session {
    pub(crate) async fn active_task_context(&self, done: &Arc<Notify>) -> Option<Arc<TurnContext>> {
        let active = self.active_turn.lock().await;
        let task = active.as_ref()?.task.as_ref()?;
        (Arc::ptr_eq(&task.done, done) && !task.cancellation_token.is_cancelled())
            .then(|| Arc::clone(&task.turn_context))
    }

    pub(super) async fn capture_context_transition(
        &self,
        context: &Arc<TurnContext>,
        done: &Arc<Notify>,
        cancellation_token: &CancellationToken,
    ) -> Option<ContextTransitionTarget> {
        let active = self.active_turn.lock().await;
        let task = active.as_ref()?.task.as_ref()?;
        if cancellation_token.is_cancelled()
            || task.cancellation_token.is_cancelled()
            || !Arc::ptr_eq(&task.done, done)
            || !Arc::ptr_eq(&task.turn_context, context)
        {
            return None;
        }
        Some(ContextTransitionTarget {
            done: Arc::clone(&task.done),
            context: Arc::clone(context),
            settings: context.current_settings.load_full(),
            cancellation_token: cancellation_token.clone(),
        })
    }

    async fn publish_context_transition(
        &self,
        target: &ContextTransitionTarget,
        mut context: TurnContext,
        kind: ContextTransitionKind,
    ) -> Result<Arc<TurnContext>, ContextTransitionError> {
        let mut active = self.active_turn.lock().await;
        let task = active
            .as_mut()
            .and_then(|turn| turn.task.as_mut())
            .ok_or(ContextTransitionError::Unavailable)?;
        target.check(Some(task))?;
        if matches!(kind, ContextTransitionKind::Workspace) {
            context.multi_agent_version =
                self.resolve_multi_agent_version_for_model(context.model_info(), &context.config);
        }
        self.services
            .thread_extension_data
            .insert(context.model_info().as_ref().clone());
        let context = Arc::new(context);
        task.turn_context = Arc::clone(&context);
        if matches!(kind, ContextTransitionKind::Workspace)
            && self.git_enrichment_policy == GitEnrichmentPolicy::Fresh
            && context
                .environments
                .single_local_environment_cwd()
                .is_some()
        {
            context.turn_metadata_state.spawn_git_enrichment_task();
        }
        Ok(context)
    }

    pub(super) async fn refresh_active_turn_context(
        &self,
        current: &Arc<TurnContext>,
        done: &Arc<Notify>,
        cancellation_token: &CancellationToken,
    ) -> CodexResult<Arc<TurnContext>> {
        let mut target = self
            .capture_context_transition(current, done, cancellation_token)
            .await
            .ok_or(CodexErr::TurnAborted)?;
        loop {
            let prepared = self
                .prepare_workspace_turn_context(current, Arc::clone(&target.settings))
                .await;
            match self
                .publish_context_transition(&target, prepared, ContextTransitionKind::Workspace)
                .await
            {
                Ok(context) => return Ok(context),
                Err(ContextTransitionError::SettingsChanged(settings)) => {
                    target.settings = settings
                }
                Err(ContextTransitionError::Unavailable) => return Err(CodexErr::TurnAborted),
            }
        }
    }

    pub(super) async fn reroute_active_turn_context(
        &self,
        current: &Arc<TurnContext>,
        done: &Arc<Notify>,
        profile_name: &str,
        attempted: &HashSet<ModelRoutingCandidate>,
        cancellation_token: &CancellationToken,
    ) -> CodexResult<Option<Arc<TurnContext>>> {
        let mut target = self
            .capture_context_transition(current, done, cancellation_token)
            .await
            .ok_or(CodexErr::TurnAborted)?;
        loop {
            let selection = self
                .select_model_routing_context(
                    current,
                    profile_name,
                    attempted,
                    ModelRoutingOwner::Active(&target),
                )
                .await;
            let result = if let Some(selection) = selection {
                if !self
                    .wait_for_model_routing_retry(selection.retry_at, cancellation_token)
                    .await
                {
                    return Err(CodexErr::TurnAborted);
                }
                let mut context = selection.context;
                context.model_routing_previous_candidate = None;
                context.model_routing_selection_reason = None;
                self.publish_context_transition(&target, context, ContextTransitionKind::Routing)
                    .await
                    .map(Some)
            } else {
                let active = self.active_turn.lock().await;
                target
                    .check(active.as_ref().and_then(|turn| turn.task.as_ref()))
                    .map(|()| None)
            };
            match result {
                Ok(context) => return Ok(context),
                Err(ContextTransitionError::SettingsChanged(settings)) => {
                    target.settings = settings
                }
                Err(ContextTransitionError::Unavailable) => return Err(CodexErr::TurnAborted),
            }
        }
    }
}

#[cfg(test)]
#[path = "context_transition_tests.rs"]
mod tests;
