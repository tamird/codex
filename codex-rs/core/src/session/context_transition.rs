//! Publishes rebuilt contexts only to the task and settings that prepared them.

use std::sync::Arc;

use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::GitEnrichmentPolicy;
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
    ) -> Result<Arc<TurnContext>, ContextTransitionError> {
        let mut active = self.active_turn.lock().await;
        let task = active
            .as_mut()
            .and_then(|turn| turn.task.as_mut())
            .ok_or(ContextTransitionError::Unavailable)?;
        target.check(Some(task))?;
        context.multi_agent_version =
            self.resolve_multi_agent_version_for_model(context.model_info(), &context.config);
        self.services
            .thread_extension_data
            .insert(context.model_info().as_ref().clone());
        let context = Arc::new(context);
        task.turn_context = Arc::clone(&context);
        if self.git_enrichment_policy == GitEnrichmentPolicy::Fresh
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
            match self.publish_context_transition(&target, prepared).await {
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
