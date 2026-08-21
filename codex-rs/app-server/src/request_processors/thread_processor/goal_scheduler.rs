use super::*;
use codex_state::ActiveGoalSupervisorSchedule;

const SCHEDULER_MATERIALIZED_RELEASE_POLL_INTERVAL: Duration = Duration::from_secs(1);

impl ThreadRequestProcessor {
    pub(crate) async fn activate_goal_supervisor_schedule(
        &self,
        schedule: ActiveGoalSupervisorSchedule,
    ) -> Result<(), String> {
        self.activate_goal_supervisor_schedule_inner(schedule)
            .await
            .map_err(|err| err.message)
    }

    async fn activate_goal_supervisor_schedule_inner(
        &self,
        schedule: ActiveGoalSupervisorSchedule,
    ) -> Result<(), JSONRPCErrorError> {
        let thread_id = schedule.thread_id;
        let _thread_list_state_permit = self
            .acquire_thread_resume_permit(&ThreadResumeParams {
                thread_id: thread_id.to_string(),
                ..Default::default()
            })
            .await?;
        if self
            .pending_thread_unloads
            .lock()
            .await
            .contains(&thread_id)
        {
            return Err(invalid_request(format!(
                "thread {thread_id} is closing; retry goal supervisor activation after the thread is closed"
            )));
        }

        if let Ok(observed_thread) = self.thread_manager.get_thread(thread_id).await
            && let Ok(thread) = self.thread_manager.get_thread(thread_id).await
            && Arc::ptr_eq(&observed_thread, &thread)
        {
            self.ensure_background_listener_for_thread(thread_id, Arc::clone(&thread))
                .await?;
            thread
                .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
                .await;
            return Ok(());
        }

        let thread_id_string = thread_id.to_string();
        let stored_thread = self
            .read_stored_thread_for_resume(
                thread_id_string.as_str(),
                /*path*/ None,
                /*include_history*/ false,
            )
            .await?;
        let (thread_history, _stored_thread) = self
            .load_resume_initial_history_from_stored_thread(stored_thread)
            .await?;
        let history_cwd = thread_history.session_cwd();
        let mut request_overrides = None;
        let mut typesafe_overrides = ConfigOverrides {
            codex_linux_sandbox_exe: self.arg0_paths.codex_linux_sandbox_exe.clone(),
            main_execve_wrapper_exe: self.arg0_paths.main_execve_wrapper_exe.clone(),
            ..Default::default()
        };
        self.load_and_apply_persisted_resume_metadata(
            &thread_history,
            &mut request_overrides,
            &mut typesafe_overrides,
        )
        .await;
        let config = self
            .config_manager
            .load_for_cwd(request_overrides, typesafe_overrides, history_cwd)
            .await
            .map_err(|err| config_load_error(&err))?;

        let new_thread = self
            .thread_manager
            .resume_thread_with_history(
                config,
                thread_history,
                self.auth_manager.clone(),
                /*parent_trace*/ None,
                Default::default(),
            )
            .await
            .map_err(|err| internal_error(format!("error resuming thread: {err}")))?;
        let NewThread {
            thread_id: resumed_thread_id,
            thread,
            ..
        } = new_thread;
        if resumed_thread_id != thread_id {
            return Err(internal_error(format!(
                "goal scheduler resumed thread {resumed_thread_id} for expected thread {thread_id}"
            )));
        }

        self.ensure_background_listener_for_thread(thread_id, Arc::clone(&thread))
            .await?;

        thread
            .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
            .await;
        let processor = self.clone();
        tokio::spawn(async move {
            processor
                .release_scheduler_materialized_thread_after_terminal_action(schedule, thread)
                .await;
        });
        Ok(())
    }

    async fn ensure_background_listener_for_thread(
        &self,
        thread_id: ThreadId,
        thread: Arc<CodexThread>,
    ) -> Result<(), JSONRPCErrorError> {
        let thread_id_string = thread_id.to_string();
        self.thread_watch_manager
            .upsert_thread(&thread_id_string)
            .await;
        let thread_state = self.thread_state_manager.thread_state(thread_id).await;
        self.ensure_listener_task_running(thread_id, thread, thread_state)
            .await
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "subscriber attachment and scheduler-owned unload must be serialized"
    )]
    async fn release_scheduler_materialized_thread_after_terminal_action(
        &self,
        mut activated_schedule: ActiveGoalSupervisorSchedule,
        thread: Arc<CodexThread>,
    ) {
        let Some(state_db) = self.state_db.as_ref() else {
            return;
        };
        loop {
            tokio::time::sleep(SCHEDULER_MATERIALIZED_RELEASE_POLL_INTERVAL).await;
            if !self
                .thread_state_manager
                .subscribed_connection_ids(activated_schedule.thread_id)
                .await
                .is_empty()
            {
                return;
            }
            let Ok(loaded_thread) = self
                .thread_manager
                .get_thread(activated_schedule.thread_id)
                .await
            else {
                return;
            };
            if !Arc::ptr_eq(&loaded_thread, &thread) {
                return;
            }

            let current_schedule = match state_db
                .thread_goals()
                .get_active_goal_supervisor_schedule(activated_schedule.thread_id)
                .await
            {
                Ok(current_schedule) => current_schedule,
                Err(err) => {
                    warn!(
                        thread_id = %activated_schedule.thread_id,
                        "failed to inspect scheduler-materialized goal before release: {err}"
                    );
                    return;
                }
            };
            let terminal_action_persisted = match current_schedule {
                None => true,
                Some(current_schedule)
                    if current_schedule.goal_id != activated_schedule.goal_id =>
                {
                    activated_schedule = current_schedule;
                    false
                }
                Some(current_schedule) => {
                    current_schedule.snoozed_until_ms.is_some()
                        && current_schedule != activated_schedule
                }
            };
            if !terminal_action_persisted
                || thread.has_running_goal_supervisor().await
                || matches!(thread.agent_status().await, AgentStatus::Running)
            {
                continue;
            }

            {
                let mut pending_thread_unloads = self.pending_thread_unloads.lock().await;
                if !self
                    .thread_state_manager
                    .subscribed_connection_ids(activated_schedule.thread_id)
                    .await
                    .is_empty()
                    || pending_thread_unloads.contains(&activated_schedule.thread_id)
                {
                    return;
                }
                pending_thread_unloads.insert(activated_schedule.thread_id);
            }
            super::super::thread_lifecycle::unload_thread_without_subscribers(
                Arc::clone(&self.thread_manager),
                Arc::clone(&self.outgoing),
                Arc::clone(&self.pending_thread_unloads),
                self.thread_state_manager.clone(),
                self.thread_watch_manager.clone(),
                activated_schedule.thread_id,
                thread,
            )
            .await;
            return;
        }
    }
}
