//! Nonblocking agent-picker discovery, rendering, and refresh.

use super::*;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadSourceKind;
use codex_app_server_protocol::ThreadStatus;
use codex_protocol::protocol::SubAgentSource;
use std::collections::HashSet;

pub(super) const AGENT_PICKER_VIEW_ID: &str = "agent-picker";
const AGENT_PICKER_PAGE_SIZE: u32 = 100;
const AGENT_PICKER_MAX_THREADS: usize = 1_000;
const AGENT_PICKER_MAX_SCANNED_THREADS: usize = 10_000;
pub(super) const AGENT_PICKER_MAX_SCAN_DURATION: Duration = Duration::from_secs(5);

impl App {
    pub(super) fn refresh_agent_picker_threads(
        &mut self,
        server: &AppServerSession,
        root: ThreadId,
    ) {
        let Some(generation) = self.agent_navigation.begin_picker_refresh(root) else {
            if self.primary_thread_id == Some(root)
                && (self.agent_navigation.is_timed_out_picker_refresh(root)
                    || self.agent_navigation.picker_refreshes_at_capacity()
                        && !self.agent_navigation.has_picker_refresh(root))
                && !self.config.features.enabled(Feature::Collab)
                && !self.agent_navigation.has_non_primary_thread(Some(root))
            {
                self.chat_widget
                    .replace_selection_view_with_multi_agent_enable_prompt(AGENT_PICKER_VIEW_ID);
            }
            return;
        };
        let known_at_start: HashSet<ThreadId> = self
            .agent_navigation
            .tracked_thread_ids()
            .into_iter()
            .collect();
        let embedded = server.uses_embedded_app_server();
        let handle = server.request_handle();
        let events = self.app_event_tx.clone();
        let session_telemetry = self.session_telemetry.clone();
        let live: HashSet<_> = self
            .thread_event_channels
            .iter()
            .filter_map(|(&id, channel)| {
                (channel.attachment() == ThreadEventAttachment::Live).then_some(id)
            })
            .collect();
        let closed: HashSet<_> = self
            .agent_navigation
            .ordered_threads()
            .into_iter()
            .filter_map(|(id, entry)| entry.is_closed.then_some(id))
            .collect();
        tokio::spawn(async move {
            let started_at = Instant::now();
            let mut exhaustive = false;
            let mut timed_out = false;
            let mut page_count = 0;
            let mut scanned = 0;
            let mut accepted = 0;
            let result = async {
                let mut threads = Vec::new();
                let mut cursor = None;
                let mut seen_cursors = HashSet::new();
                let mut reachable = HashSet::from([root]);
                let mut children_by_parent = HashMap::<ThreadId, Vec<(ThreadId, Thread)>>::new();
                let mut parents = VecDeque::new();
                let mut legacy = false;
                let deadline = tokio::time::Instant::now() + AGENT_PICKER_MAX_SCAN_DURATION;
                loop {
                    if !seen_cursors.insert((legacy, cursor.clone())) {
                        break;
                    }
                    page_count += 1;
                    let request = handle.request_typed(ClientRequest::ThreadList {
                        request_id: RequestId::String(Uuid::new_v4().to_string()),
                        params: ThreadListParams {
                            cursor,
                            limit: Some(AGENT_PICKER_PAGE_SIZE),
                            sort_key: None,
                            sort_direction: Some(SortDirection::Asc),
                            model_providers: Some(vec![]),
                            source_kinds: Some(vec![ThreadSourceKind::SubAgentThreadSpawn]),
                            archived: None,
                            section_id: None,
                            project_id: None,
                            cwd: None,
                            use_state_db_only: !legacy,
                            search_term: None,
                            parent_thread_id: None,
                            ancestor_thread_id: (!legacy).then(|| root.to_string()),
                        },
                    });
                    tokio::pin!(request);
                    let response = tokio::select! {
                        response = &mut request => response,
                        _ = tokio::time::sleep_until(deadline), if !timed_out => {
                            timed_out = true;
                            session_telemetry.counter(
                                "codex.tui.agent_picker.timeout",
                                /*inc*/ 1,
                                &[],
                            );
                            tracing::debug!(
                                target: "codex.performance",
                                thread_id = %root,
                                pages = page_count,
                                scanned_threads = scanned,
                                accepted_threads = accepted,
                                duration_us = started_at.elapsed().as_micros(),
                                "TUI agent picker refresh timed out"
                            );
                            events.send(AppEvent::AgentPickerThreadsLoaded {
                                primary_thread_id: root,
                                generation,
                                refresh: AgentPickerRefresh::TimedOut {
                                    known_at_start: known_at_start.clone(),
                                    threads: threads.clone(),
                                },
                            });
                            request.await
                        }
                    };
                    let page: ThreadListResponse = match response {
                        Ok(page) => page,
                        Err(err) if threads.is_empty() => return Err(err.to_string()),
                        Err(err) => {
                            tracing::warn!(%err, "failed to refresh agent picker descendants");
                            break;
                        }
                    };
                    for thread in page
                        .data
                        .into_iter()
                        .take(AGENT_PICKER_MAX_SCANNED_THREADS - scanned)
                    {
                        scanned += 1;
                        let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                            parent_thread_id,
                            ..
                        }) = &thread.source
                        else {
                            continue;
                        };
                        let Ok(parent) = thread
                            .parent_thread_id
                            .as_deref()
                            .map_or(Ok(*parent_thread_id), ThreadId::from_string)
                        else {
                            continue;
                        };
                        if parent != *parent_thread_id {
                            continue;
                        }
                        let Ok(id) = ThreadId::from_string(&thread.id) else {
                            continue;
                        };
                        if reachable.contains(&parent) {
                            parents.push_back(parent);
                        }
                        children_by_parent
                            .entry(parent)
                            .or_default()
                            .push((id, thread));
                    }
                    while let Some(parent) = parents.pop_front() {
                        for (id, thread) in children_by_parent.remove(&parent).unwrap_or_default() {
                            if !reachable.insert(id) {
                                continue;
                            }
                            if !closed.contains(&id)
                                && (!matches!(thread.status, ThreadStatus::NotLoaded)
                                    || live.contains(&id))
                            {
                                accepted += 1;
                            }
                            parents.push_back(id);
                            threads.push(thread);
                        }
                    }
                    if page.next_cursor.is_none()
                        && !legacy
                        && !timed_out
                        && tokio::time::Instant::now() < deadline
                        && scanned < AGENT_PICKER_MAX_SCANNED_THREADS
                        && accepted < AGENT_PICKER_MAX_THREADS
                    {
                        legacy = true;
                        cursor = None;
                        continue;
                    }
                    if page.next_cursor.is_none()
                        || accepted >= AGENT_PICKER_MAX_THREADS
                        || scanned == AGENT_PICKER_MAX_SCANNED_THREADS
                        || tokio::time::Instant::now() >= deadline
                    {
                        exhaustive = page.next_cursor.is_none()
                            && scanned < AGENT_PICKER_MAX_SCANNED_THREADS
                            && accepted < AGENT_PICKER_MAX_THREADS
                            && legacy
                            && (!threads.is_empty() || embedded);
                        break;
                    }
                    cursor = page.next_cursor;
                }
                Ok::<_, String>(threads)
            }
            .await;

            let duration = started_at.elapsed();
            let outcome = if timed_out {
                "timed_out"
            } else if result.is_err() {
                "error"
            } else {
                "complete"
            };
            session_telemetry.record_duration(
                "codex.tui.agent_picker.duration_ms",
                duration,
                &[("outcome", outcome)],
            );
            if duration >= tui::TARGET_FRAME_INTERVAL {
                tracing::debug!(
                    target: "codex.performance",
                    thread_id = %root,
                    pages = page_count,
                    scanned_threads = scanned,
                    accepted_threads = accepted,
                    timed_out,
                    duration_us = duration.as_micros(),
                    "slow TUI agent picker refresh"
                );
            }

            events.send(AppEvent::AgentPickerThreadsLoaded {
                primary_thread_id: root,
                generation,
                refresh: AgentPickerRefresh::Completed {
                    known_at_start,
                    exhaustive,
                    result,
                },
            });
        });
    }

    pub(super) fn apply_agent_picker_thread_refresh(
        &mut self,
        app_server: &AppServerSession,
        root: ThreadId,
        generation: u64,
        refresh: AgentPickerRefresh,
    ) {
        let (known_at_start, exhaustive, result, was_timed_out) = match refresh {
            AgentPickerRefresh::TimedOut {
                known_at_start,
                threads,
            } => {
                let accepted = self
                    .agent_navigation
                    .mark_picker_refresh_timed_out(root, generation);
                if accepted && self.primary_thread_id == Some(root) {
                    let selected = self
                        .chat_widget
                        .selected_item_description_for_present_view(AGENT_PICKER_VIEW_ID)
                        .and_then(|description| ThreadId::from_string(description).ok());
                    if self
                        .agent_navigation
                        .is_current_picker_refresh_epoch(root, generation)
                    {
                        self.register_agent_picker_threads(
                            root,
                            known_at_start,
                            threads,
                            /*exhaustive*/ false,
                        );
                    }
                    if !self.config.features.enabled(Feature::Collab)
                        && !self.agent_navigation.has_non_primary_thread(Some(root))
                    {
                        self.chat_widget
                            .replace_selection_view_with_multi_agent_enable_prompt(
                                AGENT_PICKER_VIEW_ID,
                            );
                    } else {
                        let params = self.agent_picker_selection_view_params(selected);
                        self.chat_widget
                            .replace_selection_view_if_present(AGENT_PICKER_VIEW_ID, params);
                    }
                }
                return;
            }
            AgentPickerRefresh::Completed {
                known_at_start,
                exhaustive,
                result,
            } => {
                let was_timed_out = self.agent_navigation.is_timed_out_picker_refresh(root);
                let Some(is_current_epoch) = self
                    .agent_navigation
                    .finish_picker_refresh(root, generation)
                else {
                    return;
                };
                if self.primary_thread_id != Some(root) {
                    if let Some(active_root) = self.primary_thread_id
                        && self
                            .chat_widget
                            .selected_index_for_present_view(AGENT_PICKER_VIEW_ID)
                            .is_some()
                    {
                        self.refresh_agent_picker_threads(app_server, active_root);
                    }
                    return;
                }
                if !is_current_epoch {
                    if self
                        .chat_widget
                        .selected_index_for_present_view(AGENT_PICKER_VIEW_ID)
                        .is_some()
                    {
                        self.refresh_agent_picker_threads(app_server, root);
                    }
                    return;
                }
                (known_at_start, exhaustive, result, was_timed_out)
            }
        };
        let selected = self
            .chat_widget
            .selected_item_description_for_present_view(AGENT_PICKER_VIEW_ID)
            .and_then(|description| ThreadId::from_string(description).ok());
        match result {
            Ok(threads) => {
                self.register_agent_picker_threads(root, known_at_start, threads, exhaustive);
            }
            Err(err) => {
                tracing::warn!(%err, "failed to refresh agent picker descendants");
            }
        }
        if !self.config.features.enabled(Feature::Collab)
            && !self.agent_navigation.has_non_primary_thread(Some(root))
        {
            if was_timed_out {
                return;
            }
            self.chat_widget
                .replace_selection_view_with_multi_agent_enable_prompt(AGENT_PICKER_VIEW_ID);
            return;
        }
        let params = self.agent_picker_selection_view_params(selected);
        self.chat_widget
            .replace_selection_view_if_present(AGENT_PICKER_VIEW_ID, params);
    }

    fn register_agent_picker_threads(
        &mut self,
        root: ThreadId,
        known_at_start: HashSet<ThreadId>,
        threads: Vec<Thread>,
        exhaustive: bool,
    ) {
        let mut accepted = 0;
        let mut seen = HashSet::new();
        for thread in threads {
            let Ok(id) = ThreadId::from_string(&thread.id) else {
                continue;
            };
            let live = self
                .thread_event_channels
                .get(&id)
                .is_some_and(|channel| channel.attachment() == ThreadEventAttachment::Live);
            let previous = self.agent_navigation.get(&id);
            if known_at_start.contains(&id) && previous.is_none() && !live {
                continue;
            }
            seen.insert(id);
            let is_closed = matches!(thread.status, ThreadStatus::NotLoaded);
            let parent_owned = crate::app_server_session::thread_blocks_direct_input(&thread);
            if is_closed && (live || previous.is_none() && !(thread.ephemeral && parent_owned))
                || !is_closed
                    && (accepted == AGENT_PICKER_MAX_THREADS
                        || previous.is_some_and(|entry| entry.is_closed))
            {
                continue;
            }
            accepted += usize::from(!is_closed);
            let nickname = thread
                .agent_nickname
                .or_else(|| previous.and_then(|entry| entry.agent_nickname.clone()));
            let role = thread
                .agent_role
                .or_else(|| previous.and_then(|entry| entry.agent_role.clone()));
            let path = crate::app_server_session::source_agent_path(&thread.source);
            if parent_owned {
                self.agent_navigation.mark_parent_owned(id);
            }
            self.upsert_agent_picker_thread(id, nickname, role, is_closed);
            self.agent_navigation.set_agent_path(id, path);
            if !live && matches!(thread.status, ThreadStatus::Active { .. }) {
                self.agent_navigation.mark_running_from_snapshot(id);
            } else if !live {
                self.agent_navigation.set_running(id, /*is_running*/ false);
            }
        }
        if exhaustive {
            for id in known_at_start {
                if id != root
                    && !seen.contains(&id)
                    && !self.thread_event_channels.contains_key(&id)
                {
                    self.agent_navigation.remove(id);
                }
            }
        }
        self.sync_active_agent_label();
    }
}
