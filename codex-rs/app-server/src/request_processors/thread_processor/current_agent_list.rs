use super::*;

#[path = "current_agent_list_helpers.rs"]
mod helpers;
use helpers::*;

/// Parameters for a `thread/list` request over one canonical current-agent snapshot.
///
/// Persisted spawn edges retain ownership history. They are not membership and must not be used to
/// decide which threads this request returns.
pub(super) struct CurrentAgentThreadListParams {
    pub(super) root_thread_id: ThreadId,
    pub(super) direct_children_only: bool,
    pub(super) members: Vec<CurrentAgentMember>,
    pub(super) cursor: Option<String>,
    pub(super) limit: usize,
    pub(super) sort_key: StoreThreadSortKey,
    pub(super) sort_direction: SortDirection,
    pub(super) model_providers: Option<Vec<String>>,
    pub(super) source_kinds: Option<Vec<ThreadSourceKind>>,
    pub(super) archived: Option<bool>,
    pub(super) section_id: Option<Option<String>>,
    pub(super) cwd_filters: Option<Vec<PathBuf>>,
    pub(super) search_term: Option<String>,
}

#[cfg(test)]
#[path = "current_agent_list_tests.rs"]
mod tests;

/// One current agent combined with persisted display and ordering metadata when it exists.
struct CurrentAgentThreadListEntry {
    thread: Thread,
    source: codex_protocol::protocol::SessionSource,
    archived: bool,
    created_at_millis: i64,
    updated_at_millis: i64,
    recency_at_millis: i64,
    section_position: Option<i64>,
}

enum CurrentAgentHydrationKind {
    Persisted,
    LiveFallback,
    MinimalFallback,
}

impl ThreadRequestProcessor {
    pub(super) async fn current_agent_thread_list_response(
        &self,
        params: CurrentAgentThreadListParams,
    ) -> Result<ThreadListResponse, JSONRPCErrorError> {
        let started_at = std::time::Instant::now();
        let CurrentAgentThreadListParams {
            root_thread_id,
            direct_children_only,
            members,
            cursor,
            limit,
            sort_key,
            sort_direction,
            model_providers,
            source_kinds,
            archived,
            section_id,
            cwd_filters,
            search_term,
        } = params;
        if sort_key == StoreThreadSortKey::SectionPosition && !matches!(section_id, Some(Some(_))) {
            return Err(invalid_request(
                "section-position sorting requires a section filter",
            ));
        }
        let members = members
            .into_iter()
            .filter(|member| {
                current_agent_member_matches_relation(member, root_thread_id, direct_children_only)
            })
            .collect::<Vec<_>>();
        if members.is_empty() {
            return Ok(empty_thread_list_response());
        }

        let stored_threads = self
            .thread_store
            .read_threads(StoreReadThreadsParams {
                thread_ids: members.iter().map(|member| member.thread_id).collect(),
            })
            .await
            .map_err(thread_store_list_error)?;
        let persisted_count = stored_threads.len();
        let mut stored_threads = stored_threads
            .into_iter()
            .map(|thread| (thread.thread_id, thread))
            .collect::<HashMap<_, _>>();
        let search_term = search_term
            .as_deref()
            .map(str::trim)
            .filter(|search_term| !search_term.is_empty())
            .map(str::to_string);
        let mut entries = Vec::with_capacity(members.len());
        let member_count = members.len();
        let mut live_fallback_count = 0_usize;
        let mut minimal_fallback_count = 0_usize;
        let hydration_inputs = members
            .into_iter()
            .map(|member| {
                let stored_thread = stored_threads.remove(&member.thread_id);
                (member, stored_thread)
            })
            .collect::<Vec<_>>();
        let hydrated_threads = futures::future::join_all(hydration_inputs.into_iter().map(
            |(member, stored_thread)| self.hydrate_current_agent_thread(member, stored_thread),
        ))
        .await;
        for hydrated_thread in hydrated_threads {
            let (entry, hydration_kind) = hydrated_thread?;
            match hydration_kind {
                CurrentAgentHydrationKind::Persisted => {}
                CurrentAgentHydrationKind::LiveFallback => live_fallback_count += 1,
                CurrentAgentHydrationKind::MinimalFallback => minimal_fallback_count += 1,
            }
            if current_agent_thread_matches_filters(
                &entry,
                model_providers.as_deref(),
                source_kinds.as_deref(),
                archived,
                section_id.as_ref(),
                cwd_filters.as_deref(),
                search_term.as_deref(),
            ) {
                entries.push(entry);
            }
        }
        entries.sort_by(|left, right| {
            let ordering = current_agent_thread_sort_value(left, sort_key)
                .cmp(&current_agent_thread_sort_value(right, sort_key))
                .then_with(|| left.thread.id.cmp(&right.thread.id));
            match sort_direction {
                SortDirection::Asc => ordering,
                SortDirection::Desc => ordering.reverse(),
            }
        });

        let start = match cursor {
            Some(cursor) => {
                let cursor = parse_current_agent_thread_list_cursor(
                    &cursor,
                    root_thread_id,
                    direct_children_only,
                    sort_key,
                    sort_direction,
                )?;
                entries
                    .iter()
                    .position(|entry| {
                        let ordering = current_agent_thread_entry_to_cursor_ordering(
                            entry,
                            sort_key,
                            sort_direction,
                            cursor.anchor_sort_value,
                            cursor.anchor_thread_id,
                        );
                        if cursor.inclusive {
                            ordering != std::cmp::Ordering::Less
                        } else {
                            ordering == std::cmp::Ordering::Greater
                        }
                    })
                    .unwrap_or(entries.len())
            }
            None => 0,
        };
        let end = start.saturating_add(limit).min(entries.len());
        let page = &entries[start..end];
        let next_cursor = (end < entries.len())
            .then(|| page.last())
            .flatten()
            .map(|entry| {
                encode_current_agent_thread_list_cursor(
                    root_thread_id,
                    direct_children_only,
                    sort_key,
                    sort_direction,
                    entry,
                    /*inclusive*/ false,
                )
            })
            .transpose()?;
        let backwards_cursor = page
            .first()
            .map(|entry| {
                encode_current_agent_thread_list_cursor(
                    root_thread_id,
                    direct_children_only,
                    sort_key,
                    opposite_sort_direction(sort_direction),
                    entry,
                    /*inclusive*/ true,
                )
            })
            .transpose()?;
        let mut data = page
            .iter()
            .map(|entry| entry.thread.clone())
            .collect::<Vec<_>>();
        enrich_loaded_threads(
            &self.thread_manager,
            &self.thread_watch_manager,
            data.as_mut_slice(),
            |thread| thread,
        )
        .await;
        tracing::event!(
            target: "codex_current_agent_list",
            tracing::Level::DEBUG,
            event.name = "codex.current_agents.list",
            thread.id = %root_thread_id,
            member_count,
            returned_count = data.len(),
            metadata_batch_queries = 1,
            scalar_thread_reads = 0,
            persisted_count,
            live_fallback_count,
            minimal_fallback_count,
            elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            "listed canonical current-agent membership"
        );
        Ok(ThreadListResponse {
            data,
            next_cursor,
            backwards_cursor,
        })
    }

    async fn hydrate_current_agent_thread(
        &self,
        member: CurrentAgentMember,
        stored_thread: Option<StoredThread>,
    ) -> Result<(CurrentAgentThreadListEntry, CurrentAgentHydrationKind), JSONRPCErrorError> {
        let thread_id = member.thread_id;
        let loaded_thread = self.thread_manager.get_thread(thread_id).await.ok();
        let stored_values = stored_thread.as_ref().map(|stored_thread| {
            (
                with_thread_spawn_agent_metadata(
                    stored_thread.source.clone(),
                    stored_thread.agent_nickname.clone(),
                    stored_thread.agent_role.clone(),
                ),
                stored_thread.archived_at.is_some(),
                stored_thread.created_at.timestamp_millis(),
                stored_thread.updated_at.timestamp_millis(),
                stored_thread.recency_at.timestamp_millis(),
                stored_thread.section_position,
            )
        });
        let persisted_thread = stored_thread.map(|stored_thread| {
            thread_from_stored_thread(
                stored_thread,
                self.config.model_provider_id.as_str(),
                &self.config.cwd,
            )
            .0
        });

        let (
            mut thread,
            mut source,
            archived,
            created_at_millis,
            updated_at_millis,
            recency_at_millis,
            section_position,
            hydration_kind,
        ) = if let Some(loaded_thread) = loaded_thread.as_ref() {
            let config_snapshot = loaded_thread.config_snapshot().await;
            let fallback_thread =
                build_thread_from_loaded_snapshot(thread_id, &config_snapshot, loaded_thread);
            let thread = merge_current_agent_live_thread(persisted_thread, fallback_thread);
            match stored_values {
                Some((source, archived, created, updated, recency, section_position)) => (
                    thread,
                    source,
                    archived,
                    created,
                    updated,
                    recency,
                    section_position,
                    CurrentAgentHydrationKind::Persisted,
                ),
                None => (
                    thread,
                    config_snapshot.session_source,
                    false,
                    0,
                    0,
                    0,
                    None,
                    CurrentAgentHydrationKind::LiveFallback,
                ),
            }
        } else if let (
            Some(thread),
            Some((source, archived, created, updated, recency, section_position)),
        ) = (persisted_thread, stored_values)
        {
            (
                thread,
                source,
                archived,
                created,
                updated,
                recency,
                section_position,
                CurrentAgentHydrationKind::Persisted,
            )
        } else {
            let (thread, source, archived, created, updated, recency, section_position) =
                minimal_current_agent_thread(&self.config, &member);
            (
                thread,
                source,
                archived,
                created,
                updated,
                recency,
                section_position,
                CurrentAgentHydrationKind::MinimalFallback,
            )
        };
        apply_current_agent_member(&mut thread, &member);
        apply_current_agent_member_core_source(&mut source, &member);
        Ok((
            CurrentAgentThreadListEntry {
                thread,
                source,
                archived,
                created_at_millis,
                updated_at_millis,
                recency_at_millis,
                section_position,
            },
            hydration_kind,
        ))
    }
}

fn current_agent_member_matches_relation(
    member: &CurrentAgentMember,
    root_thread_id: ThreadId,
    direct_children_only: bool,
) -> bool {
    !direct_children_only || member.parent_thread_id == root_thread_id
}

fn merge_current_agent_live_thread(
    persisted_thread: Option<Thread>,
    fallback_thread: Thread,
) -> Thread {
    let Some(mut thread) = persisted_thread else {
        return fallback_thread;
    };
    if thread.path.is_none() {
        thread.path.clone_from(&fallback_thread.path);
    }
    thread.session_id.clone_from(&fallback_thread.session_id);
    thread.ephemeral = fallback_thread.ephemeral;
    thread.can_accept_direct_input = fallback_thread.can_accept_direct_input;
    thread
}

fn minimal_current_agent_thread(
    config: &Config,
    member: &CurrentAgentMember,
) -> (
    Thread,
    codex_protocol::protocol::SessionSource,
    bool,
    i64,
    i64,
    i64,
    Option<i64>,
) {
    let agent_path = member.agent_path.clone();
    let depth = agent_path.as_ref().map_or(1, |agent_path| {
        i32::try_from(agent_path.as_str().matches('/').count().saturating_sub(1))
            .unwrap_or(i32::MAX)
    });
    let source = codex_protocol::protocol::SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: member.parent_thread_id,
        depth,
        agent_path,
        agent_nickname: member.agent_nickname.clone(),
        agent_role: member.agent_role.clone(),
    });
    let thread_id = member.thread_id.to_string();
    (
        Thread {
            id: thread_id.clone(),
            extra: None,
            session_id: thread_id,
            forked_from_id: None,
            parent_thread_id: Some(member.parent_thread_id.to_string()),
            preview: String::new(),
            ephemeral: true,
            section: None,
            section_entered_at: None,
            project_id: None,
            history_mode: codex_app_server_protocol::ThreadHistoryMode::Legacy,
            model_provider: config.model_provider_id.clone(),
            created_at: 0,
            updated_at: 0,
            recency_at: Some(0),
            status: ThreadStatus::NotLoaded,
            agent_status: Some(member.status.clone().into()),
            path: None,
            cwd: config.cwd.clone(),
            cli_version: env!("CARGO_PKG_VERSION").to_string(),
            source: source.clone().into(),
            can_accept_direct_input: None,
            thread_source: Some(codex_app_server_protocol::ThreadSource::Subagent),
            agent_nickname: member.agent_nickname.clone(),
            agent_role: member.agent_role.clone(),
            git_info: None,
            name: None,
            turns: Vec::new(),
        },
        source,
        false,
        0,
        0,
        0,
        None,
    )
}

fn apply_current_agent_member(thread: &mut Thread, member: &CurrentAgentMember) {
    thread.parent_thread_id = Some(member.parent_thread_id.to_string());
    thread.agent_status = Some(member.status.clone().into());
    thread.agent_nickname.clone_from(&member.agent_nickname);
    thread.agent_role.clone_from(&member.agent_role);
    apply_current_agent_member_source(&mut thread.source, member);
}

fn apply_current_agent_member_source(source: &mut SessionSource, member: &CurrentAgentMember) {
    if let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        agent_path,
        agent_nickname,
        agent_role,
        ..
    }) = source
    {
        *parent_thread_id = member.parent_thread_id;
        agent_path.clone_from(&member.agent_path);
        agent_nickname.clone_from(&member.agent_nickname);
        agent_role.clone_from(&member.agent_role);
    }
}

fn apply_current_agent_member_core_source(
    source: &mut codex_protocol::protocol::SessionSource,
    member: &CurrentAgentMember,
) {
    if let codex_protocol::protocol::SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        agent_path,
        agent_nickname,
        agent_role,
        ..
    }) = source
    {
        *parent_thread_id = member.parent_thread_id;
        agent_path.clone_from(&member.agent_path);
        agent_nickname.clone_from(&member.agent_nickname);
        agent_role.clone_from(&member.agent_role);
    }
}
