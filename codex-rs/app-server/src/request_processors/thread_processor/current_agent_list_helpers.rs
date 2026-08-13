use super::*;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CurrentAgentThreadListCursor {
    pub(super) root_thread_id: ThreadId,
    pub(super) direct_children_only: bool,
    pub(super) sort_key: StoreThreadSortKey,
    pub(super) sort_direction: SortDirection,
    pub(super) anchor_sort_value: i64,
    pub(super) anchor_thread_id: ThreadId,
    pub(super) inclusive: bool,
}

pub(super) fn current_agent_thread_matches_filters(
    entry: &CurrentAgentThreadListEntry,
    model_providers: Option<&[String]>,
    source_kinds: Option<&[ThreadSourceKind]>,
    archived: Option<bool>,
    section_id: Option<&Option<String>>,
    cwd_filters: Option<&[PathBuf]>,
    search_term: Option<&str>,
) -> bool {
    if archived.is_some_and(|archived| entry.archived != archived)
        || model_providers.is_some_and(|providers| {
            !providers.is_empty()
                && !providers
                    .iter()
                    .any(|provider| provider == &entry.thread.model_provider)
        })
    {
        return false;
    }

    let source_matches = match source_kinds {
        None => true,
        Some([]) => codex_core::INTERACTIVE_SESSION_SOURCES.contains(&entry.source),
        Some(source_kinds) => source_kind_matches(&entry.source, source_kinds),
    };
    if !source_matches {
        return false;
    }

    let section_matches = match section_id {
        None => true,
        Some(None) => entry.thread.section.is_none(),
        Some(Some(section_id)) => entry
            .thread
            .section
            .as_ref()
            .is_some_and(|section| &section.id == section_id),
    };
    if !section_matches {
        return false;
    }

    if cwd_filters.is_some_and(|expected_cwds| {
        !expected_cwds.iter().any(|expected_cwd| {
            path_utils::paths_match_after_normalization(entry.thread.cwd.as_path(), expected_cwd)
        })
    }) {
        return false;
    }

    search_term.is_none_or(|search_term| {
        entry.thread.preview.contains(search_term)
            || entry
                .thread
                .name
                .as_deref()
                .is_some_and(|name| name.contains(search_term))
    })
}

pub(super) fn current_agent_thread_sort_value(
    entry: &CurrentAgentThreadListEntry,
    sort_key: StoreThreadSortKey,
) -> i64 {
    match sort_key {
        StoreThreadSortKey::CreatedAt => entry.created_at_millis,
        StoreThreadSortKey::UpdatedAt => entry.updated_at_millis,
        StoreThreadSortKey::RecencyAt => entry.recency_at_millis,
        StoreThreadSortKey::SectionPosition => entry.section_position.unwrap_or_default(),
    }
}

pub(super) fn current_agent_thread_entry_to_cursor_ordering(
    entry: &CurrentAgentThreadListEntry,
    sort_key: StoreThreadSortKey,
    sort_direction: SortDirection,
    anchor_sort_value: i64,
    anchor_thread_id: ThreadId,
) -> std::cmp::Ordering {
    let ordering = current_agent_thread_sort_value(entry, sort_key)
        .cmp(&anchor_sort_value)
        .then_with(|| entry.thread.id.cmp(&anchor_thread_id.to_string()));
    match sort_direction {
        SortDirection::Asc => ordering,
        SortDirection::Desc => ordering.reverse(),
    }
}

pub(super) fn opposite_sort_direction(sort_direction: SortDirection) -> SortDirection {
    match sort_direction {
        SortDirection::Asc => SortDirection::Desc,
        SortDirection::Desc => SortDirection::Asc,
    }
}

pub(super) fn empty_thread_list_response() -> ThreadListResponse {
    ThreadListResponse {
        data: Vec::new(),
        next_cursor: None,
        backwards_cursor: None,
    }
}

pub(super) fn encode_current_agent_thread_list_cursor(
    root_thread_id: ThreadId,
    direct_children_only: bool,
    sort_key: StoreThreadSortKey,
    sort_direction: SortDirection,
    anchor: &CurrentAgentThreadListEntry,
    inclusive: bool,
) -> Result<String, JSONRPCErrorError> {
    let anchor_thread_id = ThreadId::from_string(&anchor.thread.id)
        .map_err(|err| internal_error(format!("current agent has invalid thread id: {err}")))?;
    serde_json::to_string(&CurrentAgentThreadListCursor {
        root_thread_id,
        direct_children_only,
        sort_key,
        sort_direction,
        anchor_sort_value: current_agent_thread_sort_value(anchor, sort_key),
        anchor_thread_id,
        inclusive,
    })
    .map_err(|err| internal_error(format!("failed to encode thread/list cursor: {err}")))
}

pub(super) fn parse_current_agent_thread_list_cursor(
    cursor: &str,
    root_thread_id: ThreadId,
    direct_children_only: bool,
    sort_key: StoreThreadSortKey,
    sort_direction: SortDirection,
) -> Result<CurrentAgentThreadListCursor, JSONRPCErrorError> {
    let cursor = serde_json::from_str::<CurrentAgentThreadListCursor>(cursor)
        .map_err(|err| invalid_request(format!("invalid thread/list cursor: {err}")))?;
    if cursor.root_thread_id != root_thread_id
        || cursor.direct_children_only != direct_children_only
        || cursor.sort_key != sort_key
        || cursor.sort_direction != sort_direction
    {
        return Err(invalid_request(
            "thread/list cursor does not match the requested relation or sort order",
        ));
    }
    Ok(cursor)
}
