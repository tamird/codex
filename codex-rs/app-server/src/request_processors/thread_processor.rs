use super::persisted_resume_settings::PersistedResumeSettings;
use super::persisted_resume_settings::latest_persisted_resume_settings;
use super::thread_enrichment::enrich_loaded_threads;
use super::thread_fork_goal::inherit_thread_goal_snapshot;
use super::thread_fork_handoff::ForkHandoff;
use super::thread_input::can_accept_direct_input;
use super::thread_input::ensure_direct_input_allowed;
use super::*;
use crate::error_code::method_not_found;
use codex_app_server_protocol::SelectedCapabilityRoot;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::ThreadHistoryMode as ApiThreadHistoryMode;
use codex_app_server_protocol::ThreadRevertParams;
use codex_app_server_protocol::ThreadRevertResponse;
use codex_app_server_protocol::ThreadRevertedNotification;
use codex_app_server_protocol::ThreadSection;
use codex_app_server_protocol::ThreadSectionAppearance;
use codex_app_server_protocol::ThreadSectionMoveParams;
use codex_app_server_protocol::ThreadSectionMoveResponse;
use codex_core::CurrentAgentMember;
use codex_extension_api::ExtensionDataInit;
use codex_extension_api::ThreadIdleCause;
use codex_protocol::SanitizedGitUrl;
use codex_protocol::config_types::MultiAgentMode;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::ScanOutcome;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadsParams;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::File;
use std::sync::LazyLock;
use std::time::SystemTime;

mod current_agent_list;
mod subagent_history_projection;
use current_agent_list::CurrentAgentThreadListParams;
use subagent_history_projection::SubagentHistoryProjection;

pub(super) const THREAD_LIST_DEFAULT_LIMIT: usize = 25;
pub(super) const THREAD_LIST_MAX_LIMIT: usize = 100;
const CODEX_TUI_CLIENT_NAME: &str = "codex-tui";
const THREAD_ROLLBACK_DEPRECATION_SUMMARY: &str =
    "thread/rollback is deprecated and will be removed soon";
const PAGINATED_FULL_HISTORY_DEPRECATION_SUMMARY: &str = "Full-history hydration is deprecated for paginated threads; use `excludeTurns: true`, then page with `thread/turns/list` and `thread/items/list`.";
const PAGINATED_THREAD_READ_DEPRECATION_SUMMARY: &str = "Full-history hydration is deprecated for paginated threads; omit `includeTurns` or set it to `false`, then page with `thread/turns/list` and `thread/items/list`.";
const MAX_LEGACY_PAGE_DEPTH_HINTS: usize = 256;
const MAX_LEGACY_PAGE_DEPTH_HINT_BYTES: usize = 512 * 1024;
const MAX_LEGACY_DISPLAYED_TURNS: usize = 256;
const MAX_LEGACY_DISPLAYED_TURN_BYTES: usize = 2 * 1024 * 1024;
// Feedback and SQLite subscribers capture TRACE by default, so history events must be opt-in.
static HISTORY_IO_OBSERVATION_ENABLED: LazyLock<bool> =
    LazyLock::new(|| std::env::var_os("FRODEX_HISTORY_IO_TRACE").is_some_and(|value| value == "1"));

/// Controls ordering, item hydration, and live-thread eligibility for indexed Legacy turns.
struct ProjectedLegacyThreadTurnsPageOptions {
    sort_direction: SortDirection,
    items_view: Option<TurnItemsView>,
    allow_running: bool,
}

/// Identifies one mutable active rollout without retaining conversation contents.
#[derive(Clone, Eq, PartialEq)]
struct LegacyRolloutGeneration {
    path: PathBuf,
    len: u64,
    modified_at: Option<SystemTime>,
    #[cfg(unix)]
    device_and_inode: (u64, u64),
}

impl LegacyRolloutGeneration {
    async fn capture(path: &Path) -> std::io::Result<Self> {
        let metadata = tokio::fs::metadata(path).await?;
        Ok(Self {
            path: path.to_path_buf(),
            len: metadata.len(),
            modified_at: metadata.modified().ok(),
            #[cfg(unix)]
            device_and_inode: {
                use std::os::unix::fs::MetadataExt as _;
                (metadata.dev(), metadata.ino())
            },
        })
    }
}

/// Records the validated expansion depth for one active-rollout generation and page cursor.
struct LegacyPageDepthHint {
    generation: LegacyRolloutGeneration,
    /// A missing cursor identifies the thread's initial page.
    cursor_turn_id: Option<String>,
    page_size: usize,
    reference_depth: usize,
}

impl LegacyPageDepthHint {
    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.generation.path.as_os_str().len()
            + self.cursor_turn_id.as_ref().map_or(0, String::len)
    }
}

/// Remembers validated root-based expansion depths for opaque descending page cursors.
#[derive(Default)]
struct LegacyPageDepthHints {
    entries: VecDeque<LegacyPageDepthHint>,
    estimated_bytes: usize,
}

impl LegacyPageDepthHints {
    fn lookup(
        &mut self,
        generation: &LegacyRolloutGeneration,
        turn_id: Option<&str>,
        page_size: usize,
    ) -> Option<usize> {
        self.entries.retain(|entry| {
            entry.generation.path != generation.path || entry.generation == *generation
        });
        self.estimated_bytes = self
            .entries
            .iter()
            .map(LegacyPageDepthHint::estimated_bytes)
            .sum();
        let index = self.entries.iter().position(|entry| {
            entry.generation == *generation
                && entry.cursor_turn_id.as_deref() == turn_id
                && entry.page_size == page_size
        })?;
        let entry = self.entries.remove(index)?;
        let reference_depth = entry.reference_depth;
        self.entries.push_back(entry);
        Some(reference_depth)
    }

    fn insert(&mut self, entry: LegacyPageDepthHint) {
        self.entries.retain(|previous| {
            previous.generation.path != entry.generation.path
                || previous.generation == entry.generation
                    && !(previous.cursor_turn_id == entry.cursor_turn_id
                        && previous.page_size == entry.page_size)
        });
        self.estimated_bytes = self
            .entries
            .iter()
            .map(LegacyPageDepthHint::estimated_bytes)
            .sum();
        let entry_bytes = entry.estimated_bytes();
        if entry_bytes > MAX_LEGACY_PAGE_DEPTH_HINT_BYTES {
            return;
        }
        while self.entries.len() == MAX_LEGACY_PAGE_DEPTH_HINTS
            || self.estimated_bytes.saturating_add(entry_bytes) > MAX_LEGACY_PAGE_DEPTH_HINT_BYTES
        {
            let Some(evicted) = self.entries.pop_front() else {
                break;
            };
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_sub(evicted.estimated_bytes());
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(entry_bytes);
        self.entries.push_back(entry);
    }
}

async fn stage_pending_project_metadata(
    thread_manager: &ThreadManager,
    thread_store: &dyn ThreadStore,
    project_id: Option<&str>,
    operation: &'static str,
) -> Result<Option<ThreadId>, JSONRPCErrorError> {
    let Some(project_id) = project_id else {
        return Ok(None);
    };
    let thread_id = thread_manager.reserve_thread_id();
    thread_store
        .stage_pending_thread_metadata(
            thread_id,
            StoreThreadMetadataPatch {
                project_id: Some(Some(project_id.to_string())),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| match error {
            ThreadStoreError::Unsupported { .. } => {
                method_not_found(format!("{operation} is unavailable without sqlite state"))
            }
            ThreadStoreError::InvalidRequest { message } => invalid_request(message),
            error => internal_error(format!("failed to stage {operation} metadata: {error}")),
        })?;
    Ok(Some(thread_id))
}

async fn remove_pending_project_metadata(
    thread_store: &dyn ThreadStore,
    thread_id: Option<ThreadId>,
) {
    let Some(thread_id) = thread_id else {
        return;
    };
    if let Err(error) = thread_store.remove_pending_thread_metadata(thread_id).await {
        warn!("failed to remove staged project metadata for {thread_id}: {error}");
    }
}

/// Preserves item IDs already returned for bounded Legacy turns in this app-server process.
///
/// Legacy records do not carry item IDs, so bounded windows can assign different synthetic IDs
/// when the same turn is reconstructed with another page size. The cache is intentionally
/// process-local: after restart, a complete projection may expose canonical full-history IDs.
#[derive(Default)]
struct LegacyDisplayedTurnItems {
    entries: VecDeque<LegacyDisplayedTurnItemIds>,
    estimated_bytes: usize,
}

struct LegacyDisplayedTurnItemIds {
    thread_id: ThreadId,
    turn_id: String,
    item_signatures: Vec<String>,
    item_ids: Vec<String>,
}

impl LegacyDisplayedTurnItemIds {
    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.turn_id.len()
            + self.item_signatures.iter().map(String::len).sum::<usize>()
            + self.item_ids.iter().map(String::len).sum::<usize>()
    }
}

impl LegacyDisplayedTurnItems {
    fn stabilize(&mut self, thread_id: ThreadId, turns: &mut [Turn]) {
        for turn in turns {
            let Some(item_signatures) = turn
                .items
                .iter()
                .map(legacy_thread_item_signature)
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            let entry_index = self
                .entries
                .iter()
                .position(|entry| entry.thread_id == thread_id && entry.turn_id == turn.id);
            if let Some(entry_index) = entry_index {
                let Some(entry) = self.entries.remove(entry_index) else {
                    continue;
                };
                self.estimated_bytes = self.estimated_bytes.saturating_sub(entry.estimated_bytes());
                if entry.item_signatures == item_signatures
                    && entry.item_ids.len() == turn.items.len()
                    && let Some(stabilized_items) = turn
                        .items
                        .iter()
                        .zip(entry.item_ids.iter())
                        .map(|(item, item_id)| legacy_thread_item_with_id(item, item_id))
                        .collect::<Option<Vec<_>>>()
                {
                    turn.items = stabilized_items;
                    self.estimated_bytes =
                        self.estimated_bytes.saturating_add(entry.estimated_bytes());
                    self.entries.push_back(entry);
                    continue;
                }
            }

            let entry = LegacyDisplayedTurnItemIds {
                thread_id,
                turn_id: turn.id.clone(),
                item_signatures,
                item_ids: turn
                    .items
                    .iter()
                    .map(|item| item.id().to_string())
                    .collect(),
            };
            let entry_bytes = entry.estimated_bytes();
            if entry_bytes > MAX_LEGACY_DISPLAYED_TURN_BYTES {
                continue;
            }
            while self.entries.len() == MAX_LEGACY_DISPLAYED_TURNS
                || self.estimated_bytes.saturating_add(entry_bytes)
                    > MAX_LEGACY_DISPLAYED_TURN_BYTES
            {
                let Some(evicted) = self.entries.pop_front() else {
                    break;
                };
                self.estimated_bytes = self
                    .estimated_bytes
                    .saturating_sub(evicted.estimated_bytes());
            }
            self.estimated_bytes = self.estimated_bytes.saturating_add(entry_bytes);
            self.entries.push_back(entry);
        }
    }
}

fn legacy_thread_item_signature(item: &ThreadItem) -> Option<String> {
    let mut value = serde_json::to_value(item).ok()?;
    value.as_object_mut()?.remove("id")?;
    serde_json::to_string(&value).ok()
}

fn legacy_thread_item_with_id(item: &ThreadItem, item_id: &str) -> Option<ThreadItem> {
    let mut value = serde_json::to_value(item).ok()?;
    value.as_object_mut()?.insert(
        "id".to_string(),
        serde_json::Value::String(item_id.to_string()),
    );
    serde_json::from_value(value).ok()
}

struct ThreadListFilters {
    model_providers: Option<Vec<String>>,
    source_kinds: Option<Vec<ThreadSourceKind>>,
    archived: bool,
    section_id: Option<Option<String>>,
    project_id: StoreClearableField<String>,
    cwd_filters: Option<Vec<PathBuf>>,
    search_term: Option<String>,
    use_state_db_only: bool,
    relation_filter: Option<StoreThreadRelationFilter>,
}

struct ThreadRevertRuntimeSnapshot {
    config: Config,
    settings: CodexThreadSettingsOverrides,
    client_mcp_extensions: ClientMcpExtensions,
}

fn collect_resume_override_mismatches(
    request: &ThreadResumeParams,
    config_snapshot: &ThreadConfigSnapshot,
) -> Vec<String> {
    let mut mismatch_details = Vec::new();

    if let Some(requested_model) = request.model.as_deref()
        && requested_model != config_snapshot.model
    {
        mismatch_details.push(format!(
            "model requested={requested_model} active={}",
            config_snapshot.model
        ));
    }
    if let Some(requested_provider) = request.model_provider.as_deref()
        && requested_provider != config_snapshot.model_provider_id
    {
        mismatch_details.push(format!(
            "model_provider requested={requested_provider} active={}",
            config_snapshot.model_provider_id
        ));
    }
    if let Some(requested_service_tier) = request.service_tier.as_ref()
        && requested_service_tier != &config_snapshot.service_tier
    {
        mismatch_details.push(format!(
            "service_tier requested={requested_service_tier:?} active={:?}",
            config_snapshot.service_tier
        ));
    }
    if let Some(requested_cwd) = request.cwd.as_deref() {
        let requested_cwd_path = std::path::PathBuf::from(requested_cwd);
        if requested_cwd_path != config_snapshot.cwd().as_path() {
            mismatch_details.push(format!(
                "cwd requested={} active={}",
                requested_cwd_path.display(),
                config_snapshot.cwd().display()
            ));
        }
    }
    if let Some(requested_runtime_workspace_roots) = request.runtime_workspace_roots.as_ref() {
        let requested_runtime_workspace_roots = requested_runtime_workspace_roots.to_vec();
        if requested_runtime_workspace_roots != config_snapshot.workspace_roots {
            mismatch_details.push(format!(
                "runtime_workspace_roots requested={requested_runtime_workspace_roots:?} active={:?}",
                config_snapshot.workspace_roots
            ));
        }
    }
    if let Some(requested_approval) = request.approval_policy.as_ref() {
        let active_approval: AskForApproval = config_snapshot.approval_policy.into();
        if requested_approval != &active_approval {
            mismatch_details.push(format!(
                "approval_policy requested={requested_approval:?} active={active_approval:?}"
            ));
        }
    }
    if let Some(requested_review_policy) = request.approvals_reviewer.as_ref() {
        let active_review_policy: codex_app_server_protocol::ApprovalsReviewer =
            config_snapshot.approvals_reviewer.into();
        if requested_review_policy != &active_review_policy {
            mismatch_details.push(format!(
                "approvals_reviewer requested={requested_review_policy:?} active={active_review_policy:?}"
            ));
        }
    }
    if let Some(requested_sandbox) = request.sandbox.as_ref() {
        let active_sandbox = config_snapshot.sandbox_policy();
        let sandbox_matches = matches!(
            (requested_sandbox, &active_sandbox),
            (
                SandboxMode::ReadOnly,
                codex_protocol::protocol::SandboxPolicy::ReadOnly { .. }
            ) | (
                SandboxMode::WorkspaceWrite,
                codex_protocol::protocol::SandboxPolicy::WorkspaceWrite { .. }
            ) | (
                SandboxMode::DangerFullAccess,
                codex_protocol::protocol::SandboxPolicy::DangerFullAccess
            ) | (
                SandboxMode::DangerFullAccess,
                codex_protocol::protocol::SandboxPolicy::ExternalSandbox { .. }
            )
        );
        if !sandbox_matches {
            mismatch_details.push(format!(
                "sandbox requested={requested_sandbox:?} active={active_sandbox:?}"
            ));
        }
    }
    if request.permissions.is_some() {
        mismatch_details.push(format!(
            "permissions override was provided and ignored while running; active={:?}",
            config_snapshot.active_permission_profile
        ));
    }
    if let Some(requested_personality) = request.personality.as_ref()
        && config_snapshot.personality.as_ref() != Some(requested_personality)
    {
        mismatch_details.push(format!(
            "personality requested={requested_personality:?} active={:?}",
            config_snapshot.personality
        ));
    }

    if request.config.is_some() {
        mismatch_details
            .push("config overrides were provided and ignored while running".to_string());
    }
    if request.base_instructions.is_some() {
        mismatch_details
            .push("baseInstructions override was provided and ignored while running".to_string());
    }
    if request.developer_instructions.is_some() {
        mismatch_details.push(
            "developerInstructions override was provided and ignored while running".to_string(),
        );
    }
    mismatch_details
}

fn merge_persisted_resume_metadata(
    request_overrides: &mut Option<HashMap<String, serde_json::Value>>,
    typesafe_overrides: &mut ConfigOverrides,
    persisted_metadata: &ThreadMetadata,
) {
    if has_model_resume_override(request_overrides.as_ref(), typesafe_overrides) {
        return;
    }

    typesafe_overrides.model = persisted_metadata.model.clone();
    typesafe_overrides.model_provider = Some(persisted_metadata.model_provider.clone());

    if let Some(reasoning_effort) = persisted_metadata.reasoning_effort.as_ref() {
        request_overrides.get_or_insert_with(HashMap::new).insert(
            "model_reasoning_effort".to_string(),
            serde_json::Value::String(reasoning_effort.to_string()),
        );
    }
}

fn normalize_thread_list_cwd_filters(
    cwd: Option<ThreadListCwdFilter>,
) -> Result<Option<Vec<PathBuf>>, JSONRPCErrorError> {
    let Some(cwd) = cwd else {
        return Ok(None);
    };

    let cwds = match cwd {
        ThreadListCwdFilter::One(cwd) => vec![cwd],
        ThreadListCwdFilter::Many(cwds) => cwds,
    };
    let mut normalized_cwds = Vec::with_capacity(cwds.len());
    for cwd in cwds {
        let cwd = AbsolutePathBuf::relative_to_current_dir(cwd.as_str())
            .map(AbsolutePathBuf::into_path_buf)
            .map_err(|err| {
                invalid_params(format!("invalid thread/list cwd filter `{cwd}`: {err}"))
            })?;
        normalized_cwds.push(cwd);
    }

    Ok(Some(normalized_cwds))
}

fn has_model_resume_override(
    request_overrides: Option<&HashMap<String, serde_json::Value>>,
    typesafe_overrides: &ConfigOverrides,
) -> bool {
    typesafe_overrides.model.is_some()
        || typesafe_overrides.model_provider.is_some()
        || request_overrides.is_some_and(|overrides| overrides.contains_key("model"))
        || request_overrides
            .is_some_and(|overrides| overrides.contains_key("model_reasoning_effort"))
}

fn has_permission_override(
    request_overrides: Option<&HashMap<String, serde_json::Value>>,
    typesafe_overrides: &ConfigOverrides,
) -> bool {
    typesafe_overrides.sandbox_mode.is_some()
        || typesafe_overrides.permission_profile.is_some()
        || typesafe_overrides.default_permissions.is_some()
        || request_overrides.is_some_and(|overrides| {
            overrides.contains_key("sandbox_mode") || overrides.contains_key("default_permissions")
        })
}

fn validate_dynamic_tools(tools: &[DynamicToolSpec]) -> Result<(), String> {
    const DYNAMIC_TOOL_NAME_MAX_LEN: usize = 128;
    const DYNAMIC_TOOL_NAMESPACE_MAX_LEN: usize = 64;
    const DYNAMIC_TOOL_NAMESPACE_DESCRIPTION_MAX_LEN: usize = 1024;
    const DYNAMIC_TOOL_IDENTIFIER_PATTERN: &str = "^[a-zA-Z0-9_-]+$";
    const RESERVED_RESPONSES_NAMESPACES: &[&str] = &[
        "api_tool",
        "browser",
        "computer",
        "container",
        "file_search",
        "functions",
        "image_gen",
        "multi_tool_use",
        "python",
        "python_user_visible",
        "submodel_delegator",
        "terminal",
        "tool_search",
        "web",
    ];

    fn escape_identifier_for_error(value: &str) -> String {
        value.escape_default().to_string()
    }

    fn validate_dynamic_tool_identifier(
        value: &str,
        label: &str,
        max_len: usize,
    ) -> Result<(), String> {
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(format!(
                "{label} must match {DYNAMIC_TOOL_IDENTIFIER_PATTERN} to match Responses API: {}",
                escape_identifier_for_error(value),
            ));
        }
        if value.chars().count() > max_len {
            return Err(format!(
                "{label} must be at most {max_len} characters to match Responses API: {}",
                escape_identifier_for_error(value),
            ));
        }
        Ok(())
    }

    fn validate_dynamic_tool<'a>(
        tool: &'a DynamicToolFunctionSpec,
        namespace: Option<&str>,
        seen: &mut HashSet<&'a str>,
    ) -> Result<(), String> {
        let name = tool.name.trim();
        if name.is_empty() {
            return Err("dynamic tool name must not be empty".to_string());
        }
        if name != tool.name {
            return Err(format!(
                "dynamic tool name has leading/trailing whitespace: {}",
                escape_identifier_for_error(&tool.name),
            ));
        }
        validate_dynamic_tool_identifier(name, "dynamic tool name", DYNAMIC_TOOL_NAME_MAX_LEN)?;
        if name == "mcp" || name.starts_with("mcp__") {
            return Err(format!("dynamic tool name is reserved: {name}"));
        }
        if !seen.insert(name) {
            if let Some(namespace) = namespace {
                return Err(format!(
                    "duplicate dynamic tool name in namespace {namespace}: {name}"
                ));
            }
            return Err(format!("duplicate dynamic tool name: {name}"));
        }
        if tool.defer_loading && namespace.is_none() {
            return Err(format!(
                "deferred dynamic tool must include a namespace: {name}"
            ));
        }

        if let Err(err) = codex_tools::parse_tool_input_schema(&tool.input_schema) {
            return Err(format!(
                "dynamic tool input schema is not supported for {name}: {err}"
            ));
        }
        Ok(())
    }

    let mut seen_tools = HashSet::new();
    let mut seen_namespaces = HashSet::new();
    for spec in tools {
        match spec {
            DynamicToolSpec::Function(tool) => {
                validate_dynamic_tool(tool, /*namespace*/ None, &mut seen_tools)?;
            }
            DynamicToolSpec::Namespace(namespace) => {
                let name = namespace.name.trim();
                if name.is_empty() {
                    return Err("dynamic tool namespace must not be empty".to_string());
                }
                if name != namespace.name {
                    return Err(format!(
                        "dynamic tool namespace has leading/trailing whitespace: {}",
                        escape_identifier_for_error(&namespace.name),
                    ));
                }
                validate_dynamic_tool_identifier(
                    name,
                    "dynamic tool namespace",
                    DYNAMIC_TOOL_NAMESPACE_MAX_LEN,
                )?;
                if namespace.description.chars().count()
                    > DYNAMIC_TOOL_NAMESPACE_DESCRIPTION_MAX_LEN
                {
                    return Err(format!(
                        "dynamic tool namespace description must be at most {DYNAMIC_TOOL_NAMESPACE_DESCRIPTION_MAX_LEN} characters"
                    ));
                }
                if name == "mcp" || name.starts_with("mcp__") {
                    return Err(format!("dynamic tool namespace is reserved: {name}"));
                }
                if RESERVED_RESPONSES_NAMESPACES.contains(&name) {
                    return Err(format!(
                        "dynamic tool namespace collides with a reserved Responses API namespace: {name}",
                    ));
                }
                if !seen_namespaces.insert(name) {
                    return Err(format!("duplicate dynamic tool namespace: {name}"));
                }
                if namespace.tools.is_empty() {
                    return Err(format!(
                        "dynamic tool namespace must contain at least one tool: {name}"
                    ));
                }
                let mut seen_namespace_tools = HashSet::new();
                for tool in &namespace.tools {
                    let DynamicToolNamespaceTool::Function(tool) = tool;
                    validate_dynamic_tool(tool, Some(name), &mut seen_namespace_tools)?;
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct ThreadRequestProcessor {
    pub(super) auth_manager: Arc<AuthManager>,
    pub(super) thread_manager: Arc<ThreadManager>,
    pub(super) outgoing: Arc<OutgoingMessageSender>,
    pub(super) arg0_paths: Arg0DispatchPaths,
    pub(super) config: Arc<Config>,
    pub(super) config_manager: ConfigManager,
    pub(super) thread_store: Arc<dyn ThreadStore>,
    pub(super) pending_thread_unloads: Arc<Mutex<HashSet<ThreadId>>>,
    /// Prevent indexed replay from replacing item IDs already returned by bounded Legacy replay.
    bounded_legacy_history_threads: Arc<Mutex<HashSet<ThreadId>>>,
    /// Keep indexed Legacy item IDs stable after a projected cold resume attaches a live writer.
    indexed_legacy_history_threads: Arc<Mutex<HashSet<ThreadId>>>,
    /// Keep one cursor protocol for Paginated history after serving an unprojected page.
    unprojected_paginated_history_threads: Arc<Mutex<HashSet<ThreadId>>>,
    legacy_displayed_turn_items: Arc<Mutex<LegacyDisplayedTurnItems>>,
    pub(super) thread_state_manager: ThreadStateManager,
    pub(super) thread_watch_manager: ThreadWatchManager,
    pub(super) thread_list_state_permit: Arc<Semaphore>,
    pub(super) fork_handoff_slots: Arc<Semaphore>,
    pub(super) thread_goal_processor: ThreadGoalRequestProcessor,
    pub(super) state_db: Option<StateDbHandle>,
    pub(super) log_db: Option<LogDbLayer>,
    pub(super) background_tasks: TaskTracker,
    pub(super) skills_watcher: Arc<SkillsWatcher>,
    pub(super) turn_cost_worker: Option<crate::turn_cost_worker::TurnCostWorkerHandle>,
    pub(super) initial_config_warnings: Arc<Vec<ConfigWarningNotification>>,
    legacy_page_depth_hints: Arc<Mutex<LegacyPageDepthHints>>,
}

/// Outcome of trying to satisfy a resume request from an already loaded thread.
enum RunningThreadResumeResult {
    /// The request was delegated to the loaded thread.
    Handled,
    /// No loaded thread handled the request.
    ///
    /// The optional stored thread contains the history-bearing probe that cold
    /// resume can reuse instead of reading the rollout again.
    NotRunning(Option<Box<StoredThread>>),
}

impl ThreadRequestProcessor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        auth_manager: Arc<AuthManager>,
        thread_manager: Arc<ThreadManager>,
        outgoing: Arc<OutgoingMessageSender>,
        arg0_paths: Arg0DispatchPaths,
        config: Arc<Config>,
        config_manager: ConfigManager,
        thread_store: Arc<dyn ThreadStore>,
        pending_thread_unloads: Arc<Mutex<HashSet<ThreadId>>>,
        thread_state_manager: ThreadStateManager,
        thread_watch_manager: ThreadWatchManager,
        thread_list_state_permit: Arc<Semaphore>,
        thread_goal_processor: ThreadGoalRequestProcessor,
        state_db: Option<StateDbHandle>,
        log_db: Option<LogDbLayer>,
        skills_watcher: Arc<SkillsWatcher>,
        turn_cost_worker: Option<crate::turn_cost_worker::TurnCostWorkerHandle>,
        initial_config_warnings: Vec<ConfigWarningNotification>,
    ) -> Self {
        Self {
            auth_manager,
            thread_manager,
            outgoing,
            arg0_paths,
            config,
            config_manager,
            thread_store,
            pending_thread_unloads,
            bounded_legacy_history_threads: Arc::default(),
            indexed_legacy_history_threads: Arc::default(),
            unprojected_paginated_history_threads: Arc::default(),
            legacy_displayed_turn_items: Arc::default(),
            thread_state_manager,
            thread_watch_manager,
            thread_list_state_permit,
            fork_handoff_slots: Arc::new(Semaphore::new(2)),
            thread_goal_processor,
            state_db,
            log_db,
            background_tasks: TaskTracker::new(),
            skills_watcher,
            turn_cost_worker,
            initial_config_warnings: Arc::new(initial_config_warnings),
            legacy_page_depth_hints: Arc::default(),
        }
    }

    pub(crate) async fn thread_start(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadStartParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
        request_context: RequestContext,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_start_inner(
            request_id,
            params,
            app_server_client_name,
            app_server_client_version,
            client_mcp_extensions,
            request_context,
        )
        .await
        .map(|()| None)
    }

    pub(crate) async fn thread_unsubscribe(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadUnsubscribeParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_unsubscribe_response_inner(params, request_id.connection_id)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_resume(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadResumeParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_resume_inner(
            request_id,
            params,
            app_server_client_name,
            app_server_client_version,
            client_mcp_extensions,
        )
        .await
        .map(|()| None)
    }

    pub(crate) async fn thread_fork(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadForkParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_fork_inner(
            request_id,
            params,
            app_server_client_name,
            app_server_client_version,
            client_mcp_extensions,
            ForkHandoff::Local,
        )
        .await
        .map(|()| None)
    }

    pub(crate) async fn thread_revert(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadRevertParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let (response, thread_id) = self
            .thread_revert_response(
                &request_id,
                params,
                app_server_client_name,
                app_server_client_version,
            )
            .await?;
        self.outgoing.send_response(request_id, response).await;
        self.outgoing
            .send_server_notification(ServerNotification::ThreadReverted(
                ThreadRevertedNotification { thread_id },
            ))
            .await;
        Ok(None)
    }

    pub(crate) async fn thread_archive(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadArchiveParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        match self.thread_archive_inner(params).await {
            Ok((response, archived_thread_ids)) => {
                self.outgoing
                    .send_response(request_id.clone(), response)
                    .await;
                for thread_id in archived_thread_ids {
                    self.outgoing
                        .send_server_notification(ServerNotification::ThreadArchived(
                            ThreadArchivedNotification { thread_id },
                        ))
                        .await;
                }
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn thread_increment_elicitation(
        &self,
        params: ThreadIncrementElicitationParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_increment_elicitation_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_decrement_elicitation(
        &self,
        params: ThreadDecrementElicitationParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_decrement_elicitation_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_set_name(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadSetNameParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        match self.thread_set_name_response_inner(params).await {
            Ok((response, notification)) => {
                self.outgoing
                    .send_response(request_id.clone(), response)
                    .await;
                if let Some(notification) = notification {
                    self.outgoing
                        .send_server_notification(ServerNotification::ThreadNameUpdated(
                            notification,
                        ))
                        .await;
                }
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn thread_metadata_update(
        &self,
        params: ThreadMetadataUpdateParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_metadata_update_response_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_section_move(
        &self,
        params: ThreadSectionMoveParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let ThreadSectionMoveParams {
            thread_id,
            section_id,
            before_thread_id,
        } = params;
        let thread_uuid = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        if section_id
            .as_deref()
            .is_some_and(|section| section.trim().is_empty())
        {
            return Err(invalid_request("sectionId must not be empty"));
        }
        if section_id.is_none() && before_thread_id.is_some() {
            return Err(invalid_request(
                "beforeThreadId requires a non-null sectionId",
            ));
        }
        let before_thread_uuid = before_thread_id
            .map(|thread_id| {
                ThreadId::from_string(&thread_id)
                    .map_err(|err| invalid_request(format!("invalid before thread id: {err}")))
            })
            .transpose()?;

        {
            let _thread_list_state_permit = self.acquire_thread_list_state_permit().await?;
            self.thread_manager
                .move_thread_to_section(thread_uuid, section_id.as_deref(), before_thread_uuid)
                .await
                .map_err(|err| core_thread_write_error("move thread in section", err))?;
        }

        Ok(Some(ClientResponsePayload::ThreadSectionMove(
            ThreadSectionMoveResponse {},
        )))
    }

    pub(crate) async fn thread_memory_mode_set(
        &self,
        params: ThreadMemoryModeSetParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_memory_mode_set_response_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn memory_reset(
        &self,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.memory_reset_response_inner()
            .await
            .map(|response: MemoryResetResponse| Some(response.into()))
    }

    pub(crate) async fn thread_unarchive(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadUnarchiveParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        match self.thread_unarchive_inner(params).await {
            Ok((response, notification)) => {
                self.outgoing
                    .send_response(request_id.clone(), response)
                    .await;
                self.outgoing
                    .send_server_notification(ServerNotification::ThreadUnarchived(notification))
                    .await;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn thread_compact_start(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadCompactStartParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_compact_start_inner(request_id, params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_background_terminals_clean(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadBackgroundTerminalsCleanParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_background_terminals_clean_inner(request_id, params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_background_terminals_list(
        &self,
        params: ThreadBackgroundTerminalsListParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_background_terminals_list_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_background_terminals_terminate(
        &self,
        params: ThreadBackgroundTerminalsTerminateParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_background_terminals_terminate_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_rollback(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadRollbackParams,
        app_server_client_name: Option<&str>,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        if app_server_client_name != Some(CODEX_TUI_CLIENT_NAME) {
            self.send_deprecation_notice(
                request_id.connection_id,
                THREAD_ROLLBACK_DEPRECATION_SUMMARY,
            )
            .await;
        }
        self.thread_rollback_inner(request_id, params)
            .await
            .map(|()| None)
    }

    async fn send_deprecation_notice(&self, connection_id: ConnectionId, summary: &str) {
        self.outgoing
            .send_server_notification_to_connections(
                &[connection_id],
                ServerNotification::DeprecationNotice(DeprecationNoticeNotification {
                    summary: summary.to_string(),
                    details: None,
                }),
            )
            .await;
    }

    pub(crate) async fn thread_list(
        &self,
        params: ThreadListParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_list_response_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_search(
        &self,
        params: ThreadSearchParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_search_response_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_search_occurrences(
        &self,
        params: ThreadSearchOccurrencesParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_search_occurrences_response_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_loaded_list(
        &self,
        params: ThreadLoadedListParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_loaded_list_response_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_read(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadReadParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let include_turns = params.include_turns;
        let thread_id = params.thread_id.clone();
        let mut response = self.thread_read_response_inner(params).await?;
        if include_turns
            && matches!(
                response.thread.history_mode,
                ApiThreadHistoryMode::Paginated
            )
        {
            self.send_deprecation_notice(
                request_id.connection_id,
                PAGINATED_THREAD_READ_DEPRECATION_SUMMARY,
            )
            .await;
        }
        if include_turns
            && let Ok(thread_id) = ThreadId::from_string(&thread_id)
            && let Some(projection) = self.subagent_history_projection(thread_id).await
        {
            projection.project_turns(&mut response.thread.turns);
        }
        Ok(Some(response.into()))
    }

    pub(crate) async fn thread_turns_list(
        &self,
        params: ThreadTurnsListParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let include_full_items = matches!(params.items_view, Some(TurnItemsView::Full));
        let thread_id = params.thread_id.clone();
        let mut response = self.thread_turns_list_response_inner(params).await?;
        if include_full_items
            && let Ok(thread_id) = ThreadId::from_string(&thread_id)
            && let Some(projection) = self.subagent_history_projection(thread_id).await
        {
            projection.project_turns(&mut response.data);
        }
        Ok(Some(response.into()))
    }

    pub(crate) async fn thread_items_list(
        &self,
        params: ThreadItemsListParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_items_list_response_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_timeline_list(
        &self,
        params: ThreadTimelineListParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|error| invalid_request(format!("invalid thread id: {error}")))?;
        let page = self
            .thread_store
            .list_timeline(StoreListTimelineParams {
                thread_id,
                cursor: params.cursor,
                page_size: params
                    .limit
                    .map(|limit| limit as usize)
                    .unwrap_or(THREAD_ITEMS_DEFAULT_LIMIT)
                    .clamp(1, THREAD_ITEMS_MAX_LIMIT),
            })
            .await
            .map_err(paginated_history_list_error)?;
        Ok(Some(
            ThreadTimelineListResponse {
                data: page.items,
                next_cursor: page.next_cursor,
                active_realtime_session_at_page_start: page.active_realtime_session_at_page_start,
            }
            .into(),
        ))
    }

    /// Builds the compatibility projection used only by full-history app-server responses.
    ///
    /// Any read failure leaves the response unfiltered. Historical APIs must remain readable when
    /// a predecessor segment or the in-memory current-membership source is unavailable.
    async fn subagent_history_projection(
        &self,
        thread_id: ThreadId,
    ) -> Option<SubagentHistoryProjection> {
        let stored_thread = match self
            .thread_store
            .read_thread(StoreReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await
        {
            Ok(stored_thread) => stored_thread,
            Err(err) => {
                warn!("failed to load rollout path for subagent history projection: {err}");
                return None;
            }
        };
        let rollout_path = stored_thread.rollout_path?;
        self.subagent_history_projection_from_rollout(thread_id, rollout_path.as_path())
            .await
    }

    async fn subagent_history_projection_from_rollout(
        &self,
        thread_id: ThreadId,
        rollout_path: &Path,
    ) -> Option<SubagentHistoryProjection> {
        let current_thread_ids = match self
            .thread_manager
            .current_agent_membership_snapshot(thread_id)
            .await
        {
            Ok(snapshot) => snapshot
                .members
                .into_iter()
                .map(|member| member.thread_id)
                .collect::<Vec<_>>(),
            Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => Vec::new(),
            Err(err) => {
                warn!("failed to load current agents for subagent history projection: {err}");
                return None;
            }
        };
        match SubagentHistoryProjection::load(
            self.config.codex_home.as_path(),
            rollout_path,
            thread_id,
            current_thread_ids,
        )
        .await
        {
            Ok(projection) => projection,
            Err(err) => {
                warn!("failed to build subagent history projection: {err}");
                None
            }
        }
    }

    pub(crate) async fn thread_shell_command(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadShellCommandParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_shell_command_inner(request_id, params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn thread_approve_guardian_denied_action(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadApproveGuardianDeniedActionParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_approve_guardian_denied_action_inner(request_id, params)
            .await
            .map(|response| Some(response.into()))
    }

    pub(crate) async fn conversation_summary(
        &self,
        params: GetConversationSummaryParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.get_thread_summary_response_inner(params)
            .await
            .map(|response| Some(response.into()))
    }

    async fn load_thread(
        &self,
        thread_id: &str,
    ) -> Result<(ThreadId, Arc<CodexThread>), JSONRPCErrorError> {
        // Resolve the core conversation handle from a v2 thread id string.
        let thread_id = ThreadId::from_string(thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;

        let thread = self
            .thread_manager
            .get_thread(thread_id)
            .await
            .map_err(|_| invalid_request(format!("thread not found: {thread_id}")))?;

        Ok((thread_id, thread))
    }

    pub(super) async fn acquire_thread_list_state_permit(
        &self,
    ) -> Result<SemaphorePermit<'_>, JSONRPCErrorError> {
        self.thread_list_state_permit
            .acquire()
            .await
            .map_err(|err| {
                internal_error(format!("failed to acquire thread list state permit: {err}"))
            })
    }

    async fn set_app_server_client_info(
        thread: &CodexThread,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
    ) -> Result<(), JSONRPCErrorError> {
        let mcp_elicitations_auto_deny = xcode_26_4_mcp_elicitations_auto_deny(
            app_server_client_name.as_deref(),
            app_server_client_version.as_deref(),
        );
        thread
            .set_app_server_client_info(
                app_server_client_name,
                app_server_client_version,
                mcp_elicitations_auto_deny,
            )
            .await
            .map_err(|err| internal_error(format!("failed to set app server client info: {err}")))
    }

    pub(super) async fn finalize_thread_teardown(&self, thread_id: ThreadId) {
        self.pending_thread_unloads.lock().await.remove(&thread_id);
        self.outgoing
            .cancel_requests_for_thread(thread_id, /*error*/ None)
            .await;
        self.thread_state_manager
            .remove_thread_state(thread_id)
            .await;
        self.thread_watch_manager
            .remove_thread(&thread_id.to_string())
            .await;
    }

    async fn thread_unsubscribe_response_inner(
        &self,
        params: ThreadUnsubscribeParams,
        connection_id: ConnectionId,
    ) -> Result<ThreadUnsubscribeResponse, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;

        if self.thread_manager.get_thread(thread_id).await.is_err() {
            self.finalize_thread_teardown(thread_id).await;
            return Ok(ThreadUnsubscribeResponse {
                status: ThreadUnsubscribeStatus::NotLoaded,
            });
        };

        let was_subscribed = self
            .thread_state_manager
            .unsubscribe_connection_from_thread(thread_id, connection_id)
            .await;

        let status = if was_subscribed {
            ThreadUnsubscribeStatus::Unsubscribed
        } else {
            ThreadUnsubscribeStatus::NotSubscribed
        };
        Ok(ThreadUnsubscribeResponse { status })
    }

    async fn prepare_thread_for_archive(&self, thread_id: ThreadId) {
        self.prepare_thread_for_removal(thread_id, "archive").await;
    }

    pub(super) async fn prepare_thread_for_removal(&self, thread_id: ThreadId, operation: &str) {
        let removed_conversation = self.thread_manager.remove_thread(&thread_id).await;
        if let Some(conversation) = removed_conversation {
            info!("thread {thread_id} was active; shutting down");
            match wait_for_thread_shutdown(&conversation).await {
                ThreadShutdownResult::Complete => {}
                ThreadShutdownResult::SubmitFailed => {
                    error!(
                        "failed to submit Shutdown to thread {thread_id}; proceeding with {operation}"
                    );
                }
                ThreadShutdownResult::TimedOut => {
                    warn!("thread {thread_id} shutdown timed out; proceeding with {operation}");
                }
            }
        }
        self.finalize_thread_teardown(thread_id).await;
    }

    fn listener_task_context(&self) -> ListenerTaskContext {
        ListenerTaskContext {
            thread_manager: Arc::clone(&self.thread_manager),
            thread_state_manager: self.thread_state_manager.clone(),
            outgoing: Arc::clone(&self.outgoing),
            pending_thread_unloads: Arc::clone(&self.pending_thread_unloads),
            thread_watch_manager: self.thread_watch_manager.clone(),
            thread_list_state_permit: self.thread_list_state_permit.clone(),
            fallback_model_provider: self.config.model_provider_id.clone(),
            codex_home: self.config.codex_home.to_path_buf(),
            skills_watcher: Arc::clone(&self.skills_watcher),
            turn_cost_worker: self.turn_cost_worker.clone(),
        }
    }

    async fn ensure_conversation_listener(
        &self,
        conversation_id: ThreadId,
        connection_id: ConnectionId,
        raw_events_enabled: bool,
    ) -> Result<EnsureConversationListenerResult, JSONRPCErrorError> {
        super::thread_lifecycle::ensure_conversation_listener(
            self.listener_task_context(),
            conversation_id,
            connection_id,
            raw_events_enabled,
        )
        .await
    }

    async fn ensure_listener_task_running(
        &self,
        conversation_id: ThreadId,
        conversation: Arc<CodexThread>,
        thread_state: Arc<Mutex<ThreadState>>,
    ) -> Result<(), JSONRPCErrorError> {
        super::thread_lifecycle::ensure_listener_task_running(
            self.listener_task_context(),
            conversation_id,
            conversation,
            thread_state,
        )
        .await
    }

    async fn thread_start_inner(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadStartParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
        request_context: RequestContext,
    ) -> Result<(), JSONRPCErrorError> {
        let ThreadStartParams {
            model,
            model_provider,
            allow_provider_model_fallback,
            service_tier,
            cwd,
            runtime_workspace_roots,
            approval_policy,
            approvals_reviewer,
            sandbox,
            permissions,
            config,
            service_name,
            base_instructions,
            developer_instructions,
            dynamic_tools,
            selected_capability_roots,
            mock_experimental_field: _mock_experimental_field,
            experimental_raw_events,
            personality,
            multi_agent_mode: _multi_agent_mode,
            ephemeral,
            history_mode,
            session_start_source,
            thread_source,
            project_id,
            environments,
        } = params;
        if matches!(
            history_mode,
            Some(codex_app_server_protocol::ThreadHistoryMode::Paginated)
        ) && !self.thread_store.supports_paginated_history_lists()
        {
            return Err(invalid_request(
                "paginated threads require thread/turns/list and thread/items/list support",
            ));
        }
        if sandbox.is_some() && permissions.is_some() {
            return Err(invalid_request(
                "`permissions` cannot be combined with `sandbox`",
            ));
        }
        if let Some(project_id) = project_id.as_ref() {
            if project_id.is_empty() {
                return Err(invalid_request("projectId must not be empty"));
            }
            let project = self
                .thread_store
                .read_project(project_id.clone())
                .await
                .map_err(|err| match err {
                    ThreadStoreError::Unsupported { operation } => {
                        unsupported_thread_store_operation(operation)
                    }
                    err => internal_error(format!("failed to read project: {err}")),
                })?;
            if project.is_none() {
                return Err(invalid_request(format!("project not found: {project_id}")));
            }
        }
        let runtime_workspace_roots = runtime_workspace_roots.map(resolve_runtime_workspace_roots);
        let environments =
            resolve_turn_environment_selections(self.thread_manager.as_ref(), environments)?;
        let mut typesafe_overrides = self.build_thread_config_overrides(
            model,
            model_provider,
            service_tier,
            cwd,
            runtime_workspace_roots,
            approval_policy,
            approvals_reviewer,
            sandbox,
            permissions,
            base_instructions,
            developer_instructions,
            personality,
        );
        typesafe_overrides.ephemeral = ephemeral;
        let listener_task_context = ListenerTaskContext {
            thread_manager: Arc::clone(&self.thread_manager),
            thread_state_manager: self.thread_state_manager.clone(),
            outgoing: Arc::clone(&self.outgoing),
            pending_thread_unloads: Arc::clone(&self.pending_thread_unloads),
            thread_watch_manager: self.thread_watch_manager.clone(),
            thread_list_state_permit: self.thread_list_state_permit.clone(),
            fallback_model_provider: self.config.model_provider_id.clone(),
            codex_home: self.config.codex_home.to_path_buf(),
            skills_watcher: Arc::clone(&self.skills_watcher),
            turn_cost_worker: self.turn_cost_worker.clone(),
        };
        let request_trace = request_context.request_trace();
        let config_manager = self.config_manager.clone();
        let thread_store = Arc::clone(&self.thread_store);
        let initial_config_warnings = Arc::clone(&self.initial_config_warnings);
        let outgoing = Arc::clone(&listener_task_context.outgoing);
        let error_request_id = request_id.clone();
        let thread_start_task = async move {
            if let Err(error) = Self::thread_start_task(
                listener_task_context,
                thread_store,
                config_manager,
                request_id,
                app_server_client_name,
                app_server_client_version,
                client_mcp_extensions,
                config,
                typesafe_overrides,
                dynamic_tools,
                selected_capability_roots.unwrap_or_default(),
                history_mode.map(Into::into),
                session_start_source,
                thread_source.map(Into::into),
                project_id,
                environments,
                service_name,
                allow_provider_model_fallback,
                experimental_raw_events,
                request_trace,
                initial_config_warnings,
            )
            .await
            {
                outgoing.send_error(error_request_id, error).await;
            }
        };
        self.background_tasks
            .spawn(thread_start_task.instrument(request_context.span()));
        Ok(())
    }

    pub(crate) async fn drain_background_tasks(&self) {
        self.background_tasks.close();
        if tokio::time::timeout(Duration::from_secs(10), self.background_tasks.wait())
            .await
            .is_err()
        {
            warn!("timed out waiting for background tasks to shut down; proceeding");
        }
    }

    pub(crate) async fn clear_all_thread_listeners(&self) {
        self.thread_state_manager.clear_all_listeners().await;
    }

    pub(crate) async fn shutdown_threads(&self) {
        let report = self
            .thread_manager
            .shutdown_all_threads_bounded(Duration::from_secs(10))
            .await;
        for thread_id in report.submit_failed {
            warn!("failed to submit Shutdown to thread {thread_id}");
        }
        for thread_id in report.timed_out {
            warn!("timed out waiting for thread {thread_id} to shut down");
        }
    }

    async fn request_trace_context(
        &self,
        request_id: &ConnectionRequestId,
    ) -> Option<codex_protocol::protocol::W3cTraceContext> {
        self.outgoing.request_trace_context(request_id).await
    }

    async fn submit_core_op(
        &self,
        request_id: &ConnectionRequestId,
        thread: &CodexThread,
        op: Op,
    ) -> CodexResult<String> {
        thread
            .submit_with_trace(op, self.request_trace_context(request_id).await)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn thread_start_task(
        listener_task_context: ListenerTaskContext,
        thread_store: Arc<dyn ThreadStore>,
        config_manager: ConfigManager,
        request_id: ConnectionRequestId,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
        config_overrides: Option<HashMap<String, serde_json::Value>>,
        typesafe_overrides: ConfigOverrides,
        dynamic_tools: Option<Vec<DynamicToolSpec>>,
        selected_capability_roots: Vec<SelectedCapabilityRoot>,
        history_mode: Option<ThreadHistoryMode>,
        session_start_source: Option<codex_app_server_protocol::ThreadStartSource>,
        thread_source: Option<codex_protocol::protocol::ThreadSource>,
        project_id: Option<String>,
        environment_selections: Option<Vec<TurnEnvironmentSelection>>,
        service_name: Option<String>,
        allow_provider_model_fallback: bool,
        experimental_raw_events: bool,
        request_trace: Option<W3cTraceContext>,
        initial_config_warnings: Arc<Vec<ConfigWarningNotification>>,
    ) -> Result<(), JSONRPCErrorError> {
        let thread_start_started_at = std::time::Instant::now();
        let requested_cwd = typesafe_overrides.cwd.clone();
        let mut config = config_manager
            .load_with_overrides(config_overrides.clone(), typesafe_overrides.clone())
            .await
            .map_err(|err| config_load_error(&err))?;
        // Project-local config can launch host processes, so only the effective
        // permissions after managed constraints can imply project trust.
        let effective_permission_profile = config.permissions.effective_permission_profile();
        let effective_permissions_trust_project = match &effective_permission_profile {
            codex_protocol::models::PermissionProfile::Disabled
            | codex_protocol::models::PermissionProfile::External { .. } => true,
            codex_protocol::models::PermissionProfile::Managed { .. } => {
                effective_permission_profile
                    .file_system_sandbox_policy()
                    .can_write_path_with_cwd(config.cwd.as_path(), config.cwd.as_path())
            }
        };

        if requested_cwd.is_some()
            && config.active_project.trust_level.is_none()
            && effective_permissions_trust_project
        {
            let trust_target = resolve_root_git_project_for_trust(LOCAL_FS.as_ref(), &config.cwd)
                .await
                .unwrap_or_else(|| config.cwd.clone());
            let current_cli_overrides = config_manager.current_cli_overrides();
            let cli_overrides_with_trust;
            let cli_overrides_for_reload = if let Err(err) =
                codex_core::config::set_project_trust_level(
                    &listener_task_context.codex_home,
                    trust_target.as_path(),
                    TrustLevel::Trusted,
                ) {
                warn!(
                    "failed to persist trusted project state for {}; continuing with in-memory trust for this thread: {err}",
                    trust_target.display()
                );
                let mut project = toml::map::Map::new();
                project.insert(
                    "trust_level".to_string(),
                    TomlValue::String("trusted".to_string()),
                );
                let mut projects = toml::map::Map::new();
                projects.insert(
                    project_trust_key(trust_target.as_path()),
                    TomlValue::Table(project),
                );
                cli_overrides_with_trust = current_cli_overrides
                    .iter()
                    .cloned()
                    .chain(std::iter::once((
                        "projects".to_string(),
                        TomlValue::Table(projects),
                    )))
                    .collect::<Vec<_>>();
                cli_overrides_with_trust.as_slice()
            } else {
                current_cli_overrides.as_slice()
            };

            config = config_manager
                .load_with_cli_overrides(
                    cli_overrides_for_reload,
                    config_overrides,
                    typesafe_overrides,
                    /*fallback_cwd*/ None,
                )
                .await
                .map_err(|err| config_load_error(&err))?;
        }

        if let Ok(Some(err)) =
            codex_core::check_execpolicy_for_warnings(&config.config_layer_stack).await
        {
            let notification = crate::exec_policy_config_warning(&err);
            if !initial_config_warnings.contains(&notification) {
                listener_task_context
                    .outgoing
                    .send_server_notification_to_connections(
                        &[request_id.connection_id],
                        ServerNotification::ConfigWarning(notification),
                    )
                    .await;
            }
        }

        let environments = environment_selections.unwrap_or_else(|| {
            listener_task_context
                .thread_manager
                .default_environment_selections(&config.cwd, &config.workspace_roots)
        });
        let dynamic_tools = dynamic_tools.unwrap_or_default();
        if !dynamic_tools.is_empty() {
            validate_dynamic_tools(&dynamic_tools).map_err(invalid_request)?;
        }
        // Count callable functions rather than top-level namespace containers.
        let dynamic_tool_count: usize = dynamic_tools
            .iter()
            .map(|tool| match tool {
                DynamicToolSpec::Function(_) => 1,
                DynamicToolSpec::Namespace(namespace) => namespace.tools.len(),
            })
            .sum();
        let history_mode = history_mode.or_else(|| {
            (!config.ephemeral && thread_store.supports_paginated_history_lists())
                .then_some(ThreadHistoryMode::Paginated)
        });
        let mut thread_extension_init = ExtensionDataInit::new();
        if !selected_capability_roots.is_empty() {
            thread_extension_init.insert(selected_capability_roots);
        }
        let mut start_options = StartThreadOptions::new(config);
        let reserved_thread_id = if start_options.config.ephemeral {
            None
        } else {
            stage_pending_project_metadata(
                listener_task_context.thread_manager.as_ref(),
                thread_store.as_ref(),
                project_id.as_deref(),
                "thread/start",
            )
            .await?
        };
        start_options.reserved_thread_id = reserved_thread_id;
        let create_thread_started_at = std::time::Instant::now();
        let new_thread = listener_task_context
            .thread_manager
            .start_thread(StartThreadOptions {
                allow_provider_model_fallback,
                initial_history: match session_start_source
                    .unwrap_or(codex_app_server_protocol::ThreadStartSource::Startup)
                {
                    codex_app_server_protocol::ThreadStartSource::Startup => InitialHistory::New,
                    codex_app_server_protocol::ThreadStartSource::Clear => InitialHistory::Cleared,
                },
                history_mode,
                thread_source,
                dynamic_tools,
                metrics_service_name: service_name,
                parent_trace: request_trace,
                environments: Some(environments),
                thread_extension_init,
                client_mcp_extensions,
                ..start_options
            })
            .instrument(tracing::info_span!(
                "app_server.thread_start.create_thread",
                otel.name = "app_server.thread_start.create_thread",
                thread_start.dynamic_tool_count = dynamic_tool_count,
            ))
            .await;
        let NewThread {
            thread_id,
            thread,
            session_configured,
            ..
        } = match new_thread {
            Ok(new_thread) => new_thread,
            Err(err) => {
                remove_pending_project_metadata(thread_store.as_ref(), reserved_thread_id).await;
                return Err(match err.details() {
                    CodexErrorDetails::InvalidRequest(message) => invalid_request(message.clone()),
                    CodexErrorDetails::UnsupportedOperation(message) => {
                        method_not_found(message.clone())
                    }
                    _ => internal_error(format!("error creating thread: {err}")),
                });
            }
        };
        let session_telemetry = thread.session_telemetry();
        session_telemetry.record_startup_phase(
            "thread_start_create_thread",
            create_thread_started_at.elapsed(),
            Some("ready"),
        );

        Self::set_app_server_client_info(
            thread.as_ref(),
            app_server_client_name,
            app_server_client_version,
        )
        .await?;

        let instruction_sources = thread.legacy_instruction_sources().await;
        let config_snapshot = thread
            .config_snapshot()
            .instrument(tracing::info_span!(
                "app_server.thread_start.config_snapshot",
                otel.name = "app_server.thread_start.config_snapshot",
            ))
            .await;
        let mut thread = build_thread_from_snapshot(
            thread_id,
            session_configured.session_id.to_string(),
            thread.multi_agent_version(),
            &config_snapshot,
            session_configured.rollout_path.clone(),
        );
        thread.project_id = project_id.clone();

        // Auto-attach a thread listener when starting a thread.
        log_listener_attach_result(
            super::thread_lifecycle::ensure_conversation_listener(
                listener_task_context.clone(),
                thread_id,
                request_id.connection_id,
                experimental_raw_events,
            )
            .instrument(tracing::info_span!(
                "app_server.thread_start.attach_listener",
                otel.name = "app_server.thread_start.attach_listener",
                thread_start.experimental_raw_events = experimental_raw_events,
            ))
            .await,
            thread_id,
            request_id.connection_id,
            "thread",
        );

        listener_task_context
            .thread_watch_manager
            .upsert_thread_silently(&thread.id)
            .instrument(tracing::info_span!(
                "app_server.thread_start.upsert_thread",
                otel.name = "app_server.thread_start.upsert_thread",
            ))
            .await;

        thread.status = resolve_thread_status(
            listener_task_context
                .thread_watch_manager
                .loaded_status_for_thread(&thread.id)
                .instrument(tracing::info_span!(
                    "app_server.thread_start.resolve_status",
                    otel.name = "app_server.thread_start.resolve_status",
                ))
                .await,
            /*has_in_progress_turn*/ false,
        );

        let sandbox = config_snapshot.sandbox_policy().into();
        let cwd = config_snapshot.cwd().clone();
        let active_permission_profile =
            thread_response_active_permission_profile(config_snapshot.active_permission_profile);
        let thread_originator = config_snapshot.originator.clone();

        let response = ThreadStartResponse {
            thread: thread.clone(),
            model: config_snapshot.model,
            model_provider: config_snapshot.model_provider_id,
            service_tier: config_snapshot.service_tier,
            cwd,
            runtime_workspace_roots: config_snapshot.workspace_roots,
            instruction_sources,
            approval_policy: config_snapshot.approval_policy.into(),
            approvals_reviewer: config_snapshot.approvals_reviewer.into(),
            sandbox,
            active_permission_profile,
            reasoning_effort: config_snapshot.reasoning_effort,
            multi_agent_mode: MultiAgentMode::ExplicitRequestOnly,
        };
        let notif = thread_started_notification(thread);
        listener_task_context
            .outgoing
            .send_response_with_thread_originator(request_id, response, thread_originator)
            .instrument(tracing::info_span!(
                "app_server.thread_start.send_response",
                otel.name = "app_server.thread_start.send_response",
            ))
            .await;

        listener_task_context
            .outgoing
            .send_server_notification(ServerNotification::ThreadStarted(notif))
            .instrument(tracing::info_span!(
                "app_server.thread_start.notify_started",
                otel.name = "app_server.thread_start.notify_started",
            ))
            .await;
        session_telemetry.record_startup_phase(
            "thread_start_total",
            thread_start_started_at.elapsed(),
            Some("ready"),
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_thread_config_overrides(
        &self,
        model: Option<String>,
        model_provider: Option<String>,
        service_tier: Option<Option<String>>,
        cwd: Option<String>,
        runtime_workspace_roots: Option<Vec<AbsolutePathBuf>>,
        approval_policy: Option<codex_app_server_protocol::AskForApproval>,
        approvals_reviewer: Option<codex_app_server_protocol::ApprovalsReviewer>,
        sandbox: Option<SandboxMode>,
        permissions: Option<String>,
        base_instructions: Option<String>,
        developer_instructions: Option<String>,
        personality: Option<Personality>,
    ) -> ConfigOverrides {
        ConfigOverrides {
            model,
            model_provider,
            service_tier,
            cwd: cwd.map(PathBuf::from),
            workspace_roots: runtime_workspace_roots,
            default_permissions: permissions,
            approval_policy: approval_policy
                .map(codex_app_server_protocol::AskForApproval::to_core),
            approvals_reviewer: approvals_reviewer
                .map(codex_app_server_protocol::ApprovalsReviewer::to_core),
            sandbox_mode: sandbox.map(SandboxMode::to_core),
            codex_linux_sandbox_exe: self.arg0_paths.codex_linux_sandbox_exe.clone(),
            main_execve_wrapper_exe: self.arg0_paths.main_execve_wrapper_exe.clone(),
            base_instructions,
            developer_instructions,
            personality,
            ..Default::default()
        }
    }

    async fn thread_archive_inner(
        &self,
        params: ThreadArchiveParams,
    ) -> Result<(ThreadArchiveResponse, Vec<String>), JSONRPCErrorError> {
        let _thread_list_state_permit = self.acquire_thread_list_state_permit().await?;
        self.thread_archive_response(params).await
    }

    async fn thread_archive_response(
        &self,
        params: ThreadArchiveParams,
    ) -> Result<(ThreadArchiveResponse, Vec<String>), JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|err| invalid_request(format!("invalid session id: {err}")))?;

        let current_agent_membership = self
            .thread_manager
            .prepare_current_agent_membership_eviction(thread_id)
            .await
            .map_err(|err| {
                internal_error(format!(
                    "failed to prepare thread subtree {thread_id} for archive: {err}"
                ))
            })?;
        let subtree_thread_ids = current_agent_membership.candidate_thread_ids().to_vec();

        let mut indexed_threads = match self
            .thread_store
            .read_threads(ReadThreadsParams {
                thread_ids: subtree_thread_ids.clone(),
            })
            .await
        {
            Ok(threads) => threads
                .into_iter()
                .map(|thread| (thread.thread_id, thread))
                .collect::<HashMap<_, _>>(),
            Err(err) => {
                warn!("failed to batch archive metadata for {thread_id}: {err}");
                HashMap::new()
            }
        };
        let mut archive_thread_ids = Vec::new();
        let mut already_archived_thread_ids = Vec::new();
        for descendant_thread_id in
            std::iter::once(thread_id).chain(subtree_thread_ids.iter().copied().skip(1))
        {
            // Archive needs current metadata, not migration of every descendant's history.
            let thread = match indexed_threads.remove(&descendant_thread_id) {
                Some(thread) => Ok(thread),
                None => {
                    self.thread_store
                        .read_thread(StoreReadThreadParams {
                            thread_id: descendant_thread_id,
                            include_archived: true,
                            include_history: false,
                        })
                        .await
                }
            };
            match thread {
                Ok(thread) => {
                    if thread.archived_at.is_some() {
                        already_archived_thread_ids.push(descendant_thread_id);
                    } else {
                        archive_thread_ids.push(descendant_thread_id);
                    }
                }
                Err(err) if descendant_thread_id == thread_id => {
                    return Err(thread_store_mutation_error("archive", err));
                }
                Err(ThreadStoreError::ThreadNotFound { .. }) => {}
                Err(err) => {
                    warn!(
                        "failed to read spawned descendant thread {descendant_thread_id} while archiving {thread_id}: {err}"
                    );
                }
            }
        }

        if archive_thread_ids.is_empty() {
            let current_agent_ids_to_evict = current_agent_membership
                .current_ids_with_current_only_descendants(&already_archived_thread_ids);
            if let Err(err) = current_agent_membership
                .evict_exact(&current_agent_ids_to_evict)
                .await
            {
                warn!(
                    "reconciled archived thread {thread_id}, but runtime shutdown reported an error: {err}"
                );
            }
            return Ok((ThreadArchiveResponse {}, Vec::new()));
        }

        if archive_thread_ids.first().copied() == Some(thread_id) {
            archive_thread_ids[1..].reverse();
        } else {
            archive_thread_ids.reverse();
        }
        // Collaboration may resume an archived descendant without unarchiving it.
        for thread_id_to_archive in
            std::iter::once(thread_id).chain(subtree_thread_ids.iter().copied().skip(1).rev())
        {
            let identity_preserved = current_agent_membership
                .unload_candidate_runtime_preserving_identity(thread_id_to_archive)
                .await
                .map_err(|err| {
                    internal_error(format!(
                        "failed to prepare thread {thread_id_to_archive} for archive: {err}"
                    ))
                })?;
            if identity_preserved {
                self.finalize_thread_teardown(thread_id_to_archive).await;
            } else {
                self.prepare_thread_for_archive(thread_id_to_archive).await;
            }
        }

        let archive_result = self
            .thread_store
            .archive_threads(StoreArchiveThreadsParams {
                thread_ids: archive_thread_ids,
                writer_lock_thread_ids: subtree_thread_ids,
            })
            .await;
        let archived_thread_ids = match archive_result {
            Ok(archived_thread_ids) => archived_thread_ids,
            Err(err) => {
                let current_agent_ids_to_evict = current_agent_membership
                    .current_ids_with_current_only_descendants(&already_archived_thread_ids);
                if let Err(cleanup_err) = current_agent_membership
                    .evict_exact(&current_agent_ids_to_evict)
                    .await
                {
                    warn!(
                        "archive failed for thread {thread_id}; prior archived identities were retired, but runtime shutdown reported an error: {cleanup_err}"
                    );
                }
                return Err(thread_store_mutation_error("archive", err));
            }
        };
        let mut current_agent_ids_to_evict = already_archived_thread_ids;
        current_agent_ids_to_evict.extend(archived_thread_ids.iter().copied());
        let current_agent_ids_to_evict = current_agent_membership
            .current_ids_with_current_only_descendants(&current_agent_ids_to_evict);
        if let Err(err) = current_agent_membership
            .evict_exact(&current_agent_ids_to_evict)
            .await
        {
            warn!(
                "archived thread {thread_id} and retired its current identities, but runtime shutdown reported an error: {err}"
            );
        }
        let archived_thread_ids = archived_thread_ids
            .into_iter()
            .map(|thread_id| thread_id.to_string())
            .collect();
        Ok((ThreadArchiveResponse {}, archived_thread_ids))
    }

    async fn thread_increment_elicitation_inner(
        &self,
        params: ThreadIncrementElicitationParams,
    ) -> Result<ThreadIncrementElicitationResponse, JSONRPCErrorError> {
        let (_, thread) = self.load_thread(&params.thread_id).await?;
        let count = thread
            .increment_out_of_band_elicitation_count()
            .await
            .map_err(|err| {
                internal_error(format!(
                    "failed to increment out-of-band elicitation counter: {err}"
                ))
            })?;
        Ok(ThreadIncrementElicitationResponse {
            count,
            paused: count > 0,
        })
    }

    async fn thread_decrement_elicitation_inner(
        &self,
        params: ThreadDecrementElicitationParams,
    ) -> Result<ThreadDecrementElicitationResponse, JSONRPCErrorError> {
        let (_, thread) = self.load_thread(&params.thread_id).await?;
        let count = thread
            .decrement_out_of_band_elicitation_count()
            .await
            .map_err(|err| match err.details() {
                CodexErrorDetails::InvalidRequest(message) => invalid_request(message.clone()),
                _ => internal_error(format!(
                    "failed to decrement out-of-band elicitation counter: {err}"
                )),
            })?;
        Ok(ThreadDecrementElicitationResponse {
            count,
            paused: count > 0,
        })
    }

    async fn thread_set_name_response_inner(
        &self,
        params: ThreadSetNameParams,
    ) -> Result<(ThreadSetNameResponse, Option<ThreadNameUpdatedNotification>), JSONRPCErrorError>
    {
        let ThreadSetNameParams { thread_id, name } = params;
        let thread_id = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        let Some(name) = codex_core::util::normalize_thread_name(&name) else {
            return Err(invalid_request("thread name must not be empty"));
        };

        let _thread_list_state_permit = self.acquire_thread_list_state_permit().await?;
        self.thread_manager
            .update_thread_metadata(
                thread_id,
                StoreThreadMetadataPatch {
                    name: Some(Some(name.clone())),
                    ..Default::default()
                },
                /*include_archived*/ false,
            )
            .await
            .map_err(|err| core_thread_write_error("set thread name", err))?;

        Ok((
            ThreadSetNameResponse {},
            Some(ThreadNameUpdatedNotification {
                thread_id: thread_id.to_string(),
                thread_name: Some(name),
            }),
        ))
    }

    async fn thread_memory_mode_set_response_inner(
        &self,
        params: ThreadMemoryModeSetParams,
    ) -> Result<ThreadMemoryModeSetResponse, JSONRPCErrorError> {
        let ThreadMemoryModeSetParams { thread_id, mode } = params;
        let thread_id = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;

        self.thread_manager
            .update_thread_metadata(
                thread_id,
                StoreThreadMetadataPatch {
                    memory_mode: Some(mode.to_core()),
                    ..Default::default()
                },
                /*include_archived*/ false,
            )
            .await
            .map_err(|err| core_thread_write_error("set thread memory mode", err))?;

        Ok(ThreadMemoryModeSetResponse {})
    }

    async fn memory_reset_response_inner(&self) -> Result<MemoryResetResponse, JSONRPCErrorError> {
        let state_db = self
            .state_db
            .clone()
            .ok_or_else(|| internal_error("sqlite state db unavailable for memory reset"))?;

        state_db
            .memories()
            .clear_memory_data()
            .await
            .map_err(|err| {
                internal_error(format!("failed to clear memory rows in memories db: {err}"))
            })?;

        clear_memory_roots_contents(&self.config.codex_home)
            .await
            .map_err(|err| {
                internal_error(format!(
                    "failed to clear memory directories under {}: {err}",
                    self.config.codex_home.display()
                ))
            })?;

        Ok(MemoryResetResponse {})
    }

    async fn thread_metadata_update_response_inner(
        &self,
        params: ThreadMetadataUpdateParams,
    ) -> Result<ThreadMetadataUpdateResponse, JSONRPCErrorError> {
        let ThreadMetadataUpdateParams {
            thread_id,
            project_id,
            git_info,
        } = params;
        let thread_uuid = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;

        if let Some(project_id) = project_id.as_ref()
            && !project_id.is_empty()
        {
            let project = self
                .thread_store
                .read_project(project_id.clone())
                .await
                .map_err(|err| match err {
                    ThreadStoreError::Unsupported { operation } => {
                        unsupported_thread_store_operation(operation)
                    }
                    err => internal_error(format!("failed to read project: {err}")),
                })?;
            if project.is_none() {
                return Err(invalid_request(format!("project not found: {project_id}")));
            }
        }

        if git_info.is_none() && project_id.is_none() {
            return Err(invalid_request(
                "thread metadata update must include at least one field",
            ));
        }

        let git_info = git_info
            .map(
                |ThreadMetadataGitInfoUpdateParams {
                     sha,
                     branch,
                     origin_url,
                 }| {
                    if sha.is_none() && branch.is_none() && origin_url.is_none() {
                        return Err(invalid_request("gitInfo must include at least one field"));
                    }

                    let origin_url =
                        Self::normalize_thread_metadata_git_field(origin_url, "gitInfo.originUrl")?;
                    let origin_url = match origin_url {
                        Some(Some(origin_url)) => {
                            Some(Some(SanitizedGitUrl::try_from(origin_url).map_err(
                                |_| invalid_request("gitInfo.originUrl must be a valid Git remote"),
                            )?))
                        }
                        Some(None) => Some(None),
                        None => None,
                    };

                    Ok(StoreGitInfoPatch {
                        sha: Self::normalize_thread_metadata_git_field(sha, "gitInfo.sha")?,
                        branch: Self::normalize_thread_metadata_git_field(
                            branch,
                            "gitInfo.branch",
                        )?,
                        origin_url,
                    })
                },
            )
            .transpose()?;

        let project_update: StoreClearableField<String> = if let Some(project_id) = project_id {
            if project_id.is_empty() {
                Some(None)
            } else {
                Some(Some(project_id))
            }
        } else {
            None
        };
        let updated_thread = {
            let _thread_list_state_permit = self.acquire_thread_list_state_permit().await?;
            let previous_project_id = if project_update.is_some() {
                Some(
                    self.thread_store
                        .read_thread(StoreReadThreadParams {
                            thread_id: thread_uuid,
                            include_archived: true,
                            include_history: false,
                        })
                        .await
                        .map_err(|err| match err {
                            ThreadStoreError::ThreadNotFound { .. } => {
                                invalid_request(format!("thread not found: {thread_id}"))
                            }
                            ThreadStoreError::Unsupported { operation } => {
                                unsupported_thread_store_operation(operation)
                            }
                            err => internal_error(format!("failed to read thread metadata: {err}")),
                        })?
                        .project_id,
                )
            } else {
                None
            };
            let patch = StoreThreadMetadataPatch {
                git_info,
                project_id: project_update.clone(),
                ..Default::default()
            };
            let updated_thread = self
                .thread_manager
                .update_thread_metadata(thread_uuid, patch, /*include_archived*/ true)
                .await
                .map_err(|err| core_thread_write_error("update thread metadata", err))?;
            if let Some(project_id) = project_update.as_ref()
                && previous_project_id.as_ref() != Some(project_id)
            {
                self.outgoing
                    .send_server_notification(ServerNotification::ThreadProjectUpdated(
                        ThreadProjectUpdatedNotification {
                            thread_id: thread_id.clone(),
                            project_id: project_id.clone(),
                        },
                    ))
                    .await;
            }
            updated_thread
        };
        let (mut thread, _) = thread_from_stored_thread(
            updated_thread,
            self.config.model_provider_id.as_str(),
            &self.config.cwd,
        );
        if let Ok(loaded_thread) = self.thread_manager.get_thread(thread_uuid).await {
            thread.session_id = loaded_thread.session_configured().session_id.to_string();
        }
        self.attach_thread_name(thread_uuid, &mut thread).await;
        thread.status = resolve_thread_status(
            self.thread_watch_manager
                .loaded_status_for_thread(&thread.id)
                .await,
            /*has_in_progress_turn*/ false,
        );

        Ok(ThreadMetadataUpdateResponse { thread })
    }

    fn normalize_thread_metadata_git_field(
        value: Option<Option<String>>,
        name: &str,
    ) -> Result<Option<Option<String>>, JSONRPCErrorError> {
        match value {
            Some(Some(value)) => {
                let value = value.trim().to_string();
                if value.is_empty() {
                    return Err(invalid_request(format!("{name} must not be empty")));
                }
                Ok(Some(Some(value)))
            }
            Some(None) => Ok(Some(None)),
            None => Ok(None),
        }
    }

    async fn thread_unarchive_inner(
        &self,
        params: ThreadUnarchiveParams,
    ) -> Result<(ThreadUnarchiveResponse, ThreadUnarchivedNotification), JSONRPCErrorError> {
        let _thread_list_state_permit = self.acquire_thread_list_state_permit().await?;
        let (response, thread_id) = self.thread_unarchive_response(params).await?;
        Ok((response, ThreadUnarchivedNotification { thread_id }))
    }

    async fn thread_unarchive_response(
        &self,
        params: ThreadUnarchiveParams,
    ) -> Result<(ThreadUnarchiveResponse, String), JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|err| invalid_request(format!("invalid session id: {err}")))?;

        let fallback_provider = self.config.model_provider_id.clone();
        let stored_thread = self
            .thread_store
            .unarchive_thread(StoreArchiveThreadParams { thread_id })
            .await
            .map_err(|err| thread_store_mutation_error("unarchive", err))?;
        let (mut thread, _) =
            thread_from_stored_thread(stored_thread, fallback_provider.as_str(), &self.config.cwd);

        thread.status = resolve_thread_status(
            self.thread_watch_manager
                .loaded_status_for_thread(&thread.id)
                .await,
            /*has_in_progress_turn*/ false,
        );
        self.attach_thread_name(thread_id, &mut thread).await;
        let thread_id = thread.id.clone();
        Ok((ThreadUnarchiveResponse { thread }, thread_id))
    }

    async fn thread_rollback_inner(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadRollbackParams,
    ) -> Result<(), JSONRPCErrorError> {
        self.thread_rollback_start(request_id, params).await
    }

    async fn thread_revert_response(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadRevertParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
    ) -> Result<(ThreadRevertResponse, String), JSONRPCErrorError> {
        let _thread_list_state_permit = self.acquire_thread_list_state_permit().await?;
        let ThreadRevertParams {
            thread_id,
            before_turn_id,
        } = params;
        let (thread_id, thread) = self.load_thread(&thread_id).await?;
        ensure_direct_input_allowed(thread.as_ref()).await?;
        let config_snapshot = thread.config_snapshot().await;
        if !matches!(config_snapshot.history_mode, ThreadHistoryMode::Paginated) {
            return Err(invalid_request(
                "thread/revert only supports paginated threads",
            ));
        }
        let runtime_snapshot = ThreadRevertRuntimeSnapshot {
            config: thread.config().await.as_ref().clone(),
            settings: thread.restorable_thread_settings().await,
            client_mcp_extensions: thread.client_mcp_extensions(),
        };

        // Subscribe before shutdown so a pending idle unload either rejects this request or can
        // no longer race the replacement runtime. The same listener then drains Core's shutdown
        // events before we replace it.
        if matches!(
            self.ensure_conversation_listener(
                thread_id,
                request_id.connection_id,
                /*raw_events_enabled*/ false,
            )
            .await?,
            EnsureConversationListenerResult::ConnectionClosed
        ) {
            return Err(internal_error(format!(
                "connection closed before thread {thread_id} could be reverted"
            )));
        }
        let thread_state = self.thread_state_manager.thread_state(thread_id).await;
        let shutdown_drain_rx = thread_state.lock().await.register_shutdown_drain_waiter();

        match wait_for_thread_shutdown(&thread).await {
            ThreadShutdownResult::Complete => {}
            ThreadShutdownResult::SubmitFailed => {
                thread_state.lock().await.take_shutdown_drain_waiter();
                return Err(internal_error(format!(
                    "failed to shut down thread {thread_id} before revert"
                )));
            }
            ThreadShutdownResult::TimedOut => {
                thread_state.lock().await.take_shutdown_drain_waiter();
                return Err(internal_error(format!(
                    "timed out shutting down thread {thread_id} before revert"
                )));
            }
        }
        let drain_result = tokio::time::timeout(Duration::from_secs(10), shutdown_drain_rx)
            .await
            .map_err(|_| {
                internal_error(format!(
                    "timed out waiting for thread {thread_id} listener to drain shutdown events"
                ))
            })
            .and_then(|result| {
                result.map_err(|_| {
                    internal_error(format!(
                        "thread {thread_id} listener stopped before draining shutdown events"
                    ))
                })
            });
        if let Err(err) = drain_result {
            thread_state.lock().await.take_shutdown_drain_waiter();
            return Err(err);
        }
        if self
            .thread_manager
            .remove_thread(&thread_id)
            .await
            .is_none()
        {
            return Err(internal_error(format!(
                "thread {thread_id} disappeared before revert"
            )));
        }
        // Keep thread state and subscriptions across the internal reload. Full teardown would
        // force clients to call thread/resume after a successful revert.
        self.outgoing
            .cancel_requests_for_thread(thread_id, /*error*/ None)
            .await;

        let revert_result = self
            .thread_store
            .revert_thread(codex_thread_store::RevertThreadParams {
                thread_id,
                before_turn_id,
            })
            .await
            .map_err(|err| thread_store_mutation_error("revert", err));
        let response = self
            .reload_paginated_thread(
                request_id,
                thread_id,
                runtime_snapshot,
                app_server_client_name,
                app_server_client_version,
            )
            .await?;
        revert_result?;
        Ok((response, thread_id.to_string()))
    }

    async fn reload_paginated_thread(
        &self,
        request_id: &ConnectionRequestId,
        thread_id: ThreadId,
        runtime_snapshot: ThreadRevertRuntimeSnapshot,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
    ) -> Result<ThreadRevertResponse, JSONRPCErrorError> {
        let ThreadRevertRuntimeSnapshot {
            config,
            settings,
            client_mcp_extensions,
        } = runtime_snapshot;
        let thread_id_string = thread_id.to_string();
        let stored_thread = self
            .read_stored_thread_for_resume(
                thread_id_string.as_str(),
                /*path*/ None,
                /*include_history*/ false,
            )
            .await?;
        let (thread_history, resume_source_thread) = self
            .load_resume_initial_history_from_stored_thread(stored_thread)
            .await?;
        let response_history = thread_history.clone();
        let NewThread {
            thread_id: resumed_thread_id,
            thread: codex_thread,
            session_configured,
            ..
        } = self
            .thread_manager
            .resume_thread_with_history(
                config,
                thread_history,
                self.auth_manager.clone(),
                self.request_trace_context(request_id).await,
                client_mcp_extensions,
            )
            .await
            .map_err(|err| internal_error(format!("error reloading thread after revert: {err}")))?;
        if resumed_thread_id != thread_id {
            return Err(internal_error(format!(
                "thread {thread_id} reloaded as {resumed_thread_id} after revert"
            )));
        }
        codex_thread
            .restore_thread_settings(settings)
            .await
            .map_err(|err| {
                internal_error(format!(
                    "failed to restore thread settings after revert: {err}"
                ))
            })?;
        Self::set_app_server_client_info(
            codex_thread.as_ref(),
            app_server_client_name,
            app_server_client_version,
        )
        .await?;
        let SessionConfiguredEvent { rollout_path, .. } = session_configured;
        let rollout_path = rollout_path.ok_or_else(|| {
            internal_error(format!(
                "rollout path missing after reloading thread {thread_id}"
            ))
        })?;
        // Revert keeps the existing thread state and subscriptions across the internal reload.
        // Start the replacement listener from that state instead of depending on the requesting
        // connection still being open.
        let thread_state = self.thread_state_manager.thread_state(thread_id).await;
        self.ensure_listener_task_running(thread_id, Arc::clone(&codex_thread), thread_state)
            .await?;
        let mut thread = self
            .load_thread_from_resume_source_or_send_internal(
                thread_id,
                codex_thread.as_ref(),
                &response_history,
                rollout_path.as_path(),
                Some(resume_source_thread),
                /*include_turns*/ false,
            )
            .await
            .map_err(internal_error)?;
        self.thread_watch_manager.upsert_thread(&thread.id).await;
        let thread_status = self
            .thread_watch_manager
            .loaded_status_for_thread(&thread.id)
            .await;
        set_thread_status_and_interrupt_stale_turns(
            &mut thread,
            thread_status,
            /*has_live_in_progress_turn*/ false,
        );
        let (turns_backwards_cursor, items_backwards_cursor) =
            Self::paginated_resume_backwards_cursors(self.thread_store.as_ref(), thread_id).await?;
        Ok(ThreadRevertResponse {
            thread,
            turns_backwards_cursor,
            items_backwards_cursor,
        })
    }

    async fn thread_rollback_start(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadRollbackParams,
    ) -> Result<(), JSONRPCErrorError> {
        let ThreadRollbackParams {
            thread_id,
            num_turns,
        } = params;

        if num_turns == 0 {
            return Err(invalid_request("numTurns must be >= 1"));
        }

        let (thread_id, thread) = self.load_thread(&thread_id).await?;
        ensure_direct_input_allowed(thread.as_ref()).await?;
        if matches!(
            thread.config_snapshot().await.history_mode,
            ThreadHistoryMode::Paginated
        ) {
            return Err(invalid_request(
                "paginated threads do not support thread/rollback",
            ));
        }

        let request = request_id.clone();

        let rollback_already_in_progress = {
            let thread_state = self.thread_state_manager.thread_state(thread_id).await;
            let mut thread_state = thread_state.lock().await;
            if thread_state.pending_rollbacks.is_some() {
                true
            } else {
                thread_state.pending_rollbacks = Some(request.clone());
                false
            }
        };
        if rollback_already_in_progress {
            return Err(invalid_request(
                "rollback already in progress for this thread",
            ));
        }

        if let Err(err) = self
            .submit_core_op(
                request_id,
                thread.as_ref(),
                Op::ThreadRollback { num_turns },
            )
            .await
        {
            // No ThreadRollback event will arrive if an error occurs.
            // Clean up and reply immediately.
            let thread_state = self.thread_state_manager.thread_state(thread_id).await;
            thread_state.lock().await.pending_rollbacks = None;

            return Err(internal_error(format!("failed to start rollback: {err}")));
        }
        Ok(())
    }

    async fn thread_compact_start_inner(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadCompactStartParams,
    ) -> Result<ThreadCompactStartResponse, JSONRPCErrorError> {
        let ThreadCompactStartParams { thread_id } = params;

        let (_, thread) = self.load_thread(&thread_id).await?;
        ensure_direct_input_allowed(thread.as_ref()).await?;
        self.submit_core_op(request_id, thread.as_ref(), Op::Compact)
            .await
            .map_err(|err| internal_error(format!("failed to start compaction: {err}")))?;
        Ok(ThreadCompactStartResponse {})
    }

    async fn thread_background_terminals_clean_inner(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadBackgroundTerminalsCleanParams,
    ) -> Result<ThreadBackgroundTerminalsCleanResponse, JSONRPCErrorError> {
        let ThreadBackgroundTerminalsCleanParams { thread_id } = params;

        let (_, thread) = self.load_thread(&thread_id).await?;
        self.submit_core_op(request_id, thread.as_ref(), Op::CleanBackgroundTerminals)
            .await
            .map_err(|err| {
                internal_error(format!("failed to clean background terminals: {err}"))
            })?;
        Ok(ThreadBackgroundTerminalsCleanResponse {})
    }

    async fn thread_background_terminals_list_inner(
        &self,
        params: ThreadBackgroundTerminalsListParams,
    ) -> Result<ThreadBackgroundTerminalsListResponse, JSONRPCErrorError> {
        let ThreadBackgroundTerminalsListParams {
            thread_id,
            cursor,
            limit,
        } = params;

        let (_, thread) = self.load_thread(&thread_id).await?;
        let terminals = thread
            .list_background_terminals()
            .await
            .into_iter()
            .map(|terminal| ThreadBackgroundTerminal {
                item_id: terminal.item_id,
                process_id: terminal.process_id,
                command: terminal.command,
                cwd: terminal.cwd.into(),
                os_pid: None,
                cpu_percent: None,
                rss_kb: None,
            })
            .collect::<Vec<_>>();

        let (data, next_cursor) = paginate_background_terminals(&terminals, cursor, limit)?;

        Ok(ThreadBackgroundTerminalsListResponse { data, next_cursor })
    }

    async fn thread_background_terminals_terminate_inner(
        &self,
        params: ThreadBackgroundTerminalsTerminateParams,
    ) -> Result<ThreadBackgroundTerminalsTerminateResponse, JSONRPCErrorError> {
        let ThreadBackgroundTerminalsTerminateParams {
            thread_id,
            process_id,
        } = params;
        let process_id = process_id.parse::<i32>().map_err(|err| {
            invalid_request(format!("invalid background terminal process id: {err}"))
        })?;

        let (_, thread) = self.load_thread(&thread_id).await?;
        let terminated = thread.terminate_background_terminal(process_id).await;
        Ok(ThreadBackgroundTerminalsTerminateResponse { terminated })
    }

    async fn thread_shell_command_inner(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadShellCommandParams,
    ) -> Result<ThreadShellCommandResponse, JSONRPCErrorError> {
        let ThreadShellCommandParams {
            thread_id,
            command,
            timeout_ms,
        } = params;
        let command = command.trim().to_string();
        if command.is_empty() {
            return Err(invalid_request("command must not be empty"));
        }

        let timeout_ms = timeout_ms
            .map(|timeout_ms| {
                u64::try_from(timeout_ms).map_err(|_| {
                    invalid_params(format!(
                        "thread/shellCommand timeoutMs must be non-negative, got {timeout_ms}"
                    ))
                })
            })
            .transpose()?;

        let (_, thread) = self.load_thread(&thread_id).await?;
        ensure_direct_input_allowed(thread.as_ref()).await?;

        // `thread/shellCommand` is app-server's local-host shell escape hatch,
        // not the normal turn-selected shell tool path.
        if self
            .thread_manager
            .environment_manager()
            .try_local_environment()
            .is_none()
        {
            return Err(internal_error("local environment is not configured"));
        }

        self.submit_core_op(
            request_id,
            thread.as_ref(),
            Op::RunUserShellCommand {
                command,
                timeout_ms,
            },
        )
        .await
        .map_err(|err| internal_error(format!("failed to start shell command: {err}")))?;
        Ok(ThreadShellCommandResponse {})
    }

    async fn thread_approve_guardian_denied_action_inner(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadApproveGuardianDeniedActionParams,
    ) -> Result<ThreadApproveGuardianDeniedActionResponse, JSONRPCErrorError> {
        let ThreadApproveGuardianDeniedActionParams { thread_id, event } = params;
        let event = serde_json::from_value(event)
            .map_err(|err| invalid_request(format!("invalid Guardian denial event: {err}")))?;
        let (_, thread) = self.load_thread(&thread_id).await?;
        ensure_direct_input_allowed(thread.as_ref()).await?;

        self.submit_core_op(
            request_id,
            thread.as_ref(),
            Op::ApproveGuardianDeniedAction { event },
        )
        .await
        .map_err(|err| internal_error(format!("failed to approve Guardian denial: {err}")))?;
        Ok(ThreadApproveGuardianDeniedActionResponse {})
    }

    async fn thread_list_response_inner(
        &self,
        params: ThreadListParams,
    ) -> Result<ThreadListResponse, JSONRPCErrorError> {
        let ThreadListParams {
            cursor,
            limit,
            sort_key,
            sort_direction,
            model_providers,
            source_kinds,
            archived,
            section_id,
            project_id,
            cwd,
            use_state_db_only,
            search_term,
            parent_thread_id,
            ancestor_thread_id,
        } = params;
        if project_id.is_some() && !self.thread_store.supports_projects() {
            return Err(unsupported_thread_store_operation("projects"));
        }
        if let Some(Some(project_id)) = project_id.as_ref() {
            if project_id.is_empty() {
                return Err(invalid_params("projectId must not be empty"));
            }
            match self.thread_store.read_project(project_id.clone()).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Err(invalid_params(format!("project not found: {project_id}")));
                }
                Err(ThreadStoreError::Unsupported { operation }) => {
                    return Err(unsupported_thread_store_operation(operation));
                }
                Err(err) => {
                    return Err(internal_error(format!("failed to read project: {err}")));
                }
            }
        }
        let cwd_filters = normalize_thread_list_cwd_filters(cwd)?;
        let (relation_filter, relation_root_id, direct_children_only) =
            match (parent_thread_id, ancestor_thread_id) {
                (Some(_), Some(_)) => {
                    return Err(invalid_request(
                        "parentThreadId and ancestorThreadId are mutually exclusive",
                    ));
                }
                (Some(parent_thread_id), None) => {
                    let parent_thread_id =
                        ThreadId::from_string(&parent_thread_id).map_err(|err| {
                            invalid_request(format!("invalid parent thread id: {err}"))
                        })?;
                    (
                        Some(StoreThreadRelationFilter::DirectChildrenOf(
                            parent_thread_id,
                        )),
                        Some(parent_thread_id),
                        true,
                    )
                }
                (None, Some(ancestor_thread_id)) => {
                    let ancestor_thread_id =
                        ThreadId::from_string(&ancestor_thread_id).map_err(|err| {
                            invalid_request(format!("invalid ancestor thread id: {err}"))
                        })?;
                    (
                        Some(StoreThreadRelationFilter::DescendantsOf(ancestor_thread_id)),
                        Some(ancestor_thread_id),
                        false,
                    )
                }
                (None, None) => (None, None, false),
            };

        let current_agent_members = if let Some(relation_root_id) = relation_root_id {
            match self
                .thread_manager
                .current_agent_membership_snapshot(relation_root_id)
                .await
            {
                Ok(snapshot) => Some(snapshot.members),
                Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => {
                    Some(Vec::new())
                }
                Err(err) => {
                    return Err(internal_error(format!(
                        "failed to list current agents for thread {relation_root_id}: {err}"
                    )));
                }
            }
        } else {
            None
        };

        let requested_page_size = limit
            .map(|value| value as usize)
            .unwrap_or(THREAD_LIST_DEFAULT_LIMIT)
            .clamp(1, THREAD_LIST_MAX_LIMIT);
        let store_sort_key = match sort_key.unwrap_or(ThreadSortKey::CreatedAt) {
            ThreadSortKey::CreatedAt => StoreThreadSortKey::CreatedAt,
            ThreadSortKey::UpdatedAt => StoreThreadSortKey::UpdatedAt,
            ThreadSortKey::RecencyAt => StoreThreadSortKey::RecencyAt,
            ThreadSortKey::SectionPosition => StoreThreadSortKey::SectionPosition,
        };
        let sort_direction = sort_direction.unwrap_or(match store_sort_key {
            StoreThreadSortKey::SectionPosition => SortDirection::Asc,
            StoreThreadSortKey::CreatedAt
            | StoreThreadSortKey::UpdatedAt
            | StoreThreadSortKey::RecencyAt => SortDirection::Desc,
        });
        if let (Some(root_thread_id), Some(members)) = (relation_root_id, current_agent_members) {
            return self
                .current_agent_thread_list_response(CurrentAgentThreadListParams {
                    root_thread_id,
                    direct_children_only,
                    members,
                    cursor,
                    limit: requested_page_size,
                    sort_key: store_sort_key,
                    sort_direction,
                    model_providers,
                    source_kinds,
                    archived,
                    section_id,
                    cwd_filters,
                    search_term,
                })
                .await;
        }
        let (stored_threads, next_cursor) = self
            .list_threads_common(
                requested_page_size,
                cursor,
                store_sort_key,
                sort_direction,
                ThreadListFilters {
                    model_providers,
                    source_kinds,
                    archived: archived.unwrap_or(false),
                    section_id,
                    project_id,
                    cwd_filters,
                    search_term,
                    use_state_db_only,
                    relation_filter,
                },
            )
            .await?;
        let backwards_cursor = stored_threads.first().and_then(|thread| {
            thread_backwards_cursor_for_sort_key(thread, store_sort_key, sort_direction)
        });
        let mut data = Vec::with_capacity(stored_threads.len());
        let fallback_provider = self.config.model_provider_id.clone();

        for stored_thread in stored_threads {
            let (thread, _) = thread_from_stored_thread(
                stored_thread,
                fallback_provider.as_str(),
                &self.config.cwd,
            );
            data.push(thread);
        }

        enrich_loaded_threads(
            &self.thread_manager,
            &self.thread_watch_manager,
            data.as_mut_slice(),
            |thread| thread,
        )
        .await;
        Ok(ThreadListResponse {
            data,
            next_cursor,
            backwards_cursor,
        })
    }

    async fn thread_search_response_inner(
        &self,
        params: ThreadSearchParams,
    ) -> Result<ThreadSearchResponse, JSONRPCErrorError> {
        let ThreadSearchParams {
            cursor,
            limit,
            sort_key,
            sort_direction,
            source_kinds,
            archived,
            search_term,
        } = params;
        let search_term = search_term.trim().to_string();
        let search_term = (!search_term.is_empty())
            .then_some(search_term)
            .ok_or_else(|| invalid_request("thread/search requires a non-empty searchTerm"))?;
        let requested_page_size = limit
            .map(|value| value as usize)
            .unwrap_or(THREAD_LIST_DEFAULT_LIMIT)
            .clamp(1, THREAD_LIST_MAX_LIMIT);
        let store_sort_key = match sort_key.unwrap_or(ThreadSearchSortKey::CreatedAt) {
            ThreadSearchSortKey::CreatedAt => StoreThreadSortKey::CreatedAt,
            ThreadSearchSortKey::UpdatedAt => StoreThreadSortKey::UpdatedAt,
            ThreadSearchSortKey::RecencyAt => StoreThreadSortKey::RecencyAt,
        };
        let store_sort_direction = sort_direction.unwrap_or(SortDirection::Desc);
        let (allowed_sources, source_kind_filter) = compute_source_filters(source_kinds);
        let mut cursor_obj = cursor;
        let mut last_cursor = cursor_obj.clone();
        let mut remaining = requested_page_size;
        let mut search_results = Vec::with_capacity(requested_page_size);
        let mut next_cursor = None;

        while remaining > 0 {
            let page = self
                .thread_store
                .search_threads(StoreSearchThreadsParams {
                    page_size: remaining.min(THREAD_LIST_MAX_LIMIT),
                    cursor: cursor_obj.clone(),
                    sort_key: store_sort_key,
                    sort_direction: match store_sort_direction {
                        SortDirection::Asc => StoreSortDirection::Asc,
                        SortDirection::Desc => StoreSortDirection::Desc,
                    },
                    allowed_sources: allowed_sources.clone(),
                    archived: archived.unwrap_or(false),
                    search_term: search_term.clone(),
                })
                .await
                .map_err(thread_store_list_error)?;

            for result in page.items {
                let source = with_thread_spawn_agent_metadata(
                    result.thread.source.clone(),
                    result.thread.agent_nickname.clone(),
                    result.thread.agent_role.clone(),
                );
                if source_kind_filter
                    .as_ref()
                    .is_none_or(|filter| source_kind_matches(&source, filter))
                {
                    search_results.push(result);
                    if search_results.len() >= requested_page_size {
                        break;
                    }
                }
            }

            remaining = requested_page_size.saturating_sub(search_results.len());
            next_cursor = page.next_cursor;
            if remaining == 0 {
                break;
            }

            let Some(cursor_val) = next_cursor.clone() else {
                break;
            };
            if last_cursor.as_ref() == Some(&cursor_val) {
                next_cursor = None;
                break;
            }
            last_cursor = Some(cursor_val.clone());
            cursor_obj = Some(cursor_val);
        }

        let backwards_cursor = search_results.first().and_then(|result| {
            thread_backwards_cursor_for_sort_key(
                &result.thread,
                store_sort_key,
                store_sort_direction,
            )
        });
        let fallback_provider = self.config.model_provider_id.clone();
        let mut data = Vec::with_capacity(search_results.len());
        for result in search_results {
            let (thread, _) = thread_from_stored_thread(
                result.thread,
                fallback_provider.as_str(),
                &self.config.cwd,
            );
            data.push(ThreadSearchResult {
                thread,
                snippet: result.snippet,
            });
        }

        enrich_loaded_threads(
            &self.thread_manager,
            &self.thread_watch_manager,
            data.as_mut_slice(),
            |result| &mut result.thread,
        )
        .await;

        Ok(ThreadSearchResponse {
            data,
            next_cursor,
            backwards_cursor,
        })
    }

    async fn thread_loaded_list_response_inner(
        &self,
        params: ThreadLoadedListParams,
    ) -> Result<ThreadLoadedListResponse, JSONRPCErrorError> {
        let ThreadLoadedListParams {
            cursor,
            limit,
            ancestor_thread_id,
        } = params;
        let mut data: Vec<String> = match ancestor_thread_id {
            Some(ancestor_thread_id) => {
                let ancestor_thread_id = ThreadId::from_string(&ancestor_thread_id)
                    .map_err(|err| invalid_request(format!("invalid ancestor thread id: {err}")))?;
                let indexed_descendants: HashSet<_> = self
                    .thread_manager
                    .list_open_agent_subtree_thread_ids(ancestor_thread_id)
                    .await
                    .map_err(|err| {
                        internal_error(format!(
                            "failed to list open spawned descendants for thread id {ancestor_thread_id}: {err}"
                        ))
                    })?
                    .into_iter()
                    .collect();
                let mut descendants = Vec::new();
                for thread_id in self.thread_manager.list_thread_ids().await {
                    if thread_id == ancestor_thread_id {
                        continue;
                    }
                    if indexed_descendants.contains(&thread_id)
                        || self
                            .thread_manager
                            .loaded_thread_descends_from(thread_id, ancestor_thread_id)
                            .await
                            .map_err(|err| {
                                internal_error(format!(
                                    "failed to resolve loaded thread ancestry for {thread_id}: {err}"
                                ))
                            })?
                    {
                        descendants.push(thread_id.to_string());
                    }
                }
                descendants
            }
            None => self
                .thread_manager
                .list_thread_ids()
                .await
                .into_iter()
                .map(|thread_id| thread_id.to_string())
                .collect(),
        };

        if data.is_empty() {
            return Ok(ThreadLoadedListResponse {
                data,
                next_cursor: None,
            });
        }

        data.sort();
        let total = data.len();
        let start = match cursor {
            Some(cursor) => {
                let cursor = match ThreadId::from_string(&cursor) {
                    Ok(id) => id.to_string(),
                    Err(_) => return Err(invalid_request(format!("invalid cursor: {cursor}"))),
                };
                match data.binary_search(&cursor) {
                    Ok(idx) => idx + 1,
                    Err(idx) => idx,
                }
            }
            None => 0,
        };

        let effective_limit = limit.unwrap_or(total as u32).max(1) as usize;
        let end = start.saturating_add(effective_limit).min(total);
        let page = data[start..end].to_vec();
        let next_cursor = page.last().filter(|_| end < total).cloned();

        Ok(ThreadLoadedListResponse {
            data: page,
            next_cursor,
        })
    }

    async fn thread_read_response_inner(
        &self,
        params: ThreadReadParams,
    ) -> Result<ThreadReadResponse, JSONRPCErrorError> {
        let ThreadReadParams {
            thread_id,
            include_turns,
        } = params;

        let thread_uuid = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;

        let thread = self
            .read_thread_view(thread_uuid, include_turns)
            .await
            .map_err(thread_read_view_error)?;
        Ok(ThreadReadResponse { thread })
    }

    /// Builds the API view for `thread/read` from persisted metadata plus optional live state.
    async fn read_thread_view(
        &self,
        thread_id: ThreadId,
        include_turns: bool,
    ) -> Result<Thread, ThreadReadViewError> {
        let loaded_thread = self.thread_manager.get_thread(thread_id).await.ok();
        let mut thread = if include_turns {
            if let Some(loaded_thread) = loaded_thread.as_ref() {
                // Loaded thread with turns: use persisted metadata when it exists,
                // but reconstruct turns from the live ThreadStore history.
                let persisted_thread = self
                    .load_persisted_thread_for_read(thread_id, /*include_turns*/ false)
                    .await?;
                self.load_live_thread_view(
                    thread_id,
                    include_turns,
                    loaded_thread,
                    persisted_thread,
                )
                .await?
            } else if let Some(thread) = self
                .load_persisted_thread_for_read(thread_id, include_turns)
                .await?
            {
                // Unloaded thread with turns: load metadata and history together
                // from the ThreadStore.
                thread
            } else {
                return Err(ThreadReadViewError::InvalidRequest(format!(
                    "thread not loaded: {thread_id}"
                )));
            }
        } else if let Some(thread) = self
            .load_persisted_thread_for_read(thread_id, include_turns)
            .await?
        {
            if let Some(loaded_thread) = loaded_thread.as_ref() {
                self.load_live_thread_view(thread_id, include_turns, loaded_thread, Some(thread))
                    .await?
            } else {
                thread
            }
        } else if let Some(loaded_thread) = loaded_thread.as_ref() {
            // Loaded metadata-only read before persistence is materialized: build
            // the response from the live thread snapshot.
            self.load_live_thread_view(
                thread_id,
                include_turns,
                loaded_thread,
                /*persisted_thread*/ None,
            )
            .await?
        } else {
            return Err(ThreadReadViewError::InvalidRequest(format!(
                "thread not loaded: {thread_id}"
            )));
        };

        let has_live_in_progress_turn = if let Some(loaded_thread) = loaded_thread.as_ref() {
            matches!(loaded_thread.agent_status().await, AgentStatus::Running)
        } else {
            false
        };

        let thread_status = self
            .thread_watch_manager
            .loaded_status_for_thread(&thread.id)
            .await;

        set_thread_status_and_interrupt_stale_turns(
            &mut thread,
            thread_status,
            has_live_in_progress_turn,
        );
        Ok(thread)
    }

    async fn load_persisted_thread_for_read(
        &self,
        thread_id: ThreadId,
        include_turns: bool,
    ) -> Result<Option<Thread>, ThreadReadViewError> {
        let fallback_provider = self.config.model_provider_id.as_str();
        let Some(mut stored_thread) = self
            .read_stored_thread_for_read(thread_id, /*include_history*/ false)
            .await?
        else {
            return Ok(None);
        };
        let paginated = matches!(stored_thread.history_mode, ThreadHistoryMode::Paginated);
        if include_turns
            && !paginated
            && let Some(stored_thread_with_history) = self
                .read_stored_thread_for_read(thread_id, /*include_history*/ true)
                .await?
        {
            stored_thread = stored_thread_with_history;
        }
        let (mut thread, history) =
            thread_from_stored_thread(stored_thread, fallback_provider, &self.config.cwd);
        if include_turns {
            if paginated {
                thread.turns = self
                    .paginated_thread_full_turns(thread_id)
                    .await
                    .map_err(ThreadReadViewError::JsonRpc)?;
            } else if let Some(history) = history {
                thread.turns = build_legacy_api_turns_from_rollout_items(&history.items);
            }
        }
        Ok(Some(thread))
    }

    async fn read_stored_thread_for_read(
        &self,
        thread_id: ThreadId,
        include_history: bool,
    ) -> Result<Option<StoredThread>, ThreadReadViewError> {
        match self
            .thread_store
            .read_thread(StoreReadThreadParams {
                thread_id,
                include_archived: true,
                include_history,
            })
            .await
        {
            Ok(stored_thread) => Ok(Some(stored_thread)),
            Err(ThreadStoreError::InvalidRequest { message })
                if message == format!("no rollout found for thread id {thread_id}") =>
            {
                Ok(None)
            }
            Err(ThreadStoreError::ThreadNotFound {
                thread_id: missing_thread_id,
            }) if missing_thread_id == thread_id => Ok(None),
            Err(ThreadStoreError::InvalidRequest { message }) => {
                Err(ThreadReadViewError::InvalidRequest(message))
            }
            Err(ThreadStoreError::Unsupported { operation }) => {
                Err(ThreadReadViewError::Unsupported(operation))
            }
            Err(err) => Err(ThreadReadViewError::Internal(format!(
                "failed to read thread: {err}"
            ))),
        }
    }

    /// Builds a `thread/read` view from a loaded thread plus optional persisted metadata.
    async fn load_live_thread_view(
        &self,
        thread_id: ThreadId,
        include_turns: bool,
        loaded_thread: &CodexThread,
        persisted_thread: Option<Thread>,
    ) -> Result<Thread, ThreadReadViewError> {
        let config_snapshot = loaded_thread.config_snapshot().await;
        if include_turns && config_snapshot.ephemeral {
            return Err(ThreadReadViewError::InvalidRequest(
                "ephemeral threads do not support includeTurns".to_string(),
            ));
        }
        let fallback_thread =
            build_thread_from_loaded_snapshot(thread_id, &config_snapshot, loaded_thread);
        let mut thread = if let Some(mut thread) = persisted_thread {
            if thread.path.is_none() {
                thread.path = fallback_thread.path.clone();
            }
            thread.session_id.clone_from(&fallback_thread.session_id);
            thread.ephemeral = fallback_thread.ephemeral;
            thread.can_accept_direct_input = fallback_thread.can_accept_direct_input;
            thread
        } else {
            fallback_thread
        };
        self.apply_thread_read_store_fields(thread_id, &mut thread, include_turns, loaded_thread)
            .await?;
        Ok(thread)
    }

    async fn apply_thread_read_store_fields(
        &self,
        thread_id: ThreadId,
        thread: &mut Thread,
        include_turns: bool,
        loaded_thread: &CodexThread,
    ) -> Result<(), ThreadReadViewError> {
        self.attach_thread_name(thread_id, thread).await;

        if include_turns {
            let config_snapshot = loaded_thread.config_snapshot().await;
            if matches!(config_snapshot.history_mode, ThreadHistoryMode::Paginated) {
                if self
                    .has_paginated_history_projection(thread_id)
                    .await
                    .map_err(ThreadReadViewError::JsonRpc)?
                {
                    self.thread_store
                        .persist_thread(thread_id, PersistContext::Standard)
                        .await
                        .map_err(|err| thread_read_history_load_error(thread_id, err))?;
                }
                thread.turns = self
                    .paginated_thread_full_turns(thread_id)
                    .await
                    .map_err(ThreadReadViewError::JsonRpc)?;
            } else {
                let history = loaded_thread
                    .load_history(/*include_archived*/ true)
                    .await
                    .map_err(|err| thread_read_history_load_error(thread_id, err))?;
                thread.turns = build_legacy_api_turns_from_rollout_items(&history.items);
            }
        }

        Ok(())
    }

    async fn thread_turns_list_response_inner(
        &self,
        params: ThreadTurnsListParams,
    ) -> Result<ThreadTurnsListResponse, JSONRPCErrorError> {
        let ThreadTurnsListParams {
            thread_id,
            cursor,
            limit,
            sort_direction,
            items_view,
        } = params;
        let thread_uuid = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        let legacy_rollout_path = match self
            .thread_store
            .read_thread(StoreReadThreadParams {
                thread_id: thread_uuid,
                include_archived: true,
                include_history: false,
            })
            .await
        {
            Ok(thread) if thread.history_mode == ThreadHistoryMode::Paginated => {
                return self
                    .paginated_thread_turns_list_response(
                        thread_uuid,
                        cursor,
                        limit,
                        sort_direction,
                        items_view,
                    )
                    .await;
            }
            Ok(thread) => thread.rollout_path,
            Err(ThreadStoreError::InvalidRequest { message })
                if message == format!("no rollout found for thread id {thread_uuid}") =>
            {
                None
            }
            Err(ThreadStoreError::ThreadNotFound { thread_id }) if thread_id == thread_uuid => None,
            Err(ThreadStoreError::InvalidRequest { message }) => {
                return Err(invalid_request(message));
            }
            Err(ThreadStoreError::Unsupported { operation }) => {
                return Err(unsupported_thread_store_operation(operation));
            }
            Err(err) => return Err(internal_error(format!("failed to read thread: {err}"))),
        };
        let sort_direction = sort_direction.unwrap_or(SortDirection::Desc);
        let indexed_legacy_generation = self
            .indexed_legacy_history_threads
            .lock()
            .await
            .contains(&thread_uuid);
        if let Some(local_store) = self
            .thread_store
            .as_any()
            .downcast_ref::<codex_thread_store::LocalThreadStore>()
            && let Some(mut response) = self
                .projected_legacy_thread_turns_list_response(
                    local_store,
                    thread_uuid,
                    cursor.clone(),
                    limit,
                    ProjectedLegacyThreadTurnsPageOptions {
                        sort_direction,
                        items_view,
                        allow_running: indexed_legacy_generation,
                    },
                )
                .await?
        {
            if indexed_legacy_generation
                && let Some(thread) = self.thread_manager.get_thread(thread_uuid).await.ok()
                && matches!(thread.agent_status().await, AgentStatus::Running)
            {
                let active_turn = {
                    let thread_state = self.thread_state_manager.thread_state(thread_uuid).await;
                    thread_state.lock().await.active_turn_snapshot()
                };
                if let Some(active_turn) = active_turn
                    && (matches!(sort_direction, SortDirection::Asc) || cursor.is_none())
                {
                    let active_turn_id = active_turn.id.clone();
                    let active_turn_is_in_page =
                        response.data.iter().any(|turn| turn.id == active_turn_id);
                    let mut page = codex_app_server_protocol::TurnsPage::from(response);
                    if matches!(sort_direction, SortDirection::Desc) {
                        if !active_turn_is_in_page
                            && page.data.len() == thread_turns_page_size(limit)
                            && let Some(omitted_turn) = page.data.pop()
                        {
                            page.next_cursor = Some(serialize_thread_turns_cursor(
                                &omitted_turn.id,
                                /*include_anchor*/ true,
                            )?);
                        }
                        super::thread_lifecycle::merge_active_turn_into_page(
                            &mut page,
                            active_turn,
                            &ThreadResumeInitialTurnsPageParams {
                                limit,
                                sort_direction: Some(sort_direction),
                                items_view,
                            },
                        );
                        page.backwards_cursor = Some(serialize_thread_turns_cursor(
                            &active_turn_id,
                            /*include_anchor*/ true,
                        )?);
                    } else if matches!(sort_direction, SortDirection::Asc) {
                        if !active_turn_is_in_page
                            && page.next_cursor.is_none()
                            && page.data.len() == thread_turns_page_size(limit)
                        {
                            page.next_cursor = Some(serialize_thread_turns_cursor(
                                &active_turn_id,
                                /*include_anchor*/ true,
                            )?);
                        }
                        super::thread_lifecycle::merge_active_turn_into_page(
                            &mut page,
                            active_turn,
                            &ThreadResumeInitialTurnsPageParams {
                                limit,
                                sort_direction: Some(sort_direction),
                                items_view,
                            },
                        );
                    }
                    normalize_thread_turns_status(
                        &mut page.data,
                        self.thread_watch_manager
                            .loaded_status_for_thread(&thread_uuid.to_string())
                            .await,
                        /*has_live_in_progress_turn*/ true,
                    );
                    response = ThreadTurnsListResponse {
                        data: page.data,
                        next_cursor: page.next_cursor,
                        backwards_cursor: page.backwards_cursor,
                    };
                }
            }
            return Ok(response);
        }
        let parsed_cursor = cursor
            .as_deref()
            .map(parse_thread_turns_cursor)
            .transpose()?;
        let history_window = self
            .load_thread_turns_list_history(
                thread_uuid,
                legacy_rollout_path.as_deref(),
                parsed_cursor.as_ref(),
                limit,
                sort_direction,
            )
            .await
            .map_err(thread_read_view_error)?;
        // Reference-backed legacy history expands only until this page has coherent turns.
        // Other legacy history still replays the complete rollout on each request because
        // rollback and compaction events can change earlier turns.
        let loaded_thread = self.thread_manager.get_thread(thread_uuid).await.ok();
        let has_live_running_thread = match loaded_thread.as_ref() {
            Some(thread) => matches!(thread.agent_status().await, AgentStatus::Running),
            None => false,
        };
        let active_turn = if loaded_thread.is_some() {
            // Persisted history may not yet include the currently running turn. The
            // app-server listener has already projected live turn events into ThreadState,
            // so merge that in-memory snapshot before paginating.
            let thread_state = self.thread_state_manager.thread_state(thread_uuid).await;
            let state = thread_state.lock().await;
            state.active_turn_snapshot()
        } else {
            None
        };
        let mut response = build_thread_turns_page_response(
            &history_window.items,
            self.thread_watch_manager
                .loaded_status_for_thread(&thread_uuid.to_string())
                .await,
            has_live_running_thread,
            active_turn,
            ThreadTurnsPageOptions {
                cursor: cursor.as_deref(),
                limit,
                sort_direction,
                items_view: items_view.unwrap_or(TurnItemsView::Summary),
                has_older_reference: history_window.has_older_reference,
            },
        )?;
        if matches!(sort_direction, SortDirection::Desc)
            && history_window.has_older_reference
            && !indexed_legacy_generation
        {
            self.bounded_legacy_history_threads
                .lock()
                .await
                .insert(thread_uuid);
        }
        self.legacy_displayed_turn_items
            .lock()
            .await
            .stabilize(thread_uuid, &mut response.data);
        Ok(response)
    }

    async fn thread_search_occurrences_response_inner(
        &self,
        params: ThreadSearchOccurrencesParams,
    ) -> Result<ThreadSearchOccurrencesResponse, JSONRPCErrorError> {
        let ThreadSearchOccurrencesParams {
            thread_id,
            search_term,
            cursor,
            limit,
        } = params;
        let thread_id = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        if search_term.trim().is_empty() {
            return Err(invalid_request(
                "thread/searchOccurrences requires a non-empty searchTerm",
            ));
        }
        let page_size = limit
            .map(|value| value as usize)
            .unwrap_or(THREAD_SEARCH_OCCURRENCES_DEFAULT_LIMIT)
            .clamp(1, THREAD_SEARCH_OCCURRENCES_MAX_LIMIT);
        let displayed_unprojected_history = self
            .unprojected_paginated_history_threads
            .lock()
            .await
            .contains(&thread_id);
        if let Some(store) = self
            .thread_store
            .as_any()
            .downcast_ref::<codex_thread_store::LocalThreadStore>()
            && (displayed_unprojected_history
                || !store
                    .has_history_projection(thread_id)
                    .await
                    .map_err(|err| {
                        internal_error(format!(
                            "failed to inspect thread history projection: {err}"
                        ))
                    })?)
        {
            // Full-history search requires the SQLite projection. Keep thread open, list, and
            // fork bounded; pay the one-time lineage scan only for this explicit search request.
            store
                .rebuild_history_projection(thread_id)
                .await
                .map_err(|err| {
                    internal_error(format!(
                        "failed to rebuild thread history projection: {err}"
                    ))
                })?;
        }
        let page = self
            .thread_store
            .search_thread_occurrences(StoreSearchThreadOccurrencesParams {
                thread_id,
                search_term,
                cursor,
                page_size,
            })
            .await
            .map_err(|err| match err {
                ThreadStoreError::InvalidRequest { message } => invalid_request(message),
                ThreadStoreError::Unsupported { operation } => {
                    unsupported_thread_store_operation(operation)
                }
                ThreadStoreError::ThreadNotFound { thread_id } => {
                    invalid_request(format!("no rollout found for thread id {thread_id}"))
                }
                err => internal_error(format!("failed to search thread occurrences: {err}")),
            })?;
        Ok(ThreadSearchOccurrencesResponse {
            data: page
                .items
                .into_iter()
                .map(|item| ThreadSearchOccurrence {
                    turn_id: item.turn_id,
                    item_id: item.item_id,
                    snippet: item.snippet,
                    snippet_match_range: ThreadSearchTextRange {
                        start: item.snippet_match_range.start,
                        end: item.snippet_match_range.end,
                    },
                    turn_cursor: item.turn_cursor,
                })
                .collect(),
            next_cursor: page.next_cursor,
        })
    }

    async fn paginated_thread_turns_list_response(
        &self,
        thread_id: ThreadId,
        cursor: Option<String>,
        limit: Option<u32>,
        sort_direction: Option<SortDirection>,
        items_view: Option<TurnItemsView>,
    ) -> Result<ThreadTurnsListResponse, JSONRPCErrorError> {
        let items_view = items_view.unwrap_or(TurnItemsView::Summary);
        let page_size = thread_turns_page_size(limit);
        let api_sort_direction = sort_direction.unwrap_or(SortDirection::Desc);
        let sort_direction = match api_sort_direction {
            SortDirection::Asc => StoreSortDirection::Asc,
            SortDirection::Desc => StoreSortDirection::Desc,
        };
        let use_unprojected_history = match cursor.as_deref() {
            Some(cursor) => parse_thread_turns_cursor(cursor).is_ok(),
            None => {
                self.unprojected_paginated_history_threads
                    .lock()
                    .await
                    .contains(&thread_id)
                    || !self.has_paginated_history_projection(thread_id).await?
            }
        };
        if use_unprojected_history
            && let Some(response) = self
                .unprojected_paginated_thread_turns_list_response(
                    thread_id,
                    cursor.as_deref(),
                    limit,
                    api_sort_direction,
                    items_view,
                )
                .await?
        {
            return Ok(response);
        }
        // `Full` is only a temporary compatibility path. Keep it out of ThreadStore's API:
        // load turn shells here, then hydrate their items below.
        let stored_items_view = match items_view {
            TurnItemsView::NotLoaded => StoredTurnItemsView::NotLoaded,
            TurnItemsView::Summary => StoredTurnItemsView::Summary,
            TurnItemsView::Full => StoredTurnItemsView::NotLoaded,
        };
        let page = self
            .thread_store
            .list_turns(StoreListTurnsParams {
                thread_id,
                include_archived: true,
                cursor: cursor.clone(),
                page_size,
                sort_direction,
                items_view: stored_items_view,
            })
            .await
            .map_err(|err| match err {
                ThreadStoreError::InvalidRequest { message } => invalid_request(message),
                ThreadStoreError::Unsupported { operation } => {
                    unsupported_thread_store_operation(operation)
                }
                ThreadStoreError::ThreadNotFound { thread_id } => {
                    invalid_request(format!("no rollout found for thread id {thread_id}"))
                }
                err => internal_error(format!("failed to list thread history: {err}")),
            })?;
        let mut turns = Vec::with_capacity(page.turns.len());
        for turn in page.turns {
            let mut turn = stored_turn_to_api_turn(turn, items_view)?;
            if matches!(items_view, TurnItemsView::Full) {
                turn.items = self
                    .paginated_turn_full_items(thread_id, turn.id.as_str())
                    .await?;
            }
            turns.push(turn);
        }
        let loaded_thread = self.thread_manager.get_thread(thread_id).await.ok();
        let has_live_running_thread = match loaded_thread.as_ref() {
            Some(thread) => matches!(thread.agent_status().await, AgentStatus::Running),
            None => false,
        };
        normalize_thread_turns_status(
            &mut turns,
            self.thread_watch_manager
                .loaded_status_for_thread(&thread_id.to_string())
                .await,
            has_live_running_thread,
        );
        Ok(ThreadTurnsListResponse {
            data: turns,
            next_cursor: page.next_cursor,
            backwards_cursor: page.backwards_cursor,
        })
    }

    async fn has_paginated_history_projection(
        &self,
        thread_id: ThreadId,
    ) -> Result<bool, JSONRPCErrorError> {
        let Some(store) = self
            .thread_store
            .as_any()
            .downcast_ref::<codex_thread_store::LocalThreadStore>()
        else {
            return Ok(true);
        };
        store
            .has_history_projection(thread_id)
            .await
            .map_err(|err| {
                internal_error(format!("failed to read thread history projection: {err}"))
            })
    }

    async fn unprojected_paginated_thread_turns_list_response(
        &self,
        thread_id: ThreadId,
        cursor: Option<&str>,
        limit: Option<u32>,
        sort_direction: SortDirection,
        items_view: TurnItemsView,
    ) -> Result<Option<ThreadTurnsListResponse>, JSONRPCErrorError> {
        if !self
            .thread_store
            .as_any()
            .is::<codex_thread_store::LocalThreadStore>()
        {
            return Ok(None);
        }
        let Some(stored_thread) = self
            .read_stored_thread_for_read(thread_id, /*include_history*/ false)
            .await
            .map_err(thread_read_view_error)?
        else {
            return Ok(None);
        };
        let Some(rollout_path) = stored_thread.rollout_path else {
            return Ok(None);
        };
        let physical_history_mode = codex_rollout::read_session_meta_line(rollout_path.as_path())
            .await
            .map_err(|err| {
                thread_read_view_error(ThreadReadViewError::Internal(format!(
                    "failed to read thread metadata {}: {err}",
                    rollout_path.display()
                )))
            })?
            .meta
            .history_mode;
        let parsed_cursor = cursor.map(parse_thread_turns_cursor).transpose()?;
        let history_window = if physical_history_mode == ThreadHistoryMode::Paginated {
            if parsed_cursor.is_none() && matches!(sort_direction, SortDirection::Desc) {
                match self
                    .load_recent_paginated_turn_window(rollout_path.as_path(), limit)
                    .await
                    .map_err(|err| {
                        thread_read_view_error(ThreadReadViewError::Internal(format!(
                            "failed to reverse scan thread history {}: {err}",
                            rollout_path.display()
                        )))
                    })? {
                    Some(window) => window,
                    None => self
                        .load_reference_backed_turn_window(
                            rollout_path.as_path(),
                            parsed_cursor.as_ref(),
                            limit,
                            sort_direction,
                        )
                        .await
                        .map_err(|err| {
                            thread_read_view_error(ThreadReadViewError::Internal(format!(
                                "failed to load thread history {}: {err}",
                                rollout_path.display()
                            )))
                        })?,
                }
            } else {
                self.load_reference_backed_turn_window(
                    rollout_path.as_path(),
                    parsed_cursor.as_ref(),
                    limit,
                    sort_direction,
                )
                .await
                .map_err(|err| {
                    thread_read_view_error(ThreadReadViewError::Internal(format!(
                        "failed to load thread history {}: {err}",
                        rollout_path.display()
                    )))
                })?
            }
        } else {
            self.load_thread_turns_list_history(
                thread_id,
                Some(rollout_path.as_path()),
                parsed_cursor.as_ref(),
                limit,
                sort_direction,
            )
            .await
            .map_err(thread_read_view_error)?
        };
        let loaded_thread = self.thread_manager.get_thread(thread_id).await.ok();
        let has_live_running_thread = match loaded_thread.as_ref() {
            Some(thread) => matches!(thread.agent_status().await, AgentStatus::Running),
            None => false,
        };
        let active_turn = if loaded_thread.is_some() {
            let thread_state = self.thread_state_manager.thread_state(thread_id).await;
            let state = thread_state.lock().await;
            state.active_turn_snapshot()
        } else {
            None
        };
        let mut response = build_thread_turns_page_response_for_history_mode(
            &history_window.items,
            self.thread_watch_manager
                .loaded_status_for_thread(&thread_id.to_string())
                .await,
            has_live_running_thread,
            active_turn,
            ThreadTurnsPageOptions {
                cursor,
                limit,
                sort_direction,
                items_view,
                has_older_reference: history_window.has_older_reference,
            },
            physical_history_mode,
        )?;
        if physical_history_mode == ThreadHistoryMode::Legacy {
            self.legacy_displayed_turn_items
                .lock()
                .await
                .stabilize(thread_id, &mut response.data);
        }
        self.unprojected_paginated_history_threads
            .lock()
            .await
            .insert(thread_id);
        if physical_history_mode == ThreadHistoryMode::Paginated
            && let Some(store) = self
                .thread_store
                .as_any()
                .downcast_ref::<codex_thread_store::LocalThreadStore>()
        {
            store.schedule_history_projection_rebuild(thread_id).await;
        }
        Ok(Some(response))
    }

    async fn unprojected_paginated_thread_items_list_response(
        &self,
        thread_id: ThreadId,
        turn_id: Option<&str>,
        cursor: Option<&str>,
        page_size: usize,
        sort_direction: SortDirection,
    ) -> Result<Option<ThreadItemsListResponse>, JSONRPCErrorError> {
        let item_cursor = cursor.map(parse_thread_items_cursor).transpose()?;
        let mut turn_cursor = None;
        let mut descending_items = Vec::new();
        let mut has_older_turns;
        let mut requested_turn_loaded = false;

        loop {
            let Some(page) = self
                .unprojected_paginated_thread_turns_list_response(
                    thread_id,
                    turn_cursor.as_deref(),
                    Some(
                        page_size.clamp(THREAD_TURNS_DEFAULT_LIMIT, THREAD_TURNS_MAX_LIMIT) as u32,
                    ),
                    SortDirection::Desc,
                    TurnItemsView::Full,
                )
                .await?
            else {
                return Ok(None);
            };

            let next_turn_cursor = page.next_cursor;
            has_older_turns = next_turn_cursor.is_some();
            for turn in page.data {
                requested_turn_loaded |= turn_id == Some(turn.id.as_str());
                descending_items.extend(turn.items.into_iter().rev().map(|item| ThreadItemEntry {
                    turn_id: turn.id.clone(),
                    item,
                }));
            }

            // The cursor may name an item in another turn. Once both that anchor and the
            // requested complete turn are loaded, older turns cannot add matching items.
            if requested_turn_loaded
                && item_cursor.as_ref().is_none_or(|anchor| {
                    descending_items.iter().any(|entry| {
                        entry.turn_id == anchor.turn_id && entry.item.id() == anchor.item_id
                    })
                })
            {
                break;
            }

            if matches!(sort_direction, SortDirection::Desc) {
                let available_items = match item_cursor.as_ref() {
                    Some(anchor) => descending_items
                        .iter()
                        .position(|entry| {
                            entry.turn_id == anchor.turn_id && entry.item.id() == anchor.item_id
                        })
                        .map(|position| {
                            descending_items
                                .iter()
                                .skip(position + usize::from(!anchor.include_anchor))
                                .filter(|entry| {
                                    turn_id.is_none_or(|turn_id| entry.turn_id == turn_id)
                                })
                                .count()
                        })
                        .unwrap_or(0),
                    None => descending_items
                        .iter()
                        .filter(|entry| turn_id.is_none_or(|turn_id| entry.turn_id == turn_id))
                        .count(),
                };
                if available_items > page_size {
                    break;
                }
            }
            let Some(next_turn_cursor) = next_turn_cursor else {
                break;
            };
            if turn_cursor.as_ref() == Some(&next_turn_cursor) {
                return Err(internal_error(
                    "failed to load thread items: rollout returned a repeated turn cursor",
                ));
            }
            turn_cursor = Some(next_turn_cursor);
        }

        if matches!(sort_direction, SortDirection::Asc) {
            descending_items.reverse();
        }
        let start = match item_cursor {
            Some(anchor) => {
                let position = descending_items
                    .iter()
                    .position(|entry| {
                        entry.turn_id == anchor.turn_id && entry.item.id() == anchor.item_id
                    })
                    .ok_or_else(|| {
                        invalid_request("invalid cursor: anchor item is no longer present")
                    })?;
                position + usize::from(!anchor.include_anchor)
            }
            None => 0,
        };
        let available_items = descending_items
            .iter()
            .skip(start)
            .filter(|entry| turn_id.is_none_or(|turn_id| entry.turn_id == turn_id))
            .count();
        let has_more_items = available_items > page_size
            || (turn_id.is_none()
                && matches!(sort_direction, SortDirection::Desc)
                && has_older_turns);
        let data = descending_items
            .into_iter()
            .skip(start)
            .filter(|entry| {
                turn_id
                    .as_ref()
                    .is_none_or(|turn_id| &entry.turn_id == turn_id)
            })
            .take(page_size)
            .collect::<Vec<_>>();
        let backwards_cursor = data
            .first()
            .map(|entry| serialize_thread_items_cursor(entry, /*include_anchor*/ true))
            .transpose()?;
        let next_cursor = if has_more_items {
            data.last()
                .map(|entry| serialize_thread_items_cursor(entry, /*include_anchor*/ false))
                .transpose()?
        } else {
            None
        };

        Ok(Some(ThreadItemsListResponse {
            data,
            next_cursor,
            backwards_cursor,
        }))
    }

    async fn projected_legacy_thread_turns_list_response(
        &self,
        store: &codex_thread_store::LocalThreadStore,
        thread_id: ThreadId,
        cursor: Option<String>,
        limit: Option<u32>,
        options: ProjectedLegacyThreadTurnsPageOptions,
    ) -> Result<Option<ThreadTurnsListResponse>, JSONRPCErrorError> {
        let ProjectedLegacyThreadTurnsPageOptions {
            sort_direction,
            items_view,
            allow_running,
        } = options;
        let bounded_legacy_history = self
            .bounded_legacy_history_threads
            .lock()
            .await
            .contains(&thread_id);
        let loaded_thread = self.thread_manager.get_thread(thread_id).await.ok();
        if !allow_running
            && let Some(thread) = loaded_thread.as_ref()
            && matches!(thread.agent_status().await, AgentStatus::Running)
        {
            return Ok(None);
        }

        let items_view = items_view.unwrap_or(TurnItemsView::Summary);
        let stored_items_view = match items_view {
            TurnItemsView::NotLoaded | TurnItemsView::Full => StoredTurnItemsView::NotLoaded,
            TurnItemsView::Summary => StoredTurnItemsView::Summary,
        };
        let materialize_full_history =
            matches!(sort_direction, SortDirection::Asc) || cursor.is_some();
        let sort_direction = match sort_direction {
            SortDirection::Asc => StoreSortDirection::Asc,
            SortDirection::Desc => StoreSortDirection::Desc,
        };
        let page_params = StoreListTurnsParams {
            thread_id,
            include_archived: true,
            cursor,
            page_size: thread_turns_page_size(limit),
            sort_direction,
            items_view: stored_items_view,
        };
        if bounded_legacy_history {
            if materialize_full_history {
                let bootstrap_params = StoreListTurnsParams {
                    thread_id,
                    include_archived: true,
                    cursor: None,
                    page_size: 1,
                    sort_direction,
                    items_view: StoredTurnItemsView::NotLoaded,
                };
                store
                    .list_segmented_legacy_turns(bootstrap_params)
                    .await
                    .map_err(paginated_history_list_error)?;
            }
            return Ok(None);
        }
        let page = if materialize_full_history {
            store.list_segmented_legacy_turns(page_params).await
        } else {
            store
                .list_existing_segmented_legacy_turns(page_params)
                .await
        }
        .map_err(paginated_history_list_error)?;
        let Some(page) = page else {
            return Ok(None);
        };

        let mut turns = Vec::with_capacity(page.turns.len());
        for turn in page.turns {
            let mut turn = stored_turn_to_api_turn(turn, items_view)?;
            if matches!(items_view, TurnItemsView::Full) {
                turn.items = self
                    .projected_legacy_turn_full_items(store, thread_id, turn.id.as_str())
                    .await?;
            }
            turns.push(turn);
        }
        normalize_thread_turns_status(
            &mut turns,
            self.thread_watch_manager
                .loaded_status_for_thread(&thread_id.to_string())
                .await,
            /*has_live_in_progress_turn*/ false,
        );
        self.legacy_displayed_turn_items
            .lock()
            .await
            .stabilize(thread_id, &mut turns);
        Ok(Some(ThreadTurnsListResponse {
            data: turns,
            next_cursor: page.next_cursor,
            backwards_cursor: page.backwards_cursor,
        }))
    }

    async fn projected_legacy_turn_full_items(
        &self,
        store: &codex_thread_store::LocalThreadStore,
        thread_id: ThreadId,
        turn_id: &str,
    ) -> Result<Vec<ThreadItem>, JSONRPCErrorError> {
        let mut cursor = None;
        let mut items = Vec::new();
        loop {
            let page = store
                .list_segmented_legacy_items(StoreListItemsParams {
                    thread_id,
                    turn_id: Some(turn_id.to_string()),
                    include_archived: true,
                    cursor: cursor.clone(),
                    page_size: THREAD_ITEMS_MAX_LIMIT,
                    sort_direction: StoreSortDirection::Asc,
                    sort_key: StoreItemSortKey::CreatedAtOrdinal,
                    after_updated_at_ordinal: None,
                })
                .await
                .map_err(paginated_history_list_error)?
                .ok_or_else(|| {
                    internal_error(format!(
                        "projected legacy history disappeared while loading turn {turn_id}"
                    ))
                })?;
            for item in page.items {
                items.push(deserialize_stored_thread_item(item)?);
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(items);
            };
            if cursor.as_ref() == Some(&next_cursor) {
                return Err(internal_error(format!(
                    "failed to load projected legacy turn {turn_id}: repeated item cursor"
                )));
            }
            cursor = Some(next_cursor);
        }
    }

    // Older clients still request `itemsView: "full"` from turn pages. Keep this
    // app-server-only hydration path until those clients use `thread/items/list`.
    async fn paginated_turn_full_items(
        &self,
        thread_id: ThreadId,
        turn_id: &str,
    ) -> Result<Vec<ThreadItem>, JSONRPCErrorError> {
        let mut cursor = None;
        let mut items = Vec::new();
        loop {
            if *HISTORY_IO_OBSERVATION_ENABLED {
                tracing::event!(
                    target: "codex_history_io",
                    tracing::Level::TRACE,
                    event.name = "codex.history.turn_items.read",
                    thread.id = %thread_id,
                    turn.id = turn_id,
                    "reading persisted turn item page"
                );
            }
            let page = self
                .thread_store
                .list_items(StoreListItemsParams {
                    thread_id,
                    turn_id: Some(turn_id.to_string()),
                    include_archived: true,
                    cursor: cursor.clone(),
                    page_size: THREAD_ITEMS_MAX_LIMIT,
                    sort_direction: StoreSortDirection::Asc,
                    sort_key: StoreItemSortKey::CreatedAtOrdinal,
                    after_updated_at_ordinal: None,
                })
                .await
                .map_err(paginated_history_list_error)?;
            for item in page.items {
                items.push(deserialize_stored_thread_item(item)?);
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(items);
            };
            if cursor.as_ref() == Some(&next_cursor) {
                return Err(internal_error(format!(
                    "failed to load full turn items for {turn_id}: thread store returned a repeated cursor"
                )));
            }
            cursor = Some(next_cursor);
        }
    }

    // Older clients expect full `thread.turns` from resume and `thread/read(includeTurns=true)`.
    // Keep this slow compatibility path until all clients page history directly.
    async fn paginated_thread_full_turns(
        &self,
        thread_id: ThreadId,
    ) -> Result<Vec<Turn>, JSONRPCErrorError> {
        let mut cursor = None;
        let mut turns = Vec::new();
        loop {
            let page = self
                .paginated_thread_turns_list_response(
                    thread_id,
                    cursor.clone(),
                    Some(THREAD_TURNS_MAX_LIMIT as u32),
                    Some(SortDirection::Asc),
                    Some(TurnItemsView::Full),
                )
                .await?;
            turns.extend(page.data);
            let Some(next_cursor) = page.next_cursor else {
                return Ok(turns);
            };
            if cursor.as_ref() == Some(&next_cursor) {
                return Err(internal_error(format!(
                    "failed to load full thread turns for {thread_id}: thread store returned a repeated cursor"
                )));
            }
            cursor = Some(next_cursor);
        }
    }

    async fn paginated_resume_initial_turns_page(
        &self,
        thread_id: ThreadId,
        params: &ThreadResumeInitialTurnsPageParams,
    ) -> Result<codex_app_server_protocol::TurnsPage, JSONRPCErrorError> {
        self.paginated_thread_turns_list_response(
            thread_id,
            /*cursor*/ None,
            params.limit,
            params.sort_direction,
            params.items_view,
        )
        .await
        .map(Into::into)
    }

    async fn paginated_resume_initial_turns_page_with_active_slot(
        &self,
        thread_id: ThreadId,
        params: &ThreadResumeInitialTurnsPageParams,
    ) -> Result<codex_app_server_protocol::TurnsPage, JSONRPCErrorError> {
        // A running resume overlays the newest live turn on this durable page.
        // Reserve one row so the overlay keeps the requested limit and the
        // durable next cursor still starts after the last returned stored turn.
        let page_size = thread_turns_page_size(params.limit);
        if page_size == 1 {
            // ThreadStore does not accept an empty page. Use its backwards cursor as
            // the next cursor so the omitted durable row is returned next.
            let mut page = self
                .paginated_resume_initial_turns_page(thread_id, params)
                .await?;
            page.next_cursor = page.backwards_cursor.clone();
            page.data.clear();
            return Ok(page);
        }

        let mut params = params.clone();
        params.limit = Some((page_size - 1) as u32);
        self.paginated_resume_initial_turns_page(thread_id, &params)
            .await
    }

    async fn unprojected_paginated_resume_backwards_cursors(
        &self,
        thread_id: ThreadId,
    ) -> Result<(Option<String>, Option<String>), JSONRPCErrorError> {
        let turns_page = self
            .unprojected_paginated_thread_turns_list_response(
                thread_id,
                /*cursor*/ None,
                Some(1),
                SortDirection::Desc,
                TurnItemsView::NotLoaded,
            )
            .await?
            .ok_or_else(|| {
                internal_error(format!(
                    "failed to read unprojected turn head for thread {thread_id}"
                ))
            })?;
        let items_page = self
            .unprojected_paginated_thread_items_list_response(
                thread_id,
                /*turn_id*/ None,
                /*cursor*/ None,
                /*page_size*/ 1,
                SortDirection::Desc,
            )
            .await?
            .ok_or_else(|| {
                internal_error(format!(
                    "failed to read unprojected item head for thread {thread_id}"
                ))
            })?;
        Ok((turns_page.backwards_cursor, items_page.backwards_cursor))
    }

    pub(super) async fn paginated_resume_backwards_cursors(
        thread_store: &dyn ThreadStore,
        thread_id: ThreadId,
    ) -> Result<(Option<String>, Option<String>), JSONRPCErrorError> {
        let turns_page = thread_store
            .list_turns(StoreListTurnsParams {
                thread_id,
                include_archived: true,
                cursor: None,
                page_size: 1,
                sort_direction: StoreSortDirection::Desc,
                items_view: StoredTurnItemsView::NotLoaded,
            })
            .await
            .map_err(paginated_history_list_error)?;
        let items_page = thread_store
            .list_items(StoreListItemsParams {
                thread_id,
                turn_id: None,
                include_archived: true,
                cursor: None,
                page_size: 1,
                sort_direction: StoreSortDirection::Desc,
                sort_key: StoreItemSortKey::CreatedAtOrdinal,
                after_updated_at_ordinal: None,
            })
            .await
            .map_err(paginated_history_list_error)?;
        Ok((turns_page.backwards_cursor, items_page.backwards_cursor))
    }

    async fn thread_items_list_response_inner(
        &self,
        params: ThreadItemsListParams,
    ) -> Result<ThreadItemsListResponse, JSONRPCErrorError> {
        let ThreadItemsListParams {
            thread_id,
            turn_id,
            cursor,
            limit,
            sort_direction,
        } = params;
        let thread_id = ThreadId::from_string(&thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        let page_size = limit
            .map(|value| value as usize)
            .unwrap_or(THREAD_ITEMS_DEFAULT_LIMIT)
            .clamp(1, THREAD_ITEMS_MAX_LIMIT);
        let sort_direction = sort_direction.unwrap_or(SortDirection::Asc);
        let use_unprojected_history = match cursor.as_deref() {
            Some(cursor) => parse_thread_items_cursor(cursor).is_ok(),
            None => {
                self.unprojected_paginated_history_threads
                    .lock()
                    .await
                    .contains(&thread_id)
                    || !self.has_paginated_history_projection(thread_id).await?
            }
        };
        if use_unprojected_history
            && let Some(response) = self
                .unprojected_paginated_thread_items_list_response(
                    thread_id,
                    turn_id.as_deref(),
                    cursor.as_deref(),
                    page_size,
                    sort_direction,
                )
                .await?
        {
            return Ok(response);
        }
        let page = self
            .thread_store
            .list_items(StoreListItemsParams {
                thread_id,
                turn_id,
                include_archived: true,
                cursor,
                page_size,
                sort_direction: match sort_direction {
                    SortDirection::Asc => StoreSortDirection::Asc,
                    SortDirection::Desc => StoreSortDirection::Desc,
                },
                sort_key: StoreItemSortKey::CreatedAtOrdinal,
                after_updated_at_ordinal: None,
            })
            .await
            .map_err(|err| match err {
                ThreadStoreError::InvalidRequest { message } => invalid_request(message),
                ThreadStoreError::Unsupported { .. } => {
                    method_not_found("thread/items/list is not supported yet")
                }
                ThreadStoreError::ThreadNotFound { thread_id } => {
                    invalid_request(format!("no rollout found for thread id {thread_id}"))
                }
                err => internal_error(format!("failed to list thread items: {err}")),
            })?;
        let data = page
            .items
            .into_iter()
            .map(|stored_item| {
                let turn_id = stored_item.turn_id.clone();
                let item = deserialize_stored_thread_item(stored_item)?;
                Ok(ThreadItemEntry { turn_id, item })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ThreadItemsListResponse {
            data,
            next_cursor: page.next_cursor,
            backwards_cursor: page.backwards_cursor,
        })
    }

    async fn load_thread_turns_list_history(
        &self,
        thread_id: ThreadId,
        rollout_path: Option<&Path>,
        cursor: Option<&ThreadTurnsCursor>,
        limit: Option<u32>,
        sort_direction: SortDirection,
    ) -> Result<LegacyHistoryWindow, ThreadReadViewError> {
        if self
            .thread_store
            .as_any()
            .is::<codex_thread_store::LocalThreadStore>()
            && let Some(rollout_path) = rollout_path
        {
            match self
                .load_reference_backed_turn_window(rollout_path, cursor, limit, sort_direction)
                .await
            {
                Ok(window) => return Ok(window),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(ThreadReadViewError::Internal(format!(
                        "failed to load thread history {}: {err}",
                        rollout_path.display()
                    )));
                }
            }
        }
        match self
            .thread_store
            .read_thread(StoreReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: true,
            })
            .await
        {
            Ok(stored_thread) => {
                let history = stored_thread.history.ok_or_else(|| {
                    ThreadReadViewError::Internal(format!(
                        "thread store did not return history for thread {thread_id}"
                    ))
                })?;
                return Ok(LegacyHistoryWindow {
                    items: history.items,
                    has_older_reference: false,
                });
            }
            Err(ThreadStoreError::InvalidRequest { message })
                if message == format!("no rollout found for thread id {thread_id}") => {}
            Err(ThreadStoreError::ThreadNotFound {
                thread_id: missing_thread_id,
            }) if missing_thread_id == thread_id => {}
            Err(ThreadStoreError::InvalidRequest { message }) => {
                return Err(ThreadReadViewError::InvalidRequest(message));
            }
            Err(ThreadStoreError::Unsupported { operation }) => {
                return Err(ThreadReadViewError::Unsupported(operation));
            }
            Err(err) => {
                return Err(ThreadReadViewError::Internal(format!(
                    "failed to read thread: {err}"
                )));
            }
        }

        let thread = self
            .thread_manager
            .get_thread(thread_id)
            .await
            .map_err(|_| {
                ThreadReadViewError::InvalidRequest(format!("thread not loaded: {thread_id}"))
            })?;
        let config_snapshot = thread.config_snapshot().await;
        if config_snapshot.ephemeral {
            return Err(ThreadReadViewError::InvalidRequest(
                "ephemeral threads do not support thread/turns/list".to_string(),
            ));
        }

        thread
            .load_history(/*include_archived*/ true)
            .await
            .map(|history| LegacyHistoryWindow {
                items: history.items,
                has_older_reference: false,
            })
            .map_err(|err| thread_turns_list_history_load_error(thread_id, err))
    }

    async fn load_recent_paginated_turn_window(
        &self,
        rollout_path: &Path,
        limit: Option<u32>,
    ) -> std::io::Result<Option<LegacyHistoryWindow>> {
        if rollout_path
            .extension()
            .is_some_and(|extension| extension == "zst")
        {
            return Ok(None);
        }

        let session_meta = codex_rollout::read_session_meta_line(rollout_path).await?;
        if session_meta.meta.history_mode != ThreadHistoryMode::Paginated {
            return Ok(None);
        }
        let rollout_path = rollout_path.to_path_buf();
        let page_size = thread_turns_page_size(limit);

        tokio::task::spawn_blocking(move || {
            let mut scanner = ReverseJsonlScanner::new(File::open(rollout_path)?)?;
            let mut reversed_items = Vec::new();
            let mut started_turns = 0;

            while let Some(outcome) = scanner.scan_next_rollout_line()? {
                let line = match outcome {
                    ScanOutcome::Parsed(line) => line,
                    ScanOutcome::Rejected(_) => return Ok(None),
                };
                match &line.item {
                    RolloutItem::SessionMeta(_) => {
                        if session_meta.meta.history_base.is_some() {
                            return Ok(None);
                        }
                        reversed_items.reverse();
                        let mut items = vec![RolloutItem::SessionMeta(session_meta)];
                        items.extend(reversed_items);
                        return Ok(Some(LegacyHistoryWindow {
                            items,
                            has_older_reference: false,
                        }));
                    }
                    RolloutItem::RolloutReference(_) => return Ok(None),
                    RolloutItem::EventMsg(EventMsg::TurnStarted(_)) => {
                        started_turns += 1;
                    }
                    _ => {}
                }
                reversed_items.push(line.item);

                if started_turns >= page_size {
                    let has_older_reference = session_meta.meta.history_base.is_some()
                        || match scanner.scan_next_rollout_line()? {
                            Some(ScanOutcome::Parsed(line)) => {
                                !matches!(line.item, RolloutItem::SessionMeta(_))
                            }
                            Some(ScanOutcome::Rejected(_)) => return Ok(None),
                            None => false,
                        };
                    reversed_items.reverse();
                    let mut items = Vec::with_capacity(reversed_items.len() + 1);
                    items.push(RolloutItem::SessionMeta(session_meta));
                    items.extend(reversed_items);
                    return Ok(Some(LegacyHistoryWindow {
                        items,
                        has_older_reference,
                    }));
                }
            }

            Ok(None)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn load_reference_backed_turn_window(
        &self,
        rollout_path: &Path,
        cursor: Option<&ThreadTurnsCursor>,
        limit: Option<u32>,
        sort_direction: SortDirection,
    ) -> std::io::Result<LegacyHistoryWindow> {
        if matches!(sort_direction, SortDirection::Asc) && cursor.is_none() {
            return Ok(LegacyHistoryWindow {
                items: codex_rollout::materialize_recent_rollout_items(
                    self.config.codex_home.as_path(),
                    rollout_path,
                )
                .await?,
                has_older_reference: false,
            });
        }

        let page_size = limit
            .map(|value| value as usize)
            .unwrap_or(THREAD_TURNS_DEFAULT_LIMIT)
            .clamp(1, THREAD_TURNS_MAX_LIMIT);
        let generation = if matches!(sort_direction, SortDirection::Desc) {
            Some(LegacyRolloutGeneration::capture(rollout_path).await?)
        } else {
            None
        };
        let recent_reference_limit =
            codex_rollout::FRODEX_RECENT_ROLLOUT_SEGMENTS.saturating_sub(1);
        // A cursorless Desktop open stays within the normal five-segment read bound. Once the
        // client follows the returned cursor, the request is an explicit historical page and may
        // expand only far enough to assemble that page.
        let max_reference_limit = if cursor.is_some() {
            codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
        } else {
            recent_reference_limit
        };
        let mut ordinary_reference_limit = match (&generation, cursor) {
            (Some(generation), Some(cursor)) if !cursor.include_anchor => self
                .legacy_page_depth_hints
                .lock()
                .await
                .lookup(generation, Some(cursor.turn_id.as_str()), page_size)
                .unwrap_or(DEFAULT_ROLLOUT_REFERENCE_DEPTH),
            (Some(generation), None) => self
                .legacy_page_depth_hints
                .lock()
                .await
                .lookup(generation, /*turn_id*/ None, page_size)
                .unwrap_or(DEFAULT_ROLLOUT_REFERENCE_DEPTH),
            _ => DEFAULT_ROLLOUT_REFERENCE_DEPTH,
        }
        .min(max_reference_limit);
        let mut materializer = codex_rollout::BoundedRolloutMaterializer::new(
            self.config.codex_home.as_path(),
            rollout_path,
        );
        let mut partial_before_error = None;
        let mut last_successful_reference_limit: Option<usize> = None;
        loop {
            let materialized = match materializer.materialize(ordinary_reference_limit).await {
                Ok(materialized) => materialized,
                Err(error) => {
                    if cursor.is_some()
                        && let Some(last_successful_reference_limit) =
                            last_successful_reference_limit
                    {
                        let mut lower = last_successful_reference_limit.saturating_add(1);
                        let mut upper = ordinary_reference_limit.saturating_sub(1);
                        let mut recovered = None;
                        while lower <= upper {
                            let reference_limit = lower + (upper - lower) / 2;
                            match materializer.materialize(reference_limit).await {
                                Ok(materialized) => {
                                    let items = materialized
                                        .lines
                                        .into_iter()
                                        .map(|line| line.item)
                                        .collect::<Vec<_>>();
                                    let turns = build_legacy_api_turns_from_rollout_items(&items);
                                    if legacy_page_next_turn_id(
                                        turns.as_slice(),
                                        cursor,
                                        page_size,
                                        sort_direction,
                                    )
                                    .is_some_and(|turn_id| !turn_id.starts_with("rollout-"))
                                    {
                                        recovered = Some(items);
                                    }
                                    lower = reference_limit.saturating_add(1);
                                }
                                Err(_) => {
                                    let Some(next_upper) = reference_limit.checked_sub(1) else {
                                        break;
                                    };
                                    upper = next_upper;
                                }
                            }
                        }
                        if let Some(items) = recovered {
                            return Ok(LegacyHistoryWindow {
                                items,
                                has_older_reference: true,
                            });
                        }
                    }
                    if let Some(items) = partial_before_error {
                        return Ok(LegacyHistoryWindow {
                            items,
                            has_older_reference: true,
                        });
                    }
                    return Err(error);
                }
            };
            last_successful_reference_limit = Some(ordinary_reference_limit);
            let items = materialized
                .lines
                .into_iter()
                .map(|line| line.item)
                .collect::<Vec<_>>();
            let turns = build_legacy_api_turns_from_rollout_items(&items);
            let page_is_coherent = legacy_turn_window_is_coherent(
                turns.as_slice(),
                cursor,
                page_size,
                sort_direction,
                materialized.has_older_reference,
            );
            if page_is_coherent {
                let has_available_older_reference = materialized.has_older_reference;
                if has_available_older_reference
                    && !items.iter().any(|item| {
                        matches!(item, RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_)))
                    })
                    && let Some(generation) = generation.as_ref()
                    && LegacyRolloutGeneration::capture(rollout_path).await? == *generation
                {
                    let mut hints = self.legacy_page_depth_hints.lock().await;
                    if cursor.is_none() {
                        hints.insert(LegacyPageDepthHint {
                            generation: generation.clone(),
                            cursor_turn_id: None,
                            page_size,
                            reference_depth: ordinary_reference_limit,
                        });
                    }
                    if let Some(next_turn_id) = legacy_page_next_turn_id(
                        turns.as_slice(),
                        cursor,
                        page_size,
                        sort_direction,
                    ) && !next_turn_id.starts_with("rollout-")
                    {
                        hints.insert(LegacyPageDepthHint {
                            generation: generation.clone(),
                            cursor_turn_id: Some(next_turn_id),
                            page_size,
                            reference_depth: ordinary_reference_limit,
                        });
                    }
                }
                return Ok(LegacyHistoryWindow {
                    items,
                    has_older_reference: has_available_older_reference
                        && matches!(sort_direction, SortDirection::Desc),
                });
            }
            if !materialized.has_older_reference || ordinary_reference_limit >= max_reference_limit
            {
                return Ok(LegacyHistoryWindow {
                    items,
                    has_older_reference: (cursor.is_some()
                        || legacy_page_next_turn_id(
                            turns.as_slice(),
                            cursor,
                            page_size,
                            sort_direction,
                        )
                        .is_some_and(|turn_id| !turn_id.starts_with("rollout-")))
                        && materialized.has_older_reference
                        && matches!(sort_direction, SortDirection::Desc),
                });
            }

            partial_before_error = if cursor.is_some()
                && matches!(sort_direction, SortDirection::Desc)
                && legacy_page_next_turn_id(turns.as_slice(), cursor, page_size, sort_direction)
                    .is_some_and(|turn_id| !turn_id.starts_with("rollout-"))
            {
                Some(items)
            } else {
                None
            };

            let next_limit = ordinary_reference_limit.checked_mul(2).ok_or_else(|| {
                std::io::Error::other("rollout reference depth exceeds addressable memory")
            })?;
            ordinary_reference_limit = next_limit.min(max_reference_limit);
        }
    }

    pub(crate) fn thread_created_receiver(&self) -> broadcast::Receiver<ThreadId> {
        self.thread_manager.subscribe_thread_created()
    }

    pub(crate) async fn connection_initialized(
        &self,
        connection_id: ConnectionId,
        capabilities: ConnectionCapabilities,
    ) {
        self.thread_state_manager
            .connection_initialized(connection_id, capabilities)
            .await;
    }

    pub(crate) async fn connection_closed(&self, connection_id: ConnectionId) {
        let thread_ids = self
            .thread_state_manager
            .remove_connection(connection_id)
            .await;

        for thread_id in thread_ids {
            if self.thread_manager.get_thread(thread_id).await.is_err() {
                // Reconcile stale app-server bookkeeping when the thread has already been
                // removed from the core manager.
                self.finalize_thread_teardown(thread_id).await;
            }
        }
    }

    pub(crate) fn subscribe_running_assistant_turn_count(&self) -> watch::Receiver<usize> {
        self.thread_watch_manager.subscribe_running_turn_count()
    }

    /// Best-effort: ensure initialized connections are subscribed to this thread.
    pub(crate) async fn try_attach_thread_listener(
        &self,
        thread_id: ThreadId,
        connection_ids: Vec<ConnectionId>,
    ) {
        let mut raw_events_enabled = false;
        if let Ok(thread) = self.thread_manager.get_thread(thread_id).await {
            let config_snapshot = thread.config_snapshot().await;
            self.thread_watch_manager
                .upsert_thread(&thread_id.to_string())
                .await;
            if let Some(parent_thread_id) = config_snapshot.parent_thread_id {
                raw_events_enabled = self
                    .thread_state_manager
                    .thread_state(parent_thread_id)
                    .await
                    .lock()
                    .await
                    .experimental_raw_events;
            }
        }

        for connection_id in connection_ids {
            log_listener_attach_result(
                self.ensure_conversation_listener(thread_id, connection_id, raw_events_enabled)
                    .await,
                thread_id,
                connection_id,
                "thread",
            );
        }
    }

    async fn thread_resume_inner(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadResumeParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
    ) -> Result<(), JSONRPCErrorError> {
        if let Ok(thread_id) = ThreadId::from_string(&params.thread_id)
            && self
                .pending_thread_unloads
                .lock()
                .await
                .contains(&thread_id)
        {
            self.outgoing
                .send_error(
                    request_id,
                    invalid_request(format!(
                        "thread {thread_id} is closing; retry thread/resume after the thread is closed"
                    )),
                )
                .await;
            return Ok(());
        }

        if params.sandbox.is_some() && params.permissions.is_some() {
            self.outgoing
                .send_error(
                    request_id,
                    invalid_request("`permissions` cannot be combined with `sandbox`"),
                )
                .await;
            return Ok(());
        }
        let redact_resume_payloads =
            should_redact_thread_resume_payloads(app_server_client_name.as_deref());

        let _thread_list_state_permit = match self.acquire_thread_resume_permit(&params).await {
            Ok(permit) => permit,
            Err(error) => {
                self.outgoing.send_error(request_id, error).await;
                return Ok(());
            }
        };
        if let Ok(thread_id) = ThreadId::from_string(&params.thread_id)
            && self
                .pending_thread_unloads
                .lock()
                .await
                .contains(&thread_id)
        {
            self.outgoing
                .send_error(
                    request_id,
                    invalid_request(format!(
                        "thread {thread_id} is closing; retry thread/resume after the thread is closed"
                    )),
                )
                .await;
            return Ok(());
        }
        let stored_thread_from_running_probe = match self
            .resume_running_thread(
                &request_id,
                &params,
                app_server_client_name.clone(),
                app_server_client_version.clone(),
                /*cold_resume_history*/ None,
            )
            .await
        {
            Ok(RunningThreadResumeResult::Handled) => return Ok(()),
            Ok(RunningThreadResumeResult::NotRunning(stored_thread)) => stored_thread,
            Err(error) => {
                self.outgoing.send_error(request_id, error).await;
                return Ok(());
            }
        };

        let ThreadResumeParams {
            thread_id,
            history,
            path,
            model,
            model_provider,
            service_tier,
            cwd,
            runtime_workspace_roots,
            approval_policy,
            approvals_reviewer,
            sandbox,
            permissions,
            config: mut request_overrides,
            base_instructions,
            developer_instructions,
            personality,
            exclude_turns,
            initial_turns_page,
        } = params;
        let include_turns = !exclude_turns;

        let resume_result = if let Some(history) = history {
            self.resume_thread_from_history(history.as_slice())
                .await
                .map(|thread_history| (thread_history, None))
        } else if let Some(stored_thread) = stored_thread_from_running_probe {
            self.load_resume_initial_history_from_stored_thread_with_options(
                *stored_thread,
                include_turns,
            )
            .await
            .map(|(thread_history, stored_thread)| (thread_history, Some(stored_thread)))
        } else {
            match self
                .read_stored_thread_for_resume(
                    &thread_id,
                    path.as_ref(),
                    /*include_history*/ false,
                )
                .await
            {
                Ok(stored_thread) => self
                    .load_resume_initial_history_from_stored_thread_with_options(
                        stored_thread,
                        include_turns,
                    )
                    .await
                    .map(|(thread_history, stored_thread)| (thread_history, Some(stored_thread))),
                Err(error) => Err(error),
            }
        };
        let (thread_history, resume_source_thread) = match resume_result {
            Ok(value) => value,
            Err(error) => {
                self.outgoing.send_error(request_id, error).await;
                return Ok(());
            }
        };
        let paginated_thread_id = resume_source_thread.as_ref().and_then(|thread| {
            matches!(thread.history_mode, ThreadHistoryMode::Paginated).then_some(thread.thread_id)
        });
        let paginated_resume = paginated_thread_id.is_some();
        if paginated_resume && include_turns {
            self.send_deprecation_notice(
                request_id.connection_id,
                PAGINATED_FULL_HISTORY_DEPRECATION_SUMMARY,
            )
            .await;
        }
        let indexed_legacy_thread_id = if !include_turns
            && let Some(thread) = resume_source_thread.as_ref()
            && matches!(thread.history_mode, ThreadHistoryMode::Legacy)
            && let Some(store) = self
                .thread_store
                .as_any()
                .downcast_ref::<codex_thread_store::LocalThreadStore>()
            && store
                .has_complete_segmented_legacy_projection(thread.thread_id)
                .await
                .map_err(thread_store_resume_read_error)?
        {
            Some(thread.thread_id)
        } else {
            None
        };
        let bounded_legacy_initial_turns_history = if !paginated_resume
            && indexed_legacy_thread_id.is_none()
            && !include_turns
            && let Some(stored_thread) = resume_source_thread.as_ref()
            && matches!(stored_thread.history_mode, ThreadHistoryMode::Legacy)
            && self
                .thread_store
                .as_any()
                .is::<codex_thread_store::LocalThreadStore>()
            && let Some(page) = initial_turns_page.as_ref()
            && matches!(
                page.sort_direction.unwrap_or(SortDirection::Desc),
                SortDirection::Desc
            )
            && let Some(rollout_path) = stored_thread.rollout_path.as_deref()
        {
            match self
                .load_reference_backed_turn_window(
                    rollout_path,
                    /*cursor*/ None,
                    page.limit,
                    SortDirection::Desc,
                )
                .await
            {
                Ok(history) => Some(history),
                Err(error) => {
                    self.outgoing
                        .send_error(
                            request_id,
                            internal_error(format!(
                                "failed to load thread history {}: {error}",
                                rollout_path.display()
                            )),
                        )
                        .await;
                    return Ok(());
                }
            }
        } else {
            None
        };
        let needs_paginated_projection = paginated_resume && include_turns;
        let paginated_projection_was_missing = if let Some(thread_id) = paginated_thread_id {
            match self.has_paginated_history_projection(thread_id).await {
                Ok(has_projection) => !has_projection,
                Err(error) => {
                    self.outgoing.send_error(request_id, error).await;
                    return Ok(());
                }
            }
        } else {
            false
        };
        let mut unprojected_initial_turns_page = if paginated_projection_was_missing
            && let (Some(thread_id), Some(params)) =
                (paginated_thread_id, initial_turns_page.as_ref())
        {
            match self
                .paginated_resume_initial_turns_page(thread_id, params)
                .await
            {
                Ok(page) => Some(page),
                Err(error) => {
                    self.outgoing.send_error(request_id, error).await;
                    return Ok(());
                }
            }
        } else {
            None
        };

        // Parent-owned V2 children must resume through their owner, not caller configuration.
        if let InitialHistory::Resumed(resumed_history) = &thread_history
            && let Some((source, _)) = thread_history.get_resumed_session_sources()
            && !can_accept_direct_input(thread_history.get_multi_agent_version(), &source)
        {
            let child_thread_id = resumed_history.conversation_id;
            self.thread_manager
                .ensure_multi_agent_v2_child_loaded(child_thread_id)
                .await
                .map_err(|err| {
                    tracing::warn!(
                        thread_id = %child_thread_id,
                        error = %err,
                        "failed to resume a multi-agent v2 child through its parent"
                    );
                    invalid_request(
                        "cannot resume an unloaded multi-agent v2 sub-agent through its parent; resume the parent first, or use thread/read to inspect it",
                    )
                })?;

            let cold_resume_history = paginated_resume.then(|| thread_history.get_rollout_items());
            // Attach to the resolved child with only the caller's history-paging preferences.
            let attach_params = ThreadResumeParams {
                thread_id: child_thread_id.to_string(),
                exclude_turns,
                initial_turns_page,
                ..Default::default()
            };
            return match self
                .resume_running_thread(
                    &request_id,
                    &attach_params,
                    app_server_client_name,
                    app_server_client_version,
                    cold_resume_history,
                )
                .await?
            {
                RunningThreadResumeResult::Handled => Ok(()),
                RunningThreadResumeResult::NotRunning(_) => Err(invalid_request(
                    "cannot resume an unloaded multi-agent v2 sub-agent through its parent; resume the parent first, or use thread/read to inspect it",
                )),
            };
        }

        let history_cwd = thread_history.session_cwd();
        let runtime_workspace_roots = runtime_workspace_roots.map(resolve_runtime_workspace_roots);
        let mut typesafe_overrides = self.build_thread_config_overrides(
            model,
            model_provider,
            service_tier,
            cwd,
            runtime_workspace_roots,
            approval_policy,
            approvals_reviewer,
            sandbox,
            permissions,
            base_instructions,
            developer_instructions,
            personality,
        );
        if typesafe_overrides.approval_policy.is_none()
            && let Some(value) = request_overrides
                .as_mut()
                .and_then(|overrides| overrides.remove("approval_policy"))
        {
            let approval_policy = match serde_json::from_value(value) {
                Ok(approval_policy) => approval_policy,
                Err(err) => {
                    self.outgoing
                        .send_error(
                            request_id,
                            invalid_params(format!(
                                "invalid `approval_policy` config override: {err}"
                            )),
                        )
                        .await;
                    return Ok(());
                }
            };
            typesafe_overrides.approval_policy = Some(approval_policy);
        }
        let has_explicit_model_resume_override =
            has_model_resume_override(request_overrides.as_ref(), &typesafe_overrides);
        let persisted_metadata = self
            .load_and_apply_persisted_resume_metadata(
                &thread_history,
                &mut request_overrides,
                &mut typesafe_overrides,
            )
            .await;

        // Derive a Config using the same logic as new conversation, honoring overrides if provided.
        let mut config = match self
            .config_manager
            .load_for_cwd(request_overrides, typesafe_overrides, history_cwd)
            .await
        {
            Ok(config) => config,
            Err(err) => {
                let error = config_load_error(&err);
                self.outgoing.send_error(request_id, error).await;
                return Ok(());
            }
        };
        if !has_explicit_model_resume_override
            && persisted_metadata
                .as_ref()
                .is_some_and(|metadata| metadata.reasoning_effort.is_none())
        {
            config.model_reasoning_effort = None;
        }

        let response_history = thread_history.clone();

        match self
            .thread_manager
            .resume_thread_with_history(
                config,
                thread_history,
                self.auth_manager.clone(),
                self.request_trace_context(&request_id).await,
                client_mcp_extensions,
            )
            .await
        {
            Ok(NewThread {
                thread_id,
                thread: codex_thread,
                session_configured,
                ..
            }) => {
                if let Err(err) = Self::set_app_server_client_info(
                    codex_thread.as_ref(),
                    app_server_client_name,
                    app_server_client_version,
                )
                .await
                {
                    self.outgoing.send_error(request_id, err).await;
                    return Ok(());
                }
                let instruction_sources = codex_thread.legacy_instruction_sources().await;
                let SessionConfiguredEvent { rollout_path, .. } = session_configured;
                let Some(rollout_path) = rollout_path else {
                    let error =
                        internal_error(format!("rollout path missing for thread {thread_id}"));
                    self.outgoing.send_error(request_id, error).await;
                    return Ok(());
                };
                // Paginated JSONL is canonical, but its SQLite projection can lag after a
                // previous write failure. Persist after reopening the live writer so legacy
                // response hydration reads the latest durable turns and items.
                if needs_paginated_projection
                    && let Err(error) = self
                        .thread_store
                        .persist_thread(thread_id, PersistContext::Standard)
                        .await
                        .map_err(thread_store_resume_read_error)
                {
                    self.outgoing.send_error(request_id, error).await;
                    return Ok(());
                }
                let materialized_turns = if paginated_resume && include_turns {
                    match self.paginated_thread_full_turns(thread_id).await {
                        Ok(turns) => Some(turns),
                        Err(error) => {
                            self.outgoing.send_error(request_id, error).await;
                            return Ok(());
                        }
                    }
                } else {
                    None
                };
                // Auto-attach a thread listener when resuming a thread.
                log_listener_attach_result(
                    self.ensure_conversation_listener(
                        thread_id,
                        request_id.connection_id,
                        /*raw_events_enabled*/ false,
                    )
                    .await,
                    thread_id,
                    request_id.connection_id,
                    "thread",
                );

                let mut thread = match self
                    .load_thread_from_resume_source_or_send_internal(
                        thread_id,
                        codex_thread.as_ref(),
                        &response_history,
                        rollout_path.as_path(),
                        resume_source_thread,
                        include_turns && !paginated_resume,
                    )
                    .await
                {
                    Ok(thread) => thread,
                    Err(message) => {
                        self.outgoing
                            .send_error(request_id, internal_error(message))
                            .await;
                        return Ok(());
                    }
                };
                thread.thread_source = codex_thread
                    .config_snapshot()
                    .await
                    .thread_source
                    .map(Into::into);
                if let Some(materialized_turns) = materialized_turns {
                    thread.turns = materialized_turns;
                }

                self.thread_watch_manager.upsert_thread(&thread.id).await;

                let thread_status = self
                    .thread_watch_manager
                    .loaded_status_for_thread(&thread.id)
                    .await;

                set_thread_status_and_interrupt_stale_turns(
                    &mut thread,
                    thread_status,
                    /*has_live_in_progress_turn*/ false,
                );
                let config_snapshot = codex_thread.config_snapshot().await;
                let (turns_backwards_cursor, items_backwards_cursor) =
                    if matches!(config_snapshot.history_mode, ThreadHistoryMode::Paginated) {
                        let cursors = if paginated_projection_was_missing {
                            self.unprojected_paginated_resume_backwards_cursors(thread_id)
                                .await
                        } else {
                            Self::paginated_resume_backwards_cursors(
                                self.thread_store.as_ref(),
                                thread_id,
                            )
                            .await
                        };
                        match cursors {
                            Ok(cursors) => cursors,
                            Err(error) => {
                                self.outgoing.send_error(request_id, error).await;
                                return Ok(());
                            }
                        }
                    } else {
                        (None, None)
                    };
                let sandbox = config_snapshot.sandbox_policy().into();
                let active_permission_profile = thread_response_active_permission_profile(
                    config_snapshot.active_permission_profile,
                );
                let mut initial_turns_page = if let Some(params) = initial_turns_page.as_ref() {
                    let initial_turns_page_result = if paginated_resume {
                        match unprojected_initial_turns_page.take() {
                            Some(page) => Ok(page),
                            None => {
                                self.paginated_resume_initial_turns_page(thread_id, params)
                                    .await
                            }
                        }
                    } else if let Some(thread_id) = indexed_legacy_thread_id
                        && let Some(store) = self
                            .thread_store
                            .as_any()
                            .downcast_ref::<codex_thread_store::LocalThreadStore>()
                    {
                        self.projected_legacy_thread_turns_list_response(
                            store,
                            thread_id,
                            /*cursor*/ None,
                            params.limit,
                            ProjectedLegacyThreadTurnsPageOptions {
                                sort_direction: params
                                    .sort_direction
                                    .unwrap_or(SortDirection::Desc),
                                items_view: params.items_view,
                                allow_running: true,
                            },
                        )
                        .await?
                        .map(codex_app_server_protocol::TurnsPage::from)
                        .ok_or_else(|| {
                            internal_error(format!(
                                "indexed Legacy history disappeared during resume for thread {thread_id}"
                            ))
                        })
                    } else {
                        let (history_items, has_older_reference) =
                            bounded_legacy_initial_turns_history.as_ref().map_or_else(
                                || (response_history.get_rollout_items(), false),
                                |history| (history.items.as_slice(), history.has_older_reference),
                            );
                        build_thread_resume_initial_turns_page(
                            history_items,
                            thread.status.clone(),
                            /*has_live_running_thread*/ false,
                            /*active_turn*/ None,
                            params,
                            has_older_reference,
                        )
                    };
                    match initial_turns_page_result {
                        Ok(page) => Some(page),
                        Err(error) => {
                            self.outgoing.send_error(request_id, error).await;
                            return Ok(());
                        }
                    }
                } else {
                    None
                };
                if indexed_legacy_thread_id.is_some() {
                    self.indexed_legacy_history_threads
                        .lock()
                        .await
                        .insert(thread_id);
                }
                if !paginated_resume
                    && indexed_legacy_thread_id.is_none()
                    && !include_turns
                    && bounded_legacy_initial_turns_history.is_some()
                {
                    self.bounded_legacy_history_threads
                        .lock()
                        .await
                        .insert(thread_id);
                }
                let token_usage_turn_id = (include_turns || paginated_resume)
                    .then(|| {
                        let turns = if thread.turns.is_empty() {
                            initial_turns_page
                                .as_ref()
                                .map_or(&[][..], |page| page.data.as_slice())
                        } else {
                            thread.turns.as_slice()
                        };
                        restored_token_usage_turn_id(response_history.get_rollout_items(), turns)
                    })
                    .filter(|turn_id| !turn_id.is_empty());
                if let Some(projection) = self
                    .subagent_history_projection_from_rollout(thread_id, rollout_path.as_path())
                    .await
                {
                    projection.project_turns(&mut thread.turns);
                    if let Some(initial_turns_page) = initial_turns_page.as_mut() {
                        projection.project_turns(&mut initial_turns_page.data);
                    }
                }
                if redact_resume_payloads {
                    redact_thread_resume_payloads(&mut thread.turns);
                    if let Some(initial_turns_page) = initial_turns_page.as_mut() {
                        redact_thread_resume_payloads(&mut initial_turns_page.data);
                    }
                }

                let thread_originator = config_snapshot.originator.clone();
                let response = ThreadResumeResponse {
                    thread,
                    model: session_configured.model,
                    model_provider: session_configured.model_provider_id,
                    service_tier: session_configured.service_tier,
                    cwd: session_configured.cwd,
                    runtime_workspace_roots: config_snapshot.workspace_roots,
                    instruction_sources,
                    approval_policy: session_configured.approval_policy.into(),
                    approvals_reviewer: session_configured.approvals_reviewer.into(),
                    sandbox,
                    active_permission_profile,
                    reasoning_effort: session_configured.reasoning_effort,
                    multi_agent_mode: MultiAgentMode::ExplicitRequestOnly,
                    initial_turns_page,
                    turns_backwards_cursor,
                    items_backwards_cursor,
                };

                let connection_id = request_id.connection_id;
                self.outgoing
                    .send_response_with_thread_originator(request_id, response, thread_originator)
                    .await;
                // `excludeTurns` is explicitly the cheap resume path, so avoid
                // rebuilding history only to attribute a replayed usage update.
                if let Some(token_usage_turn_id) = token_usage_turn_id {
                    // The client needs restored usage before it starts another turn.
                    // Sending after the response preserves JSON-RPC request ordering while
                    // still filling the status line before the next turn lifecycle begins.
                    send_thread_token_usage_update_to_connection(
                        &self.outgoing,
                        connection_id,
                        thread_id,
                        codex_thread.as_ref(),
                        token_usage_turn_id,
                    )
                    .await;
                }
                self.thread_goal_processor
                    .emit_resume_goal_snapshot(thread_id)
                    .await;
                codex_thread
                    .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
                    .await;
            }
            Err(err) => {
                let error = match err.details() {
                    CodexErrorDetails::InvalidRequest(message) => invalid_request(message.clone()),
                    _ => internal_error(format!("error resuming thread: {err}")),
                };
                self.outgoing.send_error(request_id, error).await;
            }
        }
        Ok(())
    }

    async fn load_and_apply_persisted_resume_metadata(
        &self,
        thread_history: &InitialHistory,
        request_overrides: &mut Option<HashMap<String, serde_json::Value>>,
        typesafe_overrides: &mut ConfigOverrides,
    ) -> Option<ThreadMetadata> {
        let InitialHistory::Resumed(resumed_history) = thread_history else {
            return None;
        };
        if let Some(persisted_settings) = latest_persisted_resume_settings(&resumed_history.history)
        {
            if typesafe_overrides.approval_policy.is_none() {
                typesafe_overrides.approval_policy = Some(persisted_settings.approval_policy);
            }
            if typesafe_overrides.approvals_reviewer.is_none()
                && !request_overrides
                    .as_ref()
                    .is_some_and(|overrides| overrides.contains_key("approvals_reviewer"))
            {
                typesafe_overrides.approvals_reviewer = persisted_settings.approvals_reviewer;
            }
            if !has_permission_override(request_overrides.as_ref(), typesafe_overrides) {
                typesafe_overrides.persisted_permission_profile_id = persisted_settings
                    .active_permission_profile
                    .map(|profile| profile.id);
            }
        }
        let state_db_ctx = self.state_db.clone()?;
        let persisted_metadata = state_db_ctx
            .get_thread(resumed_history.conversation_id)
            .await
            .ok()
            .flatten()?;
        merge_persisted_resume_metadata(request_overrides, typesafe_overrides, &persisted_metadata);
        Some(persisted_metadata)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn resume_running_thread(
        &self,
        request_id: &ConnectionRequestId,
        params: &ThreadResumeParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        cold_resume_history: Option<&[RolloutItem]>,
    ) -> Result<RunningThreadResumeResult, JSONRPCErrorError> {
        let running_thread = if params.history.is_some() {
            if let Ok(existing_thread_id) = ThreadId::from_string(&params.thread_id)
                && self
                    .thread_manager
                    .get_thread(existing_thread_id)
                    .await
                    .is_ok()
            {
                return Err(invalid_request(format!(
                    "cannot resume thread {existing_thread_id} with history while it is already running"
                )));
            }
            None
        } else if let Ok(existing_thread_id) = ThreadId::from_string(&params.thread_id)
            && let Ok(existing_thread) = self.thread_manager.get_thread(existing_thread_id).await
        {
            let source_thread = self
                .read_stored_thread_for_resume(
                    &params.thread_id,
                    /*path*/ None,
                    /*include_history*/ false,
                )
                .await?;
            Some((existing_thread_id, existing_thread, source_thread))
        } else {
            let source_thread = self
                .read_stored_thread_for_resume(
                    &params.thread_id,
                    params.path.as_ref(),
                    /*include_history*/ false,
                )
                .await?;
            let existing_thread_id = source_thread.thread_id;
            match self.thread_manager.get_thread(existing_thread_id).await {
                Ok(existing_thread) => Some((existing_thread_id, existing_thread, source_thread)),
                Err(_) => {
                    return Ok(RunningThreadResumeResult::NotRunning(Some(Box::new(
                        source_thread,
                    ))));
                }
            }
        };

        if let Some((existing_thread_id, existing_thread, mut source_thread)) = running_thread {
            let paginated_resume =
                matches!(source_thread.history_mode, ThreadHistoryMode::Paginated);
            let existing_thread_rollout_path = existing_thread.rollout_path();
            let active_path = existing_thread_rollout_path
                .as_ref()
                .or(source_thread.rollout_path.as_ref());
            if let (Some(requested_path), Some(active_path)) = (params.path.as_ref(), active_path)
                && !path_utils::paths_match_after_normalization(requested_path, active_path)
            {
                return Err(invalid_request(format!(
                    "cannot resume running thread {existing_thread_id} with stale path: requested `{}`, active `{}`",
                    requested_path.display(),
                    active_path.display()
                )));
            }
            let config_snapshot = existing_thread.config_snapshot().await;
            let mismatch_details = collect_resume_override_mismatches(params, &config_snapshot);
            if !mismatch_details.is_empty() {
                let has_subscribers = !self
                    .thread_state_manager
                    .subscribed_connection_ids(existing_thread_id)
                    .await
                    .is_empty();
                let loaded_status = self
                    .thread_watch_manager
                    .loaded_status_for_thread(&existing_thread_id.to_string())
                    .await;
                let is_running =
                    matches!(existing_thread.agent_status().await, AgentStatus::Running);

                // Parent-owned V2 children must not be rebuilt from public resume overrides.
                if can_accept_direct_input(
                    existing_thread.multi_agent_version(),
                    &config_snapshot.session_source,
                ) && !has_subscribers
                    && matches!(loaded_status, ThreadStatus::Idle)
                    && !is_running
                {
                    // A loaded idle thread is only a cache entry. Shut it down
                    // before removing it so cold resume cannot duplicate a
                    // thread that timed out during shutdown.
                    match wait_for_thread_shutdown(&existing_thread).await {
                        ThreadShutdownResult::Complete => {
                            self.thread_manager.remove_thread(&existing_thread_id).await;
                            self.finalize_thread_teardown(existing_thread_id).await;
                            // Shutdown can flush newer rollout items, so reload the
                            // stored thread before starting the replacement session.
                            return Ok(RunningThreadResumeResult::NotRunning(None));
                        }
                        ThreadShutdownResult::SubmitFailed => {
                            warn!("failed to submit Shutdown to thread {existing_thread_id}");
                        }
                        ThreadShutdownResult::TimedOut => {
                            warn!("thread {existing_thread_id} shutdown timed out");
                        }
                    }
                }

                // Preserve rejoin semantics when another client can still observe
                // the loaded thread or shutdown did not complete.
                tracing::warn!(
                    "thread/resume overrides ignored for loaded thread {}: {}",
                    existing_thread_id,
                    mismatch_details.join("; ")
                );
            }
            let redact_resume_payloads =
                should_redact_thread_resume_payloads(app_server_client_name.as_deref());
            let include_turns = !params.exclude_turns;
            if paginated_resume && include_turns {
                self.send_deprecation_notice(
                    request_id.connection_id,
                    PAGINATED_FULL_HISTORY_DEPRECATION_SUMMARY,
                )
                .await;
            }
            let indexed_legacy_generation = !paginated_resume
                && !include_turns
                && self
                    .indexed_legacy_history_threads
                    .lock()
                    .await
                    .contains(&existing_thread_id);
            let projected_legacy_initial_turns_page = if indexed_legacy_generation
                && let Some(page) = params.initial_turns_page.as_ref()
                && let Some(store) = self
                    .thread_store
                    .as_any()
                    .downcast_ref::<codex_thread_store::LocalThreadStore>()
            {
                if !store
                    .has_complete_segmented_legacy_projection(existing_thread_id)
                    .await
                    .map_err(thread_store_resume_read_error)?
                {
                    self.thread_store
                        .persist_thread(existing_thread_id, PersistContext::Standard)
                        .await
                        .map_err(thread_store_resume_read_error)?;
                }
                Some(
                    self.projected_legacy_thread_turns_list_response(
                        store,
                        existing_thread_id,
                        /*cursor*/ None,
                        page.limit,
                        ProjectedLegacyThreadTurnsPageOptions {
                            sort_direction: page.sort_direction.unwrap_or(SortDirection::Desc),
                            items_view: page.items_view,
                            allow_running: true,
                        },
                    )
                    .await?
                    .map(codex_app_server_protocol::TurnsPage::from)
                    .ok_or_else(|| {
                        internal_error(format!(
                            "indexed Legacy history disappeared during running resume for thread {existing_thread_id}"
                        ))
                    })?,
                )
            } else {
                None
            };
            let bounded_legacy_history = if !paginated_resume
                && !indexed_legacy_generation
                && !include_turns
                && self
                    .thread_store
                    .as_any()
                    .is::<codex_thread_store::LocalThreadStore>()
                && let Some(page) = params.initial_turns_page.as_ref()
                && matches!(
                    page.sort_direction.unwrap_or(SortDirection::Desc),
                    SortDirection::Desc
                )
                && let Some(rollout_path) = active_path
            {
                Some(
                    self.load_reference_backed_turn_window(
                        rollout_path,
                        /*cursor*/ None,
                        page.limit,
                        SortDirection::Desc,
                    )
                    .await
                    .map_err(|error| {
                        internal_error(format!(
                            "failed to load thread history {}: {error}",
                            rollout_path.display()
                        ))
                    })?,
                )
            } else {
                None
            };
            let used_bounded_legacy_history = bounded_legacy_history.is_some();
            let needs_history = !paginated_resume
                && !indexed_legacy_generation
                && (include_turns
                    || (params.initial_turns_page.is_some() && bounded_legacy_history.is_none()));
            if needs_history {
                let source_thread_id = source_thread.thread_id.to_string();
                let source_rollout_path = source_thread.rollout_path.clone();
                source_thread = self
                    .read_stored_thread_for_resume(
                        &source_thread_id,
                        source_rollout_path.as_ref(),
                        /*include_history*/ true,
                    )
                    .await?;
            }
            if paginated_resume {
                self.thread_store
                    .persist_thread(existing_thread_id, PersistContext::Standard)
                    .await
                    .map_err(thread_store_resume_read_error)?;
            }
            let (history_items, history_has_older_reference) =
                if let Some(window) = bounded_legacy_history {
                    (window.items, window.has_older_reference)
                } else if needs_history {
                    (
                        source_thread
                            .history
                            .take()
                            .map(|history| history.items)
                            .ok_or_else(|| {
                                internal_error(format!(
                                    "thread {existing_thread_id} did not include persisted history"
                                ))
                            })?,
                        false,
                    )
                } else {
                    (Vec::new(), false)
                };
            if !paginated_resume && used_bounded_legacy_history {
                self.bounded_legacy_history_threads
                    .lock()
                    .await
                    .insert(existing_thread_id);
            }

            let thread_state = self
                .thread_state_manager
                .thread_state(existing_thread_id)
                .await;
            self.ensure_listener_task_running(
                existing_thread_id,
                existing_thread.clone(),
                thread_state.clone(),
            )
            .await?;
            Self::set_app_server_client_info(
                existing_thread.as_ref(),
                app_server_client_name,
                app_server_client_version,
            )
            .await?;

            let mut thread_summary = self.stored_thread_to_api_thread(
                source_thread,
                config_snapshot.model_provider_id.as_str(),
                /*include_turns*/ false,
            );
            thread_summary.session_id = existing_thread.session_configured().session_id.to_string();
            thread_summary.thread_source = config_snapshot.thread_source.clone().map(Into::into);
            thread_summary.can_accept_direct_input = Some(can_accept_direct_input(
                existing_thread.multi_agent_version(),
                &config_snapshot.session_source,
            ));
            let instruction_sources = existing_thread.legacy_instruction_sources().await;

            let listener_command_tx = {
                let thread_state = thread_state.lock().await;
                thread_state.listener_command_tx()
            };
            let Some(listener_command_tx) = listener_command_tx else {
                return Err(internal_error(format!(
                    "failed to enqueue running thread resume for thread {existing_thread_id}: thread listener is not running"
                )));
            };

            let (emit_thread_goal_update, thread_goal_state_db) = self
                .thread_goal_processor
                .pending_resume_goal_state(existing_thread.as_ref())
                .await;
            let paginated_turns = if paginated_resume && include_turns {
                Some(self.paginated_thread_full_turns(existing_thread_id).await?)
            } else {
                None
            };
            let paginated_initial_turns_page = if paginated_resume {
                match params.initial_turns_page.as_ref() {
                    Some(params) => Some(
                        self.paginated_resume_initial_turns_page(existing_thread_id, params)
                            .await?,
                    ),
                    None => None,
                }
            } else {
                projected_legacy_initial_turns_page.clone()
            };
            let cold_resume_token_usage_turn_id = cold_resume_history
                .map(|history| {
                    let turns = paginated_turns
                        .as_deref()
                        .filter(|turns| !turns.is_empty())
                        .unwrap_or_else(|| {
                            paginated_initial_turns_page
                                .as_ref()
                                .map_or(&[][..], |page| page.data.as_slice())
                        });
                    restored_token_usage_turn_id(history, turns)
                })
                .filter(|turn_id| !turn_id.is_empty());
            let paginated_initial_turns_page_with_active_slot = if paginated_resume {
                match params.initial_turns_page.as_ref() {
                    Some(params)
                        if matches!(
                            params.sort_direction.unwrap_or(SortDirection::Desc),
                            SortDirection::Desc
                        ) =>
                    {
                        Some(
                            self.paginated_resume_initial_turns_page_with_active_slot(
                                existing_thread_id,
                                params,
                            )
                            .await?,
                        )
                    }
                    Some(_) | None => None,
                }
            } else if indexed_legacy_generation
                && let Some(page_params) = params.initial_turns_page.as_ref()
                && matches!(
                    page_params.sort_direction.unwrap_or(SortDirection::Desc),
                    SortDirection::Desc
                )
                && let Some(store) = self
                    .thread_store
                    .as_any()
                    .downcast_ref::<codex_thread_store::LocalThreadStore>()
            {
                let page_size = thread_turns_page_size(page_params.limit);
                if page_size == 1 {
                    let mut page = projected_legacy_initial_turns_page
                        .clone()
                        .ok_or_else(|| {
                            internal_error(format!(
                                "indexed Legacy history disappeared during running resume for thread {existing_thread_id}"
                            ))
                        })?;
                    page.next_cursor = page.backwards_cursor.clone();
                    page.data.clear();
                    Some(page)
                } else {
                    self.projected_legacy_thread_turns_list_response(
                        store,
                        existing_thread_id,
                        /*cursor*/ None,
                        Some((page_size - 1) as u32),
                        ProjectedLegacyThreadTurnsPageOptions {
                            sort_direction: SortDirection::Desc,
                            items_view: page_params.items_view,
                            allow_running: true,
                        },
                    )
                    .await?
                    .map(codex_app_server_protocol::TurnsPage::from)
                }
            } else {
                None
            };
            let resume_cursor_store = paginated_resume.then(|| Arc::clone(&self.thread_store));

            let command = crate::thread_state::ThreadListenerCommand::SendThreadResumeResponse(
                Box::new(crate::thread_state::PendingThreadResumeRequest {
                    request_id: request_id.clone(),
                    history_items,
                    cold_resume_token_usage_turn_id,
                    history_has_older_reference,
                    config_snapshot,
                    instruction_sources,
                    thread_summary,
                    emit_thread_goal_update,
                    thread_goal_state_db,
                    include_turns,
                    initial_turns_page: params.initial_turns_page.clone(),
                    paginated_turns,
                    paginated_initial_turns_page,
                    paginated_initial_turns_page_with_active_slot,
                    resume_cursor_store,
                    redact_resume_payloads,
                }),
            );
            if listener_command_tx.send(command).is_err() {
                return Err(internal_error(format!(
                    "failed to enqueue running thread resume for thread {existing_thread_id}: thread listener command channel is closed"
                )));
            }
            return Ok(RunningThreadResumeResult::Handled);
        }
        Ok(RunningThreadResumeResult::NotRunning(None))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn resume_thread_from_history(
        &self,
        history: &[ResponseItem],
    ) -> Result<InitialHistory, JSONRPCErrorError> {
        if history.is_empty() {
            return Err(invalid_request("history must not be empty"));
        }
        Ok(InitialHistory::Forked(
            history
                .iter()
                .cloned()
                .map(|item| RolloutItem::ResponseItem(item.into()))
                .collect(),
        ))
    }

    async fn load_resume_initial_history_from_stored_thread(
        &self,
        stored_thread: StoredThread,
    ) -> Result<(InitialHistory, StoredThread), JSONRPCErrorError> {
        self.load_resume_initial_history_from_stored_thread_with_options(
            stored_thread,
            /*include_turns*/ true,
        )
        .await
    }

    async fn load_resume_initial_history_from_stored_thread_with_options(
        &self,
        stored_thread: StoredThread,
        include_turns: bool,
    ) -> Result<(InitialHistory, StoredThread), JSONRPCErrorError> {
        self.ensure_selected_rollout(&stored_thread).await?;
        let indexed_legacy_resume = !include_turns
            && matches!(stored_thread.history_mode, ThreadHistoryMode::Legacy)
            && if let Some(store) = self
                .thread_store
                .as_any()
                .downcast_ref::<codex_thread_store::LocalThreadStore>()
            {
                store
                    .has_complete_segmented_legacy_projection(stored_thread.thread_id)
                    .await
                    .map_err(thread_store_resume_read_error)?
            } else {
                false
            };
        if matches!(stored_thread.history_mode, ThreadHistoryMode::Paginated)
            || indexed_legacy_resume
        {
            let model_context = self
                .thread_store
                .load_latest_model_context(StoreLoadThreadHistoryParams {
                    thread_id: stored_thread.thread_id,
                    include_archived: true,
                })
                .await
                .map_err(thread_store_resume_read_error)?;
            let history = InitialHistory::Resumed(ResumedHistory {
                conversation_id: model_context.thread_id,
                history: Arc::new(model_context.items),
                rollout_path: stored_thread.rollout_path.clone(),
            });
            return Ok((history, stored_thread));
        }
        let thread_id = stored_thread.thread_id.to_string();
        let rollout_path = stored_thread.rollout_path.clone();
        let mut stored_thread = self
            .read_stored_thread_for_resume(
                &thread_id,
                rollout_path.as_ref(),
                /*include_history*/ true,
            )
            .await?;
        let history = self
            .stored_thread_to_initial_history(&mut stored_thread)
            .await?;
        Ok((history, stored_thread))
    }

    async fn read_stored_thread_for_resume(
        &self,
        thread_id: &str,
        path: Option<&PathBuf>,
        include_history: bool,
    ) -> Result<StoredThread, JSONRPCErrorError> {
        let result = if let Some(path) = path {
            self.thread_store
                .read_thread_by_rollout_path(StoreReadThreadByRolloutPathParams {
                    rollout_path: path.clone(),
                    include_archived: true,
                    include_history,
                })
                .await
        } else {
            let existing_thread_id = match ThreadId::from_string(thread_id) {
                Ok(id) => id,
                Err(err) => {
                    return Err(invalid_request(format!("invalid session id: {err}")));
                }
            };
            let params = StoreReadThreadParams {
                thread_id: existing_thread_id,
                include_archived: true,
                include_history,
            };
            self.thread_store.read_thread(params).await
        };

        let stored_thread = result.map_err(thread_store_resume_read_error)?;
        if let Some(requested_path) = path
            && matches!(stored_thread.history_mode, ThreadHistoryMode::Paginated)
        {
            let current_thread = self
                .thread_store
                .read_thread(StoreReadThreadParams {
                    thread_id: stored_thread.thread_id,
                    include_archived: true,
                    include_history: false,
                })
                .await
                .map_err(thread_store_resume_read_error)?;
            if let Some(current_path) = current_thread.rollout_path.as_ref()
                && !path_utils::paths_match_after_normalization(
                    codex_rollout::plain_rollout_path(requested_path).as_path(),
                    codex_rollout::plain_rollout_path(current_path).as_path(),
                )
            {
                return Err(invalid_request(format!(
                    "cannot resume paginated thread {} with stale path: requested {}, current {}; omit path and resume by thread id",
                    stored_thread.thread_id,
                    requested_path.display(),
                    current_path.display()
                )));
            }
        }
        if stored_thread.archived_at.is_some() {
            let thread_id = stored_thread.thread_id;
            return Err(invalid_request(format!(
                "session {thread_id} is archived. Run `codex unarchive {thread_id}` to unarchive it first."
            )));
        }

        Ok(stored_thread)
    }

    async fn ensure_selected_rollout(
        &self,
        stored_thread: &StoredThread,
    ) -> Result<(), JSONRPCErrorError> {
        if stored_thread.history_mode == ThreadHistoryMode::Legacy
            && stored_thread
                .rollout_path
                .as_ref()
                .is_some_and(|path| !path.starts_with(self.config.codex_home.as_path()))
        {
            let indexed = match self.state_db.as_ref() {
                Some(state_db) => state_db
                    .get_thread(stored_thread.thread_id)
                    .await
                    .map_err(|error| {
                        internal_error(format!("failed to check selected rollout: {error}"))
                    })?
                    .is_some(),
                None => false,
            };
            // An explicitly supplied external Legacy file has no home-selected rollout until
            // its first resume. Existing indexed selections still require the check below.
            if !indexed {
                return Ok(());
            }
        }
        let selected = self
            .thread_store
            .read_thread(StoreReadThreadParams {
                thread_id: stored_thread.thread_id,
                include_archived: true,
                include_history: false,
            })
            .await
            .map_err(thread_store_resume_read_error)?;
        let requested_path = stored_thread
            .rollout_path
            .as_deref()
            .map(codex_rollout::plain_rollout_path);
        let selected_path = selected
            .rollout_path
            .as_deref()
            .map(codex_rollout::plain_rollout_path);
        if requested_path != selected_path {
            return Err(invalid_request(format!(
                "rollout path does not select the current rollout for thread {}",
                stored_thread.thread_id
            )));
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn stored_thread_to_initial_history(
        &self,
        stored_thread: &mut StoredThread,
    ) -> Result<InitialHistory, JSONRPCErrorError> {
        let thread_id = stored_thread.thread_id;
        let history = stored_thread
            .history
            .take()
            .map(|history| history.items)
            .ok_or_else(|| {
                internal_error(format!(
                    "thread {thread_id} did not include persisted history"
                ))
            })?;
        Ok(InitialHistory::Resumed(ResumedHistory {
            conversation_id: thread_id,
            history: Arc::new(history),
            rollout_path: stored_thread.rollout_path.clone(),
        }))
    }

    fn stored_thread_to_api_thread(
        &self,
        stored_thread: StoredThread,
        fallback_provider: &str,
        include_turns: bool,
    ) -> Thread {
        let (mut thread, history) =
            thread_from_stored_thread(stored_thread, fallback_provider, &self.config.cwd);
        if include_turns && let Some(history) = history {
            populate_thread_turns_from_history(
                &mut thread,
                &history.items,
                /*active_turn*/ None,
            );
        }
        thread
    }

    async fn read_stored_thread_for_new_fork(
        &self,
        thread_id: ThreadId,
        include_history: bool,
    ) -> Result<StoredThread, JSONRPCErrorError> {
        self.thread_store
            .read_thread(StoreReadThreadParams {
                thread_id,
                include_archived: true,
                include_history,
            })
            .await
            .map_err(thread_store_resume_read_error)
    }

    async fn load_thread_from_resume_source_or_send_internal(
        &self,
        thread_id: ThreadId,
        thread: &CodexThread,
        thread_history: &InitialHistory,
        rollout_path: &Path,
        resume_source_thread: Option<StoredThread>,
        include_turns: bool,
    ) -> std::result::Result<Thread, String> {
        let config_snapshot = thread.config_snapshot().await;
        let session_id = thread.session_configured().session_id.to_string();
        let can_accept_direct_input = can_accept_direct_input(
            thread.multi_agent_version(),
            &config_snapshot.session_source,
        );
        let thread = match thread_history {
            InitialHistory::Resumed(resumed) => {
                let fallback_provider = config_snapshot.model_provider_id.as_str();
                if let Some(stored_thread) = resume_source_thread {
                    let stored_thread =
                        if let Some(rollout_path) = stored_thread.rollout_path.clone() {
                            self.thread_store
                                .read_thread_by_rollout_path(StoreReadThreadByRolloutPathParams {
                                    rollout_path,
                                    include_archived: true,
                                    include_history: false,
                                })
                                .await
                                .unwrap_or(StoredThread {
                                    history: None,
                                    ..stored_thread
                                })
                        } else {
                            self.thread_store
                                .read_thread(StoreReadThreadParams {
                                    thread_id: stored_thread.thread_id,
                                    include_archived: true,
                                    include_history: false,
                                })
                                .await
                                .unwrap_or(StoredThread {
                                    history: None,
                                    ..stored_thread
                                })
                        };
                    Ok(thread_from_stored_thread(
                        stored_thread,
                        fallback_provider,
                        &self.config.cwd,
                    )
                    .0)
                } else {
                    match self
                        .thread_store
                        .read_thread(StoreReadThreadParams {
                            thread_id: resumed.conversation_id,
                            include_archived: true,
                            include_history: false,
                        })
                        .await
                    {
                        Ok(stored_thread) => Ok(thread_from_stored_thread(
                            stored_thread,
                            fallback_provider,
                            &self.config.cwd,
                        )
                        .0),
                        Err(read_err) => {
                            Err(format!("failed to read thread from store: {read_err}"))
                        }
                    }
                }
            }
            InitialHistory::Forked(items) => {
                let mut thread = build_thread_from_snapshot(
                    thread_id,
                    session_id.clone(),
                    thread.multi_agent_version(),
                    &config_snapshot,
                    Some(rollout_path.into()),
                );
                thread.preview = preview_from_rollout_items(items);
                Ok(thread)
            }
            InitialHistory::New | InitialHistory::Cleared => Err(format!(
                "failed to build resume response for thread {thread_id}: initial history missing"
            )),
        };
        let mut thread = thread?;
        thread.can_accept_direct_input = Some(can_accept_direct_input);
        thread.id = thread_id.to_string();
        thread.session_id = session_id;
        thread.path = Some(rollout_path.to_path_buf());
        if include_turns {
            let history_items = thread_history.get_rollout_items();
            populate_thread_turns_from_history(
                &mut thread,
                history_items,
                /*active_turn*/ None,
            );
        }
        self.attach_thread_name(thread_id, &mut thread).await;
        Ok(thread)
    }

    async fn attach_thread_name(&self, thread_id: ThreadId, thread: &mut Thread) {
        if let Ok(stored_thread) = self
            .thread_store
            .read_thread(StoreReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await
            && let Some(title) = stored_thread.name.as_deref().map(str::trim)
            && !title.is_empty()
        {
            if stored_thread.history_mode == ThreadHistoryMode::Paginated {
                thread.name = Some(title.to_string());
            } else {
                set_thread_name_from_title(thread, title.to_string());
            }
        }
    }

    // Keep the large fork future out of the prepare/import caller's poll frame. Nested
    // unoptimized frames otherwise exhaust the embedded app-server's worker stack at startup.
    pub(super) fn thread_fork_inner(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadForkParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
        handoff: ForkHandoff,
    ) -> impl std::future::Future<Output = Result<(), JSONRPCErrorError>> + Send + '_ {
        Box::pin(self.thread_fork_inner_impl(
            request_id,
            params,
            app_server_client_name,
            app_server_client_version,
            client_mcp_extensions,
            handoff,
        ))
    }

    async fn thread_fork_inner_impl(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadForkParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
        handoff: ForkHandoff,
    ) -> Result<(), JSONRPCErrorError> {
        let (imported, export) = match handoff {
            ForkHandoff::Local => (None, None),
            ForkHandoff::Export(permit) => (None, Some((params.clone(), permit))),
            ForkHandoff::Import(imported) => (Some(imported), None),
        };
        let imported_settings = imported.as_ref().map(|imported| imported.settings.clone());
        let ThreadForkParams {
            thread_id,
            last_turn_id,
            before_turn_id,
            path,
            model,
            model_provider,
            service_tier,
            cwd,
            runtime_workspace_roots,
            approval_policy,
            approvals_reviewer,
            sandbox,
            permissions,
            config: cli_overrides,
            base_instructions,
            developer_instructions,
            ephemeral,
            thread_source,
            exclude_turns,
            defer_goal_continuation,
        } = params;
        if *HISTORY_IO_OBSERVATION_ENABLED {
            tracing::event!(
                target: "codex_history_io",
                tracing::Level::TRACE,
                event.name = "codex.history.fork.start",
                source_thread.id = %thread_id,
                "starting fork history reads"
            );
        }
        let include_turns = !exclude_turns;
        if sandbox.is_some() && permissions.is_some() {
            return Err(invalid_request(
                "`permissions` cannot be combined with `sandbox`",
            ));
        }
        let source_thread = if let Some(imported) = imported.as_ref() {
            imported.source.clone()
        } else {
            let source = self
                .read_stored_thread_for_resume(
                    &thread_id,
                    path.as_ref(),
                    /*include_history*/ false,
                )
                .await?;
            self.ensure_selected_rollout(&source).await?;
            source
        };
        let paginated_source = matches!(source_thread.history_mode, ThreadHistoryMode::Paginated);
        if last_turn_id.is_some() && before_turn_id.is_some() {
            return Err(invalid_request(
                "`beforeTurnId` cannot be combined with `lastTurnId`",
            ));
        }
        if ephemeral && defer_goal_continuation {
            return Err(invalid_request(
                "`deferGoalContinuation` cannot be combined with `ephemeral`",
            ));
        }
        if paginated_source && ephemeral && include_turns {
            return Err(invalid_request(
                "ephemeral paginated thread/fork requires `excludeTurns: true`",
            ));
        }
        if paginated_source && include_turns {
            self.send_deprecation_notice(
                request_id.connection_id,
                PAGINATED_FULL_HISTORY_DEPRECATION_SUMMARY,
            )
            .await;
        }
        let source_thread_id = source_thread.thread_id;
        let source_thread_name = source_thread
            .name
            .as_deref()
            .and_then(codex_core::util::normalize_thread_name);
        let mut source_approvals_reviewer = None;
        let mut source_token_usage_info = None;
        let prepared_fork = if let Some(imported) = imported {
            Some(imported.prepared)
        } else if paginated_source {
            let boundary = match (last_turn_id.as_deref(), before_turn_id.as_deref()) {
                (Some(turn_id), None) => {
                    codex_thread_store::ForkBoundary::ThroughTurn(turn_id.to_string())
                }
                (None, Some(turn_id)) => {
                    codex_thread_store::ForkBoundary::BeforeTurn(turn_id.to_string())
                }
                (None, None) => codex_thread_store::ForkBoundary::Latest,
                (Some(_), Some(_)) => unreachable!("fork boundaries are mutually exclusive"),
            };
            let params = codex_thread_store::PrepareForkParams {
                thread_id: source_thread_id,
                boundary,
            };
            let local_store = self
                .thread_store
                .as_any()
                .downcast_ref::<codex_thread_store::LocalThreadStore>();
            let expected_rollout_id = if local_store.is_some() {
                Some(
                    source_thread
                        .rollout_path
                        .as_deref()
                        .and_then(codex_rollout::rollout_id_from_path)
                        .ok_or_else(|| {
                            invalid_request(format!(
                                "thread {source_thread_id} does not have a canonical rollout path"
                            ))
                        })?,
                )
            } else {
                None
            };
            let model_context =
                if matches!(&params.boundary, codex_thread_store::ForkBoundary::Latest)
                    && let Some(local_store) = local_store
                    && let Ok(parent) = self.thread_manager.get_thread(source_thread_id).await
                    && !matches!(parent.agent_status().await, AgentStatus::Running)
                    && parent.flush_rollout().await.is_ok()
                    && let Ok(Some(expected_position)) = local_store
                        .projected_history_position(source_thread_id)
                        .await
                    && Some(expected_position.thread_id) == expected_rollout_id
                    && let Some(history_before) = parent.model_history_snapshot().await
                {
                    // Conversation history updates precede durable writes, including no-turn
                    // injections. A second flush and matching snapshots exclude pending updates.
                    if parent.flush_rollout().await.is_ok()
                        && parent.model_history_snapshot().await.as_ref().is_some_and(
                            |history_after| Arc::ptr_eq(history_after, &history_before),
                        )
                        && local_store
                            .projected_history_position(source_thread_id)
                            .await
                            .ok()
                            .flatten()
                            .as_ref()
                            == Some(&expected_position)
                        && !matches!(parent.agent_status().await, AgentStatus::Running)
                    {
                        source_approvals_reviewer =
                            Some(parent.config_snapshot().await.approvals_reviewer);
                        source_token_usage_info = parent.token_usage_info().await;
                        Some((history_before, expected_position))
                    } else {
                        None
                    }
                } else {
                    None
                };
            let ephemeral_context_only =
                ephemeral && matches!(&params.boundary, codex_thread_store::ForkBoundary::Latest);
            let prepared =
                if let Some(local_store) = local_store {
                    let Some(expected_rollout_id) = expected_rollout_id else {
                        return Err(invalid_request(format!(
                            "thread {source_thread_id} does not have a canonical rollout ID"
                        )));
                    };
                    match (model_context, include_turns, ephemeral_context_only) {
                        (Some((model_context, expected_position)), true, _) => {
                            local_store
                                .prepare_fork_with_model_context_for_rollout(
                                    params,
                                    model_context,
                                    expected_position,
                                    expected_rollout_id,
                                )
                                .await
                        }
                        (Some((model_context, expected_position)), false, ephemeral) => local_store
                            .prepare_fork_without_response_history_with_model_context_for_rollout(
                                params,
                                model_context,
                                expected_position,
                                expected_rollout_id,
                                ephemeral,
                            )
                            .await,
                        (None, false, ephemeral) => {
                            local_store
                                .prepare_fork_without_response_history_for_rollout(
                                    params,
                                    expected_rollout_id,
                                    ephemeral,
                                )
                                .await
                        }
                        (None, true, _) => {
                            local_store
                                .prepare_fork_for_rollout(params, expected_rollout_id)
                                .await
                        }
                    }
                } else {
                    self.thread_store.prepare_fork(params).await
                };
            let mut prepared = prepared.map_err(|err| match err {
                ThreadStoreError::InvalidRequest { message } => invalid_request(message),
                ThreadStoreError::ThreadNotFound { thread_id } => {
                    invalid_request(format!("no rollout found for thread id {thread_id}"))
                }
                ThreadStoreError::Unsupported { .. } => {
                    method_not_found("paginated_threads is not supported yet")
                }
                err => internal_error(format!("failed to prepare paginated fork: {err}")),
            })?;
            if prepared.shared_model_response_items.is_some()
                && let Some(info) = source_token_usage_info.take()
            {
                let token_usage = RolloutItem::EventMsg(EventMsg::TokenCount(
                    codex_protocol::protocol::TokenCountEvent {
                        info: Some(info),
                        rate_limits: None,
                    },
                ));
                let model_context = Arc::make_mut(&mut prepared.model_context);
                model_context
                    .retain(|item| !matches!(item, RolloutItem::EventMsg(EventMsg::TokenCount(_))));
                model_context.push(token_usage.clone());
                if include_turns {
                    let response_history = Arc::make_mut(&mut prepared.response_history);
                    response_history.retain(|item| {
                        !matches!(item, RolloutItem::EventMsg(EventMsg::TokenCount(_)))
                    });
                    response_history.push(token_usage);
                }
            }
            Some(prepared)
        } else if export.is_some() {
            Some(
                self.prepare_legacy_handoff(&source_thread, ephemeral)
                    .await?,
            )
        } else {
            None
        };
        if let Some((params, permit)) = export {
            let prepared = prepared_fork
                .ok_or_else(|| internal_error("fork preparation returned no snapshot"))?;
            return self
                .publish_fork_handoff(request_id, params, source_thread, prepared, permit)
                .await;
        }
        let projected_response_turns = prepared_fork
            .as_ref()
            .and_then(|prepared| prepared.projected_response_turns.clone());
        let source_history_items = if let Some(prepared_fork) = prepared_fork.as_ref() {
            Arc::clone(&prepared_fork.response_history)
        } else if !include_turns && last_turn_id.is_none() && before_turn_id.is_none() {
            Arc::new(
                self.thread_store
                    .load_latest_model_context(StoreLoadThreadHistoryParams {
                        thread_id: source_thread_id,
                        include_archived: true,
                    })
                    .await
                    .map_err(thread_store_resume_read_error)?
                    .items,
            )
        } else {
            let mut source_thread_with_history = self
                .read_stored_thread_for_resume(
                    &thread_id,
                    path.as_ref(),
                    /*include_history*/ true,
                )
                .await?;
            Arc::new(
                source_thread_with_history
                    .history
                    .take()
                    .map(|history| history.items)
                    .ok_or_else(|| {
                        internal_error(format!(
                            "thread {source_thread_id} did not include persisted history"
                        ))
                    })?,
            )
        };
        let fork_snapshot = if prepared_fork.is_some() {
            // `prepare_fork` has already selected and frozen the exact physical boundary. Reusing
            // the legacy user-message count here would reinterpret that boundary against a
            // bounded/materialized context and can cut an inherited segment at the wrong turn.
            ForkSnapshot::Interrupted
        } else {
            match (last_turn_id.as_deref(), before_turn_id.as_deref()) {
                (Some(last_turn_id), None) => ForkSnapshot::TruncateBeforeNthUserMessage(
                    user_message_count_through_turn_id(&source_history_items, last_turn_id)
                        .map_err(|err| core_thread_write_error("truncate thread for fork", err))?,
                ),
                (None, Some(before_turn_id)) => ForkSnapshot::TruncateBeforeNthUserMessage(
                    user_message_count_before_turn_id(&source_history_items, before_turn_id)
                        .map_err(|err| core_thread_write_error("truncate thread for fork", err))?,
                ),
                (None, None) => ForkSnapshot::Interrupted,
                (Some(_), Some(_)) => unreachable!("fork boundaries are mutually exclusive"),
            }
        };
        let mut response_history_items = Arc::clone(&source_history_items);
        let history_cwd = Some(source_thread.cwd.clone());

        // Persist Windows sandbox mode.
        let mut cli_overrides = cli_overrides.unwrap_or_default();
        if cfg!(windows) {
            match WindowsSandboxLevel::from_config(&self.config) {
                WindowsSandboxLevel::Elevated => {
                    cli_overrides
                        .insert("windows.sandbox".to_string(), serde_json::json!("elevated"));
                }
                WindowsSandboxLevel::RestrictedToken => {
                    cli_overrides.insert(
                        "windows.sandbox".to_string(),
                        serde_json::json!("unelevated"),
                    );
                }
                WindowsSandboxLevel::Disabled => {}
            }
        }
        let request_overrides = if cli_overrides.is_empty() {
            None
        } else {
            Some(cli_overrides)
        };
        let runtime_workspace_roots = runtime_workspace_roots.map(resolve_runtime_workspace_roots);
        let mut typesafe_overrides = self.build_thread_config_overrides(
            model,
            model_provider,
            service_tier,
            cwd,
            runtime_workspace_roots,
            approval_policy,
            approvals_reviewer,
            sandbox,
            permissions,
            base_instructions,
            developer_instructions,
            /*personality*/ None,
        );
        typesafe_overrides.ephemeral = ephemeral.then_some(true);
        if typesafe_overrides.approvals_reviewer.is_none()
            && !request_overrides
                .as_ref()
                .is_some_and(|overrides| overrides.contains_key("approvals_reviewer"))
            && let Some(approvals_reviewer) = source_approvals_reviewer
        {
            typesafe_overrides.approvals_reviewer = Some(approvals_reviewer);
        }
        let restore_approval_policy = typesafe_overrides.approval_policy.is_none();
        let restore_approvals_reviewer = typesafe_overrides.approvals_reviewer.is_none()
            && !request_overrides
                .as_ref()
                .is_some_and(|overrides| overrides.contains_key("approvals_reviewer"));
        let restore_permission_profile =
            !has_permission_override(request_overrides.as_ref(), &typesafe_overrides);
        let needs_latest_settings =
            restore_approval_policy || restore_approvals_reviewer || restore_permission_profile;
        let loaded_parent_settings = if let Some(settings) = imported_settings {
            Some(settings)
        } else if paginated_source && needs_latest_settings {
            if let Ok(parent) = self.thread_manager.get_thread(source_thread_id).await {
                let snapshot = parent.thread_settings_snapshot().await;
                Some(PersistedResumeSettings {
                    approval_policy: snapshot.approval_policy,
                    approvals_reviewer: Some(snapshot.approvals_reviewer),
                    active_permission_profile: snapshot.active_permission_profile,
                })
            } else {
                None
            }
        } else {
            None
        };
        let latest_context = if paginated_source
            && needs_latest_settings
            && loaded_parent_settings.is_none()
            && (last_turn_id.is_some() || before_turn_id.is_some())
        {
            Some(
                self.thread_store
                    .load_latest_model_context(StoreLoadThreadHistoryParams {
                        thread_id: source_thread_id,
                        include_archived: true,
                    })
                    .await
                    .map_err(thread_store_resume_read_error)?
                    .items,
            )
        } else {
            None
        };
        let persisted_settings = loaded_parent_settings.or_else(|| {
            latest_persisted_resume_settings(
                latest_context
                    .as_deref()
                    .unwrap_or_else(|| source_history_items.as_ref()),
            )
        });
        if let Some(persisted_settings) = persisted_settings {
            if restore_approval_policy {
                typesafe_overrides.approval_policy = Some(persisted_settings.approval_policy);
            }
            if restore_approvals_reviewer {
                typesafe_overrides.approvals_reviewer = persisted_settings.approvals_reviewer;
            }
            if restore_permission_profile {
                typesafe_overrides.persisted_permission_profile_id = persisted_settings
                    .active_permission_profile
                    .map(|profile| profile.id);
            }
        }
        // Derive a Config using the same logic as new conversation, honoring overrides if provided.
        let config = self
            .config_manager
            .load_for_cwd(request_overrides, typesafe_overrides, history_cwd)
            .await
            .map_err(|err| config_load_error(&err))?;
        let goals_enabled = config.features.enabled(Feature::Goals);

        let fallback_model_provider = config.model_provider_id.clone();
        let parent_trace = self.request_trace_context(&request_id).await;
        let thread_source = thread_source.map(Into::into);

        let inherited_project_id = source_thread.project_id.clone();
        let reserved_thread_id = if config.ephemeral {
            None
        } else {
            stage_pending_project_metadata(
                self.thread_manager.as_ref(),
                self.thread_store.as_ref(),
                inherited_project_id.as_deref(),
                "thread/fork",
            )
            .await?
        };
        let new_thread = if let Some(prepared_fork) = prepared_fork {
            match self
                .thread_manager
                .fork_prepared_thread(
                    config,
                    prepared_fork,
                    thread_source,
                    parent_trace,
                    client_mcp_extensions,
                    reserved_thread_id,
                )
                .await
            {
                Ok((new_thread, prepared_response_history)) => {
                    response_history_items = prepared_response_history;
                    Ok(new_thread)
                }
                Err(err) => Err(err),
            }
        } else {
            match self
                .thread_manager
                .fork_thread_from_history_with_response(
                    fork_snapshot,
                    config,
                    InitialHistory::Resumed(ResumedHistory {
                        conversation_id: source_thread_id,
                        history: Arc::clone(&source_history_items),
                        rollout_path: source_thread.rollout_path.clone(),
                    }),
                    thread_source,
                    parent_trace,
                    client_mcp_extensions,
                    reserved_thread_id,
                )
                .await
            {
                Ok((new_thread, frozen_response_history)) => {
                    response_history_items = frozen_response_history;
                    Ok(new_thread)
                }
                Err(err) => Err(err),
            }
        };
        let NewThread {
            thread_id,
            thread: forked_thread,
            session_configured,
            ..
        } = match new_thread {
            Ok(new_thread) => new_thread,
            Err(err) => {
                remove_pending_project_metadata(self.thread_store.as_ref(), reserved_thread_id)
                    .await;
                return Err(match err.details() {
                    CodexErrorDetails::Io(_) | CodexErrorDetails::Json(_) => {
                        invalid_request(format!("failed to load thread {source_thread_id}: {err}"))
                    }
                    CodexErrorDetails::InvalidRequest(message) => invalid_request(message.clone()),
                    _ => internal_error(format!("error forking thread: {err}")),
                });
            }
        };

        Self::set_app_server_client_info(
            forked_thread.as_ref(),
            app_server_client_name,
            app_server_client_version,
        )
        .await?;
        if session_configured.rollout_path.is_some() {
            let preview = if paginated_source && last_turn_id.is_none() && before_turn_id.is_none()
            {
                bounded_thread_preview(source_thread.preview.clone())
            } else {
                preview_from_rollout_items(&response_history_items)
            };
            let mut metadata_patch = StoreThreadMetadataPatch {
                name: source_thread_name.clone().map(Some),
                ..Default::default()
            };
            if !preview.is_empty() {
                metadata_patch.preview = Some(preview.clone());
                metadata_patch.first_user_message = Some(preview);
            }
            if !metadata_patch.is_empty() {
                self.thread_manager
                    .update_thread_metadata(
                        thread_id,
                        metadata_patch,
                        /*include_archived*/ true,
                    )
                    .await
                    .map_err(|err| {
                        core_thread_write_error("inherit source thread metadata", err)
                    })?;
            }
        }
        let inherited_goal = if defer_goal_continuation
            && session_configured.rollout_path.is_some()
            && goals_enabled
        {
            if let Some(state_db) = forked_thread.state_db().or_else(|| self.state_db.clone()) {
                self.thread_goal_processor
                    .flush_goal_progress_for_fork(source_thread_id)
                    .await
                    .map_err(|err| {
                        internal_error(format!("failed to flush source thread goal: {err}"))
                    })?;
                inherit_thread_goal_snapshot(&state_db, source_thread_id, thread_id)
                    .await
                    .map_err(|err| {
                        internal_error(format!("failed to inherit source thread goal: {err}"))
                    })?
            } else {
                false
            }
        } else {
            false
        };
        if inherited_goal {
            self.thread_goal_processor
                .restore_inherited_goal_runtime(thread_id)
                .await;
        }

        let instruction_sources = forked_thread.legacy_instruction_sources().await;
        let token_usage_history_items =
            paginated_source.then(|| Arc::clone(&response_history_items));

        // Auto-attach a conversation listener when forking a thread.
        log_listener_attach_result(
            self.ensure_conversation_listener(
                thread_id,
                request_id.connection_id,
                /*raw_events_enabled*/ false,
            )
            .await,
            thread_id,
            request_id.connection_id,
            "thread",
        );

        let config_snapshot = forked_thread.config_snapshot().await;

        // Persistent forks materialize their own rollout immediately. Ephemeral forks rebuild
        // visible history from the frozen logical source prefix instead.
        let (mut thread, mut token_usage_turn_id) = if session_configured.rollout_path.is_some() {
            let stored_thread = self
                .read_stored_thread_for_new_fork(thread_id, include_turns && !paginated_source)
                .await?;
            let (mut thread, history) = thread_from_stored_thread(
                stored_thread,
                fallback_model_provider.as_str(),
                &self.config.cwd,
            );
            if include_turns && let Some(history) = history.as_ref() {
                populate_thread_turns_from_history(
                    &mut thread,
                    &history.items,
                    /*active_turn*/ None,
                );
            }
            let token_usage_turn_id = include_turns.then(|| {
                restored_token_usage_turn_id(
                    history
                        .as_ref()
                        .map(|history| history.items.as_slice())
                        .or_else(|| token_usage_history_items.as_deref().map(Vec::as_slice))
                        .unwrap_or(&[]),
                    thread.turns.as_slice(),
                )
            });
            (thread, token_usage_turn_id)
        } else {
            let mut thread = build_thread_from_snapshot(
                thread_id,
                session_configured.session_id.to_string(),
                forked_thread.multi_agent_version(),
                &config_snapshot,
                /*path*/ None,
            );
            thread.preview =
                if paginated_source && last_turn_id.is_none() && before_turn_id.is_none() {
                    bounded_thread_preview(source_thread.preview.clone())
                } else {
                    preview_from_rollout_items(&response_history_items)
                };
            thread.forked_from_id = Some(source_thread_id.to_string());
            if include_turns {
                populate_thread_turns_from_history(
                    &mut thread,
                    &response_history_items,
                    /*active_turn*/ None,
                );
            }
            let token_usage_turn_id = include_turns.then(|| {
                restored_token_usage_turn_id(&response_history_items, thread.turns.as_slice())
            });
            (thread, token_usage_turn_id)
        };
        if paginated_source && include_turns {
            if let Some(projected_turns) = projected_response_turns.as_ref() {
                thread.turns = projected_turns
                    .iter()
                    .cloned()
                    .map(|turn| stored_turn_to_api_turn(turn, TurnItemsView::Full))
                    .collect::<Result<Vec<_>, _>>()?;
                normalize_thread_turns_status(
                    &mut thread.turns,
                    self.thread_watch_manager
                        .loaded_status_for_thread(&thread_id.to_string())
                        .await,
                    matches!(forked_thread.agent_status().await, AgentStatus::Running),
                );
            } else if session_configured.rollout_path.is_none() {
                populate_thread_turns_from_history(
                    &mut thread,
                    &response_history_items,
                    /*active_turn*/ None,
                );
            } else {
                let loaded_status = self
                    .thread_watch_manager
                    .loaded_status_for_thread(&thread_id.to_string())
                    .await;
                let has_live_running_thread =
                    matches!(forked_thread.agent_status().await, AgentStatus::Running);
                thread.turns = reconstruct_paginated_thread_turns(
                    response_history_items.as_slice(),
                    loaded_status,
                    has_live_running_thread,
                    /*active_turn*/ None,
                );
                // Paginated projection creates visible turns only from lifecycle events. Older
                // fixtures can also contain Legacy response events that synthesize rollout turns.
                let projected_turn_ids: HashSet<_> = response_history_items
                    .iter()
                    .filter_map(|item| match item {
                        RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                            Some(event.turn_id.as_str())
                        }
                        RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                            Some(event.turn_id.as_str())
                        }
                        RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => {
                            event.turn_id.as_deref()
                        }
                        _ => None,
                    })
                    .collect();
                thread
                    .turns
                    .retain(|turn| projected_turn_ids.contains(turn.id.as_str()));
                apply_thread_turns_items_view(&mut thread.turns, TurnItemsView::Full);
            }
            token_usage_turn_id = Some(restored_token_usage_turn_id(
                token_usage_history_items
                    .as_deref()
                    .map_or(response_history_items.as_slice(), Vec::as_slice),
                thread.turns.as_slice(),
            ));
        }
        if let Some(name) = source_thread_name {
            set_thread_name_from_title(&mut thread, name);
        }
        thread.can_accept_direct_input = Some(can_accept_direct_input(
            forked_thread.multi_agent_version(),
            &config_snapshot.session_source,
        ));
        thread.session_id = session_configured.session_id.to_string();
        thread.thread_source = config_snapshot.thread_source.clone().map(Into::into);
        if thread.path.is_none() {
            thread.project_id = inherited_project_id.clone();
        }

        self.thread_watch_manager
            .upsert_thread_silently(&thread.id)
            .await;

        thread.status = resolve_thread_status(
            self.thread_watch_manager
                .loaded_status_for_thread(&thread.id)
                .await,
            /*has_in_progress_turn*/ false,
        );
        let sandbox = config_snapshot.sandbox_policy().into();
        let active_permission_profile =
            thread_response_active_permission_profile(config_snapshot.active_permission_profile);
        let thread_originator = config_snapshot.originator.clone();
        let response = ThreadForkResponse {
            thread: thread.clone(),
            model: session_configured.model,
            model_provider: session_configured.model_provider_id,
            service_tier: session_configured.service_tier,
            cwd: session_configured.cwd,
            runtime_workspace_roots: config_snapshot.workspace_roots,
            instruction_sources,
            approval_policy: session_configured.approval_policy.into(),
            approvals_reviewer: session_configured.approvals_reviewer.into(),
            sandbox,
            active_permission_profile,
            reasoning_effort: session_configured.reasoning_effort,
            multi_agent_mode: MultiAgentMode::ExplicitRequestOnly,
        };

        let notif = thread_started_notification(thread);
        let connection_id = request_id.connection_id;
        if *HISTORY_IO_OBSERVATION_ENABLED {
            tracing::event!(
                target: "codex_history_io",
                tracing::Level::TRACE,
                event.name = "codex.history.fork.complete",
                source_thread.id = %source_thread_id,
                thread.id = %thread_id,
                "completed fork history reads"
            );
        }
        self.outgoing
            .send_response_with_thread_originator(request_id, response, thread_originator)
            .await;
        // `excludeTurns` is the cheap fork path, so skip restored usage replay
        // instead of rebuilding history only to attribute a historical update.
        if let Some(token_usage_turn_id) = token_usage_turn_id {
            // Mirror the resume contract for forks: the new thread is usable as soon
            // as the response arrives, so restored usage must follow immediately.
            send_thread_token_usage_update_to_connection(
                &self.outgoing,
                connection_id,
                thread_id,
                forked_thread.as_ref(),
                token_usage_turn_id,
            )
            .await;
        }

        self.outgoing
            .send_server_notification(ServerNotification::ThreadStarted(notif))
            .await;
        if inherited_goal {
            self.thread_goal_processor
                .emit_thread_goal_snapshot(thread_id)
                .await;
        }
        Ok(())
    }

    async fn get_thread_summary_response_inner(
        &self,
        params: GetConversationSummaryParams,
    ) -> Result<GetConversationSummaryResponse, JSONRPCErrorError> {
        let fallback_provider = self.config.model_provider_id.as_str();
        let read_result = match params {
            GetConversationSummaryParams::ThreadId { conversation_id } => self
                .thread_store
                .read_thread(StoreReadThreadParams {
                    thread_id: conversation_id,
                    include_archived: true,
                    include_history: false,
                })
                .await
                .map_err(|err| conversation_summary_thread_id_read_error(conversation_id, err)),
            GetConversationSummaryParams::RolloutPath { rollout_path } => {
                let Some(local_thread_store) = self
                    .thread_store
                    .as_any()
                    .downcast_ref::<LocalThreadStore>()
                else {
                    return Err(invalid_request(
                        "rollout path queries are only supported with the local thread store",
                    ));
                };

                local_thread_store
                    .read_thread_by_rollout_path(
                        rollout_path.clone(),
                        /*include_archived*/ true,
                        /*include_history*/ false,
                    )
                    .await
                    .map_err(|err| conversation_summary_rollout_path_read_error(&rollout_path, err))
            }
        };

        let stored_thread = read_result?;
        let summary = summary_from_stored_thread(stored_thread, fallback_provider);
        Ok(GetConversationSummaryResponse { summary })
    }

    async fn list_threads_common(
        &self,
        requested_page_size: usize,
        cursor: Option<String>,
        sort_key: StoreThreadSortKey,
        sort_direction: SortDirection,
        filters: ThreadListFilters,
    ) -> Result<(Vec<StoredThread>, Option<String>), JSONRPCErrorError> {
        let ThreadListFilters {
            model_providers,
            source_kinds,
            archived,
            section_id,
            project_id,
            cwd_filters,
            search_term,
            use_state_db_only,
            relation_filter,
        } = filters;
        let mut cursor_obj = cursor;
        let mut last_cursor = cursor_obj.clone();
        let mut remaining = requested_page_size;
        let mut items = Vec::with_capacity(requested_page_size);
        let mut next_cursor: Option<String> = None;

        let model_provider_filter = match model_providers {
            Some(providers) => {
                if providers.is_empty() {
                    None
                } else {
                    Some(providers)
                }
            }
            None if relation_filter.is_some() => None,
            None => Some(vec![self.config.model_provider_id.clone()]),
        };
        let (allowed_sources_vec, source_kind_filter) =
            if relation_filter.is_some() && source_kinds.is_none() {
                (Vec::new(), None)
            } else {
                compute_source_filters(source_kinds)
            };
        let allowed_sources = allowed_sources_vec.as_slice();
        let store_sort_direction = match sort_direction {
            SortDirection::Asc => StoreSortDirection::Asc,
            SortDirection::Desc => StoreSortDirection::Desc,
        };

        while remaining > 0 {
            let page_size = remaining.min(THREAD_LIST_MAX_LIMIT);
            let page = self
                .thread_store
                .list_threads(StoreListThreadsParams {
                    page_size,
                    cursor: cursor_obj.clone(),
                    sort_key,
                    sort_direction: store_sort_direction,
                    allowed_sources: allowed_sources.to_vec(),
                    model_providers: model_provider_filter.clone(),
                    cwd_filters: cwd_filters.clone(),
                    archived,
                    section: section_id.clone(),
                    project_id: project_id.clone(),
                    search_term: search_term.clone(),
                    use_state_db_only,
                    relation_filter,
                })
                .await
                .map_err(thread_store_list_error)?;

            let mut filtered = Vec::with_capacity(page.items.len());
            for it in page.items {
                let source = with_thread_spawn_agent_metadata(
                    it.source.clone(),
                    it.agent_nickname.clone(),
                    it.agent_role.clone(),
                );
                if source_kind_filter
                    .as_ref()
                    .is_none_or(|filter| source_kind_matches(&source, filter))
                    && cwd_filters.as_ref().is_none_or(|expected_cwds| {
                        expected_cwds.iter().any(|expected_cwd| {
                            path_utils::paths_match_after_normalization(&it.cwd, expected_cwd)
                        })
                    })
                {
                    filtered.push(it);
                    if filtered.len() >= remaining {
                        break;
                    }
                }
            }
            items.extend(filtered);
            remaining = requested_page_size.saturating_sub(items.len());

            next_cursor = page.next_cursor;
            if remaining == 0 {
                break;
            }

            let Some(cursor_val) = next_cursor.clone() else {
                break;
            };
            // Break if our pagination would reuse the same cursor again; this avoids
            // an infinite loop when filtering drops everything on the page.
            if last_cursor.as_ref() == Some(&cursor_val) {
                next_cursor = None;
                break;
            }
            last_cursor = Some(cursor_val.clone());
            cursor_obj = Some(cursor_val);
        }

        Ok((items, next_cursor))
    }
}

fn legacy_page_next_turn_id(
    turns: &[Turn],
    cursor: Option<&ThreadTurnsCursor>,
    page_size: usize,
    sort_direction: SortDirection,
) -> Option<String> {
    if !matches!(sort_direction, SortDirection::Desc) {
        return None;
    }
    let anchor_index = cursor
        .and_then(|cursor| turns.iter().position(|turn| turn.id == cursor.turn_id))
        .unwrap_or(turns.len());
    let end = match cursor {
        Some(cursor) if cursor.include_anchor => anchor_index.saturating_add(1),
        Some(_) => anchor_index,
        None => turns.len(),
    };
    if end == 0 {
        return None;
    }
    turns
        .get(end.saturating_sub(page_size))
        .map(|turn| turn.id.clone())
}

fn xcode_26_4_mcp_elicitations_auto_deny(
    client_name: Option<&str>,
    client_version: Option<&str>,
) -> bool {
    // Xcode 26.4 shipped before app-server MCP elicitation requests were
    // client-visible. Keep elicitations auto-denied for that client line.
    // TODO: Remove this compatibility hack once Xcode 26.4 ages out.
    client_name == Some("Xcode")
        && client_version.is_some_and(|version| version.starts_with("26.4"))
}

const THREAD_TURNS_DEFAULT_LIMIT: usize = 25;
const THREAD_TURNS_MAX_LIMIT: usize = 100;
const THREAD_ITEMS_DEFAULT_LIMIT: usize = 25;
const THREAD_ITEMS_MAX_LIMIT: usize = 100;
const THREAD_SEARCH_OCCURRENCES_DEFAULT_LIMIT: usize = 50;
const THREAD_SEARCH_OCCURRENCES_MAX_LIMIT: usize = 250;

pub(super) fn thread_turns_page_size(limit: Option<u32>) -> usize {
    limit
        .map(|value| value as usize)
        .unwrap_or(THREAD_TURNS_DEFAULT_LIMIT)
        .clamp(1, THREAD_TURNS_MAX_LIMIT)
}

fn thread_backwards_cursor_for_sort_key(
    thread: &StoredThread,
    sort_key: StoreThreadSortKey,
    sort_direction: SortDirection,
) -> Option<String> {
    if sort_key == StoreThreadSortKey::SectionPosition {
        let position = match sort_direction {
            SortDirection::Asc => thread.section_position?.checked_add(1)?,
            SortDirection::Desc => thread.section_position?.checked_sub(1)?,
        };
        return Some(format!("{position}|{}", thread.thread_id));
    }

    let timestamp = match sort_key {
        StoreThreadSortKey::CreatedAt => thread.created_at,
        StoreThreadSortKey::UpdatedAt => thread.updated_at,
        StoreThreadSortKey::RecencyAt => thread.recency_at,
        StoreThreadSortKey::SectionPosition => unreachable!("section positions use rank cursors"),
    };
    // The state DB stores unique millisecond timestamps. Offset the reverse cursor by one
    // millisecond so the opposite-direction query includes the page anchor.
    let timestamp = match sort_direction {
        SortDirection::Asc => timestamp.checked_add_signed(ChronoDuration::milliseconds(1))?,
        SortDirection::Desc => timestamp.checked_sub_signed(ChronoDuration::milliseconds(1))?,
    };
    Some(timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
}

struct ThreadTurnsPage {
    pub(super) turns: Vec<Turn>,
    pub(super) next_cursor: Option<String>,
    pub(super) backwards_cursor: Option<String>,
}

/// A materialized legacy rollout prefix and whether older referenced segments remain.
struct LegacyHistoryWindow {
    items: Vec<RolloutItem>,
    has_older_reference: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadTurnsCursor {
    turn_id: String,
    include_anchor: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadItemsCursor {
    turn_id: String,
    item_id: String,
    include_anchor: bool,
}

fn serialize_thread_items_cursor(
    entry: &ThreadItemEntry,
    include_anchor: bool,
) -> Result<String, JSONRPCErrorError> {
    serde_json::to_string(&ThreadItemsCursor {
        turn_id: entry.turn_id.clone(),
        item_id: entry.item.id().to_string(),
        include_anchor,
    })
    .map_err(|err| internal_error(format!("failed to serialize item cursor: {err}")))
}

fn parse_thread_items_cursor(cursor: &str) -> Result<ThreadItemsCursor, JSONRPCErrorError> {
    serde_json::from_str(cursor)
        .map_err(|_| invalid_request(format!("invalid item cursor: {cursor}")))
}

fn legacy_turn_window_is_coherent(
    turns: &[Turn],
    cursor: Option<&ThreadTurnsCursor>,
    page_size: usize,
    sort_direction: SortDirection,
    has_older_reference: bool,
) -> bool {
    if turns.is_empty() {
        return !has_older_reference;
    }

    let anchor_index = match cursor {
        Some(cursor) => {
            let Some(index) = turns.iter().position(|turn| turn.id == cursor.turn_id) else {
                return !has_older_reference;
            };
            index
        }
        None => turns.len(),
    };
    if matches!(sort_direction, SortDirection::Asc) {
        return true;
    }
    let end = match cursor {
        Some(cursor) if cursor.include_anchor => anchor_index.saturating_add(1),
        Some(_) => anchor_index,
        None => turns.len(),
    };
    let start = end.saturating_sub(page_size);
    let page = &turns[start..end];
    if page.is_empty() {
        return !has_older_reference;
    }
    if page.len() < page_size && has_older_reference {
        return false;
    }

    if has_older_reference && page.iter().any(|turn| turn.id.starts_with("rollout-")) {
        // Synthetic IDs depend on the replay prefix. Reach the beginning before returning one so
        // a later page expansion cannot change a cursor anchor that the client already received.
        return false;
    }
    true
}

fn paginate_thread_turns(
    turns: Vec<Turn>,
    cursor: Option<&str>,
    limit: Option<u32>,
    sort_direction: SortDirection,
    has_older_reference: bool,
) -> Result<ThreadTurnsPage, JSONRPCErrorError> {
    if turns.is_empty() {
        return Ok(ThreadTurnsPage {
            turns: Vec::new(),
            next_cursor: None,
            backwards_cursor: None,
        });
    }

    let anchor = cursor.map(parse_thread_turns_cursor).transpose()?;
    let page_size = limit
        .map(|value| value as usize)
        .unwrap_or(THREAD_TURNS_DEFAULT_LIMIT)
        .clamp(1, THREAD_TURNS_MAX_LIMIT);

    let anchor_index = anchor
        .as_ref()
        .and_then(|anchor| turns.iter().position(|turn| turn.id == anchor.turn_id));
    if anchor.is_some() && anchor_index.is_none() {
        return Err(invalid_request(
            "invalid cursor: anchor turn is no longer present",
        ));
    }

    let mut keyed_turns: Vec<_> = turns.into_iter().enumerate().collect();
    match sort_direction {
        SortDirection::Asc => {
            if let (Some(anchor), Some(anchor_index)) = (anchor.as_ref(), anchor_index) {
                keyed_turns.retain(|(index, _)| {
                    if anchor.include_anchor {
                        *index >= anchor_index
                    } else {
                        *index > anchor_index
                    }
                });
            }
        }
        SortDirection::Desc => {
            keyed_turns.reverse();
            if let (Some(anchor), Some(anchor_index)) = (anchor.as_ref(), anchor_index) {
                keyed_turns.retain(|(index, _)| {
                    if anchor.include_anchor {
                        *index <= anchor_index
                    } else {
                        *index < anchor_index
                    }
                });
            }
        }
    }

    let more_turns_available = keyed_turns.len() > page_size
        || (has_older_reference && matches!(sort_direction, SortDirection::Desc));
    keyed_turns.truncate(page_size);
    let backwards_cursor = keyed_turns
        .first()
        .map(|(_, turn)| serialize_thread_turns_cursor(&turn.id, /*include_anchor*/ true))
        .transpose()?;
    let next_cursor = if more_turns_available {
        keyed_turns
            .last()
            .map(|(_, turn)| serialize_thread_turns_cursor(&turn.id, /*include_anchor*/ false))
            .transpose()?
    } else {
        None
    };
    let turns = keyed_turns.into_iter().map(|(_, turn)| turn).collect();

    Ok(ThreadTurnsPage {
        turns,
        next_cursor,
        backwards_cursor,
    })
}

fn serialize_thread_turns_cursor(
    turn_id: &str,
    include_anchor: bool,
) -> Result<String, JSONRPCErrorError> {
    serde_json::to_string(&ThreadTurnsCursor {
        turn_id: turn_id.to_string(),
        include_anchor,
    })
    .map_err(|err| internal_error(format!("failed to serialize cursor: {err}")))
}

fn parse_thread_turns_cursor(cursor: &str) -> Result<ThreadTurnsCursor, JSONRPCErrorError> {
    serde_json::from_str(cursor).map_err(|_| invalid_request(format!("invalid cursor: {cursor}")))
}

struct ThreadTurnsPageOptions<'a> {
    cursor: Option<&'a str>,
    limit: Option<u32>,
    sort_direction: SortDirection,
    items_view: TurnItemsView,
    has_older_reference: bool,
}

fn build_thread_turns_page_response(
    items: &[RolloutItem],
    loaded_status: ThreadStatus,
    has_live_running_thread: bool,
    active_turn: Option<Turn>,
    options: ThreadTurnsPageOptions<'_>,
) -> Result<ThreadTurnsListResponse, JSONRPCErrorError> {
    build_thread_turns_page_response_for_history_mode(
        items,
        loaded_status,
        has_live_running_thread,
        active_turn,
        options,
        ThreadHistoryMode::Legacy,
    )
}

fn build_thread_turns_page_response_for_history_mode(
    items: &[RolloutItem],
    loaded_status: ThreadStatus,
    has_live_running_thread: bool,
    active_turn: Option<Turn>,
    options: ThreadTurnsPageOptions<'_>,
    history_mode: ThreadHistoryMode,
) -> Result<ThreadTurnsListResponse, JSONRPCErrorError> {
    let mut turns = match history_mode {
        ThreadHistoryMode::Legacy => reconstruct_thread_turns_for_turns_list(
            items,
            loaded_status,
            has_live_running_thread,
            active_turn,
        ),
        ThreadHistoryMode::Paginated => reconstruct_paginated_thread_turns(
            items,
            loaded_status,
            has_live_running_thread,
            active_turn,
        ),
    };
    apply_thread_turns_items_view(&mut turns, options.items_view);
    let page = paginate_thread_turns(
        turns,
        options.cursor,
        options.limit,
        options.sort_direction,
        options.has_older_reference,
    )?;
    Ok(ThreadTurnsListResponse {
        data: page.turns,
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    })
}

/// Reconstructs projected Paginated turns without rereading inherited SQLite rows.
fn reconstruct_paginated_thread_turns(
    items: &[RolloutItem],
    loaded_status: ThreadStatus,
    has_live_running_thread: bool,
    active_turn: Option<Turn>,
) -> Vec<Turn> {
    let mut builder = ThreadHistoryBuilder::new();
    let mut projected_items: HashMap<String, Vec<ThreadItem>> = HashMap::new();
    for item in items {
        builder.handle_rollout_item(item);
        if let RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) = item {
            let materialized_item = ThreadItem::from(event.item.clone());
            let turn_items = projected_items.entry(event.turn_id.clone()).or_default();
            if let Some(existing_item) = turn_items
                .iter_mut()
                .find(|item| item.id() == materialized_item.id())
            {
                *existing_item = materialized_item;
            } else {
                turn_items.push(materialized_item);
            }
        }
    }

    let mut turns = builder.finish();
    if turns.iter().any(|turn| !turn.id.starts_with("rollout-")) {
        turns.retain(|turn| !turn.id.starts_with("rollout-"));
    }
    for turn in &mut turns {
        if let Some(items) = projected_items.remove(&turn.id) {
            turn.items = items;
        }
    }

    let has_live_in_progress_turn = has_live_running_thread
        || active_turn
            .as_ref()
            .is_some_and(|turn| matches!(turn.status, TurnStatus::InProgress));
    normalize_thread_turns_status(&mut turns, loaded_status, has_live_in_progress_turn);
    if let Some(active_turn) = active_turn {
        merge_turn_history_with_active_turn(&mut turns, active_turn);
    }
    turns
}

pub(super) fn build_thread_resume_initial_turns_page(
    items: &[RolloutItem],
    loaded_status: ThreadStatus,
    has_live_running_thread: bool,
    active_turn: Option<Turn>,
    params: &ThreadResumeInitialTurnsPageParams,
    has_older_reference: bool,
) -> Result<codex_app_server_protocol::TurnsPage, JSONRPCErrorError> {
    build_thread_turns_page_response(
        items,
        loaded_status,
        has_live_running_thread,
        active_turn,
        ThreadTurnsPageOptions {
            cursor: None,
            limit: params.limit,
            sort_direction: params.sort_direction.unwrap_or(SortDirection::Desc),
            items_view: params.items_view.unwrap_or(TurnItemsView::Summary),
            has_older_reference,
        },
    )
    .map(Into::into)
}

pub(super) fn apply_thread_turns_items_view(turns: &mut [Turn], items_view: TurnItemsView) {
    for turn in turns {
        match items_view {
            TurnItemsView::NotLoaded => {
                turn.items.clear();
                turn.items_view = TurnItemsView::NotLoaded;
            }
            TurnItemsView::Summary => {
                let first_user_message = turn
                    .items
                    .iter()
                    .find(|item| matches!(item, ThreadItem::UserMessage { .. }))
                    .cloned();
                let final_agent_message = turn
                    .items
                    .iter()
                    .rev()
                    .find(|item| matches!(item, ThreadItem::AgentMessage { .. }))
                    .cloned();
                turn.items = match (first_user_message, final_agent_message) {
                    (Some(user_message), Some(agent_message))
                        if user_message.id() != agent_message.id() =>
                    {
                        vec![user_message, agent_message]
                    }
                    (Some(user_message), _) => vec![user_message],
                    (None, Some(agent_message)) => vec![agent_message],
                    (None, None) => Vec::new(),
                };
                turn.items_view = TurnItemsView::Summary;
            }
            TurnItemsView::Full => {
                turn.items_view = TurnItemsView::Full;
            }
        }
    }
}

fn reconstruct_thread_turns_for_turns_list(
    items: &[RolloutItem],
    loaded_status: ThreadStatus,
    has_live_running_thread: bool,
    active_turn: Option<Turn>,
) -> Vec<Turn> {
    let has_live_in_progress_turn = has_live_running_thread
        || active_turn
            .as_ref()
            .is_some_and(|turn| matches!(turn.status, TurnStatus::InProgress));
    let mut turns = build_legacy_api_turns_from_rollout_items(items);
    normalize_thread_turns_status(&mut turns, loaded_status, has_live_in_progress_turn);
    if let Some(active_turn) = active_turn {
        merge_turn_history_with_active_turn(&mut turns, active_turn);
    }
    turns
}

pub(super) fn normalize_thread_turns_status(
    turns: &mut [Turn],
    loaded_status: ThreadStatus,
    has_live_in_progress_turn: bool,
) {
    let status = resolve_thread_status(loaded_status, has_live_in_progress_turn);
    if matches!(status, ThreadStatus::Active { .. }) {
        return;
    }
    for turn in turns {
        if matches!(turn.status, TurnStatus::InProgress) {
            turn.status = TurnStatus::Interrupted;
        }
    }
}

enum ThreadReadViewError {
    InvalidRequest(String),
    Unsupported(&'static str),
    Internal(String),
    JsonRpc(JSONRPCErrorError),
}

fn thread_read_view_error(err: ThreadReadViewError) -> JSONRPCErrorError {
    match err {
        ThreadReadViewError::InvalidRequest(message) => invalid_request(message),
        ThreadReadViewError::Unsupported(operation) => {
            unsupported_thread_store_operation(operation)
        }
        ThreadReadViewError::Internal(message) => internal_error(message),
        ThreadReadViewError::JsonRpc(error) => error,
    }
}

fn paginated_history_list_error(err: ThreadStoreError) -> JSONRPCErrorError {
    match err {
        ThreadStoreError::InvalidRequest { message } => invalid_request(message),
        ThreadStoreError::Unsupported { operation } => {
            unsupported_thread_store_operation(operation)
        }
        ThreadStoreError::ThreadNotFound { thread_id } => {
            invalid_request(format!("no rollout found for thread id {thread_id}"))
        }
        err => internal_error(format!("failed to list thread history: {err}")),
    }
}

fn deserialize_stored_thread_item(
    item: codex_thread_store::StoredThreadItem,
) -> Result<ThreadItem, JSONRPCErrorError> {
    serde_json::from_slice::<ThreadItem>(&item.item_json).map_err(|err| {
        internal_error(format!(
            "failed to deserialize stored thread item {}: {err}",
            item.item_id
        ))
    })
}

fn stored_turn_to_api_turn(
    turn: StoredTurn,
    items_view: TurnItemsView,
) -> Result<Turn, JSONRPCErrorError> {
    let status = match turn.status {
        StoredTurnStatus::Completed => TurnStatus::Completed,
        StoredTurnStatus::Interrupted => TurnStatus::Interrupted,
        StoredTurnStatus::Failed => TurnStatus::Failed,
        StoredTurnStatus::InProgress => TurnStatus::InProgress,
    };
    let error = turn.error.map(|error| TurnError {
        misalignment: None,
        message: error.message,
        codex_error_info: error.codex_error_info,
        additional_details: error.additional_details,
    });
    let items = turn
        .items
        .into_iter()
        .map(deserialize_stored_thread_item)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Turn {
        id: turn.turn_id,
        items,
        items_view,
        status,
        error,
        started_at: turn.started_at,
        completed_at: turn.completed_at,
        duration_ms: turn.duration_ms,
    })
}

pub(super) fn unsupported_thread_store_operation(operation: &'static str) -> JSONRPCErrorError {
    method_not_found(format!("{operation} is not supported yet"))
}

fn thread_store_list_error(err: ThreadStoreError) -> JSONRPCErrorError {
    match err {
        ThreadStoreError::InvalidRequest { message } => invalid_request(message),
        ThreadStoreError::Unsupported { operation } => {
            unsupported_thread_store_operation(operation)
        }
        err => internal_error(format!("failed to list threads: {err}")),
    }
}

fn thread_store_resume_read_error(err: ThreadStoreError) -> JSONRPCErrorError {
    match err {
        ThreadStoreError::InvalidRequest { message } | ThreadStoreError::Conflict { message } => {
            invalid_request(message)
        }
        ThreadStoreError::Unsupported { operation } => {
            unsupported_thread_store_operation(operation)
        }
        ThreadStoreError::ThreadNotFound { thread_id } => {
            invalid_request(format!("no rollout found for thread id {thread_id}"))
        }
        err => internal_error(format!("failed to read thread: {err}")),
    }
}

fn thread_turns_list_history_load_error(
    thread_id: ThreadId,
    err: ThreadStoreError,
) -> ThreadReadViewError {
    match err {
        ThreadStoreError::InvalidRequest { message }
            if message.starts_with("failed to resolve rollout path `") =>
        {
            ThreadReadViewError::InvalidRequest(format!(
                "thread {thread_id} is not materialized yet; thread/turns/list is unavailable before first user message"
            ))
        }
        ThreadStoreError::InvalidRequest { message } => {
            ThreadReadViewError::InvalidRequest(message)
        }
        ThreadStoreError::Unsupported { operation } => ThreadReadViewError::Unsupported(operation),
        err => ThreadReadViewError::Internal(format!(
            "failed to load thread history for thread {thread_id}: {err}"
        )),
    }
}

fn thread_read_history_load_error(
    thread_id: ThreadId,
    err: ThreadStoreError,
) -> ThreadReadViewError {
    match err {
        ThreadStoreError::InvalidRequest { message }
            if message.starts_with("failed to resolve rollout path `") =>
        {
            ThreadReadViewError::InvalidRequest(format!(
                "thread {thread_id} is not materialized yet; includeTurns is unavailable before first user message"
            ))
        }
        ThreadStoreError::ThreadNotFound {
            thread_id: missing_thread_id,
        } if missing_thread_id == thread_id => ThreadReadViewError::InvalidRequest(format!(
            "thread {thread_id} is not materialized yet; includeTurns is unavailable before first user message"
        )),
        ThreadStoreError::InvalidRequest { message } => {
            ThreadReadViewError::InvalidRequest(message)
        }
        ThreadStoreError::Unsupported { operation } => ThreadReadViewError::Unsupported(operation),
        err => ThreadReadViewError::Internal(format!(
            "failed to load thread history for thread {thread_id}: {err}"
        )),
    }
}

fn conversation_summary_thread_id_read_error(
    conversation_id: ThreadId,
    err: ThreadStoreError,
) -> JSONRPCErrorError {
    let no_rollout_message = format!("no rollout found for thread id {conversation_id}");
    match err {
        ThreadStoreError::InvalidRequest { message } if message == no_rollout_message => {
            conversation_summary_not_found_error(conversation_id)
        }
        ThreadStoreError::Unsupported { operation } => {
            unsupported_thread_store_operation(operation)
        }
        ThreadStoreError::ThreadNotFound { thread_id } if thread_id == conversation_id => {
            conversation_summary_not_found_error(conversation_id)
        }
        ThreadStoreError::InvalidRequest { message } => invalid_request(message),
        err => internal_error(format!(
            "failed to load conversation summary for {conversation_id}: {err}"
        )),
    }
}

fn conversation_summary_not_found_error(conversation_id: ThreadId) -> JSONRPCErrorError {
    invalid_request(format!(
        "no rollout found for conversation id {conversation_id}"
    ))
}

fn conversation_summary_rollout_path_read_error(
    path: &Path,
    err: ThreadStoreError,
) -> JSONRPCErrorError {
    match err {
        ThreadStoreError::InvalidRequest { message } => invalid_request(message),
        ThreadStoreError::Unsupported { operation } => {
            unsupported_thread_store_operation(operation)
        }
        err => internal_error(format!(
            "failed to load conversation summary from {}: {}",
            path.display(),
            err
        )),
    }
}

pub(super) fn core_thread_write_error(operation: &str, err: CodexErr) -> JSONRPCErrorError {
    match err.details() {
        CodexErrorDetails::ThreadNotFound(thread_id) => {
            invalid_request(format!("thread not found: {thread_id}"))
        }
        CodexErrorDetails::InvalidRequest(message) => invalid_request(message.clone()),
        CodexErrorDetails::UnsupportedOperation(message) => method_not_found(message.clone()),
        _ => internal_error(format!("failed to {operation}: {err}")),
    }
}

fn thread_store_mutation_error(operation: &str, err: ThreadStoreError) -> JSONRPCErrorError {
    match err {
        ThreadStoreError::InvalidRequest { message } | ThreadStoreError::Conflict { message } => {
            invalid_request(message)
        }
        ThreadStoreError::Unsupported {
            operation: unsupported_operation,
        } => unsupported_thread_store_operation(unsupported_operation),
        err => internal_error(format!("failed to {operation} session: {err}")),
    }
}

fn set_thread_name_from_title(thread: &mut Thread, title: String) {
    if title.trim().is_empty() || thread.preview.trim() == title.trim() {
        return;
    }
    thread.name = Some(title);
}

pub(crate) fn thread_from_stored_thread(
    thread: StoredThread,
    fallback_provider: &str,
    fallback_cwd: &AbsolutePathBuf,
) -> (Thread, Option<codex_thread_store::StoredThreadHistory>) {
    let path = thread.rollout_path;
    let git_info = thread.git_info.map(|info| ApiGitInfo {
        sha: info.commit_hash.map(|sha| sha.0),
        branch: info.branch,
        origin_url: info.repository_url.map(String::from),
    });
    let cwd = AbsolutePathBuf::relative_to_current_dir(path_utils::normalize_for_native_workdir(
        thread.cwd,
    ))
    .unwrap_or_else(|err| {
        warn!("failed to normalize thread cwd while reading stored thread: {err}");
        fallback_cwd.clone()
    });
    let source = with_thread_spawn_agent_metadata(
        thread.source,
        thread.agent_nickname.clone(),
        thread.agent_role.clone(),
    );
    let history = thread.history;
    let thread_id = thread.thread_id.to_string();
    let thread = Thread {
        id: thread_id.clone(),
        extra: None,
        session_id: thread_id,
        forked_from_id: thread.forked_from_id.map(|id| id.to_string()),
        parent_thread_id: thread.parent_thread_id.map(|id| id.to_string()),
        preview: bounded_thread_preview(thread.preview),
        ephemeral: false,
        section: thread.section.map(|section| ThreadSection {
            id: section.id,
            name: section.name,
            appearance: section
                .appearance
                .map(|appearance| ThreadSectionAppearance {
                    icon: appearance.icon,
                    color: appearance.color,
                }),
        }),
        section_entered_at: thread
            .section_entered_at
            .map(|entered_at| entered_at.timestamp()),
        project_id: thread.project_id,
        history_mode: thread.history_mode.into(),
        model_provider: if thread.model_provider.is_empty() {
            fallback_provider.to_string()
        } else {
            thread.model_provider
        },
        created_at: thread.created_at.timestamp(),
        updated_at: thread.updated_at.timestamp(),
        recency_at: Some(thread.recency_at.timestamp()),
        status: ThreadStatus::NotLoaded,
        agent_status: None,
        path,
        cwd,
        cli_version: thread.cli_version,
        agent_nickname: source.get_nickname(),
        agent_role: source.get_agent_role(),
        source: source.into(),
        can_accept_direct_input: None,
        thread_source: thread.thread_source.map(Into::into),
        git_info,
        name: thread.name,
        turns: Vec::new(),
    };
    (thread, history)
}

fn summary_from_stored_thread(
    thread: StoredThread,
    fallback_provider: &str,
) -> ConversationSummary {
    let path = thread.rollout_path.unwrap_or_default();
    let source = with_thread_spawn_agent_metadata(
        thread.source,
        thread.agent_nickname.clone(),
        thread.agent_role.clone(),
    );
    let git_info = thread.git_info.map(|git| ConversationGitInfo {
        sha: git.commit_hash.map(|sha| sha.0),
        branch: git.branch,
        origin_url: git.repository_url.map(String::from),
    });
    ConversationSummary {
        conversation_id: thread.thread_id,
        path,
        preview: bounded_thread_preview(thread.preview),
        // Preserve millisecond precision from the thread store so thread/list cursors
        // round-trip the same ordering key used by pagination queries.
        timestamp: Some(
            thread
                .created_at
                .to_rfc3339_opts(SecondsFormat::Millis, true),
        ),
        updated_at: Some(
            thread
                .updated_at
                .to_rfc3339_opts(SecondsFormat::Millis, true),
        ),
        model_provider: if thread.model_provider.is_empty() {
            fallback_provider.to_string()
        } else {
            thread.model_provider
        },
        cwd: thread.cwd,
        cli_version: thread.cli_version,
        source,
        git_info,
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn summary_from_state_db_metadata(
    conversation_id: ThreadId,
    path: PathBuf,
    first_user_message: Option<String>,
    preview: Option<String>,
    timestamp: String,
    updated_at: String,
    model_provider: String,
    cwd: PathBuf,
    cli_version: String,
    source: String,
    _thread_source: Option<codex_protocol::protocol::ThreadSource>,
    agent_nickname: Option<String>,
    agent_role: Option<String>,
    git_sha: Option<String>,
    git_branch: Option<String>,
    git_origin_url: Option<String>,
) -> ConversationSummary {
    let preview = bounded_thread_preview(preview.or(first_user_message).unwrap_or_default());
    let source = serde_json::from_str(&source)
        .or_else(|_| serde_json::from_value(serde_json::Value::String(source.clone())))
        .unwrap_or(codex_protocol::protocol::SessionSource::Unknown);
    let source = with_thread_spawn_agent_metadata(source, agent_nickname, agent_role);
    let git_info = if git_sha.is_none() && git_branch.is_none() && git_origin_url.is_none() {
        None
    } else {
        Some(ConversationGitInfo {
            sha: git_sha,
            branch: git_branch,
            origin_url: git_origin_url,
        })
    };
    ConversationSummary {
        conversation_id,
        path,
        preview,
        timestamp: Some(timestamp),
        updated_at: Some(updated_at),
        model_provider,
        cwd,
        cli_version,
        source,
        git_info,
    }
}

#[cfg(test)]
fn summary_from_thread_metadata(metadata: &ThreadMetadata) -> ConversationSummary {
    summary_from_state_db_metadata(
        metadata.id,
        metadata.rollout_path.clone(),
        metadata.first_user_message.clone(),
        metadata.preview.clone(),
        metadata
            .created_at
            .to_rfc3339_opts(SecondsFormat::Secs, true),
        metadata
            .updated_at
            .to_rfc3339_opts(SecondsFormat::Secs, true),
        metadata.model_provider.clone(),
        metadata.cwd.clone(),
        metadata.cli_version.clone(),
        metadata.source.clone(),
        metadata.thread_source.clone(),
        metadata.agent_nickname.clone(),
        metadata.agent_role.clone(),
        metadata.git_sha.clone(),
        metadata.git_branch.clone(),
        metadata.git_origin_url.clone().map(String::from),
    )
}

fn preview_from_rollout_items(items: &[RolloutItem]) -> String {
    items
        .iter()
        .find_map(|item| match item {
            RolloutItem::ResponseItem(item) => preview_from_response_item(item),
            RolloutItem::Compacted(compacted) => compacted
                .replacement_history
                .as_deref()
                .and_then(|items| items.iter().find_map(preview_from_response_item)),
            _ => None,
        })
        .unwrap_or_default()
}

fn bounded_thread_preview(preview: String) -> String {
    codex_protocol::protocol::bounded_thread_preview_text(preview.as_str()).unwrap_or_default()
}

fn preview_from_response_item(item: &codex_rollout::ResponseItemEnvelope) -> Option<String> {
    match codex_core::parse_turn_item(&item.item) {
        Some(codex_protocol::items::TurnItem::UserMessage(user)) => {
            codex_protocol::protocol::bounded_thread_preview_text(user.message().as_str())
        }
        _ => None,
    }
}

fn build_thread_from_snapshot(
    thread_id: ThreadId,
    session_id: String,
    multi_agent_version: Option<codex_protocol::protocol::MultiAgentVersion>,
    config_snapshot: &ThreadConfigSnapshot,
    path: Option<PathBuf>,
) -> Thread {
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    Thread {
        id: thread_id.to_string(),
        extra: None,
        session_id,
        forked_from_id: None,
        parent_thread_id: config_snapshot.parent_thread_id.map(|id| id.to_string()),
        preview: String::new(),
        ephemeral: config_snapshot.ephemeral,
        section: None,
        section_entered_at: None,
        project_id: None,
        history_mode: config_snapshot.history_mode.into(),
        model_provider: config_snapshot.model_provider_id.clone(),
        created_at: now,
        updated_at: now,
        recency_at: Some(now),
        status: ThreadStatus::NotLoaded,
        agent_status: None,
        path,
        cwd: config_snapshot.cwd().clone(),
        cli_version: env!("CARGO_PKG_VERSION").to_string(),
        agent_nickname: config_snapshot.session_source.get_nickname(),
        agent_role: config_snapshot.session_source.get_agent_role(),
        source: config_snapshot.session_source.clone().into(),
        can_accept_direct_input: Some(can_accept_direct_input(
            multi_agent_version,
            &config_snapshot.session_source,
        )),
        thread_source: config_snapshot.thread_source.clone().map(Into::into),
        git_info: None,
        name: None,
        turns: Vec::new(),
    }
}

fn paginate_background_terminals(
    terminals: &[ThreadBackgroundTerminal],
    cursor: Option<String>,
    limit: Option<u32>,
) -> Result<(Vec<ThreadBackgroundTerminal>, Option<String>), JSONRPCErrorError> {
    let start = match cursor {
        Some(cursor) => {
            let cursor = cursor
                .parse::<i32>()
                .map_err(|err| invalid_request(format!("invalid cursor: {err}")))?;
            terminals
                .iter()
                .position(|terminal| {
                    terminal
                        .process_id
                        .parse::<i32>()
                        .is_ok_and(|process_id| process_id > cursor)
                })
                .unwrap_or(terminals.len())
        }
        None => 0,
    };
    let effective_limit = limit.unwrap_or(terminals.len() as u32).max(1) as usize;
    let end = start.saturating_add(effective_limit).min(terminals.len());
    let next_cursor = (end < terminals.len()).then(|| terminals[end - 1].process_id.clone());
    Ok((terminals[start..end].to_vec(), next_cursor))
}

fn build_thread_from_loaded_snapshot(
    thread_id: ThreadId,
    config_snapshot: &ThreadConfigSnapshot,
    loaded_thread: &CodexThread,
) -> Thread {
    build_thread_from_snapshot(
        thread_id,
        loaded_thread.session_configured().session_id.to_string(),
        loaded_thread.multi_agent_version(),
        config_snapshot,
        loaded_thread.rollout_path(),
    )
}

mod goal_scheduler;
mod resume_preparation;

#[cfg(test)]
#[path = "thread_processor_tests.rs"]
mod thread_processor_tests;
