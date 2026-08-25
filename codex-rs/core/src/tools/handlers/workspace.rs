use std::collections::BTreeMap;
use std::path::PathBuf;

use codex_git_utils::GitInfo;
use codex_git_utils::collect_git_info;
use codex_git_utils::get_git_worktree_identity;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_thread_store::GitInfoPatch;
use codex_thread_store::ThreadMetadataPatch;
use codex_tools::JsonSchema;
use codex_tools::JsonToolOutput;
use codex_tools::ResponsesApiNamespace;
use codex_tools::ResponsesApiNamespaceTool;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_path_uri::PathUri;
use serde::Deserialize;
use serde_json::json;
use tracing::warn;

use crate::function_tool::FunctionCallError;
use crate::session::SessionSettingsUpdate;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;

const NAMESPACE: &str = "workspace";
const TOOL_NAME: &str = "set_cwd";

pub(crate) fn is_set_workspace_cwd_tool(tool_name: &ToolName) -> bool {
    tool_name == &ToolName::namespaced(NAMESPACE, TOOL_NAME)
}

#[derive(Deserialize)]
struct SetWorkspaceCwdArgs {
    path: String,
}

#[derive(Debug)]
struct LinkedWorktreeTarget {
    cwd: AbsolutePathBuf,
    git_info: GitInfo,
}

pub(crate) struct SetWorkspaceCwdHandler;

impl ToolExecutor<ToolInvocation> for SetWorkspaceCwdHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced(NAMESPACE, TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        let mut properties = BTreeMap::new();
        properties.insert(
            "path".to_string(),
            JsonSchema::string(Some(
                "Absolute path to the root of a linked Git worktree from the current repository."
                    .to_string(),
            )),
        );
        ToolSpec::Namespace(ResponsesApiNamespace {
            name: NAMESPACE.to_string(),
            description: "Tools for changing the working directory used by the active thread."
                .to_string(),
            tools: vec![ResponsesApiNamespaceTool::Function(ResponsesApiTool {
                name: TOOL_NAME.to_string(),
                description: "Root thread only. Immediately adopt a linked Git worktree as this thread's working directory. This must be the only tool call in the model response; the next model step and all later tool calls use the new cwd, sandbox workspace root, AGENTS.md, and skills. The path must be the exact worktree root and share the current checkout's Git common directory. Project config layers and already-running MCP servers keep their startup configuration."
                    .to_string(),
                strict: false,
                defer_loading: None,
                parameters: JsonSchema::object(
                    properties,
                    /*required*/ Some(vec!["path".to_string()]),
                    /*additional_properties*/ Some(false.into()),
                ),
                output_schema: Some(json!({
                    "type": "object",
                    "properties": {
                        "changed": { "type": "boolean" },
                        "cwd": { "type": "string" },
                        "git_branch": { "type": ["string", "null"] },
                        "metadata_persisted": { "type": "boolean" },
                        "applies_to": { "type": "string", "enum": ["subsequent_model_steps"] },
                        "previous_cwd": { "type": "string" },
                        "instruction": { "type": "string" }
                    },
                    "required": [
                        "changed",
                        "cwd",
                        "git_branch",
                        "metadata_persisted",
                        "applies_to",
                        "previous_cwd",
                        "instruction"
                    ],
                    "additionalProperties": false
                })),
            })],
        })
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let ToolInvocation {
                session,
                turn,
                step_context,
                payload,
                ..
            } = invocation;
            let ToolPayload::Function { arguments } = payload else {
                return Err(FunctionCallError::RespondToModel(format!(
                    "{NAMESPACE}.{TOOL_NAME} handler received unsupported payload"
                )));
            };
            if turn.session_source.is_non_root_agent() {
                return Err(FunctionCallError::RespondToModel(
                    "workspace.set_cwd can only be used by the root thread".to_string(),
                ));
            }
            if step_context.context_transition_has_sibling_tool() {
                return Err(FunctionCallError::RespondToModel(
                    "workspace.set_cwd must be the only tool call in a model response; retry it alone so Codex can switch contexts before running another tool"
                        .to_string(),
                ));
            }
            let Some(environment) = step_context.environments.single_local_environment() else {
                return Err(FunctionCallError::RespondToModel(
                    "workspace.set_cwd requires exactly one ready local environment".to_string(),
                ));
            };
            let current_turn_cwd = environment.cwd().to_abs_path().map_err(|err| {
                FunctionCallError::RespondToModel(format!(
                    "workspace.set_cwd could not resolve the current local cwd: {err}"
                ))
            })?;
            let args: SetWorkspaceCwdArgs = parse_arguments(&arguments)?;
            let target = validate_linked_worktree_target(&current_turn_cwd, &args.path).await?;
            let target_cwd = PathUri::from_abs_path(&target.cwd);
            let mut workspace_roots = Vec::new();
            for root in environment.workspace_roots() {
                let root = if root == environment.cwd() {
                    target_cwd.clone()
                } else {
                    root.clone()
                };
                if !workspace_roots.contains(&root) {
                    workspace_roots.push(root);
                }
            }

            // Preserve configuration ownership so the next client turn can reuse FromThread.
            let mut selection = environment.selection();
            selection.cwd = target_cwd;
            selection.workspace_roots = workspace_roots;
            let updates = SessionSettingsUpdate {
                environments: Some(TurnEnvironmentSelections::new(
                    target.cwd.clone(),
                    vec![selection],
                )),
                ..Default::default()
            };
            let current_settings = session
                .preview_settings(&SessionSettingsUpdate::default())
                .await
                .map_err(|err| {
                    FunctionCallError::RespondToModel(format!(
                        "workspace.set_cwd could not read the current thread settings: {err}"
                    ))
                })?;
            session.preview_settings(&updates).await.map_err(|err| {
                FunctionCallError::RespondToModel(format!(
                    "workspace.set_cwd would violate the current thread constraints: {err}"
                ))
            })?;
            let settings_changed = current_settings.cwd() != &target.cwd;
            let active_context_changed = current_turn_cwd != target.cwd;
            let changed = settings_changed || active_context_changed;

            let mut metadata_persisted = !changed;
            let committed_settings = if settings_changed {
                let commit = session.update_settings(updates).await.map_err(|err| {
                    FunctionCallError::RespondToModel(format!(
                        "workspace.set_cwd could not update the thread settings: {err}"
                    ))
                })?;
                Some(commit.snapshot)
            } else {
                None
            };

            if active_context_changed {
                step_context.request_turn_context_refresh();
            }
            let mut instructions_refresh = Ok(());
            if changed {
                let environments = session.services.turn_environments.snapshot().await;
                // The next model step must observe the replacement environment's filesystem.
                // Waiting here keeps the workspace transition atomic without making ordinary
                // environment snapshots block on startup.
                for environment in environments.starting() {
                    environment.wait_until_ready().await.map_err(|err| {
                        FunctionCallError::RespondToModel(format!(
                            "workspace.set_cwd could not prepare the linked worktree environment: {err}"
                        ))
                    })?;
                }
                let environments = session.services.turn_environments.snapshot().await;
                let config = session.get_config().await;
                instructions_refresh = session
                    .services
                    .agents_md_manager
                    .refresh(config.as_ref(), &environments)
                    .await
                    .map_err(|err| {
                        FunctionCallError::RespondToModel(format!(
                            "workspace.set_cwd changed the workspace, but could not reload AGENTS.md: {err}"
                        ))
                    });

                metadata_persisted = if let Some(live_thread) = session.live_thread() {
                    let git_info = GitInfoPatch {
                        sha: Some(
                            target
                                .git_info
                                .commit_hash
                                .as_ref()
                                .map(|sha| sha.0.clone()),
                        ),
                        branch: Some(target.git_info.branch.clone()),
                        origin_url: Some(target.git_info.repository_url.clone()),
                    };
                    match live_thread
                        .update_metadata(
                            ThreadMetadataPatch {
                                cwd: Some(target.cwd.clone().into_path_buf()),
                                git_info: Some(git_info),
                                ..Default::default()
                            },
                            /*include_archived*/ false,
                        )
                        .await
                    {
                        Ok(_) => true,
                        Err(err) => {
                            warn!(
                                thread_id = %session.thread_id,
                                cwd = %target.cwd.as_path().display(),
                                "workspace.set_cwd changed live settings but could not persist thread metadata: {err}"
                            );
                            false
                        }
                    }
                } else {
                    false
                };
            }

            if let Some(thread_settings) = committed_settings {
                session
                    .send_event(
                        turn.as_ref(),
                        EventMsg::ThreadSettingsApplied(ThreadSettingsAppliedEvent {
                            thread_settings,
                        }),
                    )
                    .await;
            }
            instructions_refresh?;

            Ok(boxed_tool_output(JsonToolOutput::new(json!({
                "changed": changed,
                "cwd": target.cwd,
                "git_branch": target.git_info.branch,
                "metadata_persisted": metadata_persisted,
                "applies_to": "subsequent_model_steps",
                "previous_cwd": current_turn_cwd,
                "instruction": "The context has switched. Continue the current task; all later tool calls use the new cwd by default."
            }))))
        })
    }
}

impl CoreToolRuntime for SetWorkspaceCwdHandler {}

async fn validate_linked_worktree_target(
    current_cwd: &AbsolutePathBuf,
    requested_path: &str,
) -> Result<LinkedWorktreeTarget, FunctionCallError> {
    let requested_path = PathBuf::from(requested_path);
    if !requested_path.is_absolute() {
        return Err(FunctionCallError::RespondToModel(
            "workspace.set_cwd requires an absolute path".to_string(),
        ));
    }
    let metadata = tokio::fs::metadata(&requested_path).await.map_err(|err| {
        FunctionCallError::RespondToModel(format!(
            "workspace.set_cwd could not read `{}`: {err}",
            requested_path.display()
        ))
    })?;
    if !metadata.is_dir() {
        return Err(FunctionCallError::RespondToModel(format!(
            "workspace.set_cwd target is not a directory: {}",
            requested_path.display()
        )));
    }
    let target = tokio::fs::canonicalize(&requested_path)
        .await
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "workspace.set_cwd could not canonicalize `{}`: {err}",
                requested_path.display()
            ))
        })?;
    let target = AbsolutePathBuf::try_from(target).map_err(|err| {
        FunctionCallError::RespondToModel(format!(
            "workspace.set_cwd target is not an absolute path: {err}"
        ))
    })?;
    let current_identity = get_git_worktree_identity(current_cwd.as_path())
        .await
        .ok_or_else(|| {
            FunctionCallError::RespondToModel(format!(
                "workspace.set_cwd current cwd is not in a Git worktree: {}",
                current_cwd.as_path().display()
            ))
        })?;
    let target_identity = get_git_worktree_identity(target.as_path())
        .await
        .ok_or_else(|| {
            FunctionCallError::RespondToModel(format!(
                "workspace.set_cwd target is not in a Git worktree: {}",
                target.as_path().display()
            ))
        })?;
    if target != target_identity.worktree_root {
        return Err(FunctionCallError::RespondToModel(format!(
            "workspace.set_cwd target must be the exact worktree root `{}`",
            target_identity.worktree_root.as_path().display()
        )));
    }
    if current_identity.common_dir != target_identity.common_dir {
        return Err(FunctionCallError::RespondToModel(
            "workspace.set_cwd target belongs to a different Git repository".to_string(),
        ));
    }
    if !target_identity.is_linked_worktree {
        return Err(FunctionCallError::RespondToModel(
            "workspace.set_cwd target must be a linked Git worktree, not the primary checkout"
                .to_string(),
        ));
    }
    let git_info = collect_git_info(target.as_path()).await.ok_or_else(|| {
        FunctionCallError::RespondToModel(format!(
            "workspace.set_cwd could not collect Git metadata for `{}`",
            target.as_path().display()
        ))
    })?;
    Ok(LinkedWorktreeTarget {
        cwd: target,
        git_info,
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::Command;
    use std::sync::Arc;

    use codex_login::CodexAuth;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::SessionSource;
    use codex_protocol::protocol::SubAgentSource;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use super::*;
    use crate::session::step_context::StepContext;
    use crate::session::tests::make_session_and_context;
    use crate::session::tests::make_session_and_context_with_auth_and_config_and_rx;
    use crate::tools::context::ToolCallSource;
    use crate::turn_diff_tracker::TurnDiffTracker;

    struct LinkedWorktreeFixture {
        _temp_dir: TempDir,
        primary: AbsolutePathBuf,
        linked: AbsolutePathBuf,
        unrelated: AbsolutePathBuf,
    }

    fn linked_worktree_fixture() -> LinkedWorktreeFixture {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let primary = temp_dir.path().join("primary");
        let linked = temp_dir.path().join("linked");
        let unrelated = temp_dir.path().join("unrelated");
        std::fs::create_dir(&primary).expect("create primary checkout");
        std::fs::create_dir(&unrelated).expect("create unrelated checkout");
        run_git(&primary, &["init", "-q"]);
        run_git(&primary, &["config", "user.email", "codex@example.com"]);
        run_git(&primary, &["config", "user.name", "Codex Test"]);
        std::fs::write(primary.join("README.md"), "test\n").expect("write initial file");
        run_git(&primary, &["add", "README.md"]);
        run_git(&primary, &["commit", "-qm", "initial"]);
        run_git(
            &primary,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "linked-test",
                linked.to_str().expect("linked path is UTF-8"),
            ],
        );
        run_git(&unrelated, &["init", "-q"]);

        LinkedWorktreeFixture {
            primary: absolute_canonical(&primary),
            linked: absolute_canonical(&linked),
            unrelated: absolute_canonical(&unrelated),
            _temp_dir: temp_dir,
        }
    }

    fn run_git(cwd: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn absolute_canonical(path: &Path) -> AbsolutePathBuf {
        AbsolutePathBuf::try_from(std::fs::canonicalize(path).expect("canonical path"))
            .expect("absolute path")
    }

    fn invocation(
        session: Arc<crate::session::session::Session>,
        step_context: Arc<StepContext>,
        path: &AbsolutePathBuf,
    ) -> ToolInvocation {
        let turn = Arc::clone(&step_context.turn);
        ToolInvocation {
            session,
            step_context,
            turn,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
            call_id: "workspace-call".to_string(),
            tool_name: ToolName::namespaced(NAMESPACE, TOOL_NAME),
            source: ToolCallSource::Direct,
            payload: ToolPayload::Function {
                arguments: json!({ "path": path }).to_string(),
            },
        }
    }

    #[tokio::test]
    async fn validation_rejects_nested_paths_and_unrelated_repositories() {
        let fixture = linked_worktree_fixture();
        let nested = fixture.linked.join("nested");
        std::fs::create_dir(&nested).expect("create nested directory");

        let nested_error = validate_linked_worktree_target(
            &fixture.primary,
            nested.as_path().to_str().expect("nested path is UTF-8"),
        )
        .await
        .expect_err("nested path should be rejected");
        assert!(
            matches!(
                nested_error,
                FunctionCallError::RespondToModel(ref message)
                    if message.contains("target must be the exact worktree root")
            ),
            "unexpected nested-path error: {nested_error:?}"
        );

        let unrelated_error = validate_linked_worktree_target(
            &fixture.primary,
            fixture
                .unrelated
                .as_path()
                .to_str()
                .expect("unrelated path is UTF-8"),
        )
        .await
        .expect_err("unrelated repository should be rejected");
        assert_eq!(
            unrelated_error,
            FunctionCallError::RespondToModel(
                "workspace.set_cwd target belongs to a different Git repository".to_string()
            )
        );

        let primary_error = validate_linked_worktree_target(
            &fixture.linked,
            fixture
                .primary
                .as_path()
                .to_str()
                .expect("primary path is UTF-8"),
        )
        .await
        .expect_err("primary checkout should be rejected");
        assert_eq!(
            primary_error,
            FunctionCallError::RespondToModel(
                "workspace.set_cwd target must be a linked Git worktree, not the primary checkout"
                    .to_string()
            )
        );
    }

    #[tokio::test]
    async fn handler_requests_immediate_context_refresh_and_refreshes_agents_md() {
        let fixture = linked_worktree_fixture();
        std::fs::write(
            fixture.linked.join("AGENTS.md"),
            "Use linked-worktree instructions.\n",
        )
        .expect("write linked AGENTS.md");
        let (session, turn, rx_event) = make_session_and_context_with_auth_and_config_and_rx(
            CodexAuth::from_api_key("Test API Key"),
            Vec::new(),
            |config| {
                config.cwd = fixture.primary.clone();
                config.workspace_roots = vec![fixture.primary.clone()];
            },
        )
        .await;
        let step_context = StepContext::for_test(Arc::clone(&turn));

        SetWorkspaceCwdHandler
            .handle(invocation(
                Arc::clone(&session),
                Arc::clone(&step_context),
                &fixture.linked,
            ))
            .await
            .expect("workspace cwd update should succeed");

        assert!(step_context.turn_context_refresh_requested());

        assert_eq!(
            turn.environments.single_local_environment_cwd(),
            Some(fixture.primary.clone()),
            "the active turn must retain its original cwd"
        );
        assert!(
            !turn.config.workspace_roots.contains(&fixture.linked),
            "the active turn must not gain the linked worktree as a workspace root"
        );
        let next_turn = session.new_default_turn().await;
        assert_eq!(
            next_turn.environments.single_local_environment_cwd(),
            Some(fixture.linked.clone())
        );
        assert!(next_turn.config.workspace_roots.contains(&fixture.linked));
        let loaded_agents_md = session
            .services
            .agents_md_manager
            .get_loaded()
            .await
            .expect("linked worktree AGENTS.md should load");
        assert!(
            loaded_agents_md
                .text()
                .contains("linked-worktree instructions")
        );
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx_event.recv())
            .await
            .expect("settings event should arrive")
            .expect("settings event channel should stay open");
        let EventMsg::ThreadSettingsApplied(settings) = event.msg else {
            panic!("expected ThreadSettingsApplied event");
        };
        assert_eq!(settings.thread_settings.cwd, fixture.linked);
    }

    #[tokio::test]
    async fn handler_rejects_subagents_even_when_invoked_directly() {
        let fixture = linked_worktree_fixture();
        let (session, mut turn) = make_session_and_context().await;
        turn.session_source =
            SessionSource::SubAgent(SubAgentSource::Other("workspace-test".to_string()));
        let turn = Arc::new(turn);
        let step_context = StepContext::for_test(turn);

        let result = SetWorkspaceCwdHandler
            .handle(invocation(Arc::new(session), step_context, &fixture.linked))
            .await;
        let Err(error) = result else {
            panic!("subagent call should fail");
        };
        assert_eq!(
            error,
            FunctionCallError::RespondToModel(
                "workspace.set_cwd can only be used by the root thread".to_string()
            )
        );
    }

    #[tokio::test]
    async fn handler_rejects_a_context_transition_mixed_with_another_tool() {
        let fixture = linked_worktree_fixture();
        let (session, turn) = make_session_and_context().await;
        let step_context = StepContext::for_test(Arc::new(turn));
        step_context.reject_context_transition_mixed_with_sibling_tool();

        let result = SetWorkspaceCwdHandler
            .handle(invocation(Arc::new(session), step_context, &fixture.linked))
            .await;

        let Err(error) = result else {
            panic!("mixed context transition should fail");
        };
        assert_eq!(
            error,
            FunctionCallError::RespondToModel(
                "workspace.set_cwd must be the only tool call in a model response; retry it alone so Codex can switch contexts before running another tool"
                    .to_string()
            )
        );
    }
}
