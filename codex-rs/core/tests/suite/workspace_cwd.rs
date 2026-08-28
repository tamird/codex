use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use anyhow::Result;
use assert_matches::assert_matches;
use codex_core::config::Config;
use codex_extension_api::ExtensionRegistry;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_features::Feature;
use codex_models_manager::CustomModelConfig;
use codex_models_manager::ModelRoutingCandidate;
use codex_models_manager::ModelRoutingProfile;
use codex_models_manager::bundled_models_response;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::models::PermissionProfile;
use codex_protocol::openai_models::ModelServiceTier;
use codex_protocol::openai_models::ModelsResponse;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::TurnSettingsUpdate;
use codex_protocol::protocol::TurnSettingsUpdateOutcome;
use codex_protocol::request_user_input::RequestUserInputAnswer;
use codex_protocol::request_user_input::RequestUserInputResponse;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::turn_input::TurnInputSubmission;
use codex_protocol::turn_input::TurnStartOptions;
use codex_protocol::user_input::UserInput;
use codex_skills_extension::SkillsExtensionConfig;
use codex_skills_extension::install;
use codex_utils_absolute_path::AbsolutePathBuf;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_sequence;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::sse_completed;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::submit_thread_settings;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

fn skills_extensions() -> std::sync::Arc<ExtensionRegistry<Config>> {
    let mut extensions = ExtensionRegistryBuilder::<Config>::new();
    install(&mut extensions, |config: &Config| SkillsExtensionConfig {
        include_instructions: config.include_skill_instructions,
        max_context_tokens: config.skill_max_context_tokens,
        bundled_skills_enabled: config.bundled_skills_enabled(),
        orchestrator_skills_enabled: config.orchestrator_skills_enabled,
        shadow_selection_enabled: config.features.enabled(Feature::SkillSearch),
    });
    std::sync::Arc::new(extensions.build())
}

struct LinkedWorktreeFixture {
    _temp_dir: TempDir,
    primary: AbsolutePathBuf,
    linked: AbsolutePathBuf,
}

fn linked_worktree_fixture() -> LinkedWorktreeFixture {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let primary = temp_dir.path().join("primary");
    let linked = temp_dir.path().join("linked");
    std::fs::create_dir(&primary).expect("create primary checkout");
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

    LinkedWorktreeFixture {
        primary: absolute_canonical(&primary),
        linked: absolute_canonical(&linked),
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

fn paused_response(response_id: &str, call_id: &str) -> String {
    sse(vec![
        ev_response_created(response_id),
        ev_function_call(
            call_id,
            "request_user_input",
            &json!({
                "questions": [{
                    "id": "continue",
                    "header": "Continue",
                    "question": "Continue after updating the turn settings?",
                    "options": [{
                        "label": "Yes (Recommended)",
                        "description": "Continue the current turn."
                    }, {
                        "label": "No",
                        "description": "Stop the current turn."
                    }]
                }]
            })
            .to_string(),
        ),
        ev_completed(response_id),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_cwd_switches_context_before_the_next_model_step() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const CWD_CALL_ID: &str = "set-cwd";
    const REPEAT_CWD_CALL_ID: &str = "repeat-set-cwd";
    const PROFILE: &str = "workspace-route";
    const MODEL_A: &str = "workspace-initial";
    const MODEL_B: &str = "workspace-future";
    const MODEL_C: &str = "workspace-active";
    const TURN_STATE_HEADER: &str = "x-codex-turn-state";
    const PROMPT: &str = "move into the linked worktree and continue";
    const STEER_PROMPT: &str = "preserve this steering across the workspace change";

    let fixture = linked_worktree_fixture();
    std::fs::write(
        fixture.linked.join("AGENTS.md"),
        "Use linked immediate instructions.\n",
    )?;
    let skill_dir = fixture.linked.join(".agents/skills/linked-cwd");
    std::fs::create_dir_all(&skill_dir)?;
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: linked-cwd\ndescription: linked worktree skill\n---\n\n# Linked cwd\n",
    )?;
    let server = start_mock_server().await;
    let responses = mount_response_sequence(
        &server,
        vec![
            sse_response(paused_response("resp-1", "pause-before-cwd"))
                .insert_header(TURN_STATE_HEADER, "workspace-turn-state"),
            sse_response(sse(vec![
                ev_response_created("resp-2"),
                ev_function_call_with_namespace(
                    CWD_CALL_ID,
                    "workspace",
                    "set_cwd",
                    &json!({ "path": fixture.linked }).to_string(),
                ),
                ev_completed("resp-2"),
            ])),
            sse_response(paused_response("resp-3", "pause-after-cwd")),
            sse_response(sse(vec![
                ev_response_created("resp-4"),
                ev_function_call_with_namespace(
                    REPEAT_CWD_CALL_ID,
                    "workspace",
                    "set_cwd",
                    &json!({ "path": fixture.linked }).to_string(),
                ),
                ev_completed("resp-4"),
            ])),
            sse_response(sse_completed("resp-5")),
            sse_response(sse_completed("resp-6")),
        ],
    )
    .await;
    let primary = fixture.primary.clone();
    let models = bundled_models_response()?;
    let model = models
        .models
        .into_iter()
        .find(|model| model.slug == "gpt-5.4")
        .expect("bundled gpt-5.4 model");
    let builder = test_codex().with_model(PROFILE).with_config(move |config| {
        config.cwd = primary.clone();
        config.workspace_roots = vec![primary];
        config.custom_models = HashMap::from([(
            PROFILE.to_string(),
            CustomModelConfig {
                model: MODEL_A.to_string(),
                routing_profile: Some(ModelRoutingProfile {
                    candidates: vec![ModelRoutingCandidate {
                        model: MODEL_A.to_string(),
                        reasoning_effort: Some(ReasoningEffort::Low),
                        service_tier: None,
                    }],
                }),
                model_context_window: None,
                model_auto_compact_token_limit: None,
                trust_candidate_constraints: false,
            },
        )]);
        config.model_catalog = Some(ModelsResponse {
            models: [MODEL_A, MODEL_B, MODEL_C]
                .into_iter()
                .map(|slug| {
                    let mut model = model.clone();
                    model.slug = slug.to_string();
                    if slug == MODEL_B {
                        model.service_tiers.push(ModelServiceTier {
                            id: "flex".to_string(),
                            name: "Flex".to_string(),
                            description: "Flex service for the future-turn fixture".to_string(),
                        });
                    }
                    model
                })
                .collect(),
        });
        config.model_reasoning_effort = Some(ReasoningEffort::Low);
        config.model_reasoning_summary = Some(ReasoningSummary::Concise);
        config.service_tier = None;
        for feature in [
            Feature::WorkspaceCwdTool,
            Feature::StepModelSwitching,
            Feature::DefaultModeRequestUserInput,
            Feature::FastMode,
        ] {
            config
                .features
                .enable(feature)
                .expect("enable test feature");
        }
    });
    let test = builder
        .with_extensions(skills_extensions())
        .build(&server)
        .await?;

    let submission = test
        .codex
        .start_or_steer_turn(
            TurnInputRequest::user_input(vec![UserInput::Text {
                text: PROMPT.to_string(),
                text_elements: Vec::new(),
            }])
            .with_thread_settings(ThreadSettingsOverrides {
                approval_policy: Some(AskForApproval::OnRequest),
                permission_profile: Some(PermissionProfile::workspace_write()),
                ..Default::default()
            })
            .on_start(TurnStartOptions {
                parent_turn_id: Some("workspace-parent-turn".to_string()),
                root_turn_id: Some("workspace-root-turn".to_string()),
                turn_trigger: Some("workspace-trigger".to_string()),
                ..Default::default()
            })
            .with_responses_metadata(Some(HashMap::from([(
                "workspace_test_client".to_string(),
                "admitted".to_string(),
            )]))),
        )
        .await?;
    let turn_id = assert_matches!(submission, TurnInputSubmission::Started { turn_id } => turn_id);
    let mut started_turns = Vec::new();
    for update in [
        TurnSettingsUpdate {
            model: Some(MODEL_C.to_string()),
            effort: Some(Some(ReasoningEffort::Medium)),
            summary: Some(ReasoningSummary::Detailed),
            service_tier: Some(Some(ServiceTier::Fast.request_value().to_string())),
        },
        TurnSettingsUpdate {
            effort: Some(Some(ReasoningEffort::High)),
            ..Default::default()
        },
    ] {
        let paused = wait_for_event(&test.codex, |event| match event {
            EventMsg::TurnStarted(event) => {
                started_turns.push(event.turn_id.clone());
                false
            }
            EventMsg::RequestUserInput(_) => true,
            EventMsg::Error(error) => panic!("workspace turn failed: {}", error.message),
            _ => false,
        })
        .await;
        let paused = assert_matches!(paused, EventMsg::RequestUserInput(paused) => paused);
        assert_eq!(paused.turn_id, turn_id);
        if update.model.is_some() {
            // The active selection must differ from both admission and the
            // future defaults before workspace reconstruction begins.
            submit_thread_settings(
                &test.codex,
                ThreadSettingsOverrides {
                    model: Some(MODEL_B.to_string()),
                    effort: Some(Some(ReasoningEffort::High)),
                    summary: Some(ReasoningSummary::Auto),
                    service_tier: Some(Some(ServiceTier::Flex.request_value().to_string())),
                    ..Default::default()
                },
            )
            .await?;
            let steered =
                test.codex
                    .start_or_steer_turn(
                        TurnInputRequest::user_input(vec![UserInput::Text {
                            text: STEER_PROMPT.to_string(),
                            text_elements: Vec::new(),
                        }])
                        .with_responses_metadata(Some(HashMap::from([
                            ("workspace_test_client".to_string(), "steered".to_string()),
                        ]))),
                    )
                    .await?;
            assert_eq!(
                steered,
                TurnInputSubmission::Steered {
                    turn_id: turn_id.clone()
                }
            );
        }
        let (reply, outcome) = tokio::sync::oneshot::channel();
        test.codex
            .submit(Op::TurnSettings {
                turn_id: turn_id.clone(),
                update,
                reply,
            })
            .await?;
        let outcome = tokio::time::timeout(Duration::from_secs(/*secs*/ 10), outcome).await??;
        assert_eq!(outcome, TurnSettingsUpdateOutcome::Applied);
        test.codex
            .submit(Op::UserInputAnswer {
                id: turn_id.clone(),
                response: RequestUserInputResponse {
                    answers: HashMap::from([(
                        "continue".to_string(),
                        RequestUserInputAnswer {
                            answers: vec!["Yes (Recommended)".to_string()],
                        },
                    )]),
                },
            })
            .await?;
    }
    let completion = wait_for_event(&test.codex, |event| match event {
        EventMsg::TurnStarted(event) => {
            started_turns.push(event.turn_id.clone());
            false
        }
        EventMsg::TurnComplete(_) => true,
        EventMsg::Error(error) => panic!("workspace turn failed: {}", error.message),
        _ => false,
    })
    .await;
    let completion = assert_matches!(completion, EventMsg::TurnComplete(completion) => completion);
    assert_eq!(completion.turn_id, turn_id);
    assert_eq!(started_turns, vec![turn_id.clone()]);

    let requests = responses.requests();
    let (initial, refreshed, repeated) = assert_matches!(
        requests.as_slice(),
        [initial, _, refreshed, _, repeated] => (initial, refreshed, repeated)
    );
    assert!(initial.tool_by_name("workspace", "set_cwd").is_some());
    assert!(
        refreshed
            .function_call_output_text(CWD_CALL_ID)
            .is_some_and(|output| output.contains("subsequent_model_steps"))
    );
    assert!(
        refreshed.body_contains_text("Use linked immediate instructions."),
        "refreshed request should contain the linked worktree AGENTS.md: {}",
        refreshed.body_json()
    );
    assert!(
        refreshed.body_contains_text("linked-cwd: linked worktree skill"),
        "refreshed request should contain the linked worktree skill: {}",
        refreshed.body_json()
    );
    let linked_cwd = fixture.linked.as_path().to_string_lossy();
    let developer_texts = refreshed.message_input_texts("developer");
    assert!(
        developer_texts
            .iter()
            .any(|text| text.contains("<permissions instructions>")
                && text.contains(linked_cwd.as_ref())),
        "refreshed request should supersede the old permissions context: {developer_texts:?}"
    );
    let repeat_output = repeated
        .function_call_output_text(REPEAT_CWD_CALL_ID)
        .expect("continuation should contain the repeated cwd result");
    let repeat_output: serde_json::Value = serde_json::from_str(&repeat_output)?;
    assert_eq!(
        repeat_output["previous_cwd"],
        fixture.linked.as_path().to_string_lossy().as_ref()
    );
    assert_eq!(repeat_output["changed"], false);

    let metadata = requests
        .iter()
        .map(|request| {
            serde_json::from_str::<serde_json::Value>(
                &request
                    .header("x-codex-turn-metadata")
                    .expect("turn metadata"),
            )
        })
        .collect::<serde_json::Result<Vec<_>>>()?;
    let initial_metadata = metadata.first().expect("initial metadata");
    let timestamp = initial_metadata["turn_started_at_unix_ms"]
        .as_i64()
        .expect("turn timestamp");
    assert!(timestamp > 0);
    let session_id = initial.header("session-id").expect("initial session id");
    let mut expected_metadata = initial_metadata.clone();
    expected_metadata
        .as_object_mut()
        .expect("metadata object")
        .remove("workspaces");
    for (index, (request, metadata)) in requests.iter().zip(&metadata).enumerate() {
        let mut retained_metadata = metadata.clone();
        retained_metadata
            .as_object_mut()
            .expect("metadata object")
            .remove("workspaces");
        expected_metadata["workspace_test_client"] =
            json!(if index == 0 { "admitted" } else { "steered" });
        assert_eq!(retained_metadata, expected_metadata);
        assert_eq!(
            json!({
                "turn_id": metadata["turn_id"],
                "parent_turn_id": metadata["parent_turn_id"],
                "root_turn_id": metadata["root_turn_id"],
                "turn_trigger": metadata["turn_trigger"],
                "client": metadata["workspace_test_client"],
                "timestamp": metadata["turn_started_at_unix_ms"],
            }),
            json!({
                "turn_id": turn_id,
                "parent_turn_id": "workspace-parent-turn",
                "root_turn_id": "workspace-root-turn",
                "turn_trigger": "workspace-trigger",
                "client": if index == 0 { "admitted" } else { "steered" },
                "timestamp": timestamp,
            })
        );
        assert_eq!(request.header("session-id"), Some(session_id.clone()));
        assert_eq!(
            request.header(TURN_STATE_HEADER),
            (index != 0).then(|| "workspace-turn-state".to_string())
        );
        if index != 0 {
            assert_eq!(
                request
                    .message_input_texts("user")
                    .into_iter()
                    .filter(|text| text == PROMPT || text == STEER_PROMPT)
                    .collect::<Vec<_>>(),
                vec![PROMPT.to_string(), STEER_PROMPT.to_string()]
            );
        }
    }

    test.submit_text_turn("continue in the linked worktree")
        .await?;
    let requests = responses.requests();
    let settings = requests
        .iter()
        .map(|request| {
            let body = request.body_json();
            json!({
                "model": body["model"],
                "reasoning": body["reasoning"],
                "service_tier": body.get("service_tier"),
            })
        })
        .collect::<Vec<_>>();
    let selected = json!({
        "model": MODEL_C,
        "reasoning": {"effort": "medium", "summary": "detailed"},
        "service_tier": "priority",
    });
    let updated = json!({
        "model": MODEL_C,
        "reasoning": {"effort": "high", "summary": "detailed"},
        "service_tier": "priority",
    });
    assert_eq!(
        settings,
        vec![
            json!({
                "model": MODEL_A,
                "reasoning": {"effort": "low", "summary": "concise"},
                "service_tier": null,
            }),
            selected.clone(),
            selected,
            updated.clone(),
            updated,
            json!({
                "model": MODEL_B,
                "reasoning": {"effort": "high", "summary": "auto"},
                "service_tier": "flex",
            }),
        ]
    );
    let next_turn = requests.last().expect("next turn request");
    assert!(next_turn.body_contains_text("Use linked immediate instructions."));
    assert!(next_turn.body_contains_text("continue in the linked worktree"));
    assert_eq!(next_turn.header(TURN_STATE_HEADER), None);
    let next_metadata: serde_json::Value = serde_json::from_str(
        &next_turn
            .header("x-codex-turn-metadata")
            .expect("next turn metadata"),
    )?;
    assert_ne!(next_metadata["turn_id"], turn_id);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_cwd_rejects_a_response_with_sibling_tool_calls() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const CWD_CALL_ID: &str = "set-cwd-with-sibling";
    const SHELL_CALL_ID: &str = "sibling-shell";

    let fixture = linked_worktree_fixture();
    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call_with_namespace(
                    CWD_CALL_ID,
                    "workspace",
                    "set_cwd",
                    &json!({ "path": fixture.linked }).to_string(),
                ),
                ev_function_call(
                    SHELL_CALL_ID,
                    "exec_command",
                    &json!({ "cmd": "pwd", "max_output_tokens": 1000 }).to_string(),
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-2", "retry later"),
                ev_completed("resp-2"),
            ]),
        ],
    )
    .await;
    let primary = fixture.primary.clone();
    let mut builder = test_codex().with_config(move |config| {
        config.cwd = primary.clone();
        config.workspace_roots = vec![primary];
        config
            .features
            .enable(Feature::WorkspaceCwdTool)
            .expect("enable workspace cwd tool");
    });
    let test = builder.build(&server).await?;

    test.submit_turn_with_permission_profile(
        "try an unsafe mixed context transition",
        PermissionProfile::Disabled,
    )
    .await?;

    let requests = responses.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1]
            .function_call_output_text(CWD_CALL_ID)
            .is_some_and(|output| output.contains("must be the only tool call"))
    );
    let shell_output = requests[1]
        .function_call_output_text(SHELL_CALL_ID)
        .expect("second request should contain sibling shell output");
    assert!(
        shell_output.contains(fixture.primary.as_path().to_string_lossy().as_ref()),
        "sibling shell did not retain primary cwd: {shell_output}\n{}",
        requests[1].body_json()
    );

    Ok(())
}
