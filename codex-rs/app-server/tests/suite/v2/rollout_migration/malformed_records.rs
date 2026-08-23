use super::*;
use pretty_assertions::assert_eq;

fn response_message(role: &str, text: &str) -> Result<RolloutItem> {
    Ok(serde_json::from_value(json!({
        "type": "response_item",
        "payload": {
            "type": "message", "role": role,
            "content": [{ "type": if role == "user" { "input_text" } else { "output_text" }, "text": text }]
        }
    }))?)
}

/// Both stored reference formats can contain a partial ordinary record followed by valid data.
#[derive(Clone, Copy)]
enum MalformedHistory {
    LegacyReference,
    NativeHistoryBaseRetry,
}

#[test_case::test_case(MalformedHistory::LegacyReference; "legacy_reference")]
#[test_case::test_case(MalformedHistory::NativeHistoryBaseRetry; "native_history_base_retry")]
#[tokio::test]
async fn malformed_middle_record_preserves_context_and_bytes_across_restart(
    representation: MalformedHistory,
) -> Result<()> {
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_sequence(
        &server,
        (0..2)
            .map(|index| {
                responses::sse(vec![
                    responses::ev_response_created(&format!("reply-{index}")),
                    responses::ev_assistant_message(&format!("message-{index}"), "resumed reply"),
                    responses::ev_completed(&format!("reply-{index}")),
                ])
            })
            .collect(),
    )
    .await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(home.path())?;
    let thread_id = ThreadId::new();
    let parent_segment = SegmentId::new();
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let parent_rollout_id = ThreadId::new();
    let parent = match representation {
        MalformedHistory::NativeHistoryBaseRetry => home
            .path()
            .join("sessions/rollout_segments/2025/01/03")
            .join(format!(
                "rollout-2025-01-03T12-00-00-{thread_id}_{parent_rollout_id}.jsonl"
            )),
        MalformedHistory::LegacyReference => home
            .path()
            .join("rotated_rollout_segments")
            .join(thread_id.to_string())
            .join(parent_segment.to_string())
            .join(&filename),
    };
    let next = write_paginated_segment(
        &parent,
        home.path(),
        thread_id,
        parent_segment,
        /*start_ordinal*/ 0,
        vec![
            response_message("user", "inherited user before damage")?,
            response_message("assistant", "inherited answer before damage")?,
        ],
    )?;
    let selected = home.path().join("sessions/2025/01/03").join(filename);
    let mut items = vec![
        legacy_turn_started("stored-turn"),
        paginated_completed_user_message(
            thread_id,
            "stored-turn",
            "stored-user",
            "local user before damage",
        ),
        response_message("user", "local user before damage")?,
        serde_json::from_value(json!({"type":"response_item", "payload":{
            "type":"reasoning", "summary":[], "encrypted_content":null
        }}))?,
        response_message("assistant", "local answer after damage")?,
        legacy_turn_completed("stored-turn"),
    ];
    if matches!(representation, MalformedHistory::LegacyReference) {
        items.insert(
            0,
            legacy_segment_reference(parent.clone(), thread_id, parent_segment),
        );
    }
    write_paginated_segment(
        &selected,
        home.path(),
        thread_id,
        SegmentId::new(),
        next,
        items,
    )?;
    let mut malformed = Vec::new();
    for raw in fs::read(&selected)?.split_inclusive(|byte| *byte == b'\n') {
        let mut record: serde_json::Value = serde_json::from_slice(raw)?;
        if matches!(representation, MalformedHistory::NativeHistoryBaseRetry) {
            if record["type"] == "session_meta" {
                record["payload"]["history_base"] = json!({
                    "thread_id": parent_rollout_id,
                    "end_ordinal_exclusive": next,
                    "end_byte_offset": fs::metadata(&parent)?.len(),
                });
                writeln!(malformed, "{}", serde_json::to_string(&record)?)?;
                continue;
            }
            if record["payload"]["type"] == "item_completed" {
                // A partial event and its complete retry reuse an ordinal. Keep the retry once.
                let mut partial = format!(
                    "{{\"timestamp\":\"2025-01-03T12:00:00Z\",\"ordinal\":{},\"type\":\"event_msg\",\"payload\":{{\"type\":\"item_completed\",\"text\":\"",
                    record["ordinal"],
                ).into_bytes();
                partial.resize(1512, b'x');
                assert!(serde_json::from_slice::<serde_json::Value>(&partial).is_err());
                malformed.extend(partial);
                malformed.push(b'\n');
            }
            malformed.extend_from_slice(raw);
        } else if record["payload"]["type"] == "reasoning" {
            // A complete newline follows the interrupted JSON, with valid history after it.
            writeln!(
                malformed,
                "{{\"timestamp\":\"2025-01-03T12:00:00Z\",\"ordinal\":{},\"type\":\"response_item\",\"payload\":{{\"type\":\"reasoning\",\"encrypted_content\":\"truncated",
                record["ordinal"]
            )?;
        } else {
            malformed.extend_from_slice(raw);
        }
    }
    fs::write(&selected, &malformed)?;
    let parent_before = fs::read(&parent)?;
    let mut selected_before = malformed;
    for phase in 0..2 {
        let mut app = TestAppServer::builder()
            .with_codex_home(home.path())
            .build_initialized()
            .await?;
        let request = app
            .send_thread_resume_request(ThreadResumeParams {
                thread_id: thread_id.to_string(),
                exclude_turns: true,
                ..Default::default()
            })
            .await?;
        let resumed: ThreadResumeResponse =
            timeout(DEFAULT_READ_TIMEOUT, app.read_response(request)).await??;
        assert_eq!(resumed.thread.history_mode, ThreadHistoryMode::Paginated);
        let visible =
            read_public_history_projection(&mut app, thread_id, DEFAULT_READ_TIMEOUT).await?;
        assert!(
            visible
                .iter()
                .any(|(_, item_id, _)| item_id == "stored-user")
        );
        assert_eq!(fs::read(&parent)?, parent_before);
        assert!(fs::read(&selected)?.starts_with(&selected_before));
        timeout(
            DEFAULT_READ_TIMEOUT,
            app.start_turn_and_wait_for_completion(TurnStartParams {
                thread_id: thread_id.to_string(),
                input: vec![UserInput::Text {
                    text: format!("new user {phase}"),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            }),
        )
        .await??;
        timeout(DEFAULT_READ_TIMEOUT, app.shutdown_gracefully()).await??;
        let requests = response_mock.requests();
        let request = requests.last().expect("resumed model request");
        let users = request.message_input_texts("user");
        assert_eq!(
            users
                .iter()
                .filter(|message| message.as_str() == "inherited user before damage")
                .count(),
            1
        );
        assert_eq!(
            users
                .iter()
                .filter(|message| message.as_str() == "local user before damage")
                .count(),
            1
        );
        assert!(request.body_contains_text("inherited answer before damage"));
        assert!(request.body_contains_text("local answer after damage"));
        assert_eq!(fs::read(&parent)?, parent_before);
        let selected_after = fs::read(&selected)?;
        assert!(selected_after.starts_with(&selected_before));
        selected_before = selected_after;
    }
    Ok(())
}
