use super::*;
use pretty_assertions::assert_eq;

#[test]
fn oversized_resource_page_is_valid_json_within_budget_and_has_cursor() {
    let contents = "resource body ".repeat(32);
    let max_response_bytes = 256;

    let response = page_response(
        "skill://demo/SKILL.md",
        &contents,
        /*skill_root*/ None,
        /*start*/ 0,
        max_response_bytes,
    )
    .expect("oversized resource should be paginated");
    let serialized = serde_json::to_vec(&response).expect("serialize read response");
    let decoded: serde_json::Value =
        serde_json::from_slice(&serialized).expect("read response should remain valid JSON");

    assert!(serialized.len() <= max_response_bytes);
    assert_eq!(decoded["resource"], "skill://demo/SKILL.md");
    assert!(decoded["next_cursor"].is_string());
    assert!(response.contents.len() < contents.len());

    let next_start =
        parse_pagination_cursor(response.next_cursor.as_deref(), &contents, "skills.read")
            .expect("parse read cursor");
    let next_response = page_response(
        "skill://demo/SKILL.md",
        &contents,
        /*skill_root*/ None,
        next_start,
        max_response_bytes,
    )
    .expect("read next resource page");
    let next_serialized = serde_json::to_vec(&next_response).expect("serialize next read page");
    let _: serde_json::Value =
        serde_json::from_slice(&next_serialized).expect("next read page should be valid JSON");
    assert!(next_serialized.len() <= max_response_bytes);
    assert!(!next_response.contents.is_empty());
}

#[test]
fn read_page_rejects_non_progress_cursor() {
    let contents = "x".repeat(128);
    let empty_page = ReadResponse {
        resource: "skill://demo/SKILL.md".to_string(),
        contents: String::new(),
        skill_root: None,
        next_cursor: Some(pagination_cursor(&contents, /*offset*/ 0)),
    };
    let wrapper_only_budget = serialized_len(&empty_page).expect("empty page size");

    let error = page_response(
        "skill://demo/SKILL.md",
        &contents,
        /*skill_root*/ None,
        /*start*/ 0,
        wrapper_only_budget,
    )
    .expect_err("a page must advance its cursor");

    assert!(matches!(error, FunctionCallError::RespondToModel(_)));
}
