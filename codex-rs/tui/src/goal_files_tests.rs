use super::objective_file_reference;
use codex_app_server_client::AppServerPath;
use pretty_assertions::assert_eq;

#[test]
fn objective_file_reference_rejects_oversized_escaped_path() {
    let path = AppServerPath::from_app_server(format!(
        "/tmp/{}/goal-objective.md",
        "&".repeat(/*n*/ 1_200)
    ));

    let error = objective_file_reference(&path)
        .expect_err("a short path can still exceed the escaped objective limit");

    assert_eq!(
        error.to_string(),
        "Goal objective file reference is too large"
    );
}
