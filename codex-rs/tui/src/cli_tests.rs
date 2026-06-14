use super::*;
use clap::CommandFactory;

#[test]
fn internal_side_session_id_is_hidden_and_validated() {
    let id = "67e55044-10b1-426f-9247-bb680e5fe0c8";
    let cli = Cli::try_parse_from(["codex", "--internal-side-session-id", id])
        .expect("hidden side flag should parse");
    assert_eq!(cli.side_session_id.as_deref(), Some(id));
    assert!(Cli::try_parse_from(["codex", "--internal-side-session-id", "not-a-uuid",]).is_err());
    assert!(Cli::try_parse_from(["codex", "--internal-side-session-id", id, "question"]).is_err());
    assert!(
        Cli::try_parse_from([
            "codex",
            "--internal-side-session-id",
            id,
            "--image",
            "/tmp/image.png",
        ])
        .is_err()
    );

    let help = Cli::command().render_long_help().to_string();
    assert!(!help.contains("internal-side-session-id"));
}
