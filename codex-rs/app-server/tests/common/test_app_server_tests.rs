use std::path::Path;

use pretty_assertions::assert_eq;

use super::DISABLE_PLUGIN_STARTUP_TASKS_ARG;
use super::TestAppServer;
use super::command_arguments;

#[test]
fn program_prefix_precedes_default_and_appended_arguments() {
    let builder = TestAppServer::builder()
        .with_program_and_prefix_args(Path::new("/candidate/codex"), &["app-server"])
        .with_args(&["--extra"]);

    let args = command_arguments(&builder.program_prefix_args, &builder.args);
    assert_eq!(
        args,
        vec!["app-server", DISABLE_PLUGIN_STARTUP_TASKS_ARG, "--extra"]
    );
}

#[test]
fn standalone_program_clears_an_existing_prefix() {
    let builder = TestAppServer::builder()
        .with_program_and_prefix_args(Path::new("/candidate/codex"), &["app-server"])
        .with_program(Path::new("/candidate/codex-app-server"));

    assert_eq!(builder.program_prefix_args, Vec::<String>::new());
    assert_eq!(
        builder.args,
        vec![DISABLE_PLUGIN_STARTUP_TASKS_ARG.to_string()]
    );
}
