use super::Executable;

#[test]
fn ownership_preserves_standalone_updates_without_adopting_external_installs() {
    let standalone = Executable::Standalone("current/codex".into());
    let release = Executable::Standalone("releases/new/codex".into());
    let external = Executable::External("external/codex".into());
    for recorded in [None, Some(&release)] {
        standalone
            .ensure_matches(recorded)
            .expect("standalone ownership");
    }
    external
        .ensure_matches(Some(&external))
        .expect("same external executable");
    for (selected, recorded) in [(&standalone, &external), (&external, &standalone)] {
        let error = selected
            .ensure_matches(Some(recorded))
            .expect_err("installation mismatch");
        assert!(error.to_string().contains("different Codex installation"));
    }
}
