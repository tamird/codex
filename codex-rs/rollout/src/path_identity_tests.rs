use super::rollout_paths_match;

#[tokio::test]
async fn selection_comparison_preserves_logical_names_and_rejects_other_files() {
    let home = tempfile::tempdir().expect("home");
    let first = home.path().join("first.jsonl");
    let second = home.path().join("second.jsonl");
    std::fs::write(&first, b"same contents").expect("first");
    std::fs::write(&second, b"same contents").expect("second");
    let compressed = first.with_extension("jsonl.zst");
    std::fs::rename(&first, &compressed).expect("compressed representation");
    assert!(rollout_paths_match(&first, &compressed).await);
    assert!(!rollout_paths_match(&first, &second).await);
    assert!(!rollout_paths_match(&first, &home.path().join("missing.jsonl")).await);
}

#[cfg(unix)]
#[tokio::test]
async fn selection_comparison_resolves_aliases_before_compression_normalization() {
    let home = tempfile::tempdir().expect("home");
    let physical = home.path().join("physical");
    let alias = home.path().join("alias");
    std::fs::create_dir(&physical).expect("physical directory");
    std::os::unix::fs::symlink(&physical, &alias).expect("directory alias");
    let first = physical.join("first.jsonl");
    let aliased = alias.join("first.jsonl");
    std::fs::write(&first, b"history").expect("first");
    assert!(rollout_paths_match(&first, &aliased).await);
    let compressed = first.with_extension("jsonl.zst");
    std::fs::rename(&first, &compressed).expect("compressed representation");
    assert!(rollout_paths_match(&first, &aliased).await);
    assert!(rollout_paths_match(&compressed, &aliased).await);
    assert!(rollout_paths_match(&aliased, &first).await);
    std::fs::remove_file(compressed).expect("remove representation");
    assert!(!rollout_paths_match(&first, &aliased).await);
}
