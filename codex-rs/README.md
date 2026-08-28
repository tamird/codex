# Codex CLI

[**Codex CLI Documentation**](https://developers.openai.com/codex/cli)

## Resource placement in core and tui

These two crates separate embedded production resources from test data:

| File purpose | Location relative to the crate | How it is used |
| --- | --- | --- |
| Embedded prompts, policies, grammars, and UI assets | `assets/`, grouped by feature | `include_str!` or `include_bytes!`; Bazel includes the directory recursively as compile data |
| TUI animation frames | Existing `frames/` tree | Embedded by `src/frames.rs`; Bazel includes the directory recursively as compile data |
| Test fixtures | `tests/fixtures/` | Read at runtime using `codex_utils_cargo_bin::find_resource!`; Bazel includes the directory recursively as test data |
| Insta snapshots | Existing `snapshots/` directories beside tests | Read by Insta at test runtime |

Adding files in these locations does not require a per-file `BUILD.bazel` edit.
Keep documentation outside `assets/` and `frames/`, and do not embed test fixtures
with `include_str!` or `include_bytes!`: load them at runtime instead. For example,
a new core policy belongs in `core/assets/guardian/`,
while a test response belongs in `core/tests/fixtures/`.

Resources owned by another crate stay with that crate and are accessed through
its Rust API rather than duplicated in a consumer's compile-data list. Other
crates retain their existing layouts; this convention currently applies to
`core` and `tui`.
