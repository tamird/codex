//! Strict resolution and materialization of rollout-reference graphs.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::Metadata;
use std::io;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use codex_history::RolloutItem;
use codex_history::RolloutLine;
use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;

use crate::ARCHIVED_SESSIONS_SUBDIR;
use crate::ModelContextScan;
use crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR;
use crate::SESSIONS_SUBDIR;
use crate::compression;
use crate::recorder::RolloutRecorder;

/// Bounds cross-thread and nth-user-message fork nesting during rollout expansion.
pub const MAX_ROLLOUT_REFERENCE_DEPTH: usize = 256;

/// Number of physical same-thread rollout segments available to normal Frodex reads.
///
/// Normal reads stop before opening an older sixth segment. Explicit complete materialization and
/// model-context recovery retain their separate semantics.
pub const FRODEX_RECENT_ROLLOUT_SEGMENTS: usize = 5;

const FRODEX_RECENT_ROLLOUT_REFERENCES: usize = FRODEX_RECENT_ROLLOUT_SEGMENTS - 1;

/// Composes inherited and local compaction filters in their application order.
///
/// An outer reference constrains every segment below it. A nested reference adds its own
/// constraints for the nested segment only, so inherited entries remain first and duplicate text
/// is retained only at its first occurrence. `Some(Vec::new())` remains distinct from `None`
/// because it records that a reference explicitly carried an empty filter list.
pub fn compose_compacted_replacement_history_filter_texts(
    inherited: Option<&[String]>,
    local: Option<&[String]>,
) -> Option<Vec<String>> {
    if inherited.is_none() && local.is_none() {
        return None;
    }

    let mut composed = Vec::new();
    for text in inherited
        .unwrap_or_default()
        .iter()
        .chain(local.unwrap_or_default())
    {
        if !composed.contains(text) {
            composed.push(text.clone());
        }
    }
    Some(composed)
}

/// Selects whether reference expansion follows the complete graph or only the recent segment
/// window used for replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaterializationPolicy {
    Complete,
    RecentSegments,
    OrdinaryReferenceLimit(usize),
}

/// Tracks structural recursion and recent-segment depth independently while expanding references.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExpansionCursor {
    graph_depth: usize,
    ordinary_reference_depth: usize,
    current_thread_id: ThreadId,
}

/// Keeps cycle detection and optional immutable reuse scoped to a single reference expansion.
struct ExpansionState<'a> {
    /// Emit source SessionMeta transitions for callers reducing mixed persistence formats.
    retain_source_metadata: bool,
    /// Reference identities currently being expanded for cycle detection.
    active_segments: HashSet<ReferenceIdentity>,
    /// Physical native predecessors currently being expanded, including mixed-format cycles.
    active_history_bases: HashSet<RolloutId>,
    /// Immutable records reused only during this request's expansion attempts.
    cache: Option<&'a mut ImmutableRolloutCache>,
}

/// Owns one referenced segment while preserving chronological replay without call-stack growth.
struct ExpansionFrame {
    /// Restores the containing segment's format after a predecessor finishes expanding.
    session_meta: RolloutLine,
    lines: std::vec::IntoIter<RolloutLine>,
    materialized: Vec<RolloutLine>,
    cursor: ExpansionCursor,
    inherited_filter_texts: Option<Arc<[String]>>,
    inbound_reference: Option<InboundReference>,
    /// Expand the native predecessor before this segment's local records.
    history_base: Option<HistoryPosition>,
    /// Remove this physical identity from cycle detection when the frame completes.
    inbound_history_base: Option<RolloutId>,
}

/// Applies the source reference's filter and fork cutoff after its segment finishes expanding.
struct InboundReference {
    identity: ReferenceIdentity,
    filter_texts: Option<Arc<[String]>>,
    nth_user_message: Option<usize>,
}

impl MaterializationPolicy {
    fn ordinary_reference_limit(
        self,
        reference: &RolloutReferenceItem,
        current_thread_id: ThreadId,
    ) -> Option<usize> {
        if is_fork_boundary_reference(reference, current_thread_id) {
            return None;
        }
        match self {
            Self::Complete => None,
            Self::RecentSegments => Some(FRODEX_RECENT_ROLLOUT_REFERENCES),
            Self::OrdinaryReferenceLimit(limit) => Some(limit),
        }
    }
}

/// A bounded logical rollout prefix and whether an older ordinary reference was omitted.
pub struct BoundedRolloutLines {
    /// Materialized lines in logical rollout order.
    pub lines: Vec<RolloutLine>,
    /// True when increasing the ordinary-reference limit can reveal older history.
    pub has_older_reference: bool,
}

/// Reuses validated immutable rollout records while one caller expands its requested prefix.
pub struct BoundedRolloutMaterializer<'a> {
    codex_home: &'a Path,
    rollout_path: &'a Path,
    cache: ImmutableRolloutCache,
    /// Migration comparisons need each physical segment's persistence format.
    retain_source_metadata: bool,
}

impl<'a> BoundedRolloutMaterializer<'a> {
    /// Creates a request-local materializer that never retains records after it is dropped.
    pub fn new(codex_home: &'a Path, rollout_path: &'a Path) -> Self {
        Self {
            codex_home,
            rollout_path,
            cache: ImmutableRolloutCache::default(),
            retain_source_metadata: false,
        }
    }

    /// Includes SessionMeta transitions between expanded segments. These markers describe source
    /// formats, not additional physical records, and must not advance replay item indices.
    pub fn retaining_source_metadata(mut self) -> Self {
        self.retain_source_metadata = true;
        self
    }

    /// Expands from the active rollout while reusing previously validated immutable records.
    pub async fn materialize(
        &mut self,
        ordinary_reference_limit: usize,
    ) -> io::Result<BoundedRolloutLines> {
        let lines = load_active_rollout_lines(self.rollout_path).await?;
        self.materialize_from(lines, ordinary_reference_limit).await
    }

    /// Expands caller-decoded active records while retaining strict ancestor validation.
    ///
    /// Interactive readers can use the recorder's compatibility decoding for a damaged mutable
    /// root without relaxing validation for immutable references or snapshot publication.
    pub async fn materialize_from(
        &mut self,
        lines: Vec<RolloutLine>,
        ordinary_reference_limit: usize,
    ) -> io::Result<BoundedRolloutLines> {
        let mut has_older_reference = false;
        let lines = materialize_rollout_lines_from_with_cache(
            self.codex_home,
            lines,
            MaterializationPolicy::OrdinaryReferenceLimit(ordinary_reference_limit),
            &mut has_older_reference,
            Some(&mut self.cache),
            self.retain_source_metadata,
        )
        .await?;
        Ok(BoundedRolloutLines {
            lines,
            has_older_reference,
        })
    }
}

const MAX_REQUEST_CACHE_BYTES: usize = 8 * 1024 * 1024;
const MAX_REQUEST_CACHE_SEGMENTS: usize = 128;

/// Fingerprints the physical immutable file rather than trusting its recorded reference identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ImmutableRolloutFingerprint {
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
}

impl ImmutableRolloutFingerprint {
    fn from_metadata(metadata: &Metadata) -> io::Result<Self> {
        Ok(Self {
            len: metadata.len(),
            modified: metadata.modified()?,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            changed_seconds: metadata.ctime(),
            #[cfg(unix)]
            changed_nanoseconds: metadata.ctime_nsec(),
        })
    }
}

/// Stores original records so fork filters and cutoffs are reapplied for every expansion.
struct CachedImmutableRollout {
    path: PathBuf,
    fingerprint: ImmutableRolloutFingerprint,
    lines: Vec<RolloutLine>,
}

/// Bounds validated immutable rollout records to one request by their on-disk source sizes.
#[derive(Default)]
struct ImmutableRolloutCache {
    entries: HashMap<ReferenceIdentity, CachedImmutableRollout>,
    /// On-disk source bytes; deserialized records can occupy additional memory.
    source_bytes: usize,
    /// Partial ancestry cannot safely apply an inherited fork cutoff or replacement filter.
    saw_fork_boundary_constraints: bool,
    /// Cross-thread ancestry remains strict even when old same-thread corruption is tolerated.
    saw_cross_thread_reference: bool,
    #[cfg(test)]
    hits: usize,
    #[cfg(test)]
    full_rollout_reads: usize,
}

impl ImmutableRolloutCache {
    async fn load(
        &mut self,
        codex_home: &Path,
        path: &Path,
        identity: ReferenceIdentity,
        requested_path: &Path,
    ) -> io::Result<Vec<RolloutLine>> {
        let rotated_root = tokio::fs::canonicalize(codex_home)
            .await?
            .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR);
        let requested_canonical_path = tokio::fs::canonicalize(requested_path).await.ok();
        let cacheable_relative_path =
            path.strip_prefix(&rotated_root)
                .ok()
                .is_some_and(|relative| {
                    relative
                        .components()
                        .all(|component| matches!(component, Component::Normal(_)))
                });
        if requested_canonical_path.as_deref() != Some(path)
            || !cacheable_relative_path
            || compression::plain_rollout_path(path) != path
            || tokio::fs::symlink_metadata(requested_path)
                .await?
                .file_type()
                .is_symlink()
        {
            #[cfg(test)]
            {
                self.full_rollout_reads += 1;
            }
            return load_strict_rollout_lines(path).await;
        }

        let before = ImmutableRolloutFingerprint::from_metadata(&tokio::fs::metadata(path).await?)?;
        if let Some(entry) = self.entries.get(&identity) {
            if entry.path == path && entry.fingerprint == before {
                #[cfg(test)]
                {
                    self.hits += 1;
                }
                return Ok(entry.lines.clone());
            }

            if let Some(removed) = self.entries.remove(&identity) {
                self.source_bytes = self
                    .source_bytes
                    .saturating_sub(usize::try_from(removed.fingerprint.len).unwrap_or(usize::MAX));
            }
        }

        #[cfg(test)]
        {
            self.full_rollout_reads += 1;
        }
        let lines = load_strict_rollout_lines(path).await?;
        let after = ImmutableRolloutFingerprint::from_metadata(&tokio::fs::metadata(path).await?)?;
        if before != after {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "immutable rollout at {} changed while being read",
                    path.display()
                ),
            ));
        }

        let source_bytes = usize::try_from(after.len).unwrap_or(usize::MAX);
        if self.entries.len() < MAX_REQUEST_CACHE_SEGMENTS
            && source_bytes <= MAX_REQUEST_CACHE_BYTES.saturating_sub(self.source_bytes)
        {
            self.source_bytes += source_bytes;
            self.entries.insert(
                identity,
                CachedImmutableRollout {
                    path: path.to_path_buf(),
                    fingerprint: after,
                    lines: lines.clone(),
                },
            );
        }

        Ok(lines)
    }
}

/// The immutable identity recorded by a rollout reference.
///
/// Rollouts created before segment IDs were introduced use the single `initial` segment for a
/// thread. `LegacyInitial` preserves that identity without treating a missing segment ID as a
/// wildcard for newer segments of the same thread.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ReferenceIdentity {
    Segment {
        thread_id: ThreadId,
        rollout_id: Option<RolloutId>,
        segment_id: SegmentId,
    },
    LegacyInitial {
        thread_id: ThreadId,
        rollout_id: Option<RolloutId>,
    },
}

impl ReferenceIdentity {
    fn thread_id(self) -> ThreadId {
        match self {
            Self::Segment { thread_id, .. } | Self::LegacyInitial { thread_id, .. } => thread_id,
        }
    }

    fn rollout_id(self) -> Option<RolloutId> {
        match self {
            Self::Segment { rollout_id, .. } | Self::LegacyInitial { rollout_id, .. } => rollout_id,
        }
    }

    fn segment_key(self) -> String {
        match self {
            Self::Segment { segment_id, .. } => segment_id.to_string(),
            Self::LegacyInitial { .. } => "initial".to_string(),
        }
    }

    fn description(self) -> String {
        format!("{}/{}", self.thread_id(), self.segment_key())
    }
}

/// Resolves a reference only when a candidate rollout has the recorded immutable identity.
pub async fn resolve_rollout_reference_path(
    codex_home: &Path,
    reference: &RolloutReferenceItem,
) -> io::Result<PathBuf> {
    let identity = reference_identity(reference)?;

    if let Some(path) =
        validated_candidate(codex_home, reference.rollout_path.as_path(), identity).await?
    {
        return Ok(path);
    }

    let expected_file_name = matches!(identity, ReferenceIdentity::LegacyInitial { .. })
        .then(|| compression::plain_rollout_path(reference.rollout_path.as_path()))
        .and_then(|path| path.file_name().map(ToOwned::to_owned));
    let mut legacy_candidates = Vec::new();
    for root in [ROTATED_ROLLOUT_SEGMENTS_SUBDIR, ARCHIVED_SESSIONS_SUBDIR] {
        let directory = codex_home
            .join(root)
            .join(identity.thread_id().to_string())
            .join(identity.segment_key());
        if let Some(path) = find_valid_candidate_in_directory(
            codex_home,
            directory.as_path(),
            identity,
            expected_file_name.as_deref(),
        )
        .await?
        {
            if matches!(identity, ReferenceIdentity::LegacyInitial { .. }) {
                legacy_candidates.push(path);
            } else {
                return Ok(path);
            }
        }
    }
    match legacy_candidates.as_slice() {
        [path] => return Ok(path.clone()),
        [_, _, ..] => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "rollout reference {} is ambiguous across immutable segment directories",
                    identity.description()
                ),
            ));
        }
        [] => {}
    }

    // Older fork references recorded an absolute rollout path but no relocation timestamp. The
    // filename still identifies the physical rollout. Resolve that rollout inside the current
    // CODEX_HOME, then require the referenced SessionMeta identity before accepting it.
    if let Some(rollout_id) = identity
        .rollout_id()
        .or_else(|| crate::rollout_id_from_path(reference.rollout_path.as_path()))
        && let Some(path) = crate::find_rollout_path_by_rollout_id(codex_home, rollout_id).await?
        && let Some(path) = validated_candidate(codex_home, path.as_path(), identity).await?
    {
        return Ok(path);
    }

    if let ReferenceIdentity::Segment {
        thread_id,
        segment_id: _,
        ..
    } = identity
        && let Some(rollout_timestamp) = reference.rollout_timestamp.as_deref()
    {
        let file_name = match reference.rollout_id {
            Some(rollout_id) if rollout_id != thread_id => {
                format!("rollout-{rollout_timestamp}-{thread_id}_{rollout_id}.jsonl")
            }
            Some(_) | None => format!("rollout-{rollout_timestamp}-{thread_id}.jsonl"),
        };
        if let Some(active_path) =
            rollout_path_for_timestamp_file(codex_home, rollout_timestamp, file_name.as_str())
            && let Some(path) =
                validated_candidate(codex_home, active_path.as_path(), identity).await?
        {
            return Ok(path);
        }
        let archived_path = codex_home.join(ARCHIVED_SESSIONS_SUBDIR).join(file_name);
        if let Some(path) =
            validated_candidate(codex_home, archived_path.as_path(), identity).await?
        {
            return Ok(path);
        }
    }

    let identity_description = identity.description();
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "rollout reference {identity_description} could not be resolved from {}",
            reference.rollout_path.display()
        ),
    ))
}

/// Expands a rollout graph while retaining every physical line's lineage ordinal.
pub async fn materialize_rollout_lines(
    codex_home: &Path,
    rollout_path: &Path,
) -> io::Result<Vec<RolloutLine>> {
    let lines = load_active_rollout_lines(rollout_path).await?;
    let mut has_older_reference = false;
    materialize_rollout_lines_from_with_policy(
        codex_home,
        lines,
        MaterializationPolicy::Complete,
        &mut has_older_reference,
    )
    .await
}

/// Expands already loaded root lines without rewriting the root `SessionMeta`.
pub async fn materialize_rollout_lines_from(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
) -> io::Result<Vec<RolloutLine>> {
    let mut has_older_reference = false;
    materialize_rollout_lines_from_with_policy(
        codex_home,
        lines,
        MaterializationPolicy::Complete,
        &mut has_older_reference,
    )
    .await
}

/// Reconstructs model context from an immutable fork prefix without replaying obsolete history.
///
/// Ordinary predecessor references are expanded only until a certified segment-state checkpoint
/// is available. Unmarked compactions still require predecessor traversal for sticky settings and
/// token state, even though the returned model-visible history remains bounded.
pub async fn materialize_model_context_rollout_items_from(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
) -> io::Result<Vec<RolloutItem>> {
    let session_meta = canonical_session_meta(&lines)?.clone();
    // A checkpoint in the supplied root is independent of its predecessor. Inspect it before
    // resolving references so a missing predecessor cannot prevent a certified cold resume.
    let mut root_scan = ModelContextScan::default();
    for line in lines.iter().rev() {
        if matches!(
            line.item,
            RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
        ) {
            continue;
        }
        if root_scan.push(line.item.clone()).is_complete() {
            if root_scan.completed_at_segment_checkpoint() {
                return Ok(root_scan.finish(session_meta));
            }
            break;
        }
    }
    let mut cache = ImmutableRolloutCache::default();
    let mut ordinary_reference_limit = 0_usize;

    loop {
        let mut has_older_reference = false;
        let materialized = materialize_rollout_lines_from_with_cache(
            codex_home,
            lines.clone(),
            MaterializationPolicy::OrdinaryReferenceLimit(ordinary_reference_limit),
            &mut has_older_reference,
            Some(&mut cache),
            /*retain_source_metadata*/ false,
        )
        .await?;
        let mut scan = ModelContextScan::default();

        for line in materialized.iter().rev() {
            if matches!(
                line.item,
                RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
            ) {
                continue;
            }
            if scan.push(line.item.clone()).is_complete()
                && (!cache.saw_fork_boundary_constraints || !has_older_reference)
            {
                return Ok(scan.finish(session_meta));
            }
        }

        let mut items = scan.finish(session_meta.clone());
        if !matches!(items.first(), Some(RolloutItem::SessionMeta(_))) {
            items.insert(0, RolloutItem::SessionMeta(session_meta.clone()));
        }
        if !has_older_reference {
            return Ok(items);
        }
        ordinary_reference_limit = ordinary_reference_limit.saturating_mul(2).max(/*other*/ 1);
    }
}

/// Expands a rollout graph for user-visible replay while retaining physical lineage ordinals.
///
/// Direct fork-boundary references do not consume the segment window. Ordinary compaction
/// references nested beneath the fork boundary do, which keeps a fork's inherited prefix equal to
/// the source thread's bounded replay prefix.
pub async fn materialize_recent_rollout_lines(
    codex_home: &Path,
    rollout_path: &Path,
) -> io::Result<Vec<RolloutLine>> {
    let lines = load_active_rollout_lines(rollout_path).await?;
    materialize_recent_rollout_lines_from(codex_home, lines).await
}

/// Expands already loaded root lines using the recent-segment replay policy.
pub async fn materialize_recent_rollout_lines_from(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
) -> io::Result<Vec<RolloutLine>> {
    let mut has_older_reference = false;
    materialize_rollout_lines_from_with_policy(
        codex_home,
        lines,
        MaterializationPolicy::RecentSegments,
        &mut has_older_reference,
    )
    .await
}

/// Expands at most `ordinary_reference_limit` same-thread predecessor references.
///
/// Direct cross-thread fork references do not consume the limit. Callers can increase the limit
/// until they have enough coherent history while avoiding complete expansion of a long segment
/// chain.
pub async fn materialize_bounded_rollout_lines(
    codex_home: &Path,
    rollout_path: &Path,
    ordinary_reference_limit: usize,
) -> io::Result<BoundedRolloutLines> {
    let lines = load_active_rollout_lines(rollout_path).await?;
    materialize_bounded_rollout_lines_from(codex_home, lines, ordinary_reference_limit).await
}

async fn materialize_bounded_rollout_lines_from(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
    ordinary_reference_limit: usize,
) -> io::Result<BoundedRolloutLines> {
    let mut has_older_reference = false;
    let lines = materialize_rollout_lines_from_with_policy(
        codex_home,
        lines,
        MaterializationPolicy::OrdinaryReferenceLimit(ordinary_reference_limit),
        &mut has_older_reference,
    )
    .await?;
    Ok(BoundedRolloutLines {
        lines,
        has_older_reference,
    })
}

async fn materialize_rollout_lines_from_with_policy(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
    policy: MaterializationPolicy,
    has_older_reference: &mut bool,
) -> io::Result<Vec<RolloutLine>> {
    materialize_rollout_lines_from_with_cache(
        codex_home,
        lines,
        policy,
        has_older_reference,
        /*cache*/ None,
        /*retain_source_metadata*/ false,
    )
    .await
}

async fn materialize_rollout_lines_from_with_cache(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
    policy: MaterializationPolicy,
    has_older_reference: &mut bool,
    cache: Option<&mut ImmutableRolloutCache>,
    retain_source_metadata: bool,
) -> io::Result<Vec<RolloutLine>> {
    let root_thread_id = canonical_session_meta(&lines)?.meta.id;
    let mut state = ExpansionState {
        retain_source_metadata,
        active_segments: HashSet::new(),
        active_history_bases: HashSet::new(),
        cache,
    };

    let mut materialized = Vec::with_capacity(lines.len());
    materialized.push(lines[0].clone());
    materialized.extend(
        expand_lines(
            codex_home,
            lines,
            &mut state,
            ExpansionCursor {
                graph_depth: 0,
                ordinary_reference_depth: 0,
                current_thread_id: root_thread_id,
            },
            /*inherited_filter_texts*/ None,
            policy,
            has_older_reference,
        )
        .await?,
    );
    Ok(materialized)
}

/// Resolves a physical rollout ID and reads its exact decoded byte and ordinal prefix.
///
/// Returns the resolved path and direct records, including the canonical first `SessionMeta`.
/// Callers must check its logical thread ID before treating it as a same-thread predecessor.
/// This does not expand ancestry or rewrite compressed storage.
pub async fn load_history_base_prefix(
    codex_home: &Path,
    position: HistoryPosition,
) -> io::Result<(PathBuf, Vec<RolloutLine>)> {
    let path = crate::find_rollout_path_by_rollout_id(codex_home, position.thread_id)
        .await?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("history_base rollout {} was not found", position.thread_id),
            )
        })?;
    let path = compression::existing_rollout_path(&path)
        .await
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("history_base rollout {} disappeared", path.display()),
            )
        })?;
    // Reading ancestry must not decompress it in place or remove a compressed source that
    // a migration journal still fingerprints. Offsets address the uncompressed JSONL.
    let source_path = path.clone();
    let bytes = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
        let input = std::fs::File::open(&source_path)?;
        let mut bytes = Vec::new();
        if compression::plain_rollout_path(&source_path) != source_path {
            let mut reader =
                zstd::stream::read::Decoder::new(input)?.take(position.end_byte_offset);
            reader.read_to_end(&mut bytes)?;
        } else {
            input
                .take(position.end_byte_offset)
                .read_to_end(&mut bytes)?;
        }
        Ok(bytes)
    })
    .await
    .map_err(io::Error::other)??;
    let end = usize::try_from(position.end_byte_offset)
        .map_err(|_| io::Error::other("history_base byte offset exceeds addressable memory"))?;
    let prefix = bytes.get(..end).ok_or_else(|| {
        io::Error::other(format!(
            "history_base byte offset exceeds rollout {}",
            path.display()
        ))
    })?;
    if !prefix.ends_with(b"\n") {
        return Err(io::Error::other(format!(
            "history_base byte offset is not a record boundary in {}",
            path.display()
        )));
    }
    let mut lines = Vec::new();
    for encoded in prefix
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
    {
        let value = serde_json::from_slice(encoded)?;
        lines.push(crate::decode_rollout_line(value)?);
    }
    canonical_session_meta(&lines)?;
    let actual_end = lines
        .last()
        .and_then(|line| line.ordinal)
        .and_then(|ordinal| ordinal.checked_add(1));
    if actual_end != Some(position.end_ordinal_exclusive) {
        return Err(io::Error::other(format!(
            "history_base ordinal boundary does not match rollout {}",
            path.display()
        )));
    }
    Ok((path, lines))
}

/// Expands a rollout graph and discards only physical line metadata.
pub async fn materialize_rollout_items(
    codex_home: &Path,
    rollout_path: &Path,
) -> io::Result<Vec<RolloutItem>> {
    Ok(materialize_rollout_lines(codex_home, rollout_path)
        .await?
        .into_iter()
        .map(|line| line.item)
        .collect())
}

/// Expands a rollout graph for user-visible replay and discards physical line metadata.
pub async fn materialize_recent_rollout_items(
    codex_home: &Path,
    rollout_path: &Path,
) -> io::Result<Vec<RolloutItem>> {
    Ok(materialize_recent_rollout_lines(codex_home, rollout_path)
        .await?
        .into_iter()
        .map(|line| line.item)
        .collect())
}

async fn expand_lines(
    codex_home: &Path,
    lines: Vec<RolloutLine>,
    state: &mut ExpansionState<'_>,
    cursor: ExpansionCursor,
    inherited_filter_texts: Option<&[String]>,
    policy: MaterializationPolicy,
    has_older_reference: &mut bool,
) -> io::Result<Vec<RolloutLine>> {
    let metadata = canonical_session_meta(&lines)?;
    let history_base = (metadata.meta.history_mode == ThreadHistoryMode::Paginated)
        .then_some(metadata.meta.history_base)
        .flatten();
    let mut frames = vec![ExpansionFrame {
        session_meta: lines[0].clone(),
        materialized: Vec::with_capacity(lines.len()),
        lines: lines.into_iter(),
        cursor,
        inherited_filter_texts: inherited_filter_texts
            .map(|filter_texts| Arc::<[String]>::from(filter_texts.to_vec())),
        inbound_reference: None,
        history_base,
        inbound_history_base: None,
    }];

    while let Some(frame) = frames.last_mut() {
        if let Some(position) = frame.history_base.take() {
            let ordinary_limit = match policy {
                MaterializationPolicy::Complete => None,
                MaterializationPolicy::RecentSegments => Some(FRODEX_RECENT_ROLLOUT_REFERENCES),
                MaterializationPolicy::OrdinaryReferenceLimit(limit) => Some(limit),
            };
            if ordinary_limit.is_some_and(|limit| frame.cursor.ordinary_reference_depth >= limit) {
                *has_older_reference = true;
                continue;
            }
            if !state.active_history_bases.insert(position.thread_id) {
                return Err(io::Error::other(format!(
                    "history_base cycle detected at rollout {}",
                    position.thread_id
                )));
            }
            let (_, predecessor) = load_history_base_prefix(codex_home, position).await?;
            let metadata = canonical_session_meta(&predecessor)?;
            let fork_boundary = metadata.meta.id != frame.cursor.current_thread_id;
            if fork_boundary && frame.cursor.graph_depth >= MAX_ROLLOUT_REFERENCE_DEPTH {
                return Err(io::Error::other(format!(
                    "history_base graph exceeds maximum depth of {MAX_ROLLOUT_REFERENCE_DEPTH}"
                )));
            }
            if let Some(cache) = state.cache.as_deref_mut() {
                cache.saw_cross_thread_reference |= fork_boundary;
            }
            let predecessor_frame = ExpansionFrame {
                session_meta: predecessor[0].clone(),
                materialized: if state.retain_source_metadata {
                    vec![predecessor[0].clone()]
                } else {
                    Vec::with_capacity(predecessor.len())
                },
                cursor: ExpansionCursor {
                    graph_depth: frame.cursor.graph_depth + usize::from(fork_boundary),
                    ordinary_reference_depth: frame.cursor.ordinary_reference_depth + 1,
                    current_thread_id: metadata.meta.id,
                },
                history_base: (metadata.meta.history_mode == ThreadHistoryMode::Paginated)
                    .then_some(metadata.meta.history_base)
                    .flatten(),
                lines: predecessor.into_iter(),
                inherited_filter_texts: frame.inherited_filter_texts.clone(),
                inbound_reference: None,
                inbound_history_base: Some(position.thread_id),
            };
            frames.push(predecessor_frame);
            continue;
        }
        let Some(mut line) = frame.lines.next() else {
            let Some(mut completed) = frames.pop() else {
                return Err(io::Error::other(
                    "rollout reference expansion stack is empty",
                ));
            };
            if let Some(rollout_id) = completed.inbound_history_base {
                state.active_history_bases.remove(&rollout_id);
                let Some(parent) = frames.last_mut() else {
                    return Err(io::Error::other(
                        "history_base expansion frame has no parent",
                    ));
                };
                // A native predecessor is processed before any local records. Transfer its
                // buffer rather than moving the whole prefix once per ancestor.
                debug_assert!(
                    parent.materialized.is_empty()
                        || (state.retain_source_metadata && parent.materialized.len() == 1)
                );
                parent.materialized = completed.materialized;
                if state.retain_source_metadata {
                    parent.materialized.push(parent.session_meta.clone());
                }
                continue;
            }
            if let Some(inbound) = completed.inbound_reference {
                state.active_segments.remove(&inbound.identity);
                if let Some(filter_texts) = inbound.filter_texts.as_deref() {
                    apply_filter(&mut completed.materialized, filter_texts);
                }
                if let Some(nth_user_message) = inbound.nth_user_message {
                    truncate_before_nth_user_message(&mut completed.materialized, nth_user_message);
                }
                let Some(parent) = frames.last_mut() else {
                    return Err(io::Error::other(
                        "referenced rollout expansion frame has no parent",
                    ));
                };
                parent.materialized.extend(completed.materialized);
                if state.retain_source_metadata {
                    parent.materialized.push(parent.session_meta.clone());
                }
                continue;
            }
            return Ok(completed.materialized);
        };

        let RolloutItem::RolloutReference(reference) = &line.item else {
            if !matches!(line.item, RolloutItem::SessionMeta(_))
                && frame
                    .inherited_filter_texts
                    .as_deref()
                    .is_none_or(|filter_texts| filter_line(&mut line, filter_texts))
            {
                frame.materialized.push(line);
            }
            continue;
        };

        let reference = reference.clone();
        let cursor = frame.cursor;
        if let Some(cache) = state.cache.as_deref_mut() {
            cache.saw_fork_boundary_constraints |= reference.nth_user_message.is_some()
                || reference
                    .compacted_replacement_history_filter_texts
                    .is_some();
            cache.saw_cross_thread_reference |= reference
                .thread_id
                .is_some_and(|thread_id| thread_id != cursor.current_thread_id);
        }
        if policy
            .ordinary_reference_limit(&reference, cursor.current_thread_id)
            .is_some_and(|limit| cursor.ordinary_reference_depth >= limit)
        {
            *has_older_reference = true;
            continue;
        }

        let fork_boundary = is_fork_boundary_reference(&reference, cursor.current_thread_id);
        if fork_boundary && cursor.graph_depth >= MAX_ROLLOUT_REFERENCE_DEPTH {
            return Err(io::Error::other(format!(
                "rollout reference graph exceeds maximum depth of \
                 {MAX_ROLLOUT_REFERENCE_DEPTH}"
            )));
        }

        let identity = reference_identity(&reference)?;
        if !state.active_segments.insert(identity) {
            let identity_description = identity.description();
            return Err(io::Error::other(format!(
                "rollout reference cycle detected at {identity_description}"
            )));
        }

        let path = resolve_rollout_reference_path(codex_home, &reference).await?;
        let referenced_lines = match state.cache.as_deref_mut() {
            Some(cache) => {
                cache
                    .load(
                        codex_home,
                        path.as_path(),
                        identity,
                        reference.rollout_path.as_path(),
                    )
                    .await?
            }
            None => load_strict_rollout_lines(path.as_path()).await?,
        };
        let referenced_meta = canonical_session_meta(&referenced_lines)?;
        validate_identity(referenced_meta, identity, path.as_path())?;
        let referenced_thread_id = referenced_meta.meta.id;
        let history_base = (referenced_meta.meta.history_mode == ThreadHistoryMode::Paginated)
            .then_some(referenced_meta.meta.history_base)
            .flatten();
        let filter_texts = compose_compacted_replacement_history_filter_texts(
            frame.inherited_filter_texts.as_deref(),
            reference
                .compacted_replacement_history_filter_texts
                .as_deref(),
        )
        .map(Arc::<[String]>::from);

        frames.push(ExpansionFrame {
            session_meta: referenced_lines[0].clone(),
            materialized: if state.retain_source_metadata {
                vec![referenced_lines[0].clone()]
            } else {
                Vec::with_capacity(referenced_lines.len().saturating_sub(1))
            },
            lines: referenced_lines
                .into_iter()
                .skip(1)
                .collect::<Vec<_>>()
                .into_iter(),
            cursor: ExpansionCursor {
                graph_depth: cursor.graph_depth + usize::from(fork_boundary),
                ordinary_reference_depth: cursor.ordinary_reference_depth
                    + usize::from(!fork_boundary),
                current_thread_id: referenced_thread_id,
            },
            inherited_filter_texts: filter_texts.clone(),
            inbound_reference: Some(InboundReference {
                identity,
                filter_texts,
                nth_user_message: reference.nth_user_message,
            }),
            history_base,
            inbound_history_base: None,
        });
    }

    Err(io::Error::other(
        "rollout reference expansion stack completed without a result",
    ))
}

fn is_fork_boundary_reference(
    reference: &RolloutReferenceItem,
    current_thread_id: ThreadId,
) -> bool {
    reference.nth_user_message.is_some() || reference.thread_id != Some(current_thread_id)
}

fn reference_identity(reference: &RolloutReferenceItem) -> io::Result<ReferenceIdentity> {
    let inferred_thread_id = crate::thread_id_from_path(reference.rollout_path.as_path());
    let thread_id = reference.thread_id.or(inferred_thread_id).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout reference {} is missing thread_id",
                reference.rollout_path.display()
            ),
        )
    })?;
    Ok(match reference.segment_id {
        Some(segment_id) => ReferenceIdentity::Segment {
            thread_id,
            rollout_id: reference.rollout_id,
            segment_id,
        },
        None => ReferenceIdentity::LegacyInitial {
            thread_id,
            rollout_id: reference.rollout_id.or_else(|| {
                reference
                    .thread_id
                    .is_none()
                    .then(|| crate::rollout_id_from_path(reference.rollout_path.as_path()))
                    .flatten()
            }),
        },
    })
}

async fn validated_candidate(
    codex_home: &Path,
    path: &Path,
    identity: ReferenceIdentity,
) -> io::Result<Option<PathBuf>> {
    if approved_reference_root_path(codex_home, path).is_none() {
        return Ok(None);
    }
    let Some(path) = compression::existing_rollout_path(path).await else {
        return Ok(None);
    };
    let Some(path) = canonical_approved_reference_path(codex_home, path.as_path()).await? else {
        return Ok(None);
    };
    let meta = match read_candidate_session_meta(path.as_path()).await {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    Ok((identity_matches(&meta, identity)
        && identity.rollout_id().is_none_or(|rollout_id| {
            crate::rollout_id_from_path(compression::plain_rollout_path(path.as_path()).as_path())
                == Some(rollout_id)
        }))
    .then_some(path))
}

async fn read_candidate_session_meta(path: &Path) -> io::Result<SessionMetaLine> {
    let mut reader = compression::open_rollout_line_reader(path).await?;
    let Some(first_line) = reader.next_line().await? else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("rollout at {} is empty", path.display()),
        ));
    };
    let line = RolloutRecorder::parse_rollout_line_bytes(first_line.as_bytes())
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "rollout at {} has invalid session metadata: {err}",
                    path.display()
                ),
            )
        })?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("rollout at {} has invalid session metadata", path.display()),
            )
        })?;
    match line.item {
        RolloutItem::SessionMeta(meta) => Ok(meta),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout at {} does not start with session metadata",
                path.display()
            ),
        )),
    }
}

async fn find_valid_candidate_in_directory(
    codex_home: &Path,
    directory: &Path,
    identity: ReferenceIdentity,
    expected_file_name: Option<&std::ffi::OsStr>,
) -> io::Result<Option<PathBuf>> {
    if canonical_approved_reference_path(codex_home, directory)
        .await?
        .is_none()
    {
        return Ok(None);
    }
    // Keep the CODEX_HOME spelling when enumerating. `codex_home` itself may be a symlink, while
    // every candidate is canonicalized again before it is opened.
    let mut entries = match tokio::fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let mut candidates = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let Some(rollout_file) = compression::RolloutFile::from_path(entry.path()) else {
            continue;
        };
        if expected_file_name.is_some_and(|expected_file_name| {
            std::ffi::OsStr::new(rollout_file.plain_file_name()) != expected_file_name
        }) {
            continue;
        }
        if let Some(path) = validated_candidate(codex_home, rollout_file.path(), identity).await?
            && !candidates.contains(&path)
        {
            candidates.push(path);
        }
    }
    candidates.sort();
    match candidates.as_slice() {
        [] => Ok(None),
        [path] => Ok(Some(path.clone())),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout reference {} is ambiguous in {}: {} matching files",
                identity.description(),
                directory.display(),
                candidates.len()
            ),
        )),
    }
}

async fn canonical_approved_reference_path(
    codex_home: &Path,
    path: &Path,
) -> io::Result<Option<PathBuf>> {
    let Some(root) = approved_reference_root_path(codex_home, path) else {
        return Ok(None);
    };
    let canonical_home = match tokio::fs::canonicalize(codex_home).await {
        Ok(path) => path,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let root_metadata = match tokio::fs::symlink_metadata(root.as_path()).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    if root_metadata.file_type().is_symlink() {
        return Ok(None);
    }
    let canonical_root = match tokio::fs::canonicalize(root.as_path()).await {
        Ok(path) => path,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    if !canonical_root.starts_with(&canonical_home) {
        return Ok(None);
    }
    let canonical_path = match tokio::fs::canonicalize(path).await {
        Ok(path) => path,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    Ok(canonical_path
        .starts_with(canonical_root)
        .then_some(canonical_path))
}

fn approved_reference_root_path(codex_home: &Path, path: &Path) -> Option<PathBuf> {
    [
        SESSIONS_SUBDIR,
        ARCHIVED_SESSIONS_SUBDIR,
        ROTATED_ROLLOUT_SEGMENTS_SUBDIR,
    ]
    .into_iter()
    .map(|subdir| codex_home.join(subdir))
    .find(|root| {
        path.strip_prefix(root).ok().is_some_and(|relative| {
            relative
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
        })
    })
}

/// Mutable legacy rollouts can contain a torn ordinary record after a failed disk write.
async fn load_active_rollout_lines(path: &Path) -> io::Result<Vec<RolloutLine>> {
    let (lines, _thread_id, parse_errors) = RolloutRecorder::load_rollout_lines(path).await?;
    let session_meta = canonical_session_meta(&lines)?;
    if parse_errors != 0 && session_meta.meta.history_mode != ThreadHistoryMode::Legacy {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout at {} contains {parse_errors} invalid record(s)",
                path.display()
            ),
        ));
    }
    Ok(lines)
}

async fn load_strict_rollout_lines(path: &Path) -> io::Result<Vec<RolloutLine>> {
    let (lines, _thread_id, parse_errors) = RolloutRecorder::load_rollout_lines(path).await?;
    if parse_errors != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollout at {} contains {parse_errors} invalid record(s)",
                path.display()
            ),
        ));
    }
    canonical_session_meta(&lines)?;
    Ok(lines)
}

fn canonical_session_meta(lines: &[RolloutLine]) -> io::Result<&SessionMetaLine> {
    match lines.first().map(|line| &line.item) {
        Some(RolloutItem::SessionMeta(meta)) => Ok(meta),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rollout does not start with session metadata",
        )),
    }
}

fn validate_identity(
    meta: &SessionMetaLine,
    identity: ReferenceIdentity,
    path: &Path,
) -> io::Result<()> {
    if identity_matches(meta, identity)
        && identity.rollout_id().is_none_or(|rollout_id| {
            crate::rollout_id_from_path(compression::plain_rollout_path(path).as_path())
                == Some(rollout_id)
        })
    {
        return Ok(());
    }
    let identity_description = identity.description();
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "rollout at {} does not match reference {identity_description}",
            path.display()
        ),
    ))
}

fn identity_matches(meta: &SessionMetaLine, identity: ReferenceIdentity) -> bool {
    match identity {
        ReferenceIdentity::Segment {
            thread_id,
            segment_id,
            ..
        } => meta.meta.id == thread_id && meta.meta.segment_id == Some(segment_id),
        ReferenceIdentity::LegacyInitial { thread_id, .. } => {
            meta.meta.id == thread_id && meta.meta.segment_id.is_none()
        }
    }
}

fn rollout_path_for_timestamp_file(
    codex_home: &Path,
    rollout_timestamp: &str,
    file_name: &str,
) -> Option<PathBuf> {
    Some(
        codex_home
            .join(SESSIONS_SUBDIR)
            .join(rollout_timestamp.get(0..4)?)
            .join(rollout_timestamp.get(5..7)?)
            .join(rollout_timestamp.get(8..10)?)
            .join(file_name),
    )
}

fn apply_filter(lines: &mut Vec<RolloutLine>, filter_texts: &[String]) {
    lines.retain_mut(|line| filter_line(line, filter_texts));
}

fn filter_line(line: &mut RolloutLine, filter_texts: &[String]) -> bool {
    match &mut line.item {
        RolloutItem::Compacted(compacted) => {
            if let Some(replacement_history) = compacted.replacement_history.as_mut() {
                replacement_history
                    .retain(|item| !matches_filtered_developer_message(&item.item, filter_texts));
            }
            true
        }
        RolloutItem::ResponseItem(item) => {
            !matches_filtered_developer_message(&item.item, filter_texts)
        }
        RolloutItem::SessionMeta(_)
        | RolloutItem::RolloutReference(_)
        | RolloutItem::InterAgentCommunication(_)
        | RolloutItem::InterAgentCommunicationMetadata { .. }
        | RolloutItem::TurnContext(_)
        | RolloutItem::WorldState(_)
        | RolloutItem::SecurityRiskScore(_)
        | RolloutItem::RealtimeItem(_)
        | RolloutItem::EventMsg(_) => true,
    }
}

fn matches_filtered_developer_message(item: &ResponseItem, filter_texts: &[String]) -> bool {
    let ResponseItem::Message { role, content, .. } = item else {
        return false;
    };
    let [ContentItem::InputText { text }] = content.as_slice() else {
        return false;
    };
    role == "developer" && filter_texts.iter().any(|filter_text| filter_text == text)
}

fn truncate_before_nth_user_message(lines: &mut Vec<RolloutLine>, nth_user_message: usize) {
    if nth_user_message == usize::MAX {
        return;
    }
    let mut event_user_positions = Vec::new();
    let mut response_user_positions = Vec::new();
    // A canonical persisted turn begins at `TurnStarted`, before its user response item. Record
    // that boundary so truncation cannot retain the opening event for an excluded turn.
    let mut active_turn_start = None;
    for (index, line) in lines.iter().enumerate() {
        match &line.item {
            RolloutItem::EventMsg(EventMsg::TurnStarted(_)) => {
                active_turn_start = Some(index);
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                event_user_positions.push(active_turn_start.unwrap_or(index));
            }
            RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
                if matches!(event.item, TurnItem::UserMessage(_)) =>
            {
                event_user_positions.push(active_turn_start.unwrap_or(index));
            }
            RolloutItem::ResponseItem(item) if item.is_user_message() => {
                response_user_positions.push(active_turn_start.unwrap_or(index));
            }
            RolloutItem::EventMsg(EventMsg::TurnComplete(_) | EventMsg::TurnAborted(_)) => {
                active_turn_start = None;
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                let count = usize::try_from(rollback.num_turns).unwrap_or(usize::MAX);
                event_user_positions.truncate(event_user_positions.len().saturating_sub(count));
                response_user_positions
                    .truncate(response_user_positions.len().saturating_sub(count));
            }
            RolloutItem::SessionMeta(_)
            | RolloutItem::RolloutReference(_)
            | RolloutItem::ResponseItem(_)
            | RolloutItem::InterAgentCommunication(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::Compacted(_)
            | RolloutItem::TurnContext(_)
            | RolloutItem::WorldState(_)
            | RolloutItem::SecurityRiskScore(_)
            | RolloutItem::RealtimeItem(_)
            | RolloutItem::EventMsg(_) => {}
        }
    }
    // Canonical rollouts contain one `UserMessage` event per real user turn. Prefer those events
    // because model-visible contextual fragments also use `role: "user"`. The response-item
    // fallback preserves reference truncation for older and synthetic rollouts without events.
    let user_positions = if event_user_positions.is_empty() {
        response_user_positions
    } else {
        event_user_positions
    };
    if let Some(cutoff) = user_positions.get(nth_user_message).copied() {
        lines.truncate(cutoff);
    }
}

#[cfg(test)]
#[path = "reference_tests.rs"]
mod tests;
