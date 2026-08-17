//! Changes only an authenticated canonical record's ordinal token.

use std::borrow::Cow;
use std::sync::LazyLock;

use regex::bytes::Regex;
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use super::migration_error;
use crate::ThreadStoreResult;

/// The canonical writer emits these envelope fields before the potentially large payload.
static CANONICAL_HEAD: LazyLock<Option<Regex>> = LazyLock::new(|| {
    Regex::new(r#"\A\{"timestamp":"(?:[^"\\\r\n]|\\[^\r\n])*","ordinal":(?P<ordinal>[0-9]+),"type":"(?P<kind>[a-z_]+)","#)
        .ok()
});

/// Borrows the exact JSON ordinal token without decoding the record payload.
#[derive(Deserialize)]
pub(super) struct OrdinalRecord<'a> {
    #[serde(borrow)]
    ordinal: &'a RawValue,
    #[serde(rename = "type", borrow)]
    pub(super) kind: Cow<'a, str>,
}

#[cfg(test)]
#[path = "ordinal_rewrite_tests.rs"]
mod tests;

impl<'a> OrdinalRecord<'a> {
    /// Only for sources already proved byte-for-byte canonical. Other input keeps the full
    /// envelope decoder. Matching the header does not inspect or allocate the payload.
    pub(super) fn from_canonical(bytes: &'a [u8]) -> Option<Self> {
        let captures = CANONICAL_HEAD.as_ref()?.captures(bytes)?;
        let ordinal = captures.name("ordinal")?;
        let kind = captures.name("kind")?;
        Some(Self {
            ordinal: serde_json::from_slice(&bytes[ordinal.range()]).ok()?,
            kind: Cow::Borrowed(std::str::from_utf8(&bytes[kind.range()]).ok()?),
        })
    }

    pub(super) fn ordinal(&self) -> ThreadStoreResult<u64> {
        serde_json::from_str(self.ordinal.get()).map_err(migration_error)
    }

    /// `record` is the same JSON buffer from which this envelope was deserialized, without LF.
    pub(super) async fn write_with_ordinal<W: AsyncWrite + Unpin>(
        &self,
        record: &[u8],
        ordinal: u64,
        writer: &mut W,
    ) -> ThreadStoreResult<()> {
        let token = self.ordinal.get();
        let replacement = ordinal.to_string();
        let start = (token.as_ptr() as usize)
            .checked_sub(record.as_ptr() as usize)
            .ok_or_else(|| migration_error("ordinal token is outside its source record"))?;
        let end = start
            .checked_add(token.len())
            .ok_or_else(|| migration_error("ordinal token range overflow"))?;
        if record.get(start..end) != Some(token.as_bytes()) {
            return Err(migration_error(
                "ordinal token does not match its source record",
            ));
        }
        if token.as_bytes() == replacement.as_bytes() {
            writer.write_all(record).await.map_err(migration_error)?;
            return writer.write_all(b"\n").await.map_err(migration_error);
        }
        writer
            .write_all(&record[..start])
            .await
            .map_err(migration_error)?;
        writer
            .write_all(replacement.as_bytes())
            .await
            .map_err(migration_error)?;
        writer
            .write_all(&record[end..])
            .await
            .map_err(migration_error)?;
        writer.write_all(b"\n").await.map_err(migration_error)
    }
}
