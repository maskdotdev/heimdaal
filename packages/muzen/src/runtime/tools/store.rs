use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::contracts::{
    ArtifactKind, EvidenceLocationV1, EvidenceRefV1, EvidenceRevision, FindingPublishability,
    FindingSeverity, FindingV1, LineRangeV1, ReportStatus, ToolName, ValidationStatus,
};
use crate::runtime::contracts::{
    stable_id, ArtifactId, ArtifactKey, ArtifactView, SessionId, ToolCallId, ToolResultEnvelope,
};
use crate::runtime::repo::RepoSnapshot;

#[derive(Debug, Default)]
pub struct ConcurrentArtifactStore {
    by_id: DashMap<String, Arc<ConcurrentArtifact>>,
    order: Mutex<Vec<String>>,
}

impl ConcurrentArtifactStore {
    pub(super) fn insert(&self, key: ArtifactKey, content: String) -> ArtifactId {
        self.insert_views(key, content.clone(), content)
    }

    pub(super) fn insert_views(
        &self,
        key: ArtifactKey,
        raw_content: String,
        redacted_content: String,
    ) -> ArtifactId {
        let content_hash = stable_id(&[&redacted_content]);
        let raw_content_hash = stable_id(&[&raw_content]);
        let artifact_id = ArtifactId(format!(
            "art_{}",
            stable_id(&[&key.0, &raw_content_hash, &content_hash])
        ));
        if self
            .by_id
            .insert(
                artifact_id.0.clone(),
                Arc::new(ConcurrentArtifact {
                    artifact_id: artifact_id.clone(),
                    bytes: redacted_content.len(),
                    content_hash,
                    content: redacted_content,
                    raw_bytes: raw_content.len(),
                    raw_content_hash,
                    raw_content,
                }),
            )
            .is_none()
        {
            self.order.lock().push(artifact_id.0.clone());
        }
        artifact_id
    }

    pub fn stats(&self) -> (usize, usize) {
        let artifacts = self.by_id.iter().collect::<Vec<_>>();
        let bytes = artifacts.iter().map(|item| item.bytes).sum();
        (artifacts.len(), bytes)
    }

    pub fn get(&self, artifact_id: &ArtifactId) -> Option<ArtifactView> {
        self.by_id
            .get(&artifact_id.0)
            .map(|artifact| artifact.as_ref().view())
    }

    pub fn get_raw(&self, artifact_id: &ArtifactId) -> Option<ArtifactView> {
        self.by_id
            .get(&artifact_id.0)
            .map(|artifact| artifact.as_ref().raw_view())
    }

    pub fn list(&self) -> Vec<ArtifactView> {
        self.order
            .lock()
            .iter()
            .filter_map(|artifact_id| self.by_id.get(artifact_id))
            .map(|artifact| artifact.as_ref().view())
            .collect()
    }

    pub fn list_raw(&self) -> Vec<ArtifactView> {
        self.order
            .lock()
            .iter()
            .filter_map(|artifact_id| self.by_id.get(artifact_id))
            .map(|artifact| artifact.as_ref().raw_view())
            .collect()
    }

    pub(crate) fn merge_from(&self, other: &ConcurrentArtifactStore) {
        for redacted in other.list() {
            let raw = other
                .get_raw(&redacted.artifact_id)
                .unwrap_or_else(|| redacted.clone());
            self.insert_existing(redacted, raw);
        }
    }

    fn insert_existing(&self, redacted: ArtifactView, raw: ArtifactView) {
        let artifact_id = redacted.artifact_id.clone();
        if self
            .by_id
            .insert(
                artifact_id.0.clone(),
                Arc::new(ConcurrentArtifact {
                    artifact_id: artifact_id.clone(),
                    bytes: redacted.bytes,
                    content_hash: redacted.content_hash,
                    content: redacted.content,
                    raw_bytes: raw.bytes,
                    raw_content_hash: raw.content_hash,
                    raw_content: raw.content,
                }),
            )
            .is_none()
        {
            self.order.lock().push(artifact_id.0);
        }
    }
}

#[derive(Debug)]
struct ConcurrentArtifact {
    artifact_id: ArtifactId,
    bytes: usize,
    content_hash: String,
    content: String,
    raw_bytes: usize,
    raw_content_hash: String,
    raw_content: String,
}

impl ConcurrentArtifact {
    fn view(&self) -> ArtifactView {
        ArtifactView {
            artifact_id: self.artifact_id.clone(),
            bytes: self.bytes,
            content_hash: self.content_hash.clone(),
            content: self.content.clone(),
        }
    }

    fn raw_view(&self) -> ArtifactView {
        ArtifactView {
            artifact_id: self.artifact_id.clone(),
            bytes: self.raw_bytes,
            content_hash: self.raw_content_hash.clone(),
            content: self.raw_content.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ConcurrentFindingStore {
    by_id: DashMap<String, FindingV1>,
    order: Mutex<Vec<String>>,
}

impl ConcurrentFindingStore {
    pub(super) fn insert_from_tool_result(
        &self,
        session_id: &SessionId,
        result: &ToolResultEnvelope,
        evidence: Vec<EvidenceRefV1>,
    ) -> Option<String> {
        let data = result.data.as_ref()?;
        let id = data.get("findingId")?.as_str()?.to_string();
        let title = data.get("title")?.as_str()?.to_string();
        let claim = data.get("claim")?.as_str()?.to_string();
        let file_refs = evidence
            .iter()
            .map(|item| item.location.clone())
            .collect::<Vec<_>>();
        let validated = !evidence.is_empty();
        let finding = FindingV1 {
            id: id.clone(),
            title,
            claim,
            severity: FindingSeverity::Low,
            confidence: if validated { 0.72 } else { 0.25 },
            validation_status: if validated {
                ValidationStatus::Validated
            } else {
                ValidationStatus::Rejected
            },
            report_status: if validated {
                ReportStatus::Included
            } else {
                ReportStatus::Suppressed
            },
            publishability: if validated {
                FindingPublishability::Publishable
            } else {
                FindingPublishability::NotPublishable
            },
            evidence,
            file_refs,
            discovered_by: vec![session_id.0.clone()],
            challenged_by: Vec::new(),
        };
        if self.by_id.insert(id.clone(), finding).is_none() {
            self.order.lock().push(id.clone());
        }
        Some(id)
    }

    pub(crate) fn all(&self) -> Vec<FindingV1> {
        self.order
            .lock()
            .iter()
            .filter_map(|id| self.by_id.get(id))
            .map(|finding| finding.clone())
            .collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.by_id.len()
    }

    pub(crate) fn publishable_len(&self) -> usize {
        self.by_id
            .iter()
            .filter(|entry| {
                entry.validation_status == ValidationStatus::Validated
                    && matches!(entry.publishability, FindingPublishability::Publishable)
            })
            .count()
    }
}

pub(super) fn finding_id_for_call(call_id: &ToolCallId, title: &str, claim: &str) -> String {
    format!(
        "finding_{}",
        stable_id(&[&call_id.0, title, claim])
            .chars()
            .take(16)
            .collect::<String>()
    )
}

pub(super) fn artifact_kind_for_tool(tool: ToolName) -> Option<ArtifactKind> {
    match tool {
        ToolName::ReadDiff => Some(ArtifactKind::DiffHunk),
        ToolName::ReadFile | ToolName::ReadBaseFile | ToolName::ReadHeadFile => {
            Some(ArtifactKind::FileSlice)
        }
        ToolName::SearchText => Some(ArtifactKind::SearchResults),
        ToolName::ListChangedFiles => Some(ArtifactKind::ChangedFileList),
        ToolName::ListFiles | ToolName::FindRelatedFiles | ToolName::FindTestsForFile => {
            Some(ArtifactKind::FileList)
        }
        ToolName::ListImports => Some(ArtifactKind::ImportSummary),
        ToolName::RecordFinding | ToolName::ChallengeFinding | ToolName::Finish => None,
    }
}

pub(super) fn evidence_revision_for_tool(tool: ToolName) -> EvidenceRevision {
    match tool {
        ToolName::ReadBaseFile => EvidenceRevision::Base,
        ToolName::ReadHeadFile => EvidenceRevision::Head,
        _ => EvidenceRevision::Review,
    }
}

pub(super) fn evidence_location(
    result: &ToolResultEnvelope,
    snapshot: &RepoSnapshot,
) -> EvidenceLocationV1 {
    if let Some(path) = result
        .data
        .as_ref()
        .and_then(|data| data.get("firstMatch"))
        .and_then(|first_match| first_match.get("path"))
        .and_then(|path| path.as_str())
    {
        return EvidenceLocationV1::SinglePath {
            path: path.to_string(),
        };
    }
    if let Some(path) = result
        .data
        .as_ref()
        .and_then(|data| data.get("path"))
        .and_then(|path| path.as_str())
    {
        return EvidenceLocationV1::SinglePath {
            path: path.to_string(),
        };
    }
    if result.tool_name.as_builtin() == Some(ToolName::ReadDiff) {
        if let Some(file) = snapshot.manifest.changed_file_entries.first() {
            return EvidenceLocationV1::SinglePath {
                path: file.rel_path.display(),
            };
        }
    }
    EvidenceLocationV1::SinglePath {
        path: ".".to_string(),
    }
}

pub(super) fn evidence_line_range(result: &ToolResultEnvelope) -> Option<LineRangeV1> {
    if let Some(line_range) = result.data.as_ref().and_then(|data| data.get("lineRange")) {
        let start_line = line_range.get("startLine")?.as_u64()? as usize;
        let end_line = line_range.get("endLine")?.as_u64()? as usize;
        if start_line > 0 && end_line >= start_line {
            return Some(LineRangeV1 {
                start_line,
                end_line,
            });
        }
    }
    let line = result
        .data
        .as_ref()
        .and_then(|data| data.get("firstMatch"))
        .and_then(|first_match| first_match.get("line"))
        .and_then(|line| line.as_u64())
        .map(|line| line as usize);
    line.filter(|value| *value > 0).map(|value| LineRangeV1 {
        start_line: value,
        end_line: value,
    })
}
