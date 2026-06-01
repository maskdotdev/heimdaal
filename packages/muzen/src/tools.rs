use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};

use crate::contracts::*;
use crate::repo::RepoContext;
use crate::util::redaction_none;

#[derive(Debug)]
pub(crate) struct ToolRegistry {
    pub(crate) repo: Arc<RepoContext>,
    pub(crate) artifacts: Arc<ArtifactStore>,
}

impl ToolRegistry {
    pub(crate) fn execute(
        &self,
        session: &AgentSession,
        tool_call_id: String,
        action: ModelAction,
    ) -> Result<ToolOutcome> {
        let started = Instant::now();
        let tool = action.tool_name();
        let redaction = redaction_none();
        let outcome = match action {
            ModelAction::ListChangedFiles => self.list_changed_files(&tool_call_id, session)?,
            ModelAction::ReadDiff => self.read_diff(&tool_call_id, session)?,
            ModelAction::ListFiles => self.list_files(&tool_call_id, session)?,
            ModelAction::ReadFile(path) => self.read_file(
                &tool_call_id,
                session,
                ToolName::ReadFile,
                &path,
                EvidenceRevision::Review,
            )?,
            ModelAction::ReadHeadFile(path) => self.read_file(
                &tool_call_id,
                session,
                ToolName::ReadHeadFile,
                &path,
                EvidenceRevision::Review,
            )?,
            ModelAction::ReadBaseFile(path) => self.snapshot_unavailable(
                &tool_call_id,
                session,
                ToolName::ReadBaseFile,
                path,
                "BASE_SNAPSHOT_UNAVAILABLE",
            ),
            ModelAction::SearchText(query) => self.search_text(&tool_call_id, session, &query)?,
            ModelAction::FindRelatedFiles(path) => {
                self.find_related_files(&tool_call_id, session, &path)?
            }
            ModelAction::FindTestsForFile(path) => {
                self.find_tests_for_file(&tool_call_id, session, &path)?
            }
            ModelAction::ListImports(path) => self.list_imports(&tool_call_id, session, &path)?,
            ModelAction::RecordFinding { title, claim } => ToolOutcome::finding(
                tool_call_id,
                session.id.clone(),
                title,
                claim,
                started.elapsed(),
            ),
            ModelAction::ChallengeFinding {
                finding_id,
                rationale,
            } => {
                let content = format!("{finding_id}: {rationale}");
                let artifact_id = self.artifacts.insert(
                    ArtifactKind::ToolSummary,
                    content,
                    format!("challenge for {finding_id}"),
                    Completeness::Complete,
                    redaction.clone(),
                );
                ToolOutcome::artifact(
                    tool_call_id,
                    ToolName::ChallengeFinding,
                    artifact_id,
                    format!("challenged {finding_id}"),
                    0,
                    self.artifacts.meta(artifact_id),
                    started.elapsed(),
                )
            }
            ModelAction::Finish(reason) => {
                let artifact_id = self.artifacts.insert(
                    ArtifactKind::ToolSummary,
                    reason.clone(),
                    "session finish".to_string(),
                    Completeness::Complete,
                    redaction.clone(),
                );
                ToolOutcome::artifact(
                    tool_call_id,
                    ToolName::Finish,
                    artifact_id,
                    reason,
                    0,
                    self.artifacts.meta(artifact_id),
                    started.elapsed(),
                )
            }
        };
        if outcome.tool_result.tool_name != tool {
            bail!("tool outcome mismatch");
        }
        Ok(outcome)
    }

    pub(crate) fn list_changed_files(
        &self,
        tool_call_id: &str,
        _session: &AgentSession,
    ) -> Result<ToolOutcome> {
        let started = Instant::now();
        let content = self
            .repo
            .change
            .changed_files
            .iter()
            .map(format_changed_file)
            .collect::<Vec<_>>()
            .join("\n");
        let summary = format!(
            "listed {} changed files",
            self.repo.change.changed_files.len()
        );
        let artifact_id = self.artifacts.insert(
            ArtifactKind::ChangedFileList,
            content,
            summary.clone(),
            Completeness::Complete,
            redaction_none(),
        );
        Ok(ToolOutcome::artifact(
            tool_call_id.to_string(),
            ToolName::ListChangedFiles,
            artifact_id,
            summary,
            0,
            self.artifacts.meta(artifact_id),
            started.elapsed(),
        ))
    }

    pub(crate) fn read_diff(
        &self,
        tool_call_id: &str,
        _session: &AgentSession,
    ) -> Result<ToolOutcome> {
        let started = Instant::now();
        let mut diff = String::new();
        diff.push_str(&format!(
            "change {} {}..{}\n",
            self.repo.change.change_id,
            self.repo.change.base_revision_id,
            self.repo.change.head_revision_id
        ));
        for file in &self.repo.change.changed_files {
            diff.push_str(&format!("{}\n", format_changed_file(file)));
        }
        let mut completeness = Completeness::Complete;
        if diff.len() > self.repo.path_policy.max_diff_bytes {
            diff.truncate(self.repo.path_policy.max_diff_bytes);
            completeness = Completeness::Truncated;
        }
        let summary = "read review diff manifest".to_string();
        let artifact_id = self.artifacts.insert(
            ArtifactKind::DiffHunk,
            diff,
            summary.clone(),
            completeness,
            redaction_none(),
        );
        Ok(ToolOutcome::artifact(
            tool_call_id.to_string(),
            ToolName::ReadDiff,
            artifact_id,
            summary,
            0,
            self.artifacts.meta(artifact_id),
            started.elapsed(),
        ))
    }

    pub(crate) fn list_files(
        &self,
        tool_call_id: &str,
        _session: &AgentSession,
    ) -> Result<ToolOutcome> {
        let started = Instant::now();
        let files = self.repo.walk_files()?;
        let content = files
            .iter()
            .take(300)
            .map(|path| self.repo.display_path(path))
            .collect::<Vec<_>>()
            .join("\n");
        let completeness = if files.len() > 300 {
            Completeness::Truncated
        } else {
            Completeness::Complete
        };
        let summary = format!("listed {} files", files.len());
        let artifact_id = self.artifacts.insert(
            ArtifactKind::FileList,
            content,
            summary.clone(),
            completeness,
            redaction_none(),
        );
        Ok(ToolOutcome::artifact(
            tool_call_id.to_string(),
            ToolName::ListFiles,
            artifact_id,
            summary,
            0,
            self.artifacts.meta(artifact_id),
            started.elapsed(),
        ))
    }

    pub(crate) fn read_file(
        &self,
        tool_call_id: &str,
        _session: &AgentSession,
        tool_name: ToolName,
        relative: &Path,
        revision: EvidenceRevision,
    ) -> Result<ToolOutcome> {
        let started = Instant::now();
        let clean = self.repo.normalize_tool_path(relative)?;
        if let Some(artifact_id) = self
            .repo
            .file_cache
            .lock()
            .expect("file cache poisoned")
            .get(&clean)
            .copied()
        {
            return Ok(ToolOutcome::artifact(
                tool_call_id.to_string(),
                tool_name,
                artifact_id,
                format!("cache hit {}", clean.display()),
                0,
                self.artifacts.meta(artifact_id),
                started.elapsed(),
            ));
        }
        let read = self
            .repo
            .read_text_file(&clean, self.repo.path_policy.max_file_bytes)?;
        let line_count = read.content.lines().count();
        let summary = format!("read {} lines from {}", line_count, read.path.display());
        let bytes_read = read.content.len();
        let artifact_id = self.artifacts.insert(
            ArtifactKind::FileSlice,
            read.content,
            summary.clone(),
            read.completeness,
            redaction_none(),
        );
        self.repo
            .file_cache
            .lock()
            .expect("file cache poisoned")
            .insert(clean, artifact_id);
        let mut outcome = ToolOutcome::artifact(
            tool_call_id.to_string(),
            tool_name,
            artifact_id,
            summary,
            bytes_read,
            self.artifacts.meta(artifact_id),
            started.elapsed(),
        );
        outcome.evidence_revision = revision;
        Ok(outcome)
    }

    pub(crate) fn snapshot_unavailable(
        &self,
        tool_call_id: &str,
        _session: &AgentSession,
        tool: ToolName,
        path: PathBuf,
        code: &str,
    ) -> ToolOutcome {
        let started = Instant::now();
        ToolOutcome {
            tool_result: ToolResultV1 {
                tool_call_id: tool_call_id.to_string(),
                tool_name: tool,
                status: ToolStatus::NotFound,
                error_code: Some(code.to_string()),
                completeness: Completeness::MetadataOnly,
                artifact_ids: Vec::new(),
                summary: format!("snapshot unavailable for {}", path.display()),
                bytes_read: 0,
                bytes_returned: 0,
                duration_ms: started.elapsed().as_millis() as u64,
                redaction: redaction_none(),
            },
            artifact_id: None,
            finding: None,
            evidence_revision: EvidenceRevision::Base,
        }
    }

    pub(crate) fn search_text(
        &self,
        tool_call_id: &str,
        _session: &AgentSession,
        query: &str,
    ) -> Result<ToolOutcome> {
        let started = Instant::now();
        let needles = query
            .split('|')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        if needles.is_empty() {
            bail!("empty search query");
        }

        let mut matches = Vec::new();
        let mut completeness = Completeness::Complete;
        for path in self.repo.walk_files()? {
            if matches.len() >= self.repo.path_policy.max_search_results {
                completeness = Completeness::Truncated;
                break;
            }
            let Ok(read) = self
                .repo
                .read_text_file(&path, self.repo.path_policy.max_file_bytes)
            else {
                continue;
            };
            for (index, line) in read.content.lines().enumerate() {
                if needles.iter().any(|needle| line.contains(needle)) {
                    matches.push(format!("{}:{}:{}", path.display(), index + 1, line.trim()));
                    if matches.len() >= self.repo.path_policy.max_search_results {
                        completeness = Completeness::Truncated;
                        break;
                    }
                }
            }
        }

        let summary = format!("search {:?} returned {} matches", needles, matches.len());
        let content = matches.join("\n");
        let bytes_read = content.len();
        let artifact_id = self.artifacts.insert(
            ArtifactKind::SearchResults,
            content,
            summary.clone(),
            completeness,
            redaction_none(),
        );
        Ok(ToolOutcome::artifact(
            tool_call_id.to_string(),
            ToolName::SearchText,
            artifact_id,
            summary,
            bytes_read,
            self.artifacts.meta(artifact_id),
            started.elapsed(),
        ))
    }

    pub(crate) fn find_related_files(
        &self,
        tool_call_id: &str,
        _session: &AgentSession,
        relative: &Path,
    ) -> Result<ToolOutcome> {
        let started = Instant::now();
        let clean = self.repo.normalize_tool_path(relative)?;
        let stem = clean
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        let mut related = Vec::new();
        for path in self.repo.walk_files()? {
            if path == clean {
                continue;
            }
            let path_text = path.to_string_lossy();
            if !stem.is_empty() && path_text.contains(stem) {
                related.push(path_text.into_owned());
            }
            if related.len() >= 80 {
                break;
            }
        }
        let summary = format!(
            "found {} related files for {}",
            related.len(),
            clean.display()
        );
        let artifact_id = self.artifacts.insert(
            ArtifactKind::FileList,
            related.join("\n"),
            summary.clone(),
            if related.len() >= 80 {
                Completeness::Truncated
            } else {
                Completeness::Complete
            },
            redaction_none(),
        );
        Ok(ToolOutcome::artifact(
            tool_call_id.to_string(),
            ToolName::FindRelatedFiles,
            artifact_id,
            summary,
            0,
            self.artifacts.meta(artifact_id),
            started.elapsed(),
        ))
    }

    pub(crate) fn find_tests_for_file(
        &self,
        tool_call_id: &str,
        _session: &AgentSession,
        relative: &Path,
    ) -> Result<ToolOutcome> {
        let started = Instant::now();
        let clean = self.repo.normalize_tool_path(relative)?;
        let stem = clean
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        let mut tests = Vec::new();
        for path in self.repo.walk_files()? {
            let path_text = path.to_string_lossy();
            if path_text.contains("test") || path_text.contains("spec") {
                if stem.is_empty() || path_text.contains(stem) {
                    tests.push(path_text.into_owned());
                }
            }
            if tests.len() >= 80 {
                break;
            }
        }
        let summary = format!("found {} likely tests for {}", tests.len(), clean.display());
        let artifact_id = self.artifacts.insert(
            ArtifactKind::FileList,
            tests.join("\n"),
            summary.clone(),
            if tests.len() >= 80 {
                Completeness::Truncated
            } else {
                Completeness::Complete
            },
            redaction_none(),
        );
        Ok(ToolOutcome::artifact(
            tool_call_id.to_string(),
            ToolName::FindTestsForFile,
            artifact_id,
            summary,
            0,
            self.artifacts.meta(artifact_id),
            started.elapsed(),
        ))
    }

    pub(crate) fn list_imports(
        &self,
        tool_call_id: &str,
        _session: &AgentSession,
        relative: &Path,
    ) -> Result<ToolOutcome> {
        let started = Instant::now();
        let read = self
            .repo
            .read_text_file(relative, self.repo.path_policy.max_file_bytes)?;
        let imports = read
            .content
            .lines()
            .enumerate()
            .filter(|(_, line)| {
                let trimmed = line.trim_start();
                trimmed.starts_with("use ")
                    || trimmed.starts_with("import ")
                    || trimmed.starts_with("export ")
                    || trimmed.starts_with("require(")
                    || trimmed.starts_with("from ")
            })
            .map(|(index, line)| format!("{}:{}:{}", read.path.display(), index + 1, line.trim()))
            .collect::<Vec<_>>();
        let summary = format!(
            "listed {} imports from {}",
            imports.len(),
            read.path.display()
        );
        let artifact_id = self.artifacts.insert(
            ArtifactKind::ImportSummary,
            imports.join("\n"),
            summary.clone(),
            read.completeness,
            redaction_none(),
        );
        Ok(ToolOutcome::artifact(
            tool_call_id.to_string(),
            ToolName::ListImports,
            artifact_id,
            summary,
            0,
            self.artifacts.meta(artifact_id),
            started.elapsed(),
        ))
    }
}

#[derive(Debug)]
pub(crate) struct ToolOutcome {
    pub(crate) tool_result: ToolResultV1,
    pub(crate) artifact_id: Option<ArtifactId>,
    pub(crate) finding: Option<(String, String)>,
    pub(crate) evidence_revision: EvidenceRevision,
}

impl ToolOutcome {
    pub(crate) fn artifact(
        tool_call_id: String,
        tool: ToolName,
        artifact_id: ArtifactId,
        summary: String,
        bytes_read: usize,
        meta: Option<ArtifactMeta>,
        elapsed: Duration,
    ) -> Self {
        let completeness = meta
            .as_ref()
            .map(|value| value.completeness)
            .unwrap_or(Completeness::Complete);
        let bytes_returned = meta.as_ref().map(|value| value.bytes).unwrap_or(0);
        Self {
            tool_result: ToolResultV1 {
                tool_call_id,
                tool_name: tool,
                status: ToolStatus::Ok,
                error_code: None,
                completeness,
                artifact_ids: vec![artifact_id.as_string()],
                summary,
                bytes_read,
                bytes_returned,
                duration_ms: elapsed.as_millis() as u64,
                redaction: redaction_none(),
            },
            artifact_id: Some(artifact_id),
            finding: None,
            evidence_revision: EvidenceRevision::Review,
        }
    }

    pub(crate) fn finding(
        tool_call_id: String,
        session_id: String,
        title: String,
        claim: String,
        elapsed: Duration,
    ) -> Self {
        Self {
            tool_result: ToolResultV1 {
                tool_call_id,
                tool_name: ToolName::RecordFinding,
                status: ToolStatus::Ok,
                error_code: None,
                completeness: Completeness::Complete,
                artifact_ids: Vec::new(),
                summary: format!("recorded finding for {session_id}: {title}"),
                bytes_read: 0,
                bytes_returned: 0,
                duration_ms: elapsed.as_millis() as u64,
                redaction: redaction_none(),
            },
            artifact_id: None,
            finding: Some((title, claim)),
            evidence_revision: EvidenceRevision::Review,
        }
    }
}

pub(crate) fn format_changed_file(file: &ChangedFileEntryV1) -> String {
    let path = file
        .new_path
        .as_ref()
        .or(file.old_path.as_ref())
        .map(|value| value.display().to_string())
        .unwrap_or_else(|| "<unknown>".to_string());
    format!("{:?} {path}", file.status)
}
