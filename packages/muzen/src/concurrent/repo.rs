use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use cap_std::ambient_authority;
use ignore::WalkBuilder;

use crate::concurrent::contracts::*;
use crate::contracts::{ChangeScopeV1, ChangedFileStatus, PathPolicyV1};
use crate::repo::is_textish;

#[derive(Debug)]
pub(crate) struct RepoSnapshot {
    pub(crate) snapshot_id: SnapshotId,
    pub(crate) root_path: PathBuf,
    pub(crate) root: cap_std::fs::Dir,
    pub(crate) manifest: Arc<FileManifest>,
    pub(crate) diff: Arc<DiffArtifact>,
}

#[derive(Debug)]
pub(crate) struct FileManifest {
    pub(crate) by_path: HashMap<RepoPath, FileId>,
    pub(crate) files: Vec<FileMeta>,
    pub(crate) changed_files: Vec<FileId>,
    pub(crate) skipped: usize,
    pub(crate) bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct FileMeta {
    pub(crate) file_id: FileId,
    pub(crate) rel_path: RepoPath,
    pub(crate) size: u64,
    pub(crate) fingerprint: String,
    pub(crate) is_changed: bool,
    pub(crate) is_text_candidate: bool,
}

#[derive(Debug)]
pub(crate) struct DiffArtifact {
    pub(crate) content: String,
    pub(crate) content_hash: String,
}

#[derive(Debug)]
pub(crate) struct SnapshotBuildReport {
    pub(crate) files: usize,
    pub(crate) skipped: usize,
    pub(crate) bytes: u64,
    pub(crate) elapsed_ms: u64,
}

impl RepoSnapshot {
    pub(crate) fn build(
        root: &Path,
        policy: &PathPolicyV1,
        change: &ChangeScopeV1,
    ) -> RuntimeResult<(Arc<Self>, SnapshotBuildReport)> {
        let started = Instant::now();
        let root_path = fs::canonicalize(root).map_err(|error| {
            RuntimeError::RepoUnavailable(format!(
                "failed to canonicalize repo root {}: {error}",
                root.display()
            ))
        })?;
        if !root_path.is_dir() {
            return Err(RuntimeError::RepoUnavailable(format!(
                "repo root is not a directory: {}",
                root_path.display()
            )));
        }
        let root_dir = cap_std::fs::Dir::open_ambient_dir(&root_path, ambient_authority())
            .map_err(|error| {
                RuntimeError::RepoUnavailable(format!(
                    "failed to open capability root {}: {error}",
                    root_path.display()
                ))
            })?;
        let changed_paths = changed_paths(change);
        let mut files = Vec::new();
        let mut by_path = HashMap::new();
        let mut changed_files = Vec::new();
        let mut skipped = 0usize;
        let mut bytes = 0u64;

        let mut walker = WalkBuilder::new(&root_path);
        walker
            .hidden(false)
            .parents(false)
            .git_ignore(false)
            .git_exclude(false)
            .follow_links(false);

        for entry in walker.build() {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    skipped += 1;
                    continue;
                }
            };
            let file_type = match entry.file_type() {
                Some(file_type) => file_type,
                None => {
                    skipped += 1;
                    continue;
                }
            };
            if !file_type.is_file() {
                if file_type.is_symlink() {
                    skipped += 1;
                }
                continue;
            }
            let rel = match entry.path().strip_prefix(&root_path) {
                Ok(rel) => rel,
                Err(_) => {
                    skipped += 1;
                    continue;
                }
            };
            let rel_text = match rel.to_str() {
                Some(value) => value,
                None => {
                    skipped += 1;
                    continue;
                }
            };
            let repo_path = match RepoPath::parse(rel_text) {
                Ok(path) => path,
                Err(_) => {
                    skipped += 1;
                    continue;
                }
            };
            if is_denied(policy, repo_path.as_path()) || !is_allowed(policy, repo_path.as_path()) {
                skipped += 1;
                continue;
            }
            let meta = match entry.metadata() {
                Ok(meta) => meta,
                Err(_) => {
                    skipped += 1;
                    continue;
                }
            };
            if !meta.is_file() {
                skipped += 1;
                continue;
            }
            if files.len() >= policy.max_directory_entries {
                skipped += 1;
                break;
            }
            let size = meta.len();
            let is_text_candidate =
                is_textish(repo_path.as_path()) && size <= policy.max_file_bytes as u64;
            let is_changed = changed_paths.contains(&repo_path.display());
            let file_id = FileId(files.len() as u32);
            let fingerprint = stable_id(&[
                &repo_path.display(),
                &size.to_string(),
                &meta
                    .modified()
                    .ok()
                    .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|value| value.as_nanos().to_string())
                    .unwrap_or_default(),
            ]);
            if is_changed {
                changed_files.push(file_id);
            }
            bytes = bytes.saturating_add(size);
            by_path.insert(repo_path.clone(), file_id);
            files.push(FileMeta {
                file_id,
                rel_path: repo_path,
                size,
                fingerprint,
                is_changed,
                is_text_candidate,
            });
        }

        files.sort_by(|left, right| left.rel_path.display().cmp(&right.rel_path.display()));
        by_path.clear();
        changed_files.clear();
        for (index, file) in files.iter_mut().enumerate() {
            file.file_id = FileId(index as u32);
            if file.is_changed {
                changed_files.push(file.file_id);
            }
            by_path.insert(file.rel_path.clone(), file.file_id);
        }

        let diff = Arc::new(build_diff(change));
        let manifest_hash = stable_id(
            &files
                .iter()
                .map(|file| file.rel_path.display())
                .collect::<Vec<_>>()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        );
        let snapshot_id = SnapshotId(stable_id(&[
            &root_path.display().to_string(),
            &change.change_id,
            &change.base_revision_id,
            &change.head_revision_id,
            &diff.content_hash,
            &manifest_hash,
            &CONCURRENT_CONTRACT_VERSION.to_string(),
            &REDACTION_POLICY_VERSION.to_string(),
        ]));
        let manifest = Arc::new(FileManifest {
            by_path,
            files,
            changed_files,
            skipped,
            bytes,
        });
        let report = SnapshotBuildReport {
            files: manifest.files.len(),
            skipped,
            bytes,
            elapsed_ms: started.elapsed().as_millis() as u64,
        };
        Ok((
            Arc::new(Self {
                snapshot_id,
                root_path,
                root: root_dir,
                manifest,
                diff,
            }),
            report,
        ))
    }

    pub(crate) fn lookup(&self, path: &RepoPath) -> RuntimeResult<&FileMeta> {
        let file_id = self
            .manifest
            .by_path
            .get(path)
            .ok_or(RuntimeError::RepoAccessDenied)?;
        self.file(*file_id)
    }

    pub(crate) fn file(&self, file_id: FileId) -> RuntimeResult<&FileMeta> {
        self.manifest
            .files
            .get(file_id.0 as usize)
            .ok_or(RuntimeError::Invariant("file_id not present in manifest"))
    }

    pub(crate) fn list_files(&self) -> Vec<String> {
        self.manifest
            .files
            .iter()
            .filter(|file| file.is_text_candidate)
            .map(|file| file.rel_path.display())
            .collect()
    }

    pub(crate) fn read_bounded(
        &self,
        file_id: FileId,
        max_bytes: usize,
    ) -> RuntimeResult<(Vec<u8>, bool)> {
        let file = self.file(file_id)?;
        if !file.is_text_candidate && file.size > max_bytes as u64 {
            return Err(RuntimeError::LimitExceeded { kind: "file_bytes" });
        }
        if !file.is_text_candidate {
            return Err(RuntimeError::InvalidInput(
                "file is not text-readable".to_string(),
            ));
        }
        if file.size > max_bytes as u64 {
            return Err(RuntimeError::LimitExceeded { kind: "file_bytes" });
        }
        let mut reader = self
            .root
            .open(file.rel_path.as_path())
            .map_err(|error| RuntimeError::RepoUnavailable(format!("open failed: {error}")))?;
        let mut bytes = Vec::new();
        reader
            .by_ref()
            .take(max_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| RuntimeError::RepoUnavailable(format!("read failed: {error}")))?;
        let truncated = bytes.len() > max_bytes;
        if truncated {
            bytes.truncate(max_bytes);
        }
        if bytes.contains(&0) {
            return Err(RuntimeError::InvalidInput(
                "binary file rejected".to_string(),
            ));
        }
        Ok((bytes, truncated))
    }
}

fn changed_paths(change: &ChangeScopeV1) -> HashSet<String> {
    change
        .changed_files
        .iter()
        .filter_map(|file| file.new_path.as_ref().or(file.old_path.as_ref()))
        .filter_map(|path| path.to_str())
        .map(ToOwned::to_owned)
        .collect()
}

fn build_diff(change: &ChangeScopeV1) -> DiffArtifact {
    let mut content = format!(
        "change {} {}..{}\n",
        change.change_id, change.base_revision_id, change.head_revision_id
    );
    for file in &change.changed_files {
        let path = file
            .new_path
            .as_ref()
            .or(file.old_path.as_ref())
            .map(|value| value.display().to_string())
            .unwrap_or_else(|| "<unknown>".to_string());
        let status = match file.status {
            ChangedFileStatus::Added => "added",
            ChangedFileStatus::Modified => "modified",
            ChangedFileStatus::Deleted => "deleted",
            ChangedFileStatus::Renamed => "renamed",
            ChangedFileStatus::Copied => "copied",
            ChangedFileStatus::TypeChanged => "type_changed",
        };
        content.push_str(status);
        content.push(' ');
        content.push_str(&path);
        content.push('\n');
    }
    let content_hash = stable_id(&[&content]);
    DiffArtifact {
        content,
        content_hash,
    }
}

fn is_denied(policy: &PathPolicyV1, clean: &Path) -> bool {
    for component in clean.components() {
        let std::path::Component::Normal(part) = component else {
            continue;
        };
        let name = part.to_string_lossy();
        if !policy.allow_dot_git && name == ".git" {
            return true;
        }
        if policy
            .denied_globs
            .iter()
            .any(|glob| glob == name.as_ref() || glob == &clean.to_string_lossy())
        {
            return true;
        }
    }
    false
}

fn is_allowed(policy: &PathPolicyV1, clean: &Path) -> bool {
    policy.allowed_roots.iter().any(|root| {
        if root == Path::new(".") {
            return true;
        }
        let Ok(root) = RepoPath::from_path(root.clone()) else {
            return false;
        };
        root.as_path() == Path::new(".")
            || clean == root.as_path()
            || clean.starts_with(root.as_path())
    })
}
