use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::contracts::PathPolicyV1;

#[derive(Debug)]
pub(crate) struct RepoContext {
    pub(crate) root: PathBuf,
    pub(crate) path_policy: PathPolicyV1,
}

impl RepoContext {
    pub(crate) fn new(root: PathBuf, path_policy: PathPolicyV1) -> Result<Self> {
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to canonicalize repo path {}", root.display()))?;
        if !root.is_dir() {
            bail!("repo root is not a directory: {}", root.display());
        }
        Ok(Self { root, path_policy })
    }

    pub(crate) fn normalize_tool_path(&self, path: &Path) -> Result<PathBuf> {
        let mut clean = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Normal(part) => clean.push(part),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    bail!("repo-relative path required: {}", path.display())
                }
            }
        }
        if clean.as_os_str().is_empty() {
            clean.push(".");
        }
        if self.is_denied(&clean) {
            bail!("path denied by policy: {}", clean.display());
        }
        if !self.is_allowed_root(&clean)? {
            bail!("path outside allowed roots: {}", clean.display());
        }
        Ok(clean)
    }

    pub(crate) fn is_allowed_root(&self, clean: &Path) -> Result<bool> {
        for root in &self.path_policy.allowed_roots {
            let allowed = normalize_policy_path(root)?;
            if allowed == Path::new(".") || clean == allowed || clean.starts_with(&allowed) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn is_denied(&self, clean: &Path) -> bool {
        for component in clean.components() {
            let Component::Normal(part) = component else {
                continue;
            };
            let name = part.to_string_lossy();
            if !self.path_policy.allow_dot_git && name == ".git" {
                return true;
            }
            if self
                .path_policy
                .denied_globs
                .iter()
                .any(|glob| glob == name.as_ref() || glob == &clean.to_string_lossy())
            {
                return true;
            }
        }
        false
    }

    pub(crate) fn walk_files(&self) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut seen_entries = 0usize;
        self.walk_dir(Path::new("."), &mut files, &mut seen_entries)?;
        files.sort();
        Ok(files)
    }

    pub(crate) fn walk_dir(
        &self,
        relative: &Path,
        files: &mut Vec<PathBuf>,
        seen: &mut usize,
    ) -> Result<()> {
        if *seen >= self.path_policy.max_directory_entries {
            return Ok(());
        }
        let clean = self.normalize_tool_path(relative)?;
        let absolute = self.root.join(&clean);
        for entry in fs::read_dir(&absolute)
            .with_context(|| format!("failed to read directory {}", clean.display()))?
        {
            if *seen >= self.path_policy.max_directory_entries {
                break;
            }
            *seen += 1;
            let entry = entry?;
            let name = entry.file_name();
            let child = if clean == Path::new(".") {
                PathBuf::from(&name)
            } else {
                clean.join(&name)
            };
            if self.is_denied(&child) {
                continue;
            }
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                self.walk_dir(&child, files, seen)?;
            } else if file_type.is_file() && is_textish(&child) {
                files.push(child);
            }
        }
        Ok(())
    }
}

pub(crate) fn normalize_policy_path(path: &Path) -> Result<PathBuf> {
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("policy path must be repo-relative: {}", path.display())
            }
        }
    }
    if clean.as_os_str().is_empty() {
        clean.push(".");
    }
    Ok(clean)
}

pub(crate) fn is_textish(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("rs")
            | Some("ts")
            | Some("tsx")
            | Some("js")
            | Some("jsx")
            | Some("json")
            | Some("md")
            | Some("toml")
            | Some("yaml")
            | Some("yml")
            | Some("txt")
            | Some("py")
            | Some("mjs")
            | Some("cjs")
            | Some("html")
            | Some("css")
            | Some("sql")
    )
}
