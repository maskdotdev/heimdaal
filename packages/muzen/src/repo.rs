use std::ffi::{CString, OsStr};
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

use crate::contracts::*;

#[derive(Debug)]
pub(crate) struct RepoContext {
    pub(crate) root: PathBuf,
    pub(crate) path_policy: PathPolicyV1,
    pub(crate) change: ChangeScopeV1,
}

impl RepoContext {
    pub(crate) fn new(
        root: PathBuf,
        path_policy: PathPolicyV1,
        change: ChangeScopeV1,
    ) -> Result<Self> {
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to canonicalize repo path {}", root.display()))?;
        if !root.is_dir() {
            bail!("repo root is not a directory: {}", root.display());
        }
        Ok(Self {
            root,
            path_policy,
            change,
        })
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

    pub(crate) fn display_path(&self, relative: &Path) -> String {
        relative.to_string_lossy().into_owned()
    }

    pub(crate) fn open_readonly(&self, relative: &Path) -> Result<fs::File> {
        let clean = self.normalize_tool_path(relative)?;
        if self.path_policy.follow_symlinks {
            bail!("followSymlinks=true is intentionally unsupported in MVP");
        }
        open_relative_no_symlink(&self.root, &clean)
            .with_context(|| format!("failed to open {}", clean.display()))
    }

    pub(crate) fn read_text_file(
        &self,
        relative: &Path,
        max_bytes: usize,
    ) -> Result<ReadTextResult> {
        let clean = self.normalize_tool_path(relative)?;
        let mut file = self.open_readonly(&clean)?;
        let metadata = file
            .metadata()
            .with_context(|| format!("failed to stat {}", clean.display()))?;
        if !metadata.is_file() {
            bail!("not a regular file: {}", clean.display());
        }
        let mut bytes = Vec::new();
        std::io::Read::by_ref(&mut file)
            .take(max_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .with_context(|| format!("failed to read {}", clean.display()))?;
        let completeness = if bytes.len() > max_bytes {
            bytes.truncate(max_bytes);
            Completeness::Truncated
        } else {
            Completeness::Complete
        };
        if bytes.contains(&0) {
            bail!("binary file rejected by policy: {}", clean.display());
        }
        let content = String::from_utf8(bytes)
            .with_context(|| format!("file is not valid UTF-8: {}", clean.display()))?;
        Ok(ReadTextResult {
            path: clean,
            content,
            completeness,
        })
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

#[derive(Debug)]
pub(crate) struct ReadTextResult {
    pub(crate) path: PathBuf,
    pub(crate) content: String,
    pub(crate) completeness: Completeness,
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

#[cfg(unix)]
pub(crate) fn open_relative_no_symlink(root: &Path, relative: &Path) -> Result<fs::File> {
    let mut dir = open_dir_fd(root)?;
    let components = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_os_string()),
            Component::CurDir => None,
            _ => None,
        })
        .collect::<Vec<_>>();
    if components.is_empty() {
        bail!("cannot open repo root as a file");
    }
    for part in components.iter().take(components.len() - 1) {
        dir = openat_owned(
            dir.as_raw_fd(),
            part.as_os_str(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )?;
    }
    let file_fd = openat_owned(
        dir.as_raw_fd(),
        components.last().expect("component exists").as_os_str(),
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
    )?;
    Ok(fs::File::from(file_fd))
}

#[cfg(unix)]
pub(crate) fn open_dir_fd(path: &Path) -> Result<OwnedFd> {
    let c_path = os_str_to_cstring(path.as_os_str())?;
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("open root directory");
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(unix)]
pub(crate) fn openat_owned(parent_fd: i32, name: &OsStr, flags: i32) -> Result<OwnedFd> {
    let c_name = os_str_to_cstring(name)?;
    let fd = unsafe { libc::openat(parent_fd, c_name.as_ptr(), flags) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("openat {}", name.to_string_lossy()));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(unix)]
pub(crate) fn os_str_to_cstring(value: &OsStr) -> Result<CString> {
    CString::new(value.as_bytes()).map_err(|_| anyhow!("path contains NUL byte"))
}

#[cfg(not(unix))]
pub(crate) fn open_relative_no_symlink(root: &Path, relative: &Path) -> Result<fs::File> {
    let joined = root.join(relative);
    let canonical_root = fs::canonicalize(root)?;
    let canonical = fs::canonicalize(&joined)?;
    if !canonical.starts_with(canonical_root) {
        bail!("path escapes repo: {}", relative.display());
    }
    fs::File::open(canonical).context("open file")
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
