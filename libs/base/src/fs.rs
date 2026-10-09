#[cfg(windows)]
use std::os::windows::prelude::*;
use std::{
    fmt::{Debug, Display},
    io::Cursor,
    path::{Path, PathBuf},
    sync::atomic::{AtomicI32, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufStream as TokioBufStream},
};

use crate::message_proto::*;
// https://doc.rust-lang.org/std/os/windows/fs/trait.MetadataExt.html
use hbb_common::{
    anyhow::anyhow,
    bail,
    compress::{compress, decompress},
    config::Config,
    get_version_number, ResultType, Stream,
};

pub use crate::rsync;

static NEXT_JOB_ID: AtomicI32 = AtomicI32::new(1);

pub fn get_next_job_id() -> i32 {
    NEXT_JOB_ID.fetch_add(1, Ordering::SeqCst)
}

pub fn update_next_job_id(id: i32) {
    NEXT_JOB_ID.store(id, Ordering::SeqCst);
}

pub fn read_dir(path: &Path, include_hidden: bool) -> ResultType<FileDirectory> {
    let mut dir = FileDirectory {
        path: get_string(path),
        ..Default::default()
    };
    #[cfg(windows)]
    if "/" == &get_string(path) {
        let drives = unsafe { winapi::um::fileapi::GetLogicalDrives() };
        for i in 0..32 {
            if drives & (1 << i) != 0 {
                let name = format!(
                    "{}:",
                    std::char::from_u32('A' as u32 + i as u32).unwrap_or('A')
                );
                dir.entries.push(FileEntry {
                    name,
                    entry_type: FileType::DirDrive.into(),
                    ..Default::default()
                });
            }
        }
        return Ok(dir);
    }
    for entry in path.read_dir()?.flatten() {
        let p = entry.path();
        let name = p
            .file_name()
            .map(|p| p.to_str().unwrap_or(""))
            .unwrap_or("")
            .to_owned();
        if name.is_empty() {
            continue;
        }
        let mut is_hidden = false;
        let meta;
        if let Ok(tmp) = std::fs::symlink_metadata(&p) {
            meta = tmp;
        } else {
            continue;
        }
        // docs.microsoft.com/en-us/windows/win32/fileio/file-attribute-constants
        #[cfg(windows)]
        if meta.file_attributes() & 0x2 != 0 {
            is_hidden = true;
        }
        #[cfg(not(windows))]
        if name.find('.').unwrap_or(usize::MAX) == 0 {
            is_hidden = true;
        }
        if is_hidden && !include_hidden {
            continue;
        }
        let (entry_type, size) = {
            if p.is_dir() {
                if meta.file_type().is_symlink() {
                    (FileType::DirLink.into(), 0)
                } else {
                    (FileType::Dir.into(), 0)
                }
            } else if meta.file_type().is_symlink() {
                (FileType::FileLink.into(), 0)
            } else {
                (FileType::File.into(), meta.len())
            }
        };
        let modified_time = meta
            .modified()
            .map(|x| {
                x.duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|x| x.as_secs())
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        dir.entries.push(FileEntry {
            name: get_file_name(&p),
            entry_type,
            is_hidden,
            size,
            modified_time,
            ..Default::default()
        });
    }
    Ok(dir)
}

#[inline]
pub fn get_file_name(p: &Path) -> String {
    p.file_name()
        .map(|p| p.to_str().unwrap_or(""))
        .unwrap_or("")
        .to_owned()
}

#[inline]
pub fn get_string(path: &Path) -> String {
    path.to_str().unwrap_or("").to_owned()
}

#[inline]
pub fn get_path(path: &str) -> PathBuf {
    Path::new(path).to_path_buf()
}

#[inline]
pub fn get_home_as_string() -> String {
    get_string(&Config::get_home())
}

fn read_dir_recursive(
    path: &Path,
    prefix: &Path,
    include_hidden: bool,
) -> ResultType<Vec<FileEntry>> {
    let mut files = Vec::new();
    if path.is_dir() {
        // to-do: symbol link handling, cp the link rather than the content
        // to-do: file mode, for unix
        let fd = read_dir(path, include_hidden)?;
        for entry in fd.entries.iter() {
            match entry.entry_type.enum_value() {
                Ok(FileType::File) => {
                    let mut entry = entry.clone();
                    entry.name = get_string(&prefix.join(entry.name));
                    files.push(entry);
                }
                Ok(FileType::Dir) => {
                    if let Ok(mut tmp) = read_dir_recursive(
                        &path.join(&entry.name),
                        &prefix.join(&entry.name),
                        include_hidden,
                    ) {
                        for entry in tmp.drain(0..) {
                            files.push(entry);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(files)
    } else if path.is_file() {
        let (size, modified_time) = if let Ok(meta) = std::fs::metadata(path) {
            (
                meta.len(),
                meta.modified()
                    .map(|x| {
                        x.duration_since(std::time::SystemTime::UNIX_EPOCH)
                            .map(|x| x.as_secs())
                            .unwrap_or(0)
                    })
                    .unwrap_or(0),
            )
        } else {
            (0, 0)
        };
        files.push(FileEntry {
            entry_type: FileType::File.into(),
            size,
            modified_time,
            ..Default::default()
        });
        Ok(files)
    } else {
        bail!("Not exists");
    }
}

pub fn get_recursive_files(path: &str, include_hidden: bool) -> ResultType<Vec<FileEntry>> {
    read_dir_recursive(&get_path(path), &get_path(""), include_hidden)
}

fn read_empty_dirs_recursive(
    path: &Path,
    prefix: &Path,
    include_hidden: bool,
) -> ResultType<Vec<FileDirectory>> {
    let mut dirs = Vec::new();
    if path.is_dir() {
        // to-do: symbol link handling, cp the link rather than the content
        // to-do: file mode, for unix
        let fd = read_dir(path, include_hidden)?;
        if fd.entries.is_empty() {
            dirs.push(fd);
        } else {
            for entry in fd.entries.iter() {
                match entry.entry_type.enum_value() {
                    Ok(FileType::Dir) => {
                        if let Ok(mut tmp) = read_empty_dirs_recursive(
                            &path.join(&entry.name),
                            &prefix.join(&entry.name),
                            include_hidden,
                        ) {
                            for entry in tmp.drain(0..) {
                                dirs.push(entry);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(dirs)
    } else if path.is_file() {
        Ok(dirs)
    } else {
        bail!("Not exists");
    }
}

pub fn get_empty_dirs_recursive(
    path: &str,
    include_hidden: bool,
) -> ResultType<Vec<FileDirectory>> {
    read_empty_dirs_recursive(&get_path(path), &get_path(""), include_hidden)
}

#[inline]
pub fn is_file_exists(file_path: &str) -> bool {
    return Path::new(file_path).exists();
}

#[inline]
pub fn can_enable_overwrite_detection(version: i64) -> bool {
    version >= get_version_number("1.1.10")
}

#[repr(i32)]
#[derive(Copy, Clone, Serialize, Debug, PartialEq)]
pub enum JobType {
    Generic = 0,
    Printer = 1,
}

impl Default for JobType {
    fn default() -> Self {
        JobType::Generic
    }
}

impl From<JobType> for file_transfer_send_request::FileType {
    fn from(t: JobType) -> Self {
        match t {
            JobType::Generic => file_transfer_send_request::FileType::Generic,
            JobType::Printer => file_transfer_send_request::FileType::Printer,
        }
    }
}

impl From<i32> for JobType {
    fn from(value: i32) -> Self {
        match value {
            0 => JobType::Generic,
            1 => JobType::Printer,
            _ => JobType::Generic,
        }
    }
}

impl Into<i32> for JobType {
    fn into(self) -> i32 {
        self as i32
    }
}

impl JobType {
    pub fn from_proto(t: ::protobuf::EnumOrUnknown<file_transfer_send_request::FileType>) -> Self {
        match t.enum_value() {
            Ok(file_transfer_send_request::FileType::Generic) => JobType::Generic,
            Ok(file_transfer_send_request::FileType::Printer) => JobType::Printer,
            _ => JobType::Generic,
        }
    }
}

#[derive(Debug)]
pub enum DataSource {
    FilePath(PathBuf),
    MemoryCursor(Cursor<Vec<u8>>),
}

impl Default for DataSource {
    fn default() -> Self {
        DataSource::FilePath(PathBuf::new())
    }
}

impl serde::Serialize for DataSource {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            DataSource::FilePath(p) => serializer.serialize_str(p.to_str().unwrap_or("")),
            DataSource::MemoryCursor(_) => serializer.serialize_str(""),
        }
    }
}

impl Display for DataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataSource::FilePath(p) => write!(f, "File: {}", p.to_string_lossy().to_string()),
            DataSource::MemoryCursor(_) => write!(f, "Bytes"),
        }
    }
}

impl DataSource {
    fn to_meta(&self) -> String {
        match self {
            DataSource::FilePath(p) => p.to_string_lossy().to_string(),
            DataSource::MemoryCursor(_) => "".to_string(),
        }
    }
}

enum DataStream {
    FileStream(File),
    BufStream(TokioBufStream<Cursor<Vec<u8>>>),
}

impl Debug for DataStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataStream::FileStream(fs) => write!(f, "{:?}", fs),
            DataStream::BufStream(_) => write!(f, "BufStream"),
        }
    }
}

impl DataStream {
    async fn write_all(&mut self, buf: &[u8]) -> ResultType<()> {
        match self {
            DataStream::FileStream(fs) => fs.write_all(buf).await?,
            DataStream::BufStream(bs) => bs.write_all(buf).await?,
        }
        Ok(())
    }

    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            DataStream::FileStream(fs) => fs.read(buf).await,
            DataStream::BufStream(bs) => bs.read(buf).await,
        }
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct FileDigest {
    pub size: u64,
    pub modified: u64,
}

/// State machine for rsync-style incremental transfer of the current file.
/// Both sides start in `Off` and only enter the protocol after an explicit
/// overwrite confirm (`offset_blk == 0`) when eligibility holds.
///
/// Reader = the side that has the NEW file and streams data (read job).
/// Writer = the side that has the OLD file and receives data (write job).
#[derive(Debug)]
pub enum RsyncState {
    Off,
    // ---- reader (new-file side) ----
    /// Confirmed overwrite; waiting for RsyncMeta from the writer.
    AwaitSignature { since: std::time::Instant },
    /// Receiving signature chunks.
    WaitingSigChunks { assembler: rsync::ChunkAssembler },
    /// Signature complete; the caller is running the blocking diff.
    Diffing,
    /// Streaming delta chunks from the spool (one per pump tick).
    SendingDelta {
        reader: rsync::SpoolReader,
        next_chunk: u32,
        num_chunks: u32,
        file_size: u64,
    },
    /// Last delta chunk sent; waiting for the writer's per-file ack before
    /// advancing (an apply failure must be able to revert this file to
    /// legacy transfer).
    AwaitApply { since: std::time::Instant },
    // ---- writer (old-file side) ----
    /// Confirmed overwrite; the caller should compute and send the signature.
    PrepareSignature,
    /// Signature sent; waiting for RsyncDeltaMeta.
    AwaitDelta,
    /// Receiving delta chunks (accumulated in memory, bounded by
    /// `rsync::MAX_DELTA_BYTES`).
    ReceivingDelta {
        assembler: rsync::ChunkAssembler,
        next_index: u32,
    },
    /// Delta complete; the caller should run apply + verify.
    DeltaReady {
        delta: Vec<u8>,
        new_file_size: u64,
        sha256: [u8; 32],
        last_modified: u64,
    },
}

impl Default for RsyncState {
    fn default() -> Self {
        Self::Off
    }
}

impl RsyncState {
    #[inline]
    pub fn is_off(&self) -> bool {
        matches!(self, Self::Off)
    }
}

/// One pump-tick unit of a read job's output.
#[derive(Debug)]
pub enum JobChunk {
    Block(FileTransferBlock),
    RsyncDelta(FileTransferRsyncChunk),
}

/// What the caller of `TransferJob::confirm` must do right after an
/// overwrite confirm, for the writer (old-file) side.
#[derive(Debug, PartialEq)]
pub enum RsyncConfirmAction {
    Nothing,
    /// Compute the signature of the old file (`rsync_signature_path`) and
    /// send RsyncMeta + chunks, then call `rsync_signature_sent`.
    ComputeSignature,
    /// The reader may be waiting for a signature that cannot be produced;
    /// send FileTransferRsyncFallback so it resumes legacy transfer now.
    SendFallback,
}

/// Parameters for the blocking apply + verify of a completed delta.
#[derive(Debug)]
pub struct RsyncApplyParams {
    pub old_file: std::path::PathBuf,
    pub delta: Vec<u8>,
    pub out_file: std::path::PathBuf,
    pub new_file_size: u64,
    pub sha256: [u8; 32],
}

#[derive(Default, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransferJob {
    pub id: i32,
    pub r#type: JobType,
    pub remote: String,
    pub data_source: DataSource,
    pub show_hidden: bool,
    pub is_remote: bool,
    pub is_last_job: bool,
    pub is_resume: bool,
    pub file_num: i32,
    #[serde(skip_serializing)]
    files: Vec<FileEntry>,
    pub conn_id: i32, // server only

    #[serde(skip_serializing)]
    data_stream: Option<DataStream>,
    pub total_size: u64,
    finished_size: u64,
    transferred: u64,
    enable_overwrite_detection: bool,
    file_confirmed: bool,
    // indicating the last file is skipped
    file_skipped: bool,
    file_is_waiting: bool,
    default_overwrite_strategy: Option<bool>,
    #[serde(skip_serializing)]
    digest: FileDigest,

    // ---- rsync incremental transfer support ----
    /// Whether rsync incremental transfer was requested for this job.
    rsync_enabled: bool,
    #[serde(skip_serializing)]
    rsync: RsyncState,
    /// Amount of `finished_size` booked for rsync progress of the current
    /// file; subtracted on fallback so progress never goes past 100%.
    #[serde(skip_serializing)]
    rsync_counted: u64,
    /// Set when the current file was finalized by rsync (rename done);
    /// consumed by the next `modify_time` call to avoid double-finalizing.
    #[serde(skip_serializing)]
    rsync_finalized: bool,
    /// True for read jobs (this side streams the new file), false for write
    /// jobs (this side holds the old file). Note `is_remote` means something
    /// different (which machine the job's path lives on).
    #[serde(skip_serializing)]
    is_reader: bool,
    /// Writer side: old file missing after overwrite confirm — the caller
    /// must send RsyncFallback (the reader is waiting for a signature).
    #[serde(skip_serializing)]
    rsync_old_file_missing: bool,
    /// Writer side: (new_file_size, sha256_new, last_modified) from
    /// RsyncDeltaMeta, kept until the delta is complete.
    #[serde(skip_serializing)]
    rsync_delta_meta: Option<(u64, [u8; 32], u64)>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct TransferJobMeta {
    #[serde(default)]
    pub id: i32,
    #[serde(default)]
    pub remote: String,
    #[serde(default)]
    pub to: String,
    #[serde(default)]
    pub show_hidden: bool,
    #[serde(default)]
    pub file_num: i32,
    #[serde(default)]
    pub is_remote: bool,
    #[serde(default)]
    pub enable_rsync: bool,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct RemoveJobMeta {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub is_remote: bool,
    #[serde(default)]
    pub no_confirm: bool,
}

#[inline]
fn get_ext(name: &str) -> &str {
    if let Some(i) = name.rfind('.') {
        return &name[i + 1..];
    }
    ""
}

#[inline]
fn is_compressed_file(name: &str) -> bool {
    let compressed_exts = ["xz", "gz", "zip", "7z", "rar", "bz2", "tgz", "png", "jpg"];
    let ext = get_ext(name);
    compressed_exts.contains(&ext)
}

pub fn validate_file_name_no_traversal(name: &str) -> ResultType<()> {
    if name.bytes().any(|b| b == 0) {
        bail!("file name contains null bytes");
    }
    let has_traversal = name
        .split(|c: char| c == '/' || (cfg!(windows) && c == '\\'))
        .filter(|s| !s.is_empty())
        .any(|s| s == "..");
    if has_traversal {
        bail!("path traversal detected in file name");
    }
    #[cfg(windows)]
    {
        if name.len() >= 2 {
            let bytes = name.as_bytes();
            if bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
                bail!("absolute path detected in file name");
            }
        }
        if name.starts_with('/') || name.starts_with('\\') {
            bail!("absolute path detected in file name");
        }
    }
    #[cfg(not(windows))]
    if name.starts_with('/') {
        bail!("absolute path detected in file name");
    }
    Ok(())
}

fn validate_transfer_file_names(files: &[FileEntry]) -> ResultType<()> {
    // Single-file transfer may use an empty relative name, because
    // the destination file path is carried by transfer metadata.
    if files.len() == 1 && files.first().map_or(false, |f| f.name.is_empty()) {
        return Ok(());
    }
    for file in files {
        if file.name.is_empty() {
            bail!("empty file name in multi-file transfer");
        }
        validate_file_name_no_traversal(&file.name)?;
    }
    Ok(())
}

#[inline]
fn validate_fs_path_argument(path: &str, arg_name: &str) -> ResultType<()> {
    if path.is_empty() {
        bail!("{arg_name} cannot be empty");
    }
    if path.bytes().any(|b| b == 0) {
        bail!("{arg_name} contains null bytes");
    }
    Ok(())
}

fn validate_no_symlink_components(base: &PathBuf, name: &str) -> ResultType<()> {
    if name.is_empty() {
        return Ok(());
    }
    let mut current = base.clone();
    for component in Path::new(name).components() {
        match component {
            std::path::Component::Normal(seg) => {
                current.push(seg);
                // Best-effort guard: path-based checks are inherently TOCTOU-prone
                // if local filesystem state changes between validation and write.
                match std::fs::symlink_metadata(&current) {
                    Ok(meta) => {
                        // This is inherent to filesystem-based checks and acknowledged as a limitation.
                        // For true protection, you'd need openat(2) / O_NOFOLLOW at write time.
                        if meta.file_type().is_symlink() {
                            bail!("symlink path component is not allowed");
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        // Component does not exist yet, continue best-effort validation.
                    }
                    Err(err) => {
                        bail!(
                            "failed to validate path component '{}': {}",
                            current.display(),
                            err
                        );
                    }
                }
            }
            std::path::Component::CurDir => {}
            _ => {
                bail!("invalid file name component");
            }
        }
    }
    Ok(())
}

/// Validate an untrusted relative file name and existing path components before joining it.
pub fn join_validated_path(base: &PathBuf, name: &str) -> ResultType<PathBuf> {
    validate_file_name_no_traversal(name)?;
    validate_no_symlink_components(base, name)?;
    Ok(TransferJob::join(base, name))
}

impl TransferJob {
    #[allow(clippy::too_many_arguments)]
    pub fn new_write(
        id: i32,
        r#type: JobType,
        remote: String,
        data_source: DataSource,
        file_num: i32,
        show_hidden: bool,
        is_remote: bool,
        enable_overwrite_detection: bool,
    ) -> Self {
        log::info!("new write {}", data_source);
        Self {
            id,
            r#type,
            remote,
            data_source,
            file_num,
            show_hidden,
            is_remote,
            files: Vec::new(),
            total_size: 0,
            enable_overwrite_detection,
            ..Default::default()
        }
    }

    pub fn with_files(mut self, files: Vec<FileEntry>) -> ResultType<Self> {
        self.set_files(files)?;
        Ok(self)
    }

    pub fn new_read(
        id: i32,
        r#type: JobType,
        remote: String,
        data_source: DataSource,
        file_num: i32,
        show_hidden: bool,
        is_remote: bool,
        enable_overwrite_detection: bool,
    ) -> ResultType<Self> {
        log::info!("new read {}", data_source);
        let (files, total_size) = match &data_source {
            DataSource::FilePath(p) => {
                let p = p.to_str().ok_or(anyhow!("Invalid path"))?;
                let files = get_recursive_files(p, show_hidden)?;
                let total_size = files.iter().map(|x| x.size).sum();
                (files, total_size)
            }
            DataSource::MemoryCursor(c) => (Vec::new(), c.get_ref().len() as u64),
        };
        Ok(Self {
            id,
            r#type,
            remote,
            data_source,
            file_num,
            show_hidden,
            is_remote,
            files,
            total_size,
            enable_overwrite_detection,
            is_reader: true,
            ..Default::default()
        })
    }

    pub async fn get_buf_data(self) -> ResultType<Option<Vec<u8>>> {
        match self.data_stream {
            Some(DataStream::BufStream(mut bs)) => {
                bs.flush().await?;
                Ok(Some(bs.into_inner().into_inner()))
            }
            _ => Ok(None),
        }
    }

    #[inline]
    pub fn files(&self) -> &Vec<FileEntry> {
        &self.files
    }

    #[inline]
    pub fn set_files(&mut self, files: Vec<FileEntry>) -> ResultType<()> {
        validate_transfer_file_names(&files)?;
        if let DataSource::FilePath(base) = &self.data_source {
            for file in &files {
                validate_no_symlink_components(base, &file.name)?;
            }
        }
        self.total_size = files.iter().map(|x| x.size).sum();
        self.files = files;
        Ok(())
    }

    #[inline]
    pub fn set_digest(&mut self, size: u64, modified: u64) {
        self.digest.size = size;
        self.digest.modified = modified;
    }

    #[inline]
    pub fn id(&self) -> i32 {
        self.id
    }

    #[inline]
    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    #[inline]
    pub fn finished_size(&self) -> u64 {
        self.finished_size
    }

    #[inline]
    pub fn transferred(&self) -> u64 {
        self.transferred
    }

    #[inline]
    pub fn file_num(&self) -> i32 {
        self.file_num
    }

    fn resolve_entry_path(&self, base: &PathBuf, name: &str) -> Option<PathBuf> {
        if self.r#type == JobType::Generic {
            match join_validated_path(base, name) {
                Ok(path) => Some(path),
                Err(err) => {
                    log::error!("Invalid file name in transfer job {}: {}", self.id, err);
                    None
                }
            }
        } else {
            Some(Self::join(base, name))
        }
    }

    pub fn modify_time(&self) {
        if self.r#type == JobType::Printer {
            return;
        }
        if self.rsync_finalized {
            // rsync_finish already renamed the verified output, restored the
            // mtime and cleaned up temps; nothing to do for this file.
            // (The flag is reset by the caller when moving to the next file.)
            return;
        }
        if let DataSource::FilePath(p) = &self.data_source {
            let file_num = self.file_num as usize;
            if file_num < self.files.len() {
                let entry = &self.files[file_num];
                let Some(path) = self.resolve_entry_path(p, &entry.name) else {
                    return;
                };
                let download_path = format!("{}.download", get_string(&path));
                let digest_path = format!("{}.digest", get_string(&path));
                std::fs::remove_file(digest_path).ok();
                std::fs::rename(download_path, &path).ok();
                filetime::set_file_mtime(
                    &path,
                    filetime::FileTime::from_unix_time(entry.modified_time as _, 0),
                )
                .ok();
            }
        }
    }

    pub fn remove_download_file(&self) {
        if self.r#type == JobType::Printer {
            return;
        }
        if let DataSource::FilePath(p) = &self.data_source {
            let file_num = self.file_num as usize;
            if file_num < self.files.len() {
                let entry = &self.files[file_num];
                let Some(path) = self.resolve_entry_path(p, &entry.name) else {
                    return;
                };
                let download_path = format!("{}.download", get_string(&path));
                let digest_path = format!("{}.digest", get_string(&path));
                std::fs::remove_file(download_path).ok();
                std::fs::remove_file(digest_path).ok();
                rsync::cleanup_rsync_temps(&path);
            }
        }
    }

    #[inline]
    pub fn set_finished_size_on_resume(&mut self) {
        if self.is_resume && self.file_num > 0 {
            let finished_size: u64 = self
                .files
                .iter()
                .take(self.file_num as usize)
                .map(|file| file.size)
                .sum();
            self.finished_size = finished_size;
        }
    }

    pub async fn write(&mut self, block: FileTransferBlock) -> ResultType<()> {
        if block.id != self.id {
            bail!("Wrong id");
        }
        if !self.rsync.is_off() {
            // Legacy blocks arriving during an rsync attempt mean the peer
            // fell back (old peer, or its own fallback decision). Reset and
            // let the normal block path recreate `<file>.download`.
            self.rsync_fallback_local().await;
        }
        match &self.data_source {
            DataSource::FilePath(p) => {
                let file_num = block.file_num as usize;
                if file_num >= self.files.len() {
                    bail!("Wrong file number");
                }
                if file_num != self.file_num as usize || self.data_stream.is_none() {
                    self.modify_time();
                    self.rsync_finalized = false;
                    if let Some(DataStream::FileStream(file)) = self.data_stream.as_mut() {
                        file.sync_all().await?;
                    }
                    self.file_num = block.file_num;
                    let entry = &self.files[file_num];
                    let (path, digest_path) = if self.r#type == JobType::Printer {
                        (p.to_string_lossy().to_string(), None)
                    } else {
                        let path = join_validated_path(p, &entry.name)?;
                        // NOTE: We intentionally keep path-based validation + regular file open here.
                        // This still has a known TOCTOU window for symlink races, but avoids a large
                        // cross-platform rewrite for now.
                        // Revisit with descriptor/handle-based no-follow open in future hardening.
                        if let Some(pp) = path.parent() {
                            std::fs::create_dir_all(pp).ok();
                        }
                        let file_path = get_string(&path);
                        (
                            format!("{}.download", &file_path),
                            Some(format!("{}.digest", &file_path)),
                        )
                    };
                    if let Some(dp) = digest_path.as_ref() {
                        if Path::new(dp).exists() {
                            std::fs::remove_file(dp)?;
                        }
                    }
                    self.data_stream = Some(DataStream::FileStream(File::create(&path).await?));
                    if let Some(dp) = digest_path.as_ref() {
                        std::fs::write(dp, json!(self.digest).to_string()).ok();
                    }
                }
            }
            DataSource::MemoryCursor(c) => {
                if self.data_stream.is_none() {
                    self.data_stream = Some(DataStream::BufStream(TokioBufStream::new(c.clone())));
                }
            }
        }
        if block.compressed {
            let tmp = decompress(&block.data);
            self.data_stream
                .as_mut()
                .ok_or(anyhow!("data stream is None"))?
                .write_all(&tmp)
                .await?;
            self.finished_size += tmp.len() as u64;
        } else {
            self.data_stream
                .as_mut()
                .ok_or(anyhow!("file is None"))?
                .write_all(&block.data)
                .await?;
            self.finished_size += block.data.len() as u64;
        }
        self.transferred += block.data.len() as u64;
        Ok(())
    }

    #[inline]
    pub fn join(p: &PathBuf, name: &str) -> PathBuf {
        if name.is_empty() {
            p.clone()
        } else {
            p.join(name)
        }
    }

    /// Open the data stream for the current file.
    /// Returns Ok(true) if job is done, Ok(false) otherwise.
    async fn open_data_stream(&mut self) -> ResultType<bool> {
        let file_num = self.file_num as usize;
        match &mut self.data_source {
            DataSource::FilePath(p) => {
                if file_num >= self.files.len() {
                    // job done
                    self.data_stream.take();
                    return Ok(true);
                };
                if self.data_stream.is_none() {
                    match File::open(Self::join(p, &self.files[file_num].name)).await {
                        Ok(file) => {
                            self.data_stream = Some(DataStream::FileStream(file));
                            self.file_confirmed = false;
                            self.file_is_waiting = false;
                        }
                        // On open error, behave the same as validation failure: advance
                        // to next file and return the error.
                        Err(err) => {
                            self.file_num += 1;
                            self.file_confirmed = false;
                            self.file_is_waiting = false;
                            return Err(err.into());
                        }
                    }
                }
            }
            DataSource::MemoryCursor(c) => {
                if self.data_stream.is_none() {
                    let mut t = std::io::Cursor::new(Vec::new());
                    std::mem::swap(&mut t, c);
                    self.data_stream = Some(DataStream::BufStream(TokioBufStream::new(t)));
                }
            }
        }
        Ok(false)
    }

    /// Get current file's digest (last_modified, file_size) for overwrite detection.
    async fn get_current_digest(&self) -> ResultType<(u64, u64)> {
        let meta = match self.data_stream.as_ref().ok_or(anyhow!("file is None"))? {
            DataStream::FileStream(file) => file.metadata().await?,
            DataStream::BufStream(_) => bail!("No digest for buf stream"),
        };
        let last_modified = meta
            .modified()?
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        Ok((last_modified, meta.len()))
    }

    async fn init_data_stream(&mut self, stream: &mut hbb_common::Stream) -> ResultType<()> {
        if self.open_data_stream().await? {
            return Ok(());
        }
        if self.r#type == JobType::Generic
            && self.enable_overwrite_detection
            && !self.file_confirmed()
            && !self.file_is_waiting()
        {
            self.send_current_digest(stream).await?;
            self.set_file_is_waiting(true);
        }
        Ok(())
    }

    /// Initialize data stream for CM (Connection Manager) scenario.
    /// Returns digest info (last_modified, file_size) if overwrite detection is enabled,
    /// so caller can send it via IPC instead of network stream.
    /// Returns Ok(None) if job is done or already initialized.
    pub async fn init_data_stream_for_cm(&mut self) -> ResultType<Option<(u64, u64)>> {
        if self.open_data_stream().await? {
            return Ok(None);
        }
        // For overwrite detection, return digest info instead of sending via stream
        if self.r#type == JobType::Generic
            && self.enable_overwrite_detection
            && !self.file_confirmed()
            && !self.file_is_waiting()
        {
            let digest = self.get_current_digest().await?;
            self.set_file_is_waiting(true);
            return Ok(Some(digest));
        }
        Ok(None)
    }

    pub async fn read(&mut self) -> ResultType<Option<FileTransferBlock>> {
        if self.r#type == JobType::Generic {
            if self.enable_overwrite_detection && !self.file_confirmed() {
                return Ok(None);
            }
        }

        let file_num = self.file_num as usize;
        let name = match &self.data_source {
            DataSource::FilePath(p) => {
                if file_num >= self.files.len() {
                    self.data_stream.take();
                    return Ok(None);
                };
                if self.files.len() == 1 && self.files[file_num].name.is_empty() {
                    p.file_name()
                        .map(|p| p.to_str().unwrap_or(""))
                        .unwrap_or("")
                } else {
                    &self.files[file_num].name
                }
            }
            DataSource::MemoryCursor(..) => "",
        };
        const BUF_SIZE: usize = 128 * 1024;
        let mut buf: Vec<u8> = vec![0; BUF_SIZE];
        let mut compressed = false;
        let mut offset: usize = 0;
        loop {
            match self
                .data_stream
                .as_mut()
                .ok_or(anyhow!("data stream is None"))?
                .read(&mut buf[offset..])
                .await
            {
                Err(err) => {
                    self.file_num += 1;
                    self.data_stream = None;
                    self.file_confirmed = false;
                    self.file_is_waiting = false;
                    return Err(err.into());
                }
                Ok(n) => {
                    offset += n;
                    if n == 0 || offset == BUF_SIZE {
                        break;
                    }
                }
            }
        }
        unsafe { buf.set_len(offset) };
        if offset == 0 {
            if matches!(self.data_source, DataSource::MemoryCursor(_)) {
                self.data_stream.take();
                return Ok(None);
            }
            self.file_num += 1;
            self.data_stream = None;
            self.file_confirmed = false;
            self.file_is_waiting = false;
        } else {
            self.finished_size += offset as u64;
            if matches!(self.data_source, DataSource::FilePath(_)) && !is_compressed_file(name) {
                let tmp = compress(&buf);
                if tmp.len() < buf.len() {
                    buf = tmp;
                    compressed = true;
                }
            }
            self.transferred += buf.len() as u64;
        }
        Ok(Some(FileTransferBlock {
            id: self.id,
            file_num: file_num as _,
            data: buf.into(),
            compressed,
            ..Default::default()
        }))
    }

    /// One unit of output per pump tick: a legacy file block or an rsync
    /// delta chunk. `None` means "nothing to send right now" (waiting for
    /// confirmation / signature / apply ack, or job done) — same semantics
    /// as `read()`.
    pub async fn read_chunk(&mut self) -> ResultType<Option<JobChunk>> {
        // Timeout-driven local fallbacks first (old peers never answer).
        let timed_out = match &self.rsync {
            RsyncState::AwaitSignature { since }
                if since.elapsed() > rsync::SIGNATURE_WAIT_TIMEOUT =>
            {
                true
            }
            RsyncState::AwaitApply { since }
                if since.elapsed() > rsync::APPLY_WAIT_TIMEOUT =>
            {
                true
            }
            _ => false,
        };
        if timed_out {
            self.rsync_fallback_local().await;
        }
        if let RsyncState::SendingDelta { .. } = &self.rsync {
            return self
                .rsync_next_delta_chunk()
                .await
                .map(|o| o.map(JobChunk::RsyncDelta));
        }
        if !self.rsync.is_off() {
            // Waiting for signature chunks / diff result / apply ack.
            return Ok(None);
        }
        self.read().await.map(|o| o.map(JobChunk::Block))
    }

    /// Emit at most one delta chunk from the spool; on exhaustion enter
    /// `AwaitApply` (the writer must ack before this file is advanced).
    async fn rsync_next_delta_chunk(&mut self) -> ResultType<Option<FileTransferRsyncChunk>> {
        let file_num = self.file_num;
        let chunk = {
            let RsyncState::SendingDelta {
                reader,
                next_chunk,
                num_chunks,
                file_size,
            } = &mut self.rsync
            else {
                unreachable!()
            };
            match reader.next_chunk()? {
                None => None,
                Some((data, compressed)) => {
                    let wire_len = data.len();
                    let chunk = FileTransferRsyncChunk {
                        id: self.id,
                        file_num,
                        chunk_index: *next_chunk,
                        is_delta: true,
                        compressed,
                        data: data.into(),
                        ..Default::default()
                    };
                    // Progress bookkeeping: honest wire bytes for speed,
                    // proportional file-size progress for the UI bar.
                    self.transferred += wire_len as u64;
                    let per_chunk = if *num_chunks > 0 {
                        *file_size / *num_chunks as u64
                    } else {
                        0
                    };
                    self.finished_size += per_chunk;
                    self.rsync_counted += per_chunk;
                    *next_chunk += 1;
                    Some(chunk)
                }
            }
        };
        match chunk {
            Some(c) => Ok(Some(c)),
            None => {
                // Snap progress to the exact file size and wait for the ack.
                if let RsyncState::SendingDelta { file_size, .. } = &self.rsync {
                    let file_size = *file_size;
                    self.finished_size += file_size.saturating_sub(self.rsync_counted);
                    self.rsync_counted = file_size;
                }
                self.rsync = RsyncState::AwaitApply {
                    since: std::time::Instant::now(),
                };
                Ok(None)
            }
        }
    }

    // Only for generic job and file stream
    async fn send_current_digest(&mut self, stream: &mut Stream) -> ResultType<()> {
        let (last_modified, file_size) = self.get_current_digest().await?;
        let mut msg = Message::new();
        let mut resp = FileResponse::new();
        resp.set_digest(FileTransferDigest {
            id: self.id,
            file_num: self.file_num,
            last_modified,
            file_size,
            is_resume: self.is_resume,
            ..Default::default()
        });
        msg.set_file_response(resp);
        stream.send(&msg).await?;
        log::info!(
            "id: {}, file_num: {}, digest message is sent. waiting for confirm. msg: {:?}",
            self.id,
            self.file_num,
            msg
        );
        Ok(())
    }

    pub fn set_overwrite_strategy(&mut self, overwrite_strategy: Option<bool>) {
        self.default_overwrite_strategy = overwrite_strategy;
    }

    pub fn default_overwrite_strategy(&self) -> Option<bool> {
        self.default_overwrite_strategy
    }

    pub fn set_file_confirmed(&mut self, file_confirmed: bool) {
        log::info!("id: {}, file_confirmed: {}", self.id, file_confirmed);
        self.file_confirmed = file_confirmed;
        self.file_skipped = false;
    }

    pub fn set_file_is_waiting(&mut self, file_is_waiting: bool) {
        self.file_is_waiting = file_is_waiting;
    }

    #[inline]
    pub fn file_is_waiting(&self) -> bool {
        self.file_is_waiting
    }

    #[inline]
    pub fn file_confirmed(&self) -> bool {
        self.file_confirmed
    }

    /// Indicating whether the last file is skipped
    #[inline]
    pub fn file_skipped(&self) -> bool {
        self.file_skipped
    }

    /// Indicating whether the whole task is skipped
    #[inline]
    pub fn job_skipped(&self) -> bool {
        self.file_skipped() && self.files.len() == 1
    }

    /// Check whether the job is completed after `read` returns `None`
    /// This is a helper function which gives additional lifecycle when the job reads `None`.
    /// If returns `true`, it means we can delete the job automatically. `False` otherwise.
    ///
    /// [`Note`]
    /// Conditions:
    /// 1. Files are not waiting for confirmation by peers.
    #[inline]
    pub fn job_completed(&self) -> bool {
        // has no error, Condition 2
        !self.enable_overwrite_detection || (!self.file_confirmed && !self.file_is_waiting)
    }

    /// Get job error message, useful for getting status when job had finished
    pub fn job_error(&self) -> Option<String> {
        if self.job_skipped() {
            return Some("skipped".to_string());
        }
        None
    }

    pub fn set_file_skipped(&mut self) -> bool {
        log::debug!("skip file {} in job {}", self.file_num, self.id);
        self.data_stream.take();
        self.set_file_confirmed(false);
        self.set_file_is_waiting(false);
        self.file_num += 1;
        self.file_skipped = true;
        true
    }

    async fn set_stream_offset(&mut self, file_num: usize, offset: u64) {
        if file_num >= self.files.len() {
            return;
        }
        if let DataSource::FilePath(p) = &self.data_source {
            let entry = &self.files[file_num];
            let Some(path) = self.resolve_entry_path(p, &entry.name) else {
                return;
            };
            let file_path = get_string(&path);
            let download_path = format!("{}.download", &file_path);
            let digest_path = format!("{}.digest", &file_path);

            let mut f = if Path::new(&download_path).exists() && Path::new(&digest_path).exists() {
                // If both download and digest files exist, seek (writer) to the offset
                // NOTE: same as write path: best-effort symlink validation happened earlier,
                // but this reopen remains TOCTOU-prone by design for now.
                match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&download_path)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        log::warn!("Failed to open file {}: {}", download_path, e);
                        return;
                    }
                }
            } else if Path::new(&file_path).exists() {
                // If `file_path` exists, seek (reader) to the offset
                match File::open(&file_path).await {
                    Ok(f) => f,
                    Err(e) => {
                        log::warn!("Failed to open file {}: {}", file_path, e);
                        return;
                    }
                }
            } else {
                log::warn!(
                    "File {} not found, cannot seek to offset {}",
                    file_path,
                    offset
                );
                return;
            };
            if f.seek(std::io::SeekFrom::Start(offset)).await.is_ok() {
                self.data_stream = Some(DataStream::FileStream(f));
                self.transferred += offset;
                self.finished_size += offset;
            }
        }
    }

    pub async fn confirm(&mut self, r: &FileTransferSendConfirmRequest) -> bool {
        if self.file_num() != r.file_num {
            // This branch will always be hit if:
            // 1. `confirm()` is called in `ui_cm_interface.rs`
            // 2. Not resuming
            //
            // It is ok. Because `confirm()` in `ui_cm_interface.rs` is only used for resuming.
            log::info!("file num truncated, ignoring");
        } else {
            match r.union {
                Some(file_transfer_send_confirm_request::Union::Skip(s)) => {
                    if s {
                        self.set_file_skipped();
                    } else {
                        self.set_file_confirmed(true);
                    }
                }
                Some(file_transfer_send_confirm_request::Union::OffsetBlk(offset)) => {
                    self.set_file_confirmed(true);
                    // If offset is greater than 0, we need to seek to the offset
                    if offset > 0 {
                        self.set_stream_offset(r.file_num as usize, offset as u64)
                            .await;
                    } else {
                        // A plain overwrite confirm: consider rsync mode for
                        // this file. Skip (above) and resume (offset > 0)
                        // never enter rsync.
                        self.rsync_on_overwrite_confirm();
                    }
                }
                _ => {}
            }
        }
        true
    }

    #[inline]
    pub fn gen_meta(&self) -> TransferJobMeta {
        TransferJobMeta {
            id: self.id,
            remote: self.remote.to_string(),
            to: self.data_source.to_meta(),
            file_num: self.file_num,
            show_hidden: self.show_hidden,
            is_remote: self.is_remote,
            enable_rsync: self.rsync_enabled,
        }
    }

    // ===================== rsync incremental transfer =====================

    /// Enable/disable rsync incremental transfer for this job. Must be set
    /// before the job starts. Requires overwrite detection to be enabled as
    /// well (checked in `rsync_mode_possible`).
    pub fn set_rsync_enabled(&mut self, on: bool) {
        self.rsync_enabled = on;
    }

    #[inline]
    pub fn rsync_enabled(&self) -> bool {
        self.rsync_enabled
    }

    /// Current rsync state (for callers and tests).
    #[inline]
    pub fn rsync_state(&self) -> &RsyncState {
        &self.rsync
    }

    fn rsync_mode_possible(&self) -> bool {
        self.rsync_enabled
            && self.r#type == JobType::Generic
            && self.enable_overwrite_detection
            && matches!(self.data_source, DataSource::FilePath(_))
    }

    /// Absolute path of the file currently being transferred, if any.
    fn current_file_path(&self) -> Option<std::path::PathBuf> {
        if let DataSource::FilePath(p) = &self.data_source {
            let file_num = self.file_num as usize;
            if file_num < self.files.len() {
                let entry = &self.files[file_num];
                return self.resolve_entry_path(p, &entry.name);
            }
        }
        None
    }

    #[inline]
    fn current_entry_size(&self) -> u64 {
        self.files
            .get(self.file_num as usize)
            .map(|f| f.size)
            .unwrap_or(0)
    }

    /// Hook called from `confirm()` on a plain overwrite confirm
    /// (`offset_blk == 0`). Both sides evaluate eligibility independently
    /// and reach the same size conclusion, so the writer only needs to send
    /// an explicit fallback when the old file itself is missing.
    fn rsync_on_overwrite_confirm(&mut self) {
        self.rsync = RsyncState::Off;
        if !self.rsync_mode_possible() {
            return;
        }
        let new_size_ok = if self.is_reader {
            self.current_entry_size() >= rsync::MIN_RSYNC_SIZE
        } else {
            self.digest.size >= rsync::MIN_RSYNC_SIZE
        };
        if !new_size_ok {
            // The peer reaches the same conclusion silently; legacy flows.
            return;
        }
        if self.is_reader {
            log::info!(
                "job {}: rsync enabled for file {} (await signature)",
                self.id,
                self.file_num
            );
            self.rsync = RsyncState::AwaitSignature {
                since: std::time::Instant::now(),
            };
        } else if self
            .current_file_path()
            .map(|p| p.is_file())
            .unwrap_or(false)
        {
            log::info!(
                "job {}: rsync enabled for file {} (prepare signature)",
                self.id,
                self.file_num
            );
            self.rsync = RsyncState::PrepareSignature;
        } else {
            // Old file missing: the reader is waiting in AwaitSignature and
            // must be told to fall back now instead of timing out.
            self.rsync = RsyncState::PrepareSignature;
            self.rsync_old_file_missing = true;
        }
    }

    /// Writer-side action required after `confirm()`.
    pub fn take_rsync_confirm_action(&mut self) -> RsyncConfirmAction {
        if !matches!(self.rsync, RsyncState::PrepareSignature) {
            return RsyncConfirmAction::Nothing;
        }
        if self.rsync_old_file_missing {
            self.rsync_old_file_missing = false;
            self.rsync = RsyncState::Off;
            return RsyncConfirmAction::SendFallback;
        }
        RsyncConfirmAction::ComputeSignature
    }

    /// Path of the old file whose signature should be computed (writer side,
    /// `PrepareSignature` state).
    pub fn rsync_signature_path(&self) -> Option<std::path::PathBuf> {
        if matches!(self.rsync, RsyncState::PrepareSignature) {
            self.current_file_path()
        } else {
            None
        }
    }

    /// Called after the caller finished sending RsyncMeta + signature chunks.
    pub fn rsync_signature_sent(&mut self) {
        if matches!(self.rsync, RsyncState::PrepareSignature) {
            self.rsync = RsyncState::AwaitDelta;
        }
    }

    // ---- reader (new-file side) message intake ----

    pub fn on_rsync_meta(&mut self, meta: &FileTransferRsyncMeta) {
        if !matches!(self.rsync, RsyncState::AwaitSignature { .. }) {
            log::warn!("job {}: unexpected rsync meta in {:?}", self.id, self.rsync);
            return;
        }
        let mut assembler = rsync::ChunkAssembler::new();
        assembler.set_expected(meta.sig_len, meta.num_chunks);
        self.rsync = RsyncState::WaitingSigChunks { assembler };
    }

    /// Feed one signature chunk. Returns the assembled signature bytes when
    /// the stream is complete (the job then enters `Diffing`).
    pub fn on_rsync_sig_chunk(&mut self, chunk: &FileTransferRsyncChunk) -> ResultType<Option<Vec<u8>>> {
        let complete = match &mut self.rsync {
            RsyncState::WaitingSigChunks { assembler } => {
                assembler.feed(chunk.chunk_index, chunk.compressed, &chunk.data)?
            }
            _ => {
                // Stale chunk (e.g. after fallback); ignore.
                return Ok(None);
            }
        };
        if complete {
            if let RsyncState::WaitingSigChunks { assembler } = std::mem::take(&mut self.rsync) {
                let bytes = assembler.finish();
                self.rsync = RsyncState::Diffing;
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }

    /// Spool path for the delta of the current file (reader side). The
    /// caller runs the blocking diff into this path.
    pub fn rsync_delta_spool_path(&self) -> Option<std::path::PathBuf> {
        self.current_file_path().map(|p| rsync::delta_spool_path(&p))
    }

    /// New-file path for the diff (reader side).
    pub fn rsync_new_file_path(&self) -> Option<std::path::PathBuf> {
        self.current_file_path()
    }

    /// Enter `SendingDelta` after a successful diff. The caller has already
    /// sent RsyncDeltaMeta with `info`.
    pub fn begin_sending_delta(&mut self, spool: &Path, info: rsync::DeltaInfo) -> ResultType<()> {
        if !matches!(self.rsync, RsyncState::Diffing) {
            bail!("not diffing");
        }
        let reader = rsync::SpoolReader::new(spool)?;
        self.rsync = RsyncState::SendingDelta {
            reader,
            next_chunk: 0,
            num_chunks: info.num_chunks,
            file_size: info.new_file_size,
        };
        Ok(())
    }

    /// Reader received the writer's per-file ack: finalize this file and
    /// advance, mirroring the end-of-file logic of `read()`.
    pub fn on_rsync_ack(&mut self) {
        if !matches!(self.rsync, RsyncState::AwaitApply { .. }) {
            return;
        }
        self.file_num += 1;
        self.data_stream = None;
        self.file_confirmed = false;
        self.file_is_waiting = false;
        self.rsync = RsyncState::Off;
        self.rsync_counted = 0;
        self.rsync_finalized = false;
    }

    // ---- writer (old-file side) message intake ----

    pub fn on_rsync_delta_meta(&mut self, meta: &FileTransferRsyncDeltaMeta) -> ResultType<()> {
        if !matches!(self.rsync, RsyncState::AwaitDelta) {
            log::warn!(
                "job {}: unexpected rsync delta meta in {:?}",
                self.id,
                self.rsync
            );
            return Ok(());
        }
        if meta.sha256.len() != 32 {
            bail!("rsync delta meta: bad sha256 length {}", meta.sha256.len());
        }
        if meta.delta_len > rsync::MAX_DELTA_BYTES {
            bail!(
                "rsync delta meta: delta too large ({} bytes)",
                meta.delta_len
            );
        }
        let mut sha256 = [0u8; 32];
        sha256.copy_from_slice(&meta.sha256);
        let mut assembler = rsync::ChunkAssembler::new();
        assembler.set_expected(meta.delta_len, meta.num_chunks);
        self.rsync = RsyncState::ReceivingDelta {
            assembler,
            next_index: 0,
        };
        self.rsync_delta_meta = Some((meta.new_file_size, sha256, meta.last_modified));
        Ok(())
    }

    /// Feed one delta chunk. Returns `RsyncApplyParams` when the delta is
    /// complete (job enters `DeltaReady`); the caller runs apply + verify.
    pub fn on_rsync_delta_chunk(
        &mut self,
        chunk: &FileTransferRsyncChunk,
    ) -> ResultType<Option<RsyncApplyParams>> {
        let complete = match &mut self.rsync {
            RsyncState::ReceivingDelta { assembler, .. } => {
                assembler.feed(chunk.chunk_index, chunk.compressed, &chunk.data)?
            }
            _ => {
                // Stale chunk; ignore.
                return Ok(None);
            }
        };
        if !complete {
            return Ok(None);
        }
        let (delta, meta) = match (std::mem::take(&mut self.rsync), self.rsync_delta_meta.take()) {
            (
                RsyncState::ReceivingDelta { assembler, .. },
                Some((new_file_size, sha256, last_modified)),
            ) => (assembler.finish(), (new_file_size, sha256, last_modified)),
            _ => {
                bail!("rsync delta complete but meta missing");
            }
        };
        let old_file = self
            .current_file_path()
            .ok_or(anyhow!("rsync apply: no current file path"))?;
        let out_file = rsync::rsync_out_path(&old_file);
        self.rsync = RsyncState::DeltaReady {
            delta: delta.clone(),
            new_file_size: meta.0,
            sha256: meta.1,
            last_modified: meta.2,
        };
        Ok(Some(RsyncApplyParams {
            old_file,
            delta,
            out_file,
            new_file_size: meta.0,
            sha256: meta.1,
        }))
    }

    /// Writer-side finalize after `apply_and_verify` succeeded: replace the
    /// target with the verified output, restore mtime, clean up temps.
    pub fn rsync_finish(&mut self) {
        if let Some(target) = self.current_file_path() {
            let target_str = get_string(&target);
            let out = rsync::rsync_out_path(&target);
            if let Err(e) = std::fs::rename(&out, &target) {
                log::error!("job {}: rsync finalize rename failed: {}", self.id, e);
            }
            let _ = std::fs::remove_file(format!("{}.download", target_str));
            let _ = std::fs::remove_file(format!("{}.digest", target_str));
            if let RsyncState::DeltaReady { last_modified, .. } = &self.rsync {
                let _ = filetime::set_file_mtime(
                    &target,
                    filetime::FileTime::from_unix_time(*last_modified as _, 0),
                );
            }
            rsync::cleanup_rsync_temps(&target);
        }
        self.rsync_finalized = true;
        self.rsync = RsyncState::Off;
    }

    /// Revert the current file to legacy whole-file transfer. Local state is
    /// reset unconditionally; `reason` is only for logging. The caller is
    /// responsible for sending the FileTransferRsyncFallback message when
    /// the peer needs to be told (use `rsync_fallback` semantics at the
    /// call sites).
    pub async fn rsync_fallback_local(&mut self) {
        log::info!(
            "job {}: rsync fallback for file {} (state was {:?})",
            self.id,
            self.file_num,
            self.rsync
        );
        if let Some(p) = self.current_file_path() {
            rsync::cleanup_rsync_temps(&p);
        }
        self.finished_size = self.finished_size.saturating_sub(self.rsync_counted);
        self.rsync_counted = 0;
        self.rsync_delta_meta = None;
        self.rsync_old_file_missing = false;
        self.rsync = RsyncState::Off;
        if self.is_reader {
            self.reset_stream_to_start().await;
        } else {
            // The next legacy block recreates `<file>.download` from scratch.
            self.data_stream = None;
        }
    }

    /// Reopen the current file at offset 0 WITHOUT touching
    /// `file_confirmed`/`file_is_waiting` (unlike `open_data_stream`, which
    /// would clear the confirm state and re-trigger the digest round-trip).
    async fn reset_stream_to_start(&mut self) {
        if let DataSource::FilePath(p) = &self.data_source {
            let file_num = self.file_num as usize;
            if file_num < self.files.len() {
                let entry = &self.files[file_num];
                let Some(path) = self.resolve_entry_path(p, &entry.name) else {
                    return;
                };
                match tokio::fs::File::open(&path).await {
                    Ok(mut f) => {
                        if f.seek(std::io::SeekFrom::Start(0)).await.is_ok() {
                            self.data_stream = Some(DataStream::FileStream(f));
                        }
                    }
                    Err(e) => {
                        log::warn!("job {}: rsync reset open failed: {}", self.id, e);
                    }
                }
            }
        }
    }
}

#[inline]
pub fn new_error<T: std::string::ToString>(id: i32, err: T, file_num: i32) -> Message {
    let mut resp = FileResponse::new();
    resp.set_error(FileTransferError {
        id,
        error: err.to_string(),
        file_num,
        ..Default::default()
    });
    let mut msg_out = Message::new();
    msg_out.set_file_response(resp);
    msg_out
}

#[inline]
pub fn new_dir(id: i32, path: String, files: Vec<FileEntry>) -> Message {
    let mut resp = FileResponse::new();
    resp.set_dir(FileDirectory {
        id,
        path,
        entries: files,
        ..Default::default()
    });
    let mut msg_out = Message::new();
    msg_out.set_file_response(resp);
    msg_out
}

#[inline]
pub fn new_block(block: FileTransferBlock) -> Message {
    let mut resp = FileResponse::new();
    resp.set_block(block);
    let mut msg_out = Message::new();
    msg_out.set_file_response(resp);
    msg_out
}

// ---- rsync message builders ----
// The controlling side sends rsync messages as FileAction, the controlled
// side as FileResponse; both variants are provided for each message.

fn build_rsync_action<F>(f: F) -> Message
where
    F: FnOnce(&mut FileAction),
{
    let mut action = FileAction::new();
    f(&mut action);
    let mut msg_out = Message::new();
    msg_out.set_file_action(action);
    msg_out
}

fn build_rsync_response<F>(f: F) -> Message
where
    F: FnOnce(&mut FileResponse),
{
    let mut resp = FileResponse::new();
    f(&mut resp);
    let mut msg_out = Message::new();
    msg_out.set_file_response(resp);
    msg_out
}

#[inline]
pub fn new_rsync_chunk_action(chunk: FileTransferRsyncChunk) -> Message {
    build_rsync_action(|a| a.set_rsync_chunk(chunk))
}

#[inline]
pub fn new_rsync_chunk_response(chunk: FileTransferRsyncChunk) -> Message {
    build_rsync_response(|r| r.set_rsync_chunk(chunk))
}

#[inline]
pub fn new_rsync_meta_action(meta: FileTransferRsyncMeta) -> Message {
    build_rsync_action(|a| a.set_rsync_meta(meta))
}

#[inline]
pub fn new_rsync_meta_response(meta: FileTransferRsyncMeta) -> Message {
    build_rsync_response(|r| r.set_rsync_meta(meta))
}

#[inline]
pub fn new_rsync_delta_meta_action(meta: FileTransferRsyncDeltaMeta) -> Message {
    build_rsync_action(|a| a.set_rsync_delta_meta(meta))
}

#[inline]
pub fn new_rsync_delta_meta_response(meta: FileTransferRsyncDeltaMeta) -> Message {
    build_rsync_response(|r| r.set_rsync_delta_meta(meta))
}

#[inline]
pub fn new_rsync_fallback_action(id: i32, file_num: i32, reason: &str) -> Message {
    build_rsync_action(|a| {
        a.set_rsync_fallback(FileTransferRsyncFallback {
            id,
            file_num,
            reason: reason.to_string(),
            ..Default::default()
        })
    })
}

#[inline]
pub fn new_rsync_fallback_response(id: i32, file_num: i32, reason: &str) -> Message {
    build_rsync_response(|r| {
        r.set_rsync_fallback(FileTransferRsyncFallback {
            id,
            file_num,
            reason: reason.to_string(),
            ..Default::default()
        })
    })
}

#[inline]
pub fn new_rsync_ack_action(id: i32, file_num: i32) -> Message {
    build_rsync_action(|a| {
        a.set_rsync_ack(FileTransferRsyncAck {
            id,
            file_num,
            ..Default::default()
        })
    })
}

#[inline]
pub fn new_rsync_ack_response(id: i32, file_num: i32) -> Message {
    build_rsync_response(|r| {
        r.set_rsync_ack(FileTransferRsyncAck {
            id,
            file_num,
            ..Default::default()
        })
    })
}

#[inline]
pub fn new_send_confirm(r: FileTransferSendConfirmRequest) -> Message {
    let mut msg_out = Message::new();
    let mut action = FileAction::new();
    action.set_send_confirm(r);
    msg_out.set_file_action(action);
    msg_out
}

#[inline]
pub fn new_receive(
    id: i32,
    path: String,
    file_num: i32,
    files: Vec<FileEntry>,
    total_size: u64,
    enable_rsync: bool,
) -> Message {
    let mut action = FileAction::new();
    action.set_receive(FileTransferReceiveRequest {
        id,
        path,
        files,
        file_num,
        total_size,
        enable_rsync,
        ..Default::default()
    });
    let mut msg_out = Message::new();
    msg_out.set_file_action(action);
    msg_out
}

#[inline]
pub fn new_send(
    id: i32,
    r#type: JobType,
    path: String,
    file_num: i32,
    include_hidden: bool,
    enable_rsync: bool,
) -> Message {
    log::info!("new send: {}, id: {}, rsync: {}", path, id, enable_rsync);
    let mut action = FileAction::new();
    let t: file_transfer_send_request::FileType = r#type.into();
    action.set_send(FileTransferSendRequest {
        id,
        path,
        include_hidden,
        file_num,
        file_type: t.into(),
        enable_rsync,
        ..Default::default()
    });
    let mut msg_out = Message::new();
    msg_out.set_file_action(action);
    msg_out
}

#[inline]
pub fn new_done(id: i32, file_num: i32) -> Message {
    let mut resp = FileResponse::new();
    resp.set_done(FileTransferDone {
        id,
        file_num,
        ..Default::default()
    });
    let mut msg_out = Message::new();
    msg_out.set_file_response(resp);
    msg_out
}

#[inline]
pub fn remove_job(id: i32, jobs: &mut Vec<TransferJob>) -> Option<TransferJob> {
    jobs.iter()
        .position(|x| x.id() == id)
        .map(|index| jobs.remove(index))
}

#[inline]
pub fn get_job(id: i32, jobs: &mut [TransferJob]) -> Option<&mut TransferJob> {
    jobs.iter_mut().find(|x| x.id() == id)
}

#[inline]
pub fn get_job_immutable(id: i32, jobs: &[TransferJob]) -> Option<&TransferJob> {
    jobs.iter().find(|x| x.id() == id)
}

async fn init_jobs(jobs: &mut Vec<TransferJob>, stream: &mut hbb_common::Stream) -> ResultType<()> {
    for job in jobs.iter_mut() {
        if job.is_last_job {
            continue;
        }
        if let Err(err) = job.init_data_stream(stream).await {
            stream
                .send(&new_error(job.id(), err, job.file_num()))
                .await?;
        }
    }
    Ok(())
}

pub async fn handle_read_jobs(
    jobs: &mut Vec<TransferJob>,
    stream: &mut hbb_common::Stream,
) -> ResultType<String> {
    init_jobs(jobs, stream).await?;

    let mut job_log = Default::default();
    let mut finished = Vec::new();
    for job in jobs.iter_mut() {
        if job.is_last_job {
            continue;
        }
        match job.read_chunk().await {
            Err(err) => {
                stream
                    .send(&new_error(job.id(), err, job.file_num()))
                    .await?;
            }
            Ok(Some(JobChunk::Block(block))) => {
                stream.send(&new_block(block)).await?;
            }
            Ok(Some(JobChunk::RsyncDelta(chunk))) => {
                stream.send(&new_rsync_chunk_response(chunk)).await?;
            }
            Ok(None) => {
                if job.job_completed() {
                    job_log = serialize_transfer_job(job, true, false, "");
                    finished.push(job.id());
                    match job.job_error() {
                        Some(err) => {
                            job_log = serialize_transfer_job(job, false, false, &err);
                            stream
                                .send(&new_error(job.id(), err, job.file_num()))
                                .await?
                        }
                        None => stream.send(&new_done(job.id(), job.file_num())).await?,
                    }
                } else {
                    // waiting confirmation.
                }
            }
        }
        // Break to handle jobs one by one.
        break;
    }
    for id in finished {
        let _ = remove_job(id, jobs);
    }
    Ok(job_log)
}

pub fn remove_all_empty_dir(path: &Path) -> ResultType<()> {
    let fd = read_dir(path, true)?;
    for entry in fd.entries.iter() {
        match entry.entry_type.enum_value() {
            Ok(FileType::Dir) => {
                remove_all_empty_dir(&path.join(&entry.name)).ok();
            }
            Ok(FileType::DirLink) | Ok(FileType::FileLink) => {
                std::fs::remove_file(path.join(&entry.name)).ok();
            }
            _ => {}
        }
    }
    std::fs::remove_dir(path).ok();
    Ok(())
}

#[inline]
pub fn remove_file(file: &str) -> ResultType<()> {
    validate_fs_path_argument(file, "file path")?;
    std::fs::remove_file(get_path(file))?;
    Ok(())
}

#[inline]
pub fn create_dir(dir: &str) -> ResultType<()> {
    validate_fs_path_argument(dir, "directory path")?;
    std::fs::create_dir_all(get_path(dir))?;
    Ok(())
}

#[inline]
pub fn rename_file(path: &str, new_name: &str) -> ResultType<()> {
    validate_fs_path_argument(path, "path")?;
    if new_name.is_empty() {
        bail!("new file name cannot be empty");
    }
    validate_file_name_no_traversal(new_name)?;
    let path = std::path::Path::new(&path);
    if path.exists() {
        let dir = path
            .parent()
            .ok_or(anyhow!("Parent directoy of {path:?} not exists"))?;
        let new_path = dir.join(&new_name);
        std::fs::rename(&path, &new_path)?;
        Ok(())
    } else {
        bail!("{path:?} not exists");
    }
}

#[inline]
pub fn transform_windows_path(entries: &mut Vec<FileEntry>) {
    for entry in entries {
        entry.name = entry.name.replace('\\', "/");
    }
}

pub enum DigestCheckResult {
    IsSame,
    NeedConfirm(FileTransferDigest),
    NoSuchFile,
}

#[inline]
pub fn is_write_need_confirmation(
    is_resume: bool,
    file_path: &str,
    digest: &FileTransferDigest,
) -> ResultType<DigestCheckResult> {
    let path = Path::new(file_path);
    let digest_file = format!("{}.digest", file_path);
    let download_file = format!("{}.download", file_path);
    if is_resume && Path::new(&digest_file).exists() && Path::new(&download_file).exists() {
        // If the digest file exists, it means the file was transferred before.
        // We can use the digest file to check whether the file is the same.
        if let Ok(content) = std::fs::read_to_string(digest_file) {
            if let Ok(local_digest) = serde_json::from_str::<FileDigest>(&content) {
                let is_identical = local_digest.modified == digest.last_modified
                    && local_digest.size == digest.file_size;
                if is_identical {
                    if let Ok(download_metadata) = std::fs::metadata(download_file) {
                        // Get the file size of the local file
                        // Only send confirmation if the file is not empty.
                        let transferred_size = download_metadata.len();
                        if transferred_size > 0 {
                            return Ok(DigestCheckResult::NeedConfirm(FileTransferDigest {
                                id: digest.id,
                                file_num: digest.file_num,
                                last_modified: digest.last_modified,
                                file_size: digest.file_size,
                                is_identical,
                                transferred_size,
                                ..Default::default()
                            }));
                        }
                    }
                }
            }
        }
    }

    if path.exists() && path.is_file() {
        let metadata = std::fs::metadata(path)?;
        let modified_time = metadata.modified()?;
        let remote_mt = Duration::from_secs(digest.last_modified);
        let local_mt = modified_time.duration_since(UNIX_EPOCH)?;
        // [Note]
        // We decide to give the decision whether to override the existing file to users,
        // which obey the behavior of the file manager in our system.
        let mut is_identical = false;
        if remote_mt == local_mt && digest.file_size == metadata.len() {
            is_identical = true;
        }
        Ok(DigestCheckResult::NeedConfirm(FileTransferDigest {
            id: digest.id,
            file_num: digest.file_num,
            last_modified: local_mt.as_secs(),
            file_size: metadata.len(),
            is_identical,
            ..Default::default()
        }))
    } else {
        // If the file does not exist, or the digest file and download file do not exist, we return NoSuchFile.
        Ok(DigestCheckResult::NoSuchFile)
    }
}

pub fn serialize_transfer_jobs(jobs: &[TransferJob]) -> String {
    let mut v = vec![];
    for job in jobs {
        let value = serde_json::to_value(job).unwrap_or_default();
        v.push(value);
    }
    serde_json::to_string(&v).unwrap_or_default()
}

pub fn serialize_transfer_job(job: &TransferJob, done: bool, cancel: bool, error: &str) -> String {
    let mut value = serde_json::to_value(job).unwrap_or_default();
    value["done"] = json!(done);
    value["cancel"] = json!(cancel);
    value["error"] = json!(error);
    serde_json::to_string(&value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn new(prefix: &str) -> Self {
            Self {
                path: unique_temp_dir(prefix),
            }
        }

        fn join(&self, path: &str) -> PathBuf {
            self.path.join(path)
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("{}_{}_{}", prefix, std::process::id(), timestamp))
    }

    fn new_file_entry(name: &str) -> FileEntry {
        let mut entry = FileEntry::new();
        entry.name = name.to_string();
        entry
    }

    fn new_validation_job(id: i32) -> TransferJob {
        TransferJob::new_write(
            id,
            JobType::Generic,
            "/fake/remote".to_string(),
            DataSource::FilePath(std::env::temp_dir().join(format!("rustdesk_validation_{id}"))),
            0,
            false,
            true,
            false,
        )
    }

    fn new_write_job(id: i32, download_dir: PathBuf, name: &str) -> ResultType<TransferJob> {
        let job = TransferJob::new_write(
            id,
            JobType::Generic,
            "/fake/remote".to_string(),
            DataSource::FilePath(download_dir),
            0,
            false,
            true,
            false,
        )
        .with_files(vec![new_file_entry(name)])?;
        Ok(job)
    }

    fn assert_err_contains(err: anyhow::Error, expected: &str) {
        assert!(
            err.to_string().contains(expected),
            "expected error containing '{}', got: {}",
            expected,
            err
        );
    }

    #[test]
    fn path_traversal_e2e_write_rejects_relative_escape() {
        let tmp_root = TestTempDir::new("rustdesk_e2e_relative");
        let downloads = tmp_root.join("downloads");
        std::fs::create_dir_all(&downloads).expect("create downloads dir");

        let err = new_write_job(1, downloads, "../traversal_proof.txt")
            .expect_err("relative path traversal must be rejected");
        assert_err_contains(err, "path traversal");
        assert!(!tmp_root.join("traversal_proof.txt").exists());
    }

    #[test]
    fn path_traversal_e2e_write_rejects_absolute_path() {
        let tmp_root = TestTempDir::new("rustdesk_e2e_absolute");
        let downloads = tmp_root.join("downloads");
        let absolute_target = tmp_root.join("fake_ssh").join("authorized_keys");
        std::fs::create_dir_all(&downloads).expect("create downloads dir");

        let err = new_write_job(2, downloads, &absolute_target.to_string_lossy())
            .expect_err("absolute path must be rejected");
        assert_err_contains(err, "absolute path");
        assert!(!absolute_target.exists());
    }

    #[test]
    #[cfg_attr(windows, ignore = "requires symlink privilege to create test symlink")]
    fn path_traversal_e2e_write_rejects_symlink_escape() {
        let tmp_root = TestTempDir::new("rustdesk_e2e_symlink");
        let downloads = tmp_root.join("downloads");
        let outside = tmp_root.join("outside");
        let escaped_target = outside.join("escape.txt");
        std::fs::create_dir_all(&downloads).expect("create downloads dir");
        std::fs::create_dir_all(&outside).expect("create outside dir");

        let symlink_path = downloads.join("link");
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            symlink(&outside, &symlink_path).expect("create symlink for test");
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::symlink_dir;
            symlink_dir(&outside, &symlink_path).expect("create directory symlink for test");
        }

        let err = new_write_job(3, downloads, "link/escape.txt")
            .expect_err("symlink traversal must be rejected");
        assert_err_contains(err, "symlink");
        assert!(!escaped_target.exists());
    }

    #[test]
    fn set_files_allows_single_empty_name_for_single_file_transfer() {
        let mut job = new_validation_job(101);
        assert!(job.set_files(vec![new_file_entry("")]).is_ok());
    }

    #[test]
    fn set_files_rejects_empty_name_in_multi_file_transfer() {
        let mut job = new_validation_job(102);
        let err = job
            .set_files(vec![new_file_entry(""), new_file_entry("ok.txt")])
            .expect_err("empty name in multi-file transfer must be rejected");
        assert_err_contains(err, "empty file name");
    }

    #[test]
    fn set_files_rejects_null_byte_name() {
        let mut job = new_validation_job(103);
        let err = job
            .set_files(vec![new_file_entry("bad\0name.txt")])
            .expect_err("null byte in file name must be rejected");
        assert_err_contains(err, "null bytes");
    }

    #[test]
    fn set_files_rejects_mixed_entries_when_one_is_traversal() {
        let mut job = new_validation_job(104);
        let err = job
            .set_files(vec![
                new_file_entry("safe/file.txt"),
                new_file_entry("../../escape.txt"),
            ])
            .expect_err("any traversal entry must reject the full file list");
        assert_err_contains(err, "path traversal");
    }

    #[cfg(windows)]
    #[test]
    fn set_files_rejects_unc_absolute_path() {
        let mut job = new_validation_job(105);
        let err = job
            .set_files(vec![new_file_entry("\\\\server\\share\\payload.txt")])
            .expect_err("UNC absolute path must be rejected");
        assert_err_contains(err, "absolute path");
    }

    #[cfg(not(windows))]
    #[test]
    fn set_files_allows_backslash_prefixed_name_on_unix() {
        let mut job = new_validation_job(105);
        assert!(job
            .set_files(vec![new_file_entry("\\\\server\\share\\payload.txt")])
            .is_ok());
    }

    #[test]
    fn remove_file_rejects_empty_path() {
        let err = remove_file("").expect_err("empty file path must be rejected");
        assert_err_contains(err, "cannot be empty");
    }

    #[test]
    fn remove_file_rejects_null_byte_path() {
        let err = remove_file("bad\0path").expect_err("null byte path must be rejected");
        assert_err_contains(err, "null bytes");
    }

    #[test]
    fn create_dir_rejects_empty_path() {
        let err = create_dir("").expect_err("empty directory path must be rejected");
        assert_err_contains(err, "cannot be empty");
    }

    #[test]
    fn create_dir_rejects_null_byte_path() {
        let err = create_dir("bad\0path").expect_err("null byte path must be rejected");
        assert_err_contains(err, "null bytes");
    }

    #[test]
    fn rename_file_rejects_invalid_new_name() {
        let tmp_root = TestTempDir::new("rustdesk_rename_invalid");
        let src = tmp_root.join("source.txt");
        std::fs::create_dir_all(&tmp_root.path).expect("create temp dir");
        std::fs::write(&src, b"content").expect("create source file");

        let src_str = src.to_string_lossy().to_string();

        let err_empty =
            rename_file(&src_str, "").expect_err("empty new file name must be rejected");
        assert_err_contains(err_empty, "cannot be empty");

        let err_traversal = rename_file(&src_str, "../escape.txt")
            .expect_err("traversal new file name must be rejected");
        assert_err_contains(err_traversal, "path traversal");

        let err_null = rename_file(&src_str, "bad\0name.txt")
            .expect_err("null byte in new file name must be rejected");
        assert_err_contains(err_null, "null bytes");

        #[cfg(windows)]
        {
            let err_abs = rename_file(&src_str, "C:\\Windows\\Temp\\payload.txt")
                .expect_err("absolute new file name must be rejected");
            assert_err_contains(err_abs, "absolute path");
        }
        #[cfg(not(windows))]
        {
            let err_abs = rename_file(&src_str, "/tmp/payload.txt")
                .expect_err("absolute new file name must be rejected");
            assert_err_contains(err_abs, "absolute path");
        }
    }

    #[test]
    fn rename_file_accepts_valid_new_name() {
        let tmp_root = TestTempDir::new("rustdesk_rename_ok");
        let src = tmp_root.join("rename_src.txt");
        let dst = tmp_root.join("renamed.txt");
        std::fs::create_dir_all(&tmp_root.path).expect("create temp dir");
        std::fs::write(&src, b"content").expect("create source file");

        let src_str = src.to_string_lossy().to_string();
        rename_file(&src_str, "renamed.txt").expect("rename should succeed");

        assert!(!src.exists());
        assert!(dst.exists());
    }

    #[cfg(windows)]
    #[test]
    fn set_files_rejects_windows_drive_absolute_path() {
        let mut job = new_validation_job(106);
        let err = job
            .set_files(vec![new_file_entry("C:\\Windows\\Temp\\payload.txt")])
            .expect_err("drive-letter absolute path must be rejected");
        assert_err_contains(err, "absolute path");
    }

    #[cfg(windows)]
    #[test]
    fn set_files_rejects_windows_verbatim_drive_absolute_path() {
        let mut job = new_validation_job(1061);
        let err = job
            .set_files(vec![new_file_entry(r"\\?\C:\Windows\Temp\x.txt")])
            .expect_err("verbatim drive absolute path must be rejected");
        assert_err_contains(err, "absolute path");
    }

    // ---- rsync state machine tests ----

    const BIG_SIZE: u64 = 2 * 1024 * 1024;

    fn new_big_entry(name: &str) -> FileEntry {
        let mut entry = new_file_entry(name);
        entry.size = BIG_SIZE;
        entry
    }

    fn confirm_overwrite(job: &mut TransferJob, file_num: i32) {
        let req = FileTransferSendConfirmRequest {
            id: job.id(),
            file_num,
            union: Some(file_transfer_send_confirm_request::Union::OffsetBlk(0)),
            ..Default::default()
        };
        let _ = futures::executor::block_on(job.confirm(&req));
    }

    #[test]
    fn rsync_confirm_offset0_reader_enters_await_signature() {
        let tmp = TestTempDir::new("rsync_reader");
        std::fs::create_dir_all(&tmp.path).unwrap();
        let src = tmp.join("big.bin");
        std::fs::write(&src, vec![0u8; BIG_SIZE as usize]).unwrap();
        let mut job = TransferJob::new_read(
            1,
            JobType::Generic,
            "".to_string(),
            DataSource::FilePath(src),
            0,
            false,
            true,
            true,
        )
        .unwrap();
        job.set_rsync_enabled(true);
        assert!(matches!(job.rsync_state(), RsyncState::Off));
        confirm_overwrite(&mut job, 0);
        assert!(matches!(
            job.rsync_state(),
            RsyncState::AwaitSignature { .. }
        ));
    }

    #[test]
    fn rsync_confirm_reader_small_file_stays_off() {
        let tmp = TestTempDir::new("rsync_small");
        std::fs::create_dir_all(&tmp.path).unwrap();
        let src = tmp.join("small.bin");
        std::fs::write(&src, vec![0u8; 1024]).unwrap();
        let mut job = TransferJob::new_read(
            2,
            JobType::Generic,
            "".to_string(),
            DataSource::FilePath(src),
            0,
            false,
            true,
            true,
        )
        .unwrap();
        job.set_rsync_enabled(true);
        confirm_overwrite(&mut job, 0);
        assert!(job.rsync_state().is_off());
    }

    #[test]
    fn rsync_confirm_writer_with_old_file_prepares_signature() {
        let tmp = TestTempDir::new("rsync_writer");
        let dir = tmp.join("dest");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("old.db"), vec![1u8; BIG_SIZE as usize]).unwrap();
        let mut job = TransferJob::new_write(
            3,
            JobType::Generic,
            "".to_string(),
            DataSource::FilePath(dir.clone()),
            0,
            false,
            false,
            true,
        )
        .with_files(vec![new_big_entry("old.db")])
        .unwrap();
        job.set_rsync_enabled(true);
        job.set_digest(BIG_SIZE, 0);
        confirm_overwrite(&mut job, 0);
        assert!(matches!(job.rsync_state(), RsyncState::PrepareSignature));
        assert_eq!(
            job.take_rsync_confirm_action(),
            RsyncConfirmAction::ComputeSignature
        );
        assert_eq!(
            job.rsync_signature_path(),
            Some(dir.clone().join("old.db")),
            "signature path must point at the old file"
        );
        job.rsync_signature_sent();
        assert!(matches!(job.rsync_state(), RsyncState::AwaitDelta));
    }

    #[test]
    fn rsync_confirm_writer_missing_old_file_sends_fallback() {
        let tmp = TestTempDir::new("rsync_no_old");
        let dir = tmp.join("dest");
        std::fs::create_dir_all(&dir).unwrap();
        let mut job = TransferJob::new_write(
            4,
            JobType::Generic,
            "".to_string(),
            DataSource::FilePath(dir),
            0,
            false,
            false,
            true,
        )
        .with_files(vec![new_big_entry("absent.db")])
        .unwrap();
        job.set_rsync_enabled(true);
        job.set_digest(BIG_SIZE, 0);
        confirm_overwrite(&mut job, 0);
        assert_eq!(
            job.take_rsync_confirm_action(),
            RsyncConfirmAction::SendFallback
        );
        assert!(job.rsync_state().is_off());
    }

    #[test]
    fn rsync_confirm_skip_and_resume_stay_off() {
        let mut job = new_validation_job(5);
        job.set_rsync_enabled(true);
        job.set_digest(BIG_SIZE, 0);
        let skip = FileTransferSendConfirmRequest {
            id: job.id(),
            file_num: 0,
            union: Some(file_transfer_send_confirm_request::Union::Skip(true)),
            ..Default::default()
        };
        let _ = futures::executor::block_on(job.confirm(&skip));
        assert!(job.rsync_state().is_off());
        let resume = FileTransferSendConfirmRequest {
            id: job.id(),
            file_num: 0,
            union: Some(file_transfer_send_confirm_request::Union::OffsetBlk(4096)),
            ..Default::default()
        };
        let _ = futures::executor::block_on(job.confirm(&resume));
        assert!(job.rsync_state().is_off());
    }

    #[test]
    fn rsync_meta_and_chunk_intake_reader() {
        let tmp = TestTempDir::new("rsync_intake");
        std::fs::create_dir_all(&tmp.path).unwrap();
        let src = tmp.join("big.bin");
        std::fs::write(&src, vec![0u8; BIG_SIZE as usize]).unwrap();
        let mut job = TransferJob::new_read(
            6,
            JobType::Generic,
            "".to_string(),
            DataSource::FilePath(src),
            0,
            false,
            true,
            true,
        )
        .unwrap();
        job.set_rsync_enabled(true);
        confirm_overwrite(&mut job, 0);

        let payload: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
        let (c0, comp0) = crate::rsync::tests_encode_chunk_for_test(&payload[..600]);
        let (c1, comp1) = crate::rsync::tests_encode_chunk_for_test(&payload[600..]);

        job.on_rsync_meta(&FileTransferRsyncMeta {
            id: 6,
            file_num: 0,
            old_file_size: 1,
            block_size: 8192,
            sig_len: payload.len() as u64,
            num_chunks: 2,
            ..Default::default()
        });
        assert!(matches!(
            job.rsync_state(),
            RsyncState::WaitingSigChunks { .. }
        ));
        let chunk0 = FileTransferRsyncChunk {
            id: 6,
            file_num: 0,
            chunk_index: 0,
            is_delta: false,
            compressed: comp0,
            data: c0.into(),
            ..Default::default()
        };
        assert!(job.on_rsync_sig_chunk(&chunk0).unwrap().is_none());
        let chunk1 = FileTransferRsyncChunk {
            id: 6,
            file_num: 0,
            chunk_index: 1,
            is_delta: false,
            compressed: comp1,
            data: c1.into(),
            ..Default::default()
        };
        let done = job.on_rsync_sig_chunk(&chunk1).unwrap();
        assert_eq!(done.as_deref(), Some(payload.as_slice()));
        assert!(matches!(job.rsync_state(), RsyncState::Diffing));
    }

    #[tokio::test]
    async fn rsync_delta_pump_and_ack_advance() {
        let tmp = TestTempDir::new("rsync_pump");
        std::fs::create_dir_all(&tmp.path).unwrap();
        let src = tmp.join("big.bin");
        std::fs::write(&src, vec![0u8; BIG_SIZE as usize]).unwrap();
        let mut job = TransferJob::new_read(
            7,
            JobType::Generic,
            "".to_string(),
            DataSource::FilePath(src.clone()),
            0,
            false,
            true,
            true,
        )
        .unwrap();
        job.set_rsync_enabled(true);
        confirm_overwrite(&mut job, 0);

        // fabricate a diff result: spool some bytes as the "delta"
        let spool = tmp.join("spool.delta");
        std::fs::write(&spool, vec![5u8; 100]).unwrap();
        job.on_rsync_meta(&FileTransferRsyncMeta {
            id: 7,
            file_num: 0,
            old_file_size: 1,
            block_size: 8192,
            sig_len: 8,
            num_chunks: 1,
            ..Default::default()
        });
        // push the state machine forward to Diffing via the public intake
        let chunk = FileTransferRsyncChunk {
            id: 7,
            file_num: 0,
            chunk_index: 0,
            is_delta: false,
            compressed: false,
            data: vec![0u8; 8].into(),
            ..Default::default()
        };
        job.on_rsync_sig_chunk(&chunk).unwrap();
        assert!(matches!(job.rsync_state(), RsyncState::Diffing));

        job.begin_sending_delta(
            &spool,
            rsync::DeltaInfo {
                new_file_size: BIG_SIZE,
                delta_len: 100,
                num_chunks: 1,
                sha256_new: [0u8; 32],
                last_modified: 0,
            },
        )
        .unwrap();

        // one chunk per tick
        let c = job.read_chunk().await.unwrap().unwrap();
        match c {
            JobChunk::RsyncDelta(rc) => {
                assert_eq!(rc.chunk_index, 0);
                assert!(rc.is_delta);
            }
            _ => panic!("expected rsync delta chunk"),
        }
        // spool exhausted -> AwaitApply, read_chunk yields None
        assert!(job.read_chunk().await.unwrap().is_none());
        assert!(matches!(job.rsync_state(), RsyncState::AwaitApply { .. }));

        // progress snapped to full file size
        assert_eq!(job.finished_size(), BIG_SIZE);

        // ack advances the file (job has 1 file -> file_num becomes 1)
        job.on_rsync_ack();
        assert_eq!(job.file_num(), 1);
        assert!(job.rsync_state().is_off());
    }

    #[tokio::test]
    async fn rsync_fallback_resets_reader_stream_and_progress() {
        let tmp = TestTempDir::new("rsync_fb");
        std::fs::create_dir_all(&tmp.path).unwrap();
        let src = tmp.join("big.bin");
        std::fs::write(&src, vec![7u8; BIG_SIZE as usize]).unwrap();
        let mut job = TransferJob::new_read(
            8,
            JobType::Generic,
            "".to_string(),
            DataSource::FilePath(src.clone()),
            0,
            false,
            true,
            true,
        )
        .unwrap();
        job.set_rsync_enabled(true);
        confirm_overwrite(&mut job, 0);
        assert!(job.file_confirmed());
        // book some fake rsync progress
        job.on_rsync_meta(&FileTransferRsyncMeta {
            id: 8,
            file_num: 0,
            old_file_size: 1,
            block_size: 8192,
            sig_len: 8,
            num_chunks: 1,
            ..Default::default()
        });
        // simulate counted progress via delta pump path
        let spool = tmp.join("spool2.delta");
        std::fs::write(&spool, vec![9u8; 10]).unwrap();
        let chunk = FileTransferRsyncChunk {
            id: 8,
            file_num: 0,
            chunk_index: 0,
            is_delta: false,
            compressed: false,
            data: vec![0u8; 8].into(),
            ..Default::default()
        };
        job.on_rsync_sig_chunk(&chunk).unwrap();
        job.begin_sending_delta(
            &spool,
            rsync::DeltaInfo {
                new_file_size: BIG_SIZE,
                delta_len: 10,
                num_chunks: 1,
                sha256_new: [0u8; 32],
                last_modified: 0,
            },
        )
        .unwrap();
        job.read_chunk().await.unwrap().unwrap();
        job.read_chunk().await.unwrap(); // -> AwaitApply, progress snapped
        assert_eq!(job.finished_size(), BIG_SIZE);

        job.rsync_fallback_local().await;
        assert!(job.rsync_state().is_off());
        assert_eq!(
            job.finished_size(),
            0,
            "rsync progress must be subtracted on fallback"
        );
        // confirm flags preserved: the reader must NOT re-trigger a digest
        assert!(job.file_confirmed());
        assert!(!job.file_is_waiting());
        // legacy read works again from offset 0
        let out = job.read_chunk().await.unwrap().unwrap();
        assert!(matches!(out, JobChunk::Block(_)));
    }

    #[tokio::test]
    async fn rsync_writer_delta_intake_to_apply_params() {
        let tmp = TestTempDir::new("rsync_apply");
        let dir = tmp.join("dest");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("old.db"), vec![1u8; BIG_SIZE as usize]).unwrap();
        let mut job = TransferJob::new_write(
            9,
            JobType::Generic,
            "".to_string(),
            DataSource::FilePath(dir.clone()),
            0,
            false,
            false,
            true,
        )
        .with_files(vec![new_big_entry("old.db")])
        .unwrap();
        job.set_rsync_enabled(true);
        job.set_digest(BIG_SIZE, 0);
        confirm_overwrite(&mut job, 0);
        assert_eq!(
            job.take_rsync_confirm_action(),
            RsyncConfirmAction::ComputeSignature
        );
        job.rsync_signature_sent();

        let delta: Vec<u8> = (0..500u32).map(|i| i as u8).collect();
        let (d0, comp0) = crate::rsync::tests_encode_chunk_for_test(&delta[..250]);
        let (d1, comp1) = crate::rsync::tests_encode_chunk_for_test(&delta[250..]);
        job.on_rsync_delta_meta(&FileTransferRsyncDeltaMeta {
            id: 9,
            file_num: 0,
            new_file_size: BIG_SIZE,
            delta_len: delta.len() as u64,
            num_chunks: 2,
            sha256: [2u8; 32].to_vec().into(),
            last_modified: 12345,
            ..Default::default()
        })
        .unwrap();
        let c0 = FileTransferRsyncChunk {
            id: 9,
            file_num: 0,
            chunk_index: 0,
            is_delta: true,
            compressed: comp0,
            data: d0.into(),
            ..Default::default()
        };
        assert!(job.on_rsync_delta_chunk(&c0).unwrap().is_none());
        let c1 = FileTransferRsyncChunk {
            id: 9,
            file_num: 0,
            chunk_index: 1,
            is_delta: true,
            compressed: comp1,
            data: d1.into(),
            ..Default::default()
        };
        let params = job.on_rsync_delta_chunk(&c1).unwrap().expect("complete");
        assert_eq!(params.delta, delta);
        assert_eq!(params.old_file, dir.join("old.db"));
        assert_eq!(params.new_file_size, BIG_SIZE);
        assert_eq!(params.sha256, [2u8; 32]);
        assert!(matches!(job.rsync_state(), RsyncState::DeltaReady { .. }));
    }

    #[tokio::test]
    async fn rsync_finish_replaces_target_and_modify_time_skips() {
        let tmp = TestTempDir::new("rsync_finish");
        let dir = tmp.join("dest");
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("old.db");
        std::fs::write(&target, vec![1u8; BIG_SIZE as usize]).unwrap();
        // stale legacy temps that must be removed
        std::fs::write(
            format!("{}.download", get_string(&target)),
            b"stale",
        )
        .unwrap();
        let mut job = TransferJob::new_write(
            10,
            JobType::Generic,
            "".to_string(),
            DataSource::FilePath(dir.clone()),
            0,
            false,
            false,
            true,
        )
        .with_files(vec![new_big_entry("old.db")])
        .unwrap();
        job.set_rsync_enabled(true);
        job.set_digest(BIG_SIZE, 0);
        confirm_overwrite(&mut job, 0);
        job.take_rsync_confirm_action();
        job.rsync_signature_sent();

        // deliver a (fake) delta; content correctness is covered by the
        // rsync module tests — here we exercise finalize only.
        let delta = vec![0u8; 16];
        job.on_rsync_delta_meta(&FileTransferRsyncDeltaMeta {
            id: 10,
            file_num: 0,
            new_file_size: BIG_SIZE,
            delta_len: delta.len() as u64,
            num_chunks: 1,
            sha256: [0u8; 32].to_vec().into(),
            last_modified: 42,
            ..Default::default()
        })
        .unwrap();
        let c = FileTransferRsyncChunk {
            id: 10,
            file_num: 0,
            chunk_index: 0,
            is_delta: true,
            compressed: false,
            data: delta.clone().into(),
            ..Default::default()
        };
        let params = job.on_rsync_delta_chunk(&c).unwrap().unwrap();
        // write the "verified" output where apply would have put it
        std::fs::write(&params.out_file, b"newcontent").unwrap();
        job.rsync_finish();
        assert_eq!(std::fs::read(&target).unwrap(), b"newcontent");
        assert!(!Path::new(&format!("{}.download", get_string(&target))).exists());
        assert!(!params.out_file.exists());
        // modify_time must skip the already-finalized file
        job.modify_time();
        assert_eq!(std::fs::read(&target).unwrap(), b"newcontent");
    }
}
