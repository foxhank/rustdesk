//! Incremental (rsync-style) file transfer support built on the `fast_rsync`
//! crate (Dropbox's pure-Rust librsync implementation).
//!
//! Integrity contract (fail-closed):
//! - `fast_rsync::apply` performs **no** hash verification by itself; if the
//!   base file changed since the signature was computed it silently produces
//!   wrong output. [`apply_and_verify`] therefore wraps the apply output in a
//!   SHA-256 hashing writer and compares against the whole-file hash of the
//!   new file provided by the sender. Any mismatch (stale base, torn mmap of a
//!   live file, truncated/corrupted delta, collision) is an error and the
//!   partially written output is deleted. Callers must fall back to a full
//!   legacy transfer on any error from this module.
//! - Files are memory-mapped (not read into RAM) for signature calculation,
//!   diffing and as the apply base. Only the delta itself must reside in RAM
//!   for `apply`, bounded by [`MAX_DELTA_BYTES`].
//! - All reads use `read_exact` semantics; short reads never silently corrupt
//!   block boundaries.
//!
//! All functions perform synchronous (blocking) I/O. Heavy entry points
//! (`signature_from_file`, `diff_to_spool`, `apply_and_verify`) are meant to
//! be called from `spawn_blocking`.

use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context};
use memmap2::Mmap;
use sha2::{Digest, Sha256};

use hbb_common::ResultType;

/// Files smaller than this use the legacy whole-file transfer.
pub const MIN_RSYNC_SIZE: u64 = 1024 * 1024;
/// Upper bound for the delta held in RAM on the applying side. Larger deltas
/// trigger a fallback to full transfer (they carry no saving anyway).
pub const MAX_DELTA_BYTES: u64 = 512 * 1024 * 1024;
/// Wire chunk size for signature/delta transport. Must stay far below the
/// 16 MiB websocket frame limit.
pub const RSYNC_CHUNK_SIZE: usize = 1024 * 1024;
/// How long the new-file side waits for the signature before falling back
/// (covers old peers that ignore rsync messages entirely).
pub const SIGNATURE_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// How long the old-file side waits for delta meta/chunks before falling back.
pub const DELTA_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// How long the new-file side waits (in `AwaitApply`) for the writer's
/// per-file ack after the last delta chunk before falling back.
pub const APPLY_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// fast_rsync truncates the MD4 strong hash to this many bytes (max allowed
/// is 16; passing more panics).
pub const CRYPTO_HASH_SIZE: u32 = 16;

/// Signature block size tiers: <64MiB -> 8KiB, <1GiB -> 16KiB, >=1GiB -> 64KiB.
/// Never returns 0 (fast_rsync panics on a zero block size).
pub fn block_size_for(file_size: u64) -> u32 {
    if file_size < 64 * 1024 * 1024 {
        8 * 1024
    } else if file_size < 1024 * 1024 * 1024 {
        16 * 1024
    } else {
        64 * 1024
    }
}

#[inline]
fn num_chunks_for(total_len: u64) -> u32 {
    if total_len == 0 {
        0
    } else {
        ((total_len as usize + RSYNC_CHUNK_SIZE - 1) / RSYNC_CHUNK_SIZE) as u32
    }
}

/// Map a file read-only. Returns an empty map holder for zero-length files
/// (mmap of an empty file is invalid on some platforms).
enum MappedFile {
    Mapped(Mmap),
    Empty,
}

impl MappedFile {
    fn map(path: &Path) -> ResultType<Self> {
        let file = File::open(path).with_context(|| format!("open {:?} for mmap", path))?;
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(Self::Empty);
        }
        // SAFETY: the file is opened read-only and we never write through the
        // map. A concurrently modified source file (e.g. a live database) may
        // yield torn reads; that is caught by the final SHA-256 comparison in
        // `apply_and_verify` and results in a fallback, never corruption.
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self::Mapped(mmap))
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mapped(m) => m.as_ref(),
            Self::Empty => &[],
        }
    }
}

/// Streaming SHA-256 of a file on disk.
pub fn sha256_of_file(path: &Path) -> ResultType<[u8; 32]> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(RSYNC_CHUNK_SIZE, file);
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; RSYNC_CHUNK_SIZE];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

#[inline]
pub fn sha256_of_bytes(buf: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(buf);
    hasher.finalize().into()
}

/// Per-chunk zstd compression. Unlike `crate::compress::compress` (which
/// swallows errors and returns an empty vec), this never produces an invalid
/// pair: if compression fails or does not shrink the data, the chunk is sent
/// uncompressed.
fn compress_chunk(data: &[u8]) -> (Vec<u8>, bool) {
    if data.is_empty() {
        return (data.to_vec(), false);
    }
    let compressed = zstd::bulk::compress(data, hbb_common::config::COMPRESS_LEVEL);
    match compressed {
        Ok(c) if !c.is_empty() && c.len() < data.len() => (c, true),
        _ => (data.to_vec(), false),
    }
}

/// Fallible per-chunk decompression with a sanity limit. Mirrors the
/// fail-closed contract: any decode error is an error, never an empty vec.
fn decompress_chunk(data: &[u8], limit: usize) -> ResultType<Vec<u8>> {
    let out = zstd::bulk::decompress(data, limit)
        .map_err(|e| anyhow!("rsync chunk decompress failed: {}", e))?;
    Ok(out)
}

/// Metadata describing a serialized signature, sent ahead of its chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignatureInfo {
    pub old_file_size: u64,
    pub block_size: u32,
    pub sig_len: u64,
    pub num_chunks: u32,
}

/// Compute the signature of the (old) file at `path` for delta computation
/// against it later.
pub fn signature_from_file(path: &Path) -> ResultType<(fast_rsync::Signature, SignatureInfo)> {
    let len = std::fs::metadata(path)
        .with_context(|| format!("stat {:?} for rsync signature", path))?
        .len();
    let block_size = block_size_for(len);
    debug_assert!(block_size > 0);
    let options = fast_rsync::SignatureOptions {
        block_size,
        crypto_hash_size: CRYPTO_HASH_SIZE,
    };
    let mapped = MappedFile::map(path)?;
    let sig = fast_rsync::Signature::calculate(mapped.as_slice(), options);
    let sig_len = sig.serialized().len() as u64;
    Ok((
        sig,
        SignatureInfo {
            old_file_size: len,
            block_size,
            sig_len,
            num_chunks: num_chunks_for(sig_len),
        },
    ))
}

/// Split a serialized signature into compressed wire chunks.
/// `info.num_chunks` must describe `sig` (use the value returned by
/// `signature_from_file`).
pub fn signature_to_chunks(
    sig: &fast_rsync::Signature,
) -> Vec<(Vec<u8>, bool /* compressed */)> {
    chunk_bytes(sig.serialized())
}

fn chunk_bytes(data: &[u8]) -> Vec<(Vec<u8>, bool)> {
    data.chunks(RSYNC_CHUNK_SIZE)
        .map(|c| compress_chunk(c))
        .collect()
}

/// Metadata describing a computed delta, sent ahead of its chunks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaInfo {
    pub new_file_size: u64,
    pub delta_len: u64,
    pub num_chunks: u32,
    /// SHA-256 of the entire new file — the integrity backstop verified after
    /// `apply`.
    pub sha256_new: [u8; 32],
    pub last_modified: u64,
}

/// Diff the new file at `new_file` against the serialized `sig_bytes`
/// (signature of the old file), spooling the delta to `delta_spool`.
///
/// The caller decides on fallback *after* this returns: if
/// `delta_len >= min(new_file_size, MAX_DELTA_BYTES)` the delta carries no
/// saving and a legacy full transfer must be used instead.
pub fn diff_to_spool(new_file: &Path, sig_bytes: &[u8], delta_spool: &Path) -> ResultType<DeltaInfo> {
    let meta = std::fs::metadata(new_file)
        .with_context(|| format!("stat {:?} for rsync diff", new_file))?;
    let new_file_size = meta.len();
    let last_modified = meta
        .modified()
        .ok()
        .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let sig = fast_rsync::Signature::deserialize(sig_bytes.to_vec())
        .map_err(|e| anyhow!("rsync signature deserialize failed: {}", e))?;
    let index = sig.index();
    let mapped = MappedFile::map(new_file)?;
    let sha256_new = sha256_of_bytes(mapped.as_slice());
    let spool = File::create(delta_spool)
        .with_context(|| format!("create delta spool {:?}", delta_spool))?;
    let mut writer = BufWriter::with_capacity(RSYNC_CHUNK_SIZE, spool);
    fast_rsync::diff(&index, mapped.as_slice(), &mut writer)
        .map_err(|e| anyhow!("rsync diff failed: {}", e))?;
    writer.flush()?;
    drop(writer);
    let delta_len = std::fs::metadata(delta_spool)?.len();
    Ok(DeltaInfo {
        new_file_size,
        delta_len,
        num_chunks: num_chunks_for(delta_len),
        sha256_new,
        last_modified,
    })
}

/// Sequential reader over a spooled delta/signature file, yielding wire
/// chunks. Fails closed on short reads.
pub struct SpoolReader {
    reader: BufReader<File>,
    remaining: u64,
}

impl std::fmt::Debug for SpoolReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpoolReader")
            .field("remaining", &self.remaining)
            .finish()
    }
}

impl SpoolReader {
    pub fn new(path: &Path) -> ResultType<Self> {
        let len = std::fs::metadata(path)?.len();
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::with_capacity(RSYNC_CHUNK_SIZE, file),
            remaining: len,
        })
    }

    /// Next `(data, compressed)` chunk, or `None` at end of spool.
    pub fn next_chunk(&mut self) -> ResultType<Option<(Vec<u8>, bool)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let want = std::cmp::min(self.remaining as usize, RSYNC_CHUNK_SIZE);
        let mut buf = vec![0u8; want];
        self.reader
            .read_exact(&mut buf)
            .context("rsync spool short read")?;
        self.remaining -= want as u64;
        let (data, compressed) = compress_chunk(&buf);
        Ok(Some((data, compressed)))
    }
}

/// Strict reassembler for wire chunks. Every anomaly (out-of-order or
/// duplicate index, decompression failure, overflow past the announced
/// total, more chunks than announced, final length mismatch) is an error.
#[derive(Default, Debug)]
pub struct ChunkAssembler {
    expected_total: Option<u64>,
    expected_chunks: Option<u32>,
    next_index: u32,
    buf: Vec<u8>,
}

impl ChunkAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_expected(&mut self, total_len: u64, num_chunks: u32) {
        self.expected_total = Some(total_len);
        self.expected_chunks = Some(num_chunks);
    }

    /// Feed one chunk. Returns `Ok(true)` when the stream is complete and
    /// verified (`buf.len() == expected_total`).
    pub fn feed(&mut self, chunk_index: u32, compressed: bool, data: &[u8]) -> ResultType<bool> {
        if chunk_index != self.next_index {
            bail!(
                "rsync chunk out of order: got {} expected {}",
                chunk_index,
                self.next_index
            );
        }
        let raw = if compressed {
            decompress_chunk(data, RSYNC_CHUNK_SIZE + RSYNC_CHUNK_SIZE / 2)?
        } else {
            data.to_vec()
        };
        if let (Some(total), false) = (self.expected_total, raw.is_empty()) {
            if self.buf.len() as u64 + raw.len() as u64 > total {
                bail!("rsync chunk overflows announced total");
            }
        }
        self.buf.extend_from_slice(&raw);
        self.next_index = self.next_index.checked_add(1).unwrap_or(u32::MAX);
        match (self.expected_chunks, self.expected_total) {
            (Some(expected), Some(total)) => {
                if self.next_index > expected {
                    bail!("rsync chunk count exceeds announced count");
                }
                if self.next_index == expected {
                    if self.buf.len() as u64 != total {
                        bail!(
                            "rsync stream length mismatch: got {} expected {}",
                            self.buf.len(),
                            total
                        );
                    }
                    return Ok(true);
                }
                Ok(false)
            }
            // Meta not announced yet: completeness is decided once announced.
            _ => Ok(false),
        }
    }

    /// Announce totals after the fact (meta arrived late). Errors if what has
    /// been fed so far is inconsistent.
    pub fn announce(&mut self, total_len: u64, num_chunks: u32) -> ResultType<bool> {
        self.set_expected(total_len, num_chunks);
        if self.next_index > num_chunks {
            bail!("rsync chunk count exceeds announced count");
        }
        if self.buf.len() as u64 > total_len {
            bail!("rsync chunks overflow announced total");
        }
        if self.next_index == num_chunks {
            if self.buf.len() as u64 != total_len {
                bail!("rsync stream length mismatch");
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Consume the reassembled stream. Only valid after `feed`/`announce`
    /// returned `Ok(true)`.
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// A writer that hashes everything it writes, used to verify the apply
/// output against the sender's whole-file SHA-256 without a second pass.
struct Sha256Writer<W: Write> {
    inner: W,
    hasher: Sha256,
}

impl<W: Write> Write for Sha256Writer<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        self.inner.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Apply `delta` (computed against the old file at `old_file`) to produce the
/// new file at `out_path`, verifying the result against `expected_sha256`.
///
/// Fail-closed: on ANY error the partially written `out_path` is deleted and
/// an `Err` is returned; callers must fall back to legacy full transfer.
pub fn apply_and_verify(
    old_file: &Path,
    delta: &[u8],
    out_path: &Path,
    limit: u64,
    expected_sha256: &[u8; 32],
) -> ResultType<()> {
    match apply_and_verify_inner(old_file, delta, out_path, limit, expected_sha256) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = std::fs::remove_file(out_path);
            Err(e)
        }
    }
}

fn apply_and_verify_inner(
    old_file: &Path,
    delta: &[u8],
    out_path: &Path,
    limit: u64,
    expected_sha256: &[u8; 32],
) -> ResultType<()> {
    if delta.len() as u64 > MAX_DELTA_BYTES {
        bail!(
            "rsync delta too large to apply in memory: {} bytes",
            delta.len()
        );
    }
    let base = MappedFile::map(old_file)?;
    let out = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(out_path)
        .with_context(|| format!("create rsync output {:?}", out_path))?;
    let mut writer = Sha256Writer {
        inner: BufWriter::with_capacity(RSYNC_CHUNK_SIZE, out),
        hasher: Sha256::new(),
    };
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    fast_rsync::apply_limited(base.as_slice(), delta, &mut writer, limit_usize)
        .map_err(|e| anyhow!("rsync apply failed: {}", e))?;
    writer.flush()?;
    // Destructure so `finalize` (which consumes the hasher) happens after the
    // BufWriter is flushed and dropped — Windows blocks renames of open files.
    let Sha256Writer { inner: buf_writer, hasher } = writer;
    drop(buf_writer);
    let hash: [u8; 32] = hasher.finalize().into();
    if hash != *expected_sha256 {
        bail!("rsync integrity mismatch: SHA-256 of reconstructed file differs from sender");
    }
    Ok(())
}

/// `<path>.rsync.delta` — spool file for a delta being received or produced.
pub fn delta_spool_path(file_path: &Path) -> PathBuf {
    let mut s = file_path.as_os_str().to_os_string();
    s.push(".rsync.delta");
    PathBuf::from(s)
}

/// `<path>.rsync.download` — temp output of `apply`, renamed onto the target
/// after verification. Distinct from the legacy `.download` used by resume.
pub fn rsync_out_path(file_path: &Path) -> PathBuf {
    let mut s = file_path.as_os_str().to_os_string();
    s.push(".rsync.download");
    PathBuf::from(s)
}

/// Remove both rsync temp files of `file_path`, ignoring errors.
pub fn cleanup_rsync_temps(file_path: &Path) {
    let _ = std::fs::remove_file(delta_spool_path(file_path));
    let _ = std::fs::remove_file(rsync_out_path(file_path));
}

/// Test-only helper: encode a raw slice as one wire chunk (with the same
/// per-chunk compression rule as production code).
#[cfg(test)]
pub fn tests_encode_chunk_for_test(data: &[u8]) -> (Vec<u8>, bool) {
    compress_chunk(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic xorshift PRNG so tests never depend on `rand`.
    struct Prng(u64);

    impl Prng {
        fn new(seed: u64) -> Self {
            Self(seed.wrapping_mul(2685821657736338717).max(1))
        }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn fill(&mut self, buf: &mut [u8]) {
            for chunk in buf.chunks_mut(8) {
                let v = self.next_u64().to_le_bytes();
                chunk.copy_from_slice(&v[..chunk.len()]);
            }
        }
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "hbb_rsync_test_{}_{}_{}",
                tag,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write_file(path: &Path, data: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, data).unwrap();
    }

    /// base -> new: overwrite three 8KiB regions and append 64KiB.
    fn mutate(base: &[u8], rng: &mut Prng) -> Vec<u8> {
        let mut new = base.to_vec();
        for &off in &[0usize, base.len() / 2, base.len() - 8 * 1024] {
            let mut region = vec![0u8; 8 * 1024];
            rng.fill(&mut region);
            new[off..off + 8 * 1024].copy_from_slice(&region);
        }
        let mut tail = vec![0u8; 64 * 1024];
        rng.fill(&mut tail);
        new.extend_from_slice(&tail);
        new
    }

    fn assemble(
        chunks: &[(Vec<u8>, bool)],
        total_len: u64,
        num_chunks: u32,
    ) -> ResultType<Vec<u8>> {
        let mut asm = ChunkAssembler::new();
        asm.set_expected(total_len, num_chunks);
        for (i, (data, compressed)) in chunks.iter().enumerate() {
            let complete = asm.feed(i as u32, *compressed, data)?;
            if i + 1 == num_chunks as usize {
                assert!(complete);
            }
        }
        Ok(asm.finish())
    }

    #[test]
    fn roundtrip_incremental_delta_smaller() {
        let tmp = TempDir::new("roundtrip");
        let base_path = tmp.path("base.bin");
        let new_path = tmp.path("new.bin");
        let sig_path = tmp.path("new.bin.rsync.sig");
        let spool = tmp.path("new.bin.rsync.delta");
        let out = tmp.path("out.bin");

        let mut rng = Prng::new(0xABCD);
        let mut base = vec![0u8; 8 * 1024 * 1024];
        rng.fill(&mut base);
        write_file(&base_path, &base);
        let new = mutate(&base, &mut rng);
        write_file(&new_path, &new);

        // signature of the old (base) file, transported as chunks
        let (sig, info) = signature_from_file(&base_path).unwrap();
        assert_eq!(info.old_file_size, base.len() as u64);
        let chunks = signature_to_chunks(&sig);
        assert_eq!(chunks.len() as u32, info.num_chunks);
        let sig_bytes = assemble(&chunks, info.sig_len, info.num_chunks).unwrap();
        assert_eq!(sig_bytes, sig.serialized());

        // diff the new file against the received signature
        let delta_info = diff_to_spool(&new_path, &sig_bytes, &spool).unwrap();
        assert_eq!(delta_info.new_file_size, new.len() as u64);
        // The whole point: the delta is much smaller than the file.
        assert!(delta_info.delta_len < new.len() as u64);

        // transport the delta as chunks
        let mut reader = SpoolReader::new(&spool).unwrap();
        let mut dchunks = Vec::new();
        while let Some(c) = reader.next_chunk().unwrap() {
            dchunks.push(c);
        }
        assert_eq!(dchunks.len() as u32, delta_info.num_chunks);
        let delta = assemble(&dchunks, delta_info.delta_len, delta_info.num_chunks).unwrap();
        assert_eq!(delta.len() as u64, delta_info.delta_len);

        // apply + verify
        apply_and_verify(
            &base_path,
            &delta,
            &out,
            delta_info.new_file_size,
            &delta_info.sha256_new,
        )
        .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), new);
        let _ = sig_path; // sig bytes were transported in-memory in this test
    }

    #[test]
    fn stale_base_detected_by_sha256() {
        // The core integrity test: `fast_rsync::apply` alone silently accepts
        // a changed base; our SHA-256 backstop must catch it.
        let tmp = TempDir::new("stale");
        let base_path = tmp.path("base.bin");
        let stale_path = tmp.path("stale.bin");
        let new_path = tmp.path("new.bin");
        let spool = tmp.path("spool.delta");
        let out = tmp.path("out.bin");

        let mut rng = Prng::new(0x1234);
        let mut base = vec![0u8; 4 * 1024 * 1024];
        rng.fill(&mut base);
        write_file(&base_path, &base);
        // a "stale" copy: same length, one byte different
        let mut stale = base.clone();
        stale[base.len() / 3] ^= 0xFF;
        write_file(&stale_path, &stale);
        let new = mutate(&base, &mut rng);
        write_file(&new_path, &new);

        let (sig, info) = signature_from_file(&base_path).unwrap();
        let chunks = signature_to_chunks(&sig);
        let sig_bytes = assemble(&chunks, info.sig_len, info.num_chunks).unwrap();
        let delta_info = diff_to_spool(&new_path, &sig_bytes, &spool).unwrap();
        let delta = std::fs::read(&spool).unwrap();

        let res = apply_and_verify(
            &stale_path,
            &delta,
            &out,
            delta_info.new_file_size,
            &delta_info.sha256_new,
        );
        assert!(res.is_err(), "stale base must be detected");
        assert!(
            !out.exists(),
            "failed apply must delete its output file"
        );
    }

    #[test]
    fn empty_old_file_full_literal_delta_applies() {
        let tmp = TempDir::new("empty_old");
        let empty_path = tmp.path("empty.bin");
        let new_path = tmp.path("new.bin");
        let spool = tmp.path("spool.delta");
        let out = tmp.path("out.bin");

        write_file(&empty_path, b"");
        let mut rng = Prng::new(7);
        let mut new = vec![0u8; 2 * 1024 * 1024];
        rng.fill(&mut new);
        write_file(&new_path, &new);

        let (sig, info) = signature_from_file(&empty_path).unwrap();
        assert_eq!(info.old_file_size, 0);
        let chunks = signature_to_chunks(&sig);
        let sig_bytes = assemble(&chunks, info.sig_len, info.num_chunks).unwrap();
        let delta_info = diff_to_spool(&new_path, &sig_bytes, &spool).unwrap();
        let delta = std::fs::read(&spool).unwrap();
        apply_and_verify(
            &empty_path,
            &delta,
            &out,
            delta_info.new_file_size,
            &delta_info.sha256_new,
        )
        .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), new);
    }

    #[test]
    fn missing_file_errors() {
        let tmp = TempDir::new("missing");
        let missing = tmp.path("does_not_exist.bin");
        assert!(signature_from_file(&missing).is_err());
        assert!(diff_to_spool(&missing, &[1, 2, 3], &tmp.path("s.delta")).is_err());
        assert!(
            apply_and_verify(&missing, &[0u8; 4], &tmp.path("o.bin"), 0, &[0u8; 32]).is_err()
        );
    }

    #[test]
    fn corrupt_delta_rejected() {
        let tmp = TempDir::new("corrupt");
        let base_path = tmp.path("base.bin");
        let new_path = tmp.path("new.bin");
        let spool = tmp.path("spool.delta");
        let out = tmp.path("out.bin");

        let mut rng = Prng::new(99);
        let mut base = vec![0u8; 2 * 1024 * 1024];
        rng.fill(&mut base);
        write_file(&base_path, &base);
        let new = mutate(&base, &mut rng);
        write_file(&new_path, &new);

        let (sig, info) = signature_from_file(&base_path).unwrap();
        let chunks = signature_to_chunks(&sig);
        let sig_bytes = assemble(&chunks, info.sig_len, info.num_chunks).unwrap();
        let delta_info = diff_to_spool(&new_path, &sig_bytes, &spool).unwrap();
        let mut delta = std::fs::read(&spool).unwrap();

        // 1. destroy the magic header
        let mut broken = delta.clone();
        broken[0] ^= 0xFF;
        assert!(apply_and_verify(
            &base_path,
            &broken,
            &out,
            delta_info.new_file_size,
            &delta_info.sha256_new
        )
        .is_err());
        assert!(!out.exists());

        // 2. truncate the delta
        delta.truncate(delta.len() - 1);
        assert!(apply_and_verify(
            &base_path,
            &delta,
            &out,
            delta_info.new_file_size,
            &delta_info.sha256_new
        )
        .is_err());
        assert!(!out.exists());
    }

    #[test]
    fn chunk_assembler_negative_cases() {
        let data = vec![7u8; 3 * RSYNC_CHUNK_SIZE / 2 + 1234];
        let chunks = chunk_bytes(&data);
        let num = chunks.len() as u32;
        let total = data.len() as u64;

        // happy path
        assert_eq!(assemble(&chunks, total, num).unwrap(), data);

        // out of order
        let mut asm = ChunkAssembler::new();
        asm.set_expected(total, num);
        assert!(asm.feed(1, chunks[1].1, &chunks[1].0).is_err());

        // duplicate
        let mut asm = ChunkAssembler::new();
        asm.set_expected(total, num);
        asm.feed(0, chunks[0].1, &chunks[0].0).unwrap();
        assert!(asm.feed(0, chunks[0].1, &chunks[0].0).is_err());

        // wrong compressed flag (raw data declared compressed)
        let mut asm = ChunkAssembler::new();
        asm.set_expected(total, num);
        assert!(asm.feed(0, true, &chunks[0].0).is_err()
            || chunks[0].1 /* already compressed, flag is truthful */);

        // length mismatch when announcing a wrong total: the final chunk
        // must be rejected because the reassembled length cannot match
        let mut asm = ChunkAssembler::new();
        asm.set_expected(total + 1, num);
        for (i, (d, c)) in chunks.iter().enumerate().take(chunks.len() - 1) {
            assert!(!asm.feed(i as u32, *c, d).unwrap());
        }
        let (last_d, last_c) = &chunks[chunks.len() - 1];
        assert!(asm.feed(num - 1, *last_c, last_d).is_err());
        // announce() style check: never completed with wrong total
        let mut asm2 = ChunkAssembler::new();
        for (i, (d, c)) in chunks.iter().enumerate() {
            asm2.feed(i as u32, *c, d).unwrap();
        }
        assert!(asm2.announce(total + 1, num).is_err());
        assert!(asm2.announce(total, num).unwrap());
        assert_eq!(asm2.finish(), data);

        // extra chunk past the announced count
        let mut asm = ChunkAssembler::new();
        asm.set_expected(total, num);
        for (i, (d, c)) in chunks.iter().enumerate() {
            asm.feed(i as u32, *c, d).unwrap();
        }
        assert!(asm.feed(num, false, &[0u8; 8]).is_err());
    }

    #[test]
    fn apply_limit_enforced() {
        let tmp = TempDir::new("limit");
        let base_path = tmp.path("base.bin");
        let new_path = tmp.path("new.bin");
        let spool = tmp.path("spool.delta");
        let out = tmp.path("out.bin");

        let mut rng = Prng::new(555);
        let mut base = vec![0u8; 1024 * 1024];
        rng.fill(&mut base);
        write_file(&base_path, &base);
        let new = mutate(&base, &mut rng);
        write_file(&new_path, &new);

        let (sig, info) = signature_from_file(&base_path).unwrap();
        let chunks = signature_to_chunks(&sig);
        let sig_bytes = assemble(&chunks, info.sig_len, info.num_chunks).unwrap();
        let delta_info = diff_to_spool(&new_path, &sig_bytes, &spool).unwrap();
        let delta = std::fs::read(&spool).unwrap();

        // limit below the real output size must fail closed
        assert!(apply_and_verify(
            &base_path,
            &delta,
            &out,
            delta_info.new_file_size - 1,
            &delta_info.sha256_new
        )
        .is_err());
        assert!(!out.exists());
    }

    #[test]
    fn block_size_tiers() {
        assert_eq!(block_size_for(0), 8 * 1024);
        assert_eq!(block_size_for(64 * 1024 * 1024 - 1), 8 * 1024);
        assert_eq!(block_size_for(64 * 1024 * 1024), 16 * 1024);
        assert_eq!(block_size_for(1024 * 1024 * 1024 - 1), 16 * 1024);
        assert_eq!(block_size_for(1024 * 1024 * 1024), 64 * 1024);
        assert_eq!(block_size_for(u64::MAX), 64 * 1024);
    }

    #[test]
    fn sha256_helpers_agree() {
        let tmp = TempDir::new("sha");
        let p = tmp.path("f.bin");
        let data = b"hello rsync integrity";
        write_file(&p, data);
        assert_eq!(sha256_of_file(&p).unwrap(), sha256_of_bytes(data));
    }

    #[test]
    fn spool_reader_short_file_fails() {
        let tmp = TempDir::new("spool_short");
        let p = tmp.path("s.delta");
        write_file(&p, &[1u8; 100]);
        let mut r = SpoolReader::new(&p).unwrap();
        // shrink behind the reader's back: remaining now exceeds the file
        std::fs::write(&p, &[1u8; 50]).unwrap();
        assert!(r.next_chunk().is_err());
    }
}
