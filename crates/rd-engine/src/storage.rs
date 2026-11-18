use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::util::unique_path;

/// The shared `.part` file of a task.
///
/// One coordinator handle is kept for flushing, size queries and commit. Each
/// worker opens its **own** handle via [`worker_handle`](Self::worker_handle)
/// and writes with positional writes: on Windows the positional write API
/// temporarily moves the file pointer, so concurrent writes must never share
/// one handle. Writes to *different* handles of the same file are safe and
/// independent.
#[derive(Clone)]
pub struct PartFile {
    file: Arc<File>,
    path: PathBuf,
}

impl PartFile {
    /// Create (or truncate) the part file, optionally pre-allocating `size`.
    #[cfg(test)]
    pub fn create(path: &Path, size: Option<u64>) -> Result<Self> {
        Self::create_with_sparse(path, size, false)
    }

    pub fn create_with_sparse(path: &Path, size: Option<u64>, sparse: bool) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        if let Some(size) = size {
            // Large out-of-order writes should not first zero-fill the gap
            // to each high offset on NTFS. Unsupported volumes fall back.
            if sparse && size >= 8 * 1024 * 1024 {
                mark_sparse(&file);
            }
            file.set_len(size)?;
        }
        Ok(Self {
            file: Arc::new(file),
            path: path.to_path_buf(),
        })
    }

    pub fn open_existing(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(Self {
            file: Arc::new(file),
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Open an independent handle for one worker's positional writes.
    pub fn worker_handle(&self) -> Result<Arc<File>> {
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        Ok(Arc::new(file))
    }

    /// Write at `offset` through an explicit worker handle.
    #[cfg(test)]
    pub async fn write_with(&self, file: Arc<File>, offset: u64, data: Bytes) -> Result<()> {
        tokio::task::spawn_blocking(move || write_all_at(&file, &data, offset)).await?
    }

    /// Queue exactly one bounded positional write and return its allocation.
    pub fn queue_write(
        &self,
        file: Arc<File>,
        offset: u64,
        data: Vec<u8>,
    ) -> tokio::task::JoinHandle<Result<Vec<u8>>> {
        tokio::task::spawn_blocking(move || {
            write_all_at(&file, &data, offset)?;
            Ok(data)
        })
    }

    #[cfg(test)]
    pub async fn write_reusing(
        &self,
        file: Arc<File>,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Vec<u8>> {
        self.queue_write(file, offset, data).await?
    }

    /// Reset the sole non-range worker's file before replaying a failed GET.
    pub async fn reset_len(&self, size: u64) -> Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.set_len(size)).await??;
        Ok(())
    }

    /// Flush file data (not metadata) to disk before checkpointing progress.
    pub async fn sync_data(&self) -> Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.sync_data()).await??;
        Ok(())
    }

    pub async fn sync_all(&self) -> Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.sync_all()).await??;
        Ok(())
    }

    pub async fn len(&self) -> Result<u64> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || Ok(file.metadata()?.len())).await?
    }
}

#[cfg(windows)]
fn write_all_at(file: &File, mut data: &[u8], mut offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;
    while !data.is_empty() {
        let n = file.seek_write(data, offset)?;
        if n == 0 {
            return Err(
                io::Error::new(io::ErrorKind::WriteZero, "write_at wrote zero bytes").into(),
            );
        }
        data = &data[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, mut data: &[u8], mut offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;
    while !data.is_empty() {
        let n = file.write_at(data, offset)?;
        if n == 0 {
            return Err(
                io::Error::new(io::ErrorKind::WriteZero, "write_at wrote zero bytes").into(),
            );
        }
        data = &data[n..];
        offset += n as u64;
    }
    Ok(())
}

/// Windows can avoid materializing unwritten holes before positional writes.
/// This does not mark any bytes downloaded; the checkpoint map still owns that.
#[cfg(windows)]
fn mark_sparse(file: &File) {
    use std::{ffi::c_void, os::windows::io::AsRawHandle, ptr};
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn DeviceIoControl(
            handle: *mut c_void,
            code: u32,
            input: *const c_void,
            input_size: u32,
            output: *mut c_void,
            output_size: u32,
            returned: *mut u32,
            overlapped: *mut c_void,
        ) -> i32;
    }
    const FSCTL_SET_SPARSE: u32 = 0x0009_00c4;
    let mut returned = 0u32;
    // SAFETY: the owned synchronous file handle remains live throughout the
    // call. Null input requests sparse=true; no output or OVERLAPPED is used,
    // and returned points to a live DWORD as required by DeviceIoControl.
    let result = unsafe {
        DeviceIoControl(
            file.as_raw_handle(),
            FSCTL_SET_SPARSE,
            ptr::null(),
            0,
            ptr::null_mut(),
            0,
            &mut returned,
            ptr::null_mut(),
        )
    };
    if result == 0 {
        tracing::debug!(error=%io::Error::last_os_error(), "sparse files unavailable; using normal allocation");
    }
}

#[cfg(not(windows))]
fn mark_sparse(_: &File) {} // Native Unix set_len already leaves holes.

/// Drop the handle and atomically rename the part file to its final name.
/// Resolves name collisions at commit time; returns the actual final path.
pub async fn commit(part: PartFile, final_path: &Path) -> Result<PathBuf> {
    let part_path = part.path.clone();
    let final_path = final_path.to_path_buf();
    drop(part);
    tokio::task::spawn_blocking(move || {
        let dir = final_path.parent().unwrap_or(Path::new("."));
        let name = final_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "download".into());
        let target = unique_path(dir, &name);
        std::fs::rename(&part_path, &target)?;
        Ok(target)
    })
    .await?
}

pub async fn remove_file_if_exists(path: &Path) -> Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::Io(e)),
    })
    .await?
}

pub async fn file_len(path: &Path) -> Result<Option<u64>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || match std::fs::metadata(&path) {
        Ok(m) => Ok(Some(m.len())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::Io(e)),
    })
    .await?
}

/// File integrity algorithms supported for verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgo {
    Sha256,
    Blake3,
}

pub async fn hash_file(path: &Path, algo: HashAlgo) -> Result<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut file = File::open(&path)?;
        let mut buf = vec![0u8; 1024 * 1024];
        let hex_out = match algo {
            HashAlgo::Sha256 => {
                let mut hasher = Sha256::new();
                loop {
                    let n = file.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                hex::encode(hasher.finalize())
            }
            HashAlgo::Blake3 => {
                let mut hasher = blake3::Hasher::new();
                loop {
                    let n = file.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                hasher.finalize().to_hex().to_string()
            }
        };
        Ok(hex_out)
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[tokio::test]
    async fn large_sparse_file_preserves_holes_and_tail() {
        use std::os::windows::fs::{FileExt, MetadataExt};
        let dir = tempfile::tempdir().unwrap();
        let size = 3 * 1024 * 1024 * 1024u64;
        let part = PartFile::create_with_sparse(&dir.path().join("sparse.part"), Some(size), true)
            .unwrap();
        // FAT/exFAT may legitimately decline the sparse optimization.
        if part.file.metadata().unwrap().file_attributes() & 0x200 == 0 {
            return;
        }
        let handle = part.worker_handle().unwrap();
        part.queue_write(handle.clone(), size - 4096, vec![37; 4096])
            .await
            .unwrap()
            .unwrap();
        let mut hole = [1u8; 32];
        assert_eq!(handle.seek_read(&mut hole, size / 2).unwrap(), 32);
        assert_eq!(hole, [0; 32]);
        let mut tail = [0u8; 32];
        handle.seek_read(&mut tail, size - 32).unwrap();
        assert_eq!(tail, [37; 32]);
        assert_eq!(part.len().await.unwrap(), size);
    }

    #[tokio::test]
    async fn write_buffer_allocation_is_reused() {
        let dir = tempfile::tempdir().unwrap();
        let part = PartFile::create(&dir.path().join("reuse.part"), None).unwrap();
        let handle = part.worker_handle().unwrap();
        let data = vec![42; 256 * 1024];
        let allocation = data.as_ptr() as usize;
        let mut data = part.write_reusing(handle.clone(), 0, data).await.unwrap();
        assert_eq!(data.as_ptr() as usize, allocation);
        data.fill(17);
        let data = part
            .write_reusing(handle, data.len() as u64, data)
            .await
            .unwrap();
        assert_eq!(data.as_ptr() as usize, allocation);
        let actual = std::fs::read(part.path()).unwrap();
        assert!(actual[..data.len()].iter().all(|&b| b == 42));
        assert!(actual[data.len()..].iter().all(|&b| b == 17));
    }

    #[tokio::test]
    async fn concurrent_positional_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.part");
        let part = PartFile::create(&path, Some(3000)).unwrap();
        let mut handles = Vec::new();
        for i in 0..3u64 {
            let part = part.clone();
            handles.push(tokio::spawn(async move {
                // Each writer uses its own handle, mirroring real workers.
                let handle = part.worker_handle().unwrap();
                let data = Bytes::from(vec![(i + 1) as u8; 1000]);
                part.write_with(handle, i * 1000, data).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        part.sync_all().await.unwrap();
        let content = std::fs::read(&path).unwrap();
        assert_eq!(content.len(), 3000);
        assert!(content[..1000].iter().all(|&b| b == 1));
        assert!(content[1000..2000].iter().all(|&b| b == 2));
        assert!(content[2000..].iter().all(|&b| b == 3));
    }

    #[tokio::test]
    async fn sha256_known_vector() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("abc.txt");
        std::fs::write(&path, b"abc").unwrap();
        let hash = hash_file(&path, HashAlgo::Sha256).await.unwrap();
        assert_eq!(
            hash,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[tokio::test]
    async fn blake3_known_vector() {
        // Official BLAKE3 test vector for the empty input.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        std::fs::write(&path, b"").unwrap();
        let hash = hash_file(&path, HashAlgo::Blake3).await.unwrap();
        assert_eq!(
            hash,
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
    }

    #[tokio::test]
    async fn commit_renames_and_resolves_collision() {
        let dir = tempfile::tempdir().unwrap();
        let part_path = dir.path().join("f.bin.1.part");
        let part = PartFile::create(&part_path, None).unwrap();
        let handle = part.worker_handle().unwrap();
        part.write_with(handle, 0, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let final_path = dir.path().join("f.bin");
        std::fs::write(&final_path, b"existing").unwrap();
        let actual = commit(part, &final_path).await.unwrap();
        assert_eq!(actual.file_name().unwrap(), "f (1).bin");
        assert_eq!(std::fs::read(&actual).unwrap(), b"hello");
        assert!(!part_path.exists());
    }

    #[tokio::test]
    async fn write_failure_propagates() {
        let dir = tempfile::tempdir().unwrap();
        // A directory in place of the part file makes creation fail.
        let path = dir.path().join("blocked");
        std::fs::create_dir(&path).unwrap();
        let result = PartFile::create(&path, Some(10));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn remove_missing_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        remove_file_if_exists(&dir.path().join("nope"))
            .await
            .unwrap();
    }
}
