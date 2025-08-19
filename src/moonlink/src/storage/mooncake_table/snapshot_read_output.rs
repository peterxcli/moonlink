use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::storage::PuffinDeletionBlobAtRead;
use crate::table_notify::EvictedFiles;
use crate::table_notify::TableEvent;
use crate::ReadStateFilepathRemap;
use crate::{NonEvictableHandle, ReadState, Result};

use std::sync::Arc;

use tokio::sync::mpsc::Sender;

/// Mooncake snapshot for read.
///
/// Pass out two types of data files to read.
#[derive(Clone, Debug)]
pub enum DataFileForRead {
    /// Temporary data file for in-memory unpersisted data, used for union read.
    TemporaryDataFile(String),
    /// Pass out (file id, remote file path) and rely on read-through cache.
    RemoteFilePath((TableUniqueFileId, String)),
}

impl DataFileForRead {
    /// Get a file path to read.
    #[cfg(test)]
    pub fn get_file_path(&self) -> String {
        match self {
            Self::TemporaryDataFile(file) => file.clone(),
            Self::RemoteFilePath((_, file)) => file.clone(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ReadOutput {
    /// Data files contains two parts:
    /// 1. Committed and persisted data files, which consists of file id and remote path (if any).
    /// 2. Associated files, which include committed but un-persisted records.
    pub data_file_paths: Vec<DataFileForRead>,
    /// Puffin cache handles.
    pub puffin_cache_handles: Vec<NonEvictableHandle>,
    /// Deletion vectors persisted in puffin files.
    pub deletion_vectors: Vec<PuffinDeletionBlobAtRead>,
    /// Committed but un-persisted positional deletion records.
    pub position_deletes: Vec<(u32 /*file_index*/, u32 /*row_index*/)>,
    /// Contains committed but non-persisted record batches, which are persisted as temporary data files on local filesystem.
    pub associated_files: Vec<String>,
    /// Table notifier for query completion; could be none for empty read output.
    pub table_notifier: Option<Sender<TableEvent>>,
    /// Object storage cache, to pin local file cache, could be none for empty read output.
    pub object_storage_cache: Option<Arc<dyn CacheTrait>>,
    /// Filesystem accessor, to access remote storage, could be none for empty read output.
    pub filesystem_accessor: Option<Arc<dyn BaseFileSystemAccess>>,
}

impl ReadOutput {
    /// Resolve all remote filepaths and convert into [`ReadState`] for query usage.
    ///
    /// TODO(hjiang): Parallelize download and pin.
    pub async fn take_as_read_state(
        mut self,
        read_state_filepath_remap: ReadStateFilepathRemap,
    ) -> Result<Arc<ReadState>> {
        // Resolve remote data files.
        let mut resolved_data_files = Vec::with_capacity(self.data_file_paths.len());
        let mut cache_handles: Vec<NonEvictableHandle> = vec![];
        let mut evicted_files_to_delete_on_error: Vec<String> = vec![];
        for cur_data_file in self.data_file_paths.into_iter() {
            match cur_data_file {
                DataFileForRead::TemporaryDataFile(file) => resolved_data_files.push(file),
                DataFileForRead::RemoteFilePath((file_id, remote_filepath)) => {
                    // If cache or filesystem accessor not provided, fall back to remote filepath.
                    if let (Some(object_storage_cache), Some(filesystem_accessor)) = (
                        self.object_storage_cache.as_ref(),
                        self.filesystem_accessor.as_ref(),
                    ) {
                        let res = object_storage_cache
                            .get_cache_entry(
                                file_id,
                                &remote_filepath,
                                filesystem_accessor.as_ref(),
                            )
                            .await;
                        match res {
                            Ok((cache_handle, files_to_delete)) => {
                                if let Some(cache_handle) = cache_handle {
                                    resolved_data_files
                                        .push(cache_handle.get_cache_filepath().to_string());
                                    cache_handles.push(cache_handle);
                                } else {
                                    resolved_data_files.push(remote_filepath);
                                }

                                if !files_to_delete.is_empty() {
                                    if let Some(table_notifier) = self.table_notifier.as_mut() {
                                        let _ = table_notifier
                                            .send(TableEvent::EvictedFilesToDelete {
                                                evicted_files: EvictedFiles {
                                                    files: files_to_delete.to_vec(),
                                                },
                                            })
                                            .await;
                                    }
                                }
                            }
                            Err(e) => {
                                // Unpin all previously pinned cache handles before propagating error.
                                for mut handle in cache_handles.drain(..) {
                                    let files_to_delete = handle.unreference().await;
                                    evicted_files_to_delete_on_error.extend(files_to_delete);
                                }

                                // Also unpin any puffin cache handles included in this read output.
                                for mut handle in self.puffin_cache_handles.drain(..) {
                                    let files_to_delete = handle.unreference().await;
                                    evicted_files_to_delete_on_error.extend(files_to_delete);
                                }

                                // Best-effort delete any temporary associated files created for this read.
                                for file in self.associated_files.drain(..) {
                                    let _ = tokio::fs::remove_file(&file).await;
                                }

                                // Best-effort notify deletions.
                                if !evicted_files_to_delete_on_error.is_empty() {
                                    if let Some(table_notifier) = self.table_notifier.as_mut() {
                                        let _ = table_notifier
                                            .send(TableEvent::EvictedFilesToDelete {
                                                evicted_files: EvictedFiles {
                                                    files: evicted_files_to_delete_on_error,
                                                },
                                            })
                                            .await;
                                    }
                                }
                                return Err(e);
                            }
                        }
                    } else {
                        resolved_data_files.push(remote_filepath);
                    }
                }
            }
        }

        // Construct read state.
        Ok(Arc::new(ReadState::new(
            // Data file and positional deletes for query.
            resolved_data_files,
            self.puffin_cache_handles,
            self.deletion_vectors,
            self.position_deletes,
            // Fields used for read state cleanup after query completion.
            self.associated_files,
            cache_handles,
            read_state_filepath_remap,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cache::object_storage::base_cache::MockCacheTrait;
    use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
    use crate::storage::filesystem::accessor::base_filesystem_accessor::MockBaseFileSystemAccess;
    use crate::storage::mooncake_table::cache_test_utils::{
        create_infinite_object_storage_cache, import_fake_cache_entry,
    };
    use crate::storage::mooncake_table::test_utils_commons::{get_fake_file_path, FAKE_FILE_ID};
    use mockall::Sequence;
    use smallvec::SmallVec;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_take_as_read_state_unpins_on_error() {
        // Prepare a real cache and a pinned handle we will inject via mock.
        let temp_dir = tempdir().unwrap();
        let real_cache: ObjectStorageCache = create_infinite_object_storage_cache(
            &temp_dir, /*optimize_local_filesystem=*/ false,
        );
        let pinned_handle = {
            let mut cache = real_cache.clone();
            import_fake_cache_entry(&temp_dir, &mut cache).await
        };

        // The handle is pinned once now; sanity check.
        assert_eq!(
            real_cache
                .get_non_evictable_entry_ref_count(&FAKE_FILE_ID)
                .await,
            1
        );

        // Build a mock cache that returns the pinned handle first, then an error on second call.
        let mut mock_cache = MockCacheTrait::new();
        let mut seq = Sequence::new();
        let handle_clone = pinned_handle.clone();
        mock_cache
            .expect_get_cache_entry()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_, _, _| {
                let handle_clone = handle_clone.clone();
                Box::pin(async move { Ok((Some(handle_clone), SmallVec::new())) })
            });
        mock_cache
            .expect_get_cache_entry()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _| {
                Box::pin(async move {
                    Err(crate::Error::from(std::io::Error::other(
                        "mocked IO failure",
                    )))
                })
            });

        // Filesystem accessor mock (unused, but required by signature).
        let filesystem_accessor = MockBaseFileSystemAccess::new();

        // Construct ReadOutput with two remote files; second call will error.
        let fake_remote_path = get_fake_file_path(&temp_dir);
        let read_output = ReadOutput {
            data_file_paths: vec![
                DataFileForRead::RemoteFilePath((FAKE_FILE_ID, fake_remote_path.clone())),
                DataFileForRead::RemoteFilePath((FAKE_FILE_ID, fake_remote_path)),
            ],
            puffin_cache_handles: Vec::new(),
            deletion_vectors: Vec::new(),
            position_deletes: Vec::new(),
            associated_files: Vec::new(),
            table_notifier: None,
            object_storage_cache: Some(Arc::new(mock_cache)),
            filesystem_accessor: Some(Arc::new(filesystem_accessor)),
        };

        // Invoke and expect error; previously pinned handle must be unpinned.
        let res = read_output
            .clone()
            .take_as_read_state(Arc::new(|p: String| p))
            .await;
        assert!(res.is_err());

        // The handle should have been unpinned back to evictable (non-evictable ref count == 0).
        assert_eq!(
            real_cache
                .get_non_evictable_entry_ref_count(&FAKE_FILE_ID)
                .await,
            0
        );

        // Do not unreference pinned_handle again; it was unpinned by error handling.
    }
}
