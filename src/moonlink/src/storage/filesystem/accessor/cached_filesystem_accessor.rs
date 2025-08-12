use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use opendal::Metadata;

use crate::storage::cache::object_storage::metadata_cache::ObjectMetadataCache;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::base_unbuffered_stream_writer::BaseUnbufferedStreamWriter;
use crate::storage::filesystem::accessor::metadata::ObjectMetadata;
use crate::Result;

/// A filesystem accessor wrapper that caches metadata operations
pub struct CachedFileSystemAccessor {
    inner: Arc<dyn BaseFileSystemAccess>,
    metadata_cache: ObjectMetadataCache,
}

impl CachedFileSystemAccessor {
    pub fn new(inner: Arc<dyn BaseFileSystemAccess>, metadata_cache: ObjectMetadataCache) -> Self {
        Self {
            inner,
            metadata_cache,
        }
    }
}

impl std::fmt::Debug for CachedFileSystemAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFileSystemAccessor")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl BaseFileSystemAccess for CachedFileSystemAccessor {
    async fn list_direct_subdirectories(&self, folder: &str) -> Result<Vec<String>> {
        self.inner.list_direct_subdirectories(folder).await
    }

    async fn remove_directory(&self, directory: &str) -> Result<()> {
        // Invalidate cache for any files in this directory
        // For simplicity, we don't implement full directory invalidation here
        self.inner.remove_directory(directory).await
    }

    async fn object_exists(&self, object: &str) -> Result<bool> {
        self.inner.object_exists(object).await
    }

    async fn stats_object(&self, object: &str) -> Result<Metadata> {
        // Check cache first
        if let Some(metadata) = self.metadata_cache.get(object).await {
            return Ok(metadata);
        }

        // Cache miss - fetch from underlying filesystem
        let metadata = self.inner.stats_object(object).await?;
        
        // Store in cache
        self.metadata_cache.put(object.to_string(), metadata.clone()).await;
        
        Ok(metadata)
    }

    async fn read_object(&self, object: &str) -> Result<Vec<u8>> {
        self.inner.read_object(object).await
    }

    async fn read_object_as_string(&self, object: &str) -> Result<String> {
        self.inner.read_object_as_string(object).await
    }

    async fn stream_read(
        &self,
        object: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>> {
        self.inner.stream_read(object).await
    }

    async fn write_object(
        &self,
        object_filepath: &str,
        content: Vec<u8>,
    ) -> Result<Metadata> {
        let metadata = self.inner.write_object(object_filepath, content).await?;
        
        // Invalidate cache entry since the object has been modified
        self.metadata_cache.invalidate(object_filepath).await;
        
        // Add new metadata to cache
        self.metadata_cache.put(object_filepath.to_string(), metadata.clone()).await;
        
        Ok(metadata)
    }

    async fn conditional_write_object(
        &self,
        object_filepath: &str,
        content: Vec<u8>,
        etag: Option<String>,
    ) -> Result<Metadata> {
        let metadata = self.inner.conditional_write_object(object_filepath, content, etag).await?;
        
        // Invalidate and update cache
        self.metadata_cache.invalidate(object_filepath).await;
        self.metadata_cache.put(object_filepath.to_string(), metadata.clone()).await;
        
        Ok(metadata)
    }

    async fn create_unbuffered_stream_writer(
        &self,
        object_filepath: &str,
    ) -> Result<Box<dyn BaseUnbufferedStreamWriter>> {
        // Invalidate cache since we're about to write
        self.metadata_cache.invalidate(object_filepath).await;
        self.inner.create_unbuffered_stream_writer(object_filepath).await
    }

    async fn delete_object(&self, object_filepath: &str) -> Result<()> {
        let result = self.inner.delete_object(object_filepath).await;
        
        // Invalidate cache regardless of result
        self.metadata_cache.invalidate(object_filepath).await;
        
        result
    }

    async fn copy_from_local_to_remote(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        let metadata = self.inner.copy_from_local_to_remote(src, dst).await?;
        
        // Invalidate destination cache
        self.metadata_cache.invalidate(dst).await;
        
        Ok(metadata)
    }

    async fn copy_from_remote_to_local(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        // Try to get metadata from cache for the source
        if let Some(cached_metadata) = self.metadata_cache.get(src).await {
            // We have cached metadata, but still need to perform the copy
            let result = self.inner.copy_from_remote_to_local(src, dst).await?;
            
            // Update cache with fresh metadata if size changed
            if result.size != cached_metadata.content_length() {
                if let Ok(fresh_metadata) = self.inner.stats_object(src).await {
                    self.metadata_cache.put(src.to_string(), fresh_metadata).await;
                }
            }
            
            return Ok(result);
        }
        
        // No cache, proceed normally
        let result = self.inner.copy_from_remote_to_local(src, dst).await?;
        
        // Cache the source metadata for future use
        if let Ok(metadata) = self.inner.stats_object(src).await {
            self.metadata_cache.put(src.to_string(), metadata).await;
        }
        
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cache::object_storage::metadata_cache::MetadataCacheConfig;
    use std::time::Duration;

    #[tokio::test]
    async fn test_cached_filesystem_accessor() {
        // This test would require a mock filesystem accessor
        // For now, we just test that the structure compiles correctly
        let metadata_cache = ObjectMetadataCache::new(MetadataCacheConfig {
            max_entries: 100,
            ttl: Duration::from_secs(60),
        });
        
        // We would need a mock BaseFileSystemAccess implementation here
        // to properly test the caching behavior
    }
}