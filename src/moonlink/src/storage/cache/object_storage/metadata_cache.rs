use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use opendal::Metadata;
use tokio::sync::RwLock;

/// Configuration for the metadata cache
#[derive(Clone, Debug)]
pub struct MetadataCacheConfig {
    /// Maximum number of entries to cache
    pub max_entries: usize,
    /// Time-to-live for cache entries
    pub ttl: Duration,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10000,
            ttl: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// A cache entry containing metadata and timestamp
#[derive(Clone, Debug)]
struct CacheEntry {
    metadata: Metadata,
    timestamp: Instant,
}

/// Cache for object metadata to reduce stats calls
#[derive(Clone)]
pub struct ObjectMetadataCache {
    config: MetadataCacheConfig,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
}

// Manual Debug implementation for ObjectMetadataCache
impl std::fmt::Debug for ObjectMetadataCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectMetadataCache")
            .field("config", &self.config)
            .finish()
    }
}

impl ObjectMetadataCache {
    pub fn new(config: MetadataCacheConfig) -> Self {
        Self {
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get metadata from cache if available and not expired
    pub async fn get(&self, path: &str) -> Option<Metadata> {
        let cache = self.cache.read().await;
        if let Some(entry) = cache.get(path) {
            if entry.timestamp.elapsed() < self.config.ttl {
                return Some(entry.metadata.clone());
            }
        }
        None
    }

    /// Put metadata into cache, evicting oldest entries if at capacity
    pub async fn put(&self, path: String, metadata: Metadata) {
        let mut cache = self.cache.write().await;
        
        // If at capacity, remove oldest entry
        if cache.len() >= self.config.max_entries && !cache.contains_key(&path) {
            // Find and remove the oldest entry
            if let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.timestamp)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest_key);
            }
        }

        cache.insert(
            path,
            CacheEntry {
                metadata,
                timestamp: Instant::now(),
            },
        );
    }

    /// Clear all cache entries
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// Remove a specific entry from cache
    pub async fn invalidate(&self, path: &str) {
        let mut cache = self.cache.write().await;
        cache.remove(path);
    }

    /// Get the number of cached entries
    #[cfg(test)]
    pub async fn size(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }
}

/// Trait for filesystem accessors with metadata caching
#[async_trait]
pub trait CachedMetadataAccess: Send + Sync {
    /// Get object metadata, using cache if available
    async fn stats_object_cached(
        &self,
        object: &str,
        cache: &ObjectMetadataCache,
    ) -> crate::Result<Metadata>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_metadata_cache_basic() {
        let config = MetadataCacheConfig {
            max_entries: 2,
            ttl: Duration::from_secs(10),
        };
        let cache = ObjectMetadataCache::new(config);

        // Test empty cache
        assert_eq!(cache.get("test.txt").await, None);

        // Create mock metadata
        let metadata = opendal::Metadata::new(opendal::EntryMode::FILE);
        
        // Test put and get
        cache.put("test.txt".to_string(), metadata.clone()).await;
        assert!(cache.get("test.txt").await.is_some());
        assert_eq!(cache.size().await, 1);

        // Test multiple entries
        let metadata2 = opendal::Metadata::new(opendal::EntryMode::FILE);
        cache.put("test2.txt".to_string(), metadata2.clone()).await;
        assert_eq!(cache.size().await, 2);

        // Test eviction when at capacity
        let metadata3 = opendal::Metadata::new(opendal::EntryMode::FILE);
        cache.put("test3.txt".to_string(), metadata3).await;
        assert_eq!(cache.size().await, 2); // Should still be 2 due to max_entries

        // Test invalidate
        cache.invalidate("test2.txt").await;
        assert!(cache.get("test2.txt").await.is_none());

        // Test clear
        cache.clear().await;
        assert_eq!(cache.size().await, 0);
    }

    #[tokio::test]
    async fn test_metadata_cache_ttl() {
        let config = MetadataCacheConfig {
            max_entries: 10,
            ttl: Duration::from_millis(100),
        };
        let cache = ObjectMetadataCache::new(config);

        let metadata = opendal::Metadata::new(opendal::EntryMode::FILE);
        cache.put("test.txt".to_string(), metadata).await;
        
        // Should be available immediately
        assert!(cache.get("test.txt").await.is_some());
        
        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;
        
        // Should be expired now
        assert!(cache.get("test.txt").await.is_none());
    }
}