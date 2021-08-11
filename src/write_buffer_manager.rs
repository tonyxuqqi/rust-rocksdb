// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Write, ptr};

use crate::Cache;

/// WriteBufferManager is for managing memory allocation for one or more
/// MemTables.
pub struct WriteBufferManager {
    pub(crate) mgr: *mut librocksdb_sys::DBWriteBufferManager,
}

impl WriteBufferManager {
    /// `buffer_size` = 0 indicates no limit. Memory won't be capped, memory_usage()
    /// won't be valid. If `cache` is provided, we'll put dummy entries in the cache
    /// and cost the memory allocated to the cache. It can be used even if buffer_size = 0.
    pub fn new(buffer_size: usize, cache: Option<&Cache>) -> WriteBufferManager {
        WriteBufferManager {
            mgr: unsafe {
                librocksdb_sys::crocksdb_writebuffermanager_create(buffer_size, cache.map_or_else(ptr::null, |c| c.inner))
            }
        }
    }

    pub fn enabled(&self) -> bool {
        unsafe {
            librocksdb_sys::crocksdb_writebuffermanager_enabled(self.mgr)
        }
    }

    pub fn cost_to_cache(&self) -> bool {
        unsafe {
            librocksdb_sys::crocksdb_writebuffermanager_cost_to_cache(self.mgr)
        }
    }

    /// Only valid if enabled()
    pub fn memory_usage(&self) -> usize {
        unsafe {
            librocksdb_sys::crocksdb_writebuffermanager_memory_usage(self.mgr)
        }
    }

    pub fn mutable_memtable_memory_usage(&self) -> usize {
        unsafe {
            librocksdb_sys::crocksdb_writebuffermanager_mutable_memtable_memory_usage(self.mgr)
        }
    }

    pub fn buffer_size(&self) -> usize {
        unsafe {
            librocksdb_sys::crocksdb_writebuffermanager_buffer_size(self.mgr)
        }
    }
}

unsafe impl Send for WriteBufferManager {}
unsafe impl Sync for WriteBufferManager {}

impl Drop for WriteBufferManager {
    fn drop(&mut self) {
        unsafe {
            librocksdb_sys::crocksdb_writebuffermanager_destroy(self.mgr)
        }
    }
}
