// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use rocksdb::write_buffer_manager::WriteBufferManager;
use rocksdb::{
    DBOptions, Writable, DB,
};

#[test]
fn test_write_buffer_manager() {
    let mut mgr = WriteBufferManager::new(0, None);
    assert!(!mgr.enabled());
    mgr = WriteBufferManager::new(102400, None);
    let path = super::tempdir_with_prefix("_rust_rocksdb_test_compact_range");
    let mut opts = DBOptions::new();
    opts.set_write_buffer_manager(&mgr);
    opts.create_if_missing(true);
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();
    for i in 0..9999 {
        let kv = format!("k{:04}", i);
        db.put(kv.as_bytes(), kv.as_bytes()).unwrap();
    }
    let mut usage = mgr.memory_usage();
    assert!(usage > 4096, "{}", usage);
    db.flush(true).unwrap();
    usage = mgr.memory_usage();
    assert!(usage <= 4096, "{}", usage);
}
