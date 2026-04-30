/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#ifndef DINGOFS_COMMON_SYNC_POINT_H_
#define DINGOFS_COMMON_SYNC_POINT_H_

// Test-only synchronization points for deterministic concurrency tests.
//
// In production builds (NDEBUG defined), TEST_SYNC_POINT() is a no-op macro
// with zero cost. In debug/test builds, it dispatches to a registry that
// allows tests to inject callbacks at named program locations to interleave
// threads, observe ordering, or simulate races deterministically.
//
// Pattern modeled after RocksDB's util/sync_point.h.
//
// Usage in production code:
//   TEST_SYNC_POINT("ChunkWriter::DoFlushAsync:between_locks");
//
// Usage in tests:
//   SyncPoint::Get()->SetCallBack(
//       "ChunkWriter::DoFlushAsync:between_locks",
//       [&](void*) { /* do something to trigger race */ });
//   SyncPoint::Get()->EnableProcessing();
//   ... run code ...
//   SyncPoint::Get()->DisableProcessing();
//   SyncPoint::Get()->ClearAllCallBacks();

#ifdef NDEBUG

// Production: no-op.
#define TEST_SYNC_POINT(name) ((void)0)
#define TEST_SYNC_POINT_CALLBACK(name, arg) ((void)0)

#else

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace dingofs {

class SyncPoint {
 public:
  static SyncPoint* Get() {
    static SyncPoint instance;
    return &instance;
  }

  void SetCallBack(const std::string& name,
                   std::function<void(void*)> callback) {
    std::lock_guard<std::mutex> lk(mutex_);
    callbacks_[name] = std::move(callback);
  }

  void ClearAllCallBacks() {
    std::lock_guard<std::mutex> lk(mutex_);
    callbacks_.clear();
  }

  void EnableProcessing() { enabled_.store(true, std::memory_order_release); }

  void DisableProcessing() { enabled_.store(false, std::memory_order_release); }

  void Process(const std::string& name, void* arg = nullptr) {
    if (!enabled_.load(std::memory_order_acquire)) return;
    std::function<void(void*)> cb;
    {
      std::lock_guard<std::mutex> lk(mutex_);
      auto it = callbacks_.find(name);
      if (it == callbacks_.end()) return;
      cb = it->second;
    }
    cb(arg);
  }

 private:
  SyncPoint() = default;
  std::atomic<bool> enabled_{false};
  std::mutex mutex_;
  std::unordered_map<std::string, std::function<void(void*)> > callbacks_;
};

}  // namespace dingofs

#define TEST_SYNC_POINT(name) ::dingofs::SyncPoint::Get()->Process(name)
#define TEST_SYNC_POINT_CALLBACK(name, arg) \
  ::dingofs::SyncPoint::Get()->Process(name, arg)

#endif  // NDEBUG

#endif  // DINGOFS_COMMON_SYNC_POINT_H_
