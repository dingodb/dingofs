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
// Pattern modeled after RocksDB's util/sync_point.h: in production builds
// (NDEBUG defined) the SyncPoint class is not declared and the
// TEST_SYNC_POINT macros expand to nothing — production code pays zero
// cost. In Debug/test builds, callbacks can be registered at named program
// points to interleave threads, observe ordering, or simulate races.
//
// Production code embeds sync points via the macro (no #ifdef needed):
//
//   TEST_SYNC_POINT("ChunkWriter::DoFlushAsync:between_locks");
//
// Tests drive callbacks through the class API directly:
//
//   SyncPoint::GetInstance()->SetCallBack(
//       "ChunkWriter::DoFlushAsync:between_locks",
//       [&](void*) { /* trigger race */ });
//   SyncPoint::GetInstance()->EnableProcessing();
//   ... run code ...
//   SyncPoint::GetInstance()->DisableProcessing();
//   SyncPoint::GetInstance()->ClearAllCallBacks();
//
// CI contract: tests are built and executed only by the unit-test job, which
// uses CMAKE_BUILD_TYPE=Debug (no NDEBUG). The release-binary job builds
// with NDEBUG and BUILD_UNIT_TESTS=OFF, so tests using SyncPoint::GetInstance
// directly never enter that compilation. See .github/workflows/ci-build.yml.

#ifdef NDEBUG

#define TEST_SYNC_POINT(name)
#define TEST_SYNC_POINT_CALLBACK(name, arg)

#else

#include <functional>
#include <string>

namespace dingofs {

class SyncPoint {
 public:
  static SyncPoint* GetInstance();

  SyncPoint(const SyncPoint&) = delete;
  SyncPoint& operator=(const SyncPoint&) = delete;
  ~SyncPoint();

  // Register a callback to fire when Process(point) is invoked from
  // production code (typically via TEST_SYNC_POINT). Replaces any previous
  // callback bound to the same point.
  void SetCallBack(const std::string& point,
                   const std::function<void(void*)>& callback);

  // Drop all registered callbacks.
  void ClearAllCallBacks();

  // Enable sync point processing (disabled on startup).
  void EnableProcessing();

  // Disable sync point processing without clearing callbacks.
  void DisableProcessing();

  // Triggered by TEST_SYNC_POINT; invokes the registered callback for `point`
  // with `cb_arg`. No-op when processing is disabled or no callback bound.
  void Process(const std::string& point, void* cb_arg = nullptr);

  // PIMPL — implementation lives in sync_point.cc.
  struct Data;

 private:
  SyncPoint();
  Data* impl_;
};

}  // namespace dingofs

#define TEST_SYNC_POINT(name) \
  ::dingofs::SyncPoint::GetInstance()->Process(name)
#define TEST_SYNC_POINT_CALLBACK(name, arg) \
  ::dingofs::SyncPoint::GetInstance()->Process(name, arg)

#endif  // NDEBUG

#endif  // DINGOFS_COMMON_SYNC_POINT_H_
