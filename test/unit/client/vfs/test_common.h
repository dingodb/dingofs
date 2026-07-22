/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_TEST_UNIT_CLIENT_VFS_TEST_COMMON_H_
#define DINGOFS_TEST_UNIT_CLIENT_VFS_TEST_COMMON_H_

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "client/vfs/vfs_meta.h"
#include "common/meta.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

// Async waiter helper: submit N async ops then wait for all to complete.
// Usage:
//   AsyncWaiter waiter;
//   waiter.Expect(N);
//   ... submit N ops, each calling waiter.Done() in callback ...
//   waiter.Wait();
// A timed-out Wait() only records an EXPECT failure; the callbacks it was
// waiting for are still queued and still reference this object. The destructor
// drains them before the stack frame dies -- otherwise a late Done() writes to
// a dead frame, and its continuation may Execute() on an already-stopped
// executor (SIGSEGV / CHECK abort seen under heavy scheduler starvation).
struct AsyncWaiter {
  std::mutex mtx;
  std::condition_variable cv;
  int pending{0};  // guarded by mtx (destructor handshake needs the lock)

  ~AsyncWaiter() {
    std::unique_lock<std::mutex> lk(mtx);
    bool drained = cv.wait_for(lk, std::chrono::seconds(60),
                               [this] { return pending <= 0; });
    CHECK(drained) << "AsyncWaiter destroyed with " << pending
                   << " callback(s) still pending";
  }

  void Expect(int n) {
    std::lock_guard<std::mutex> lk(mtx);
    pending = n;
  }

  void Done() {
    std::lock_guard<std::mutex> lk(mtx);
    if (--pending <= 0) {
      cv.notify_all();
    }
  }

  void Wait(std::chrono::seconds timeout = std::chrono::seconds(10)) {
    std::unique_lock<std::mutex> lk(mtx);
    cv.wait_for(lk, timeout, [this] { return pending <= 0; });
    EXPECT_EQ(pending, 0);
  }
};

// Create a test FsInfo with sensible defaults.
inline FsInfo MakeTestFsInfo(uint64_t chunk_size = 64 * 1024 * 1024,
                             uint64_t block_size = 4 * 1024 * 1024) {
  FsInfo info;
  info.id = 1;
  info.name = "test_fs";
  info.chunk_size = chunk_size;
  info.block_size = block_size;
  info.uuid = "test-uuid-1234";
  info.status = FsStatus::kNormal;
  return info;
}

// Create a test Slice.
inline Slice MakeSlice(uint64_t id, int32_t pos, int32_t size,
                       int32_t off = 0, int32_t len = -1) {
  if (len < 0) len = size - off;
  return Slice{.id = id, .size = size, .off = off, .len = len, .pos = pos};
}

// Create a test file Attr.
inline Attr MakeFileAttr(Ino ino, uint64_t size = 0) {
  Attr attr;
  attr.ino = ino;
  attr.type = dingofs::kFile;
  attr.length = size;
  attr.uid = 0;
  attr.gid = 0;
  attr.mode = 0644;
  attr.nlink = 1;
  return attr;
}

// Create a test directory Attr.
inline Attr MakeDirAttr(Ino ino) {
  Attr attr;
  attr.ino = ino;
  attr.type = dingofs::kDirectory;
  attr.length = 4096;
  attr.uid = 0;
  attr.gid = 0;
  attr.mode = 0755;
  attr.nlink = 2;
  return attr;
}

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CLIENT_VFS_TEST_COMMON_H_
