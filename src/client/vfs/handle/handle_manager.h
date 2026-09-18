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

#ifndef DINGOFS_CLIENT_VFS_HANDLE_MANAGER_H
#define DINGOFS_CLIENT_VFS_HANDLE_MANAGER_H

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "bvar/reducer.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "json/value.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;
class FileReader;
class FileWriter;
class HandleManager;
class HandleManagerShard;

// temporary store .stats file data
struct FileBuffer {
  size_t size{0};
  std::unique_ptr<char[]> data{nullptr};
};

struct HandleResources {
  FileReader* reader{nullptr};
  FileWriter* writer{nullptr};
};

// Handle is a pure-data per-fh value object: identity (fh/ino/flags), the
// resources owned/borrowed for that fh (reader, writer, file_buffer), and
// an atomic refcount.  All lifecycle logic — including atomic ref ops,
// reader Close, writer return to WriterTable, and deletion — lives in
// HandleManager.  Handle has no methods that touch any of these.
struct Handle {
  Ino ino{0};
  uint64_t fh{0};
  int32_t flags{0};

  HandleResources resources;

  std::atomic<int64_t> refs{0};

  FileBuffer file_buffer;

  std::string ToString() const;
};

class HandleGuard {
 public:
  HandleGuard() = default;
  ~HandleGuard();

  HandleGuard(const HandleGuard&) = delete;
  HandleGuard& operator=(const HandleGuard&) = delete;

  HandleGuard(HandleGuard&& other) noexcept;
  HandleGuard& operator=(HandleGuard&& other) noexcept;

  Handle* get() const { return handle_; }
  Handle* operator->() const { return handle_; }
  explicit operator bool() const { return handle_ != nullptr; }

 private:
  friend class HandleManager;

  HandleGuard(HandleManager* manager, Handle* handle)
      : manager_(manager), handle_(handle) {}

  void Reset();

  HandleManager* manager_{nullptr};
  Handle* handle_{nullptr};
};

// One fh partition: its own index and lock. Handles are added and looked up
// here; HandleManager owns routing, drain, and resource return.
class alignas(64) HandleManagerShard {
 public:
  using Lock = std::unique_lock<std::mutex>;
  using HandleMap = std::unordered_map<uint64_t, Handle*>;
  struct Identity {
    Ino ino;
    uint64_t fh;
    int32_t flags;
  };

  bool Add(Handle* handle);
  Handle* Find(uint64_t fh, bool for_release);
  Handle* Remove(uint64_t fh);
  size_t Size() const;
  HandleMap ExtractAll();

  // Locked operations require LockForSnapshot(). Multi-shard callers acquire
  // locks in ascending shard order and release them before any callback or
  // I/O.
  Lock LockForSnapshot() { return Lock(mutex_); }
  size_t SizeLocked() const { return handles_.size(); }
  void CloseAdmissionLocked();
  void AppendIdentitiesLocked(std::vector<Identity>& out) const;

  // After admission closes, wait for table-resident guards, then pin and
  // detach. Callers retain every returned holder until the final flush.
  void Drain(std::vector<HandleResources>& resources,
             std::vector<Handle*>& stop_refs);
  HandleResources Detach(Handle* handle);
  void NotifyRefReleased();

 private:
  static void Ref(Handle* handle);
  static HandleResources DetachLocked(Handle* handle);

  // Guard release reads only this cold line during normal operation.
  struct alignas(64) DrainState {
    std::atomic<bool> closing{false};
    std::condition_variable cv;
  };

  mutable std::mutex mutex_;
  HandleMap handles_;
  DrainState drain_;
};

class HandleManager {
 public:
  HandleManager(VFSHub* hub) : vfs_hub_(hub){};

  ~HandleManager();

  Status Start();

  // The owner drains public operations before Stop and keeps this manager alive
  // through every guard release. Stop drains table-resident guards, flushes
  // writers, and detaches resources; erased handles remain the owner's concern.
  Status Stop();

  // Build a new Handle for (fh, ino, flags). Allocates FileReader
  // unconditionally and acquires a writer from WriterTable for any
  // writable open mode.  Returns nullptr on failure.
  Handle* NewHandle(uint64_t fh, Ino ino, int flags);

  // Used by NewHandle and the .stats path. fh must be unique. Returns false
  // after stop admission closes; ownership stays with the caller on rejection.
  bool AddHandle(Handle* handle);

  // Data-path lookup: returns empty after stop admission closes.
  HandleGuard FindHandlerGuard(uint64_t fh);

  // Release-path lookup remains available during/after Stop so a late release
  // can remove the fh identity.
  HandleGuard FindHandlerForRelease(uint64_t fh);

  void ReleaseHandler(uint64_t fh);

  // Flush dirty data for the given inode. After WriterTable adoption this
  // is O(1): a single PeekWriter lookup + Flush.
  Status FlushByIno(Ino ino);

  // Best-effort count; Dump captures a consistent identity snapshot.
  void Summary(Json::Value& value);
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  friend class HandleGuard;
  static constexpr size_t kShardCount = 64;

  using ShardLocks = std::array<HandleManagerShard::Lock, kShardCount>;

  HandleManagerShard& GetShard(uint64_t fh);
  ShardLocks LockShards();
  size_t Size() const;

  // Drop one ref on `h`; if it was the last ref, close the reader, return
  // the writer to WriterTable, and delete the handle.
  void ReleaseRefHandle(Handle* h, HandleManagerShard& shard);

  // Internal cleanup: closes reader, returns writer, deletes the handle.
  // Called from ReleaseRefHandle when refs hit 0.
  void DestroyHandle(Handle* h, HandleManagerShard& shard);

  void ReleaseGuard(Handle* h);

  // No index lock may be held while releasing resources.
  void ReleaseHandleResources(HandleResources resources);

  VFSHub* vfs_hub_{nullptr};

  std::array<HandleManagerShard, kShardCount> shards_;
  std::mutex stop_mutex_;  // Lifecycle only; never on the lookup/release path.
  bool stopped_{false};

  // metrics
  bvar::Adder<uint64_t> total_count_{"vfs_handle_total_count"};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_HANDLE_MANAGER_H
