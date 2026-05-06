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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

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

// temporary store .stats file data
struct FileBuffer {
  size_t size{0};
  std::unique_ptr<char[]> data{nullptr};
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

  FileReader* reader{nullptr};
  FileWriter* writer{nullptr};   // nullptr for O_RDONLY

  std::atomic<int64_t> refs{0};

  FileBuffer file_buffer;

  std::string ToString() const;
};

class HandleManager {
 public:
  HandleManager(VFSHub* hub) : vfs_hub_(hub) {};

  ~HandleManager();

  Status Start();

  void Stop();

  // Build a new Handle for (fh, ino, flags). Allocates FileReader
  // unconditionally and acquires a writer from WriterTable for any
  // writable open mode.  Returns nullptr on failure.
  Handle* NewHandle(uint64_t fh, Ino ino, int flags);

  // Used by the .stats path — adds an externally-built (data-only) Handle.
  void AddHandle(Handle* handle);

  Handle* FindHandler(uint64_t fh);

  void ReleaseHandler(uint64_t fh);

  void Invalidate(uint64_t fh, int64_t offset, int64_t size);

  // Flush dirty data for the given inode. After WriterTable adoption this
  // is O(1): a single PeekWriter lookup + Flush.
  Status FlushByIno(Ino ino);

  // Invalidate read cache for all handles of the given inode in the given
  // range (per-inode).
  void InvalidateByIno(Ino ino, int64_t offset, int64_t size);

  void Summary(Json::Value& value);
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  // Atomic refs ops on Handle::refs. Caller must use these instead of
  // touching Handle::refs directly.
  void AcquireRefHandle(Handle* h);

  // Drop one ref on `h`; if it was the last ref, close the reader, return
  // the writer to WriterTable, and delete the handle.
  void ReleaseRefHandle(Handle* h);

  // Internal cleanup: closes reader, returns writer, deletes the handle.
  // Called from ReleaseRefHandle when refs hit 0.
  void DestroyHandle(Handle* h);

  VFSHub* vfs_hub_{nullptr};

  std::mutex mutex_;
  bool stopped_{false};
  std::unordered_map<uint64_t, Handle*> handles_;

  // metrics
  bvar::Adder<uint64_t> total_count_{"vfs_handle_total_count"};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_HANDLE_MANAGER_H
