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

#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>

#include "client/vfs/data/ifile.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

// temporary store .stats file data
struct FileBuffer {
  size_t size{0};
  std::unique_ptr<char[]> data{nullptr};
};

struct Handle {
  Ino ino;
  uint64_t fh;
  // file ralted
  int32_t flags;
  std::unique_ptr<IFile> file;

  FileBuffer file_buffer;

  std::string ToString() const {
    std::ostringstream oss;
    oss << "Handle{ino: " << ino << ", fh; " << fh << ", flags: " << std::oct
        << flags << "}";
    return oss.str();
  }
};

using HandleSPtr = std::shared_ptr<Handle>;

class HandleManager {
 public:
  HandleManager(VFSHub* hub) : vfs_hub_(hub) {};

  ~HandleManager() { Shutdown(); }

  void AddHandle(HandleSPtr handle);

  HandleSPtr FindHandler(uint64_t fh);

  void ReleaseHandler(uint64_t fh);

  void Invalidate(uint64_t fh, int64_t offset, int64_t size);

  void Shutdown();

  void TriggerFlushAll();

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  std::mutex mutex_;
  bool shutdown_{false};
  std::unordered_map<uint64_t, HandleSPtr> handles_;
  VFSHub* vfs_hub_{nullptr};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_HANDLE_MANAGER_H
