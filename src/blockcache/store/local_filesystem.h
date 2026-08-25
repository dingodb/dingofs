/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_STORE_LOCAL_FILESYSTEM_H_
#define DINGOFS_BLOCKCACHE_STORE_LOCAL_FILESYSTEM_H_

#include <cstdint>
#include <memory>
#include <string>

#include "blockcache/common/status.h"
#include "blockcache/core/fs/filesystem.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/utils/align.h"

namespace dingofs {
namespace blockcache {

class HealthChecker;

inline constexpr uint32_t kBlockAlign = kIoAlign;

class LocalFileSystem {
 public:
  explicit LocalFileSystem(HealthChecker* health) : health_(health) {}

  LocalFileSystem(const LocalFileSystem&) = delete;
  LocalFileSystem& operator=(const LocalFileSystem&) = delete;

  Future<Status> WriteFile(std::string path, BufferViews block);
  Future<Status> ReadFile(std::string path, uint64_t offset, uint32_t length,
                          char* buffer);
  Future<Status> Link(std::string from, std::string to);
  Future<Status> Unlink(std::string path);
  Future<bool> FileExists(std::string path);

  static OpenOption BlockOpenOption() {
    return OpenOption{.io_inflight = UINT32_MAX, .register_fd = false};
  }

 private:
  static Status CheckBlock(BufferViews block);
  static Future<StatusOr<File>> OpenFile(std::string path, OpenFlags flags,
                                         bool direct);
  static Future<Status> WriteFile(File* file, BufferViews block, bool direct);

  Status Report(Status status);

  HealthChecker* health_;
};

using LocalFileSystemUPtr = std::unique_ptr<LocalFileSystem>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_STORE_LOCAL_FILESYSTEM_H_
