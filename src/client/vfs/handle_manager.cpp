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

#include "client/vfs/handle_manager.h"

#include <memory>

namespace dingofs {
namespace client {
namespace vfs {

HandlePtr HandleManager::NewHandle(Ino ino, DirIteratorUPtr dir_iterator) {
  auto handle = std::make_shared<Handle>();
  handle->ino = ino;
  handle->dir_iterator = std::move(dir_iterator);

  std::lock_guard<std::mutex> lock(mutex_);
  handle->fh = next_fh_++;
  handles_[handle->fh] = handle;

  return handle;
}

HandlePtr HandleManager::FindHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = handles_.find(fh);
  return (it == handles_.end()) ? nullptr : it->second;
}

void HandleManager::ReleaseHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);

  handles_.erase(fh);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs