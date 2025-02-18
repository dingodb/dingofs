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

#include <glog/logging.h>

namespace dingofs {
namespace client {
namespace vfs {

Handle* HandleManager::NewHandle(Ino ino) {
  auto handle = std::make_unique<Handle>();
  handle->ino = ino;

  std::lock_guard<std::mutex> lock(mutex_);
  handle->fh = next_fh_;
  auto* ptr = handle.get();
  handles_[handle->fh] = std::move(handle);

  next_fh_++;
  return ptr;
}

Handle* HandleManager::FindHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = handles_.find(fh);
  if (it == handles_.end()) {
    return nullptr;
  }
  return it->second.get();
}

void HandleManager::ReleaseHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = handles_.find(fh);
  if (it == handles_.end()) {
    LOG(WARNING) << "Fail find handle in ReleaseHandler for fh: " << fh;
    return;
  }
  handles_.erase(it);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs