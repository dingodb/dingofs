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

#include "client/vfs/handle/handle_manager.h"

#include <memory>

#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {

HandleSPtr HandleManager::NewHandle() {
  auto handle = std::make_shared<Handle>();
  std::lock_guard<std::mutex> lock(mutex_);
  handle->fh = GenFh();
  handles_[handle->fh] = handle;

  return handle;
}

HandleSPtr HandleManager::FindHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = handles_.find(fh);
  return (it == handles_.end()) ? nullptr : it->second;
}

void HandleManager::ReleaseHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);

  handles_.erase(fh);
}

void HandleManager::FlushAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [fh, handle] : handles_) {
    Status s = handle->file->Flush();
    if (s.ok()) {
      LOG(ERROR) << "Failed to flush file handle: " << fh
                 << ", error: " << s.ToString();
    }
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs