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

#include "client/meta/vfs_fh.h"
#include "client/vfs/data/file.h"
#include "common/define.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {

void HandleManager::AddHandle(HandleSPtr handle) {
  std::lock_guard<std::mutex> lock(mutex_);
  handles_[handle->fh] = std::move(handle);
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

// TODO: concurrent flush
void HandleManager::FlushAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [fh, handle] : handles_) {
    if (handle->ino == STATSINODEID) {
      continue;
    }

    CHECK_NOTNULL(handle->file);
    Status s = handle->file->Flush();
    if (!s.ok()) {
      LOG(ERROR) << "Failed to flush file handle: " << fh
                 << ", error: " << s.ToString();
    }
  }
}

bool HandleManager::Dump(Json::Value& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  Json::Value handle_array = Json::arrayValue;

  for (const auto& handle : handles_) {
    auto fileHandle = handle.second;

    Json::Value item;
    item["ino"] = fileHandle->ino;
    item["fh"] = fileHandle->fh;
    item["flags"] = fileHandle->flags;

    handle_array.append(item);
  }
  value["handlers"] = handle_array;

  LOG(INFO) << "successfuly dump " << handles_.size() << " handlers";

  return true;
}

bool HandleManager::Load(const Json::Value& value) {
  const Json::Value& handlers = value["handlers"];
  if (!handlers.isArray()) {
    LOG(ERROR) << "handlers is not an array.";
    return false;
  }
  if (handlers.empty()) {
    LOG(INFO) << "no handlers to load";
    return true;
  }

  uint64_t max_fh = 0;
  for (const auto& handler : handlers) {
    // peek inode,fh,flags
    Ino ino = handler["ino"].asUInt64();
    uint64_t fh = handler["fh"].asUInt64();
    uint flags = handler["flags"].asUInt();

    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto file_handler = std::make_shared<Handle>();
      file_handler->ino = ino;
      file_handler->fh = fh;
      file_handler->flags = flags;
      file_handler->file = std::make_unique<File>(vfs_hub_, ino);

      handles_.emplace(file_handler->fh, file_handler);
    }
    max_fh = std::max(max_fh, fh);
  }
  vfs::FhGenerator::UpdateNextFh(max_fh + 1);  // update next_fh

  LOG(INFO) << "successfuly load " << handles_.size()
            << " handlers, next fh is:" << vfs::FhGenerator::GetNextFh();

  return true;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs