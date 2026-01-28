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

#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <sstream>

#include "client/vfs/data/file.h"
#include "client/vfs/vfs_fh.h"
#include "common/const.h"

namespace dingofs {
namespace client {
namespace vfs {

std::string Handle::ToString() const {
  std::ostringstream oss;
  oss << "Handle{ino: " << ino << ", fh; " << fh << ", flags: " << std::oct
      << flags << "}";
  return oss.str();
}

void Handle::AcquireRef() {
  int64_t orgin = refs.fetch_add(1);
  VLOG(12) << fmt::format("handle-{} AcquireRef origin refs: {}", fh, orgin);
  CHECK_GE(orgin, 0);
}

void Handle::ReleaseRef() {
  int64_t copy_fh = fh;
  int64_t orgin = refs.fetch_sub(1);
  VLOG(12) << fmt::format("handle-{} ReleaseRef origin refs: {}", copy_fh,
                          orgin);
  CHECK_GT(orgin, 0);
  if (orgin == 1) {
    delete this;
  }
}

HandleManager::~HandleManager() {
  Stop();
  std::unique_lock<std::mutex> lock(mutex_);
  for (auto& [fh, handle] : handles_) {
    handle->ReleaseRef();
  }
  handles_.clear();
}

Status HandleManager::Start() { return Status::OK(); }

void HandleManager::Stop() {
  std::vector<Handle*> handles_to_close;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopped_) {
      LOG(INFO) << "HandleManager already stopped";
      return;
    }

    stopped_ = true;

    for (auto& [fh, handle] : handles_) {
      if (handle->ino == kStatsIno) {
        continue;
      }

      CHECK_NOTNULL(handle->file);
      handle->AcquireRef();
      handles_to_close.push_back(handle);
    }
  }

  for (auto* handle : handles_to_close) {
    handle->file->Close();
    handle->ReleaseRef();
  }
}

Handle* HandleManager::NewHandle(uint64_t fh, Ino ino, int flags,
                                 IFileUPtr file) {
  auto* handle = new Handle();
  handle->fh = fh;
  handle->ino = ino;
  handle->flags = flags;
  handle->file = std::move(file);

  AddHandle(handle);

  return handle;
}

void HandleManager::AddHandle(Handle* handle) {
  std::lock_guard<std::mutex> lock(mutex_);
  handle->AcquireRef();
  handles_[handle->fh] = handle;

  total_count_ << 1;
}

void HandleManager::ReleaseHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto iter = handles_.find(fh);
  if (iter == handles_.end()) {
    LOG(ERROR) << "ReleaseHandler failed, fh not found:" << fh;
    return;
  }

  iter->second->ReleaseRef();
  handles_.erase(iter);
}

Handle* HandleManager::FindHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = handles_.find(fh);
  return (it == handles_.end()) ? nullptr : it->second;
}

void HandleManager::Invalidate(uint64_t fh, int64_t offset, int64_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) {
    LOG(WARNING) << "HandleManager already stopped";
    return;
  }

  auto it = handles_.find(fh);
  if (it == handles_.end()) {
    LOG(WARNING) << "Invalidate failed, fh not found:" << fh;
    return;
  }

  auto* handle = it->second;
  if (handle->file) {
    handle->file->Invalidate(offset, size);
  } else {
    LOG(WARNING) << "Invalidate failed, file is nullptr, fh:" << fh;
  }
}

void HandleManager::Summary(Json::Value& value) {
  std::lock_guard<std::mutex> lock(mutex_);

  value["name"] = "handler";
  value["count"] = handles_.size();
  value["total_count"] = total_count_.get_value();
}

bool HandleManager::Dump(Json::Value& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  Json::Value handlers = Json::arrayValue;

  for (const auto& handle : handles_) {
    auto* fileHandle = handle.second;

    Json::Value item;
    item["ino"] = fileHandle->ino;
    item["fh"] = fileHandle->fh;
    item["flags"] = fileHandle->flags;

    handlers.append(item);
  }
  value["handlers"] = handlers;

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

    auto file = std::make_unique<File>(vfs_hub_, fh, ino);
    file->Open();

    auto* hanel = NewHandle(fh, ino, flags, std::move(file));
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