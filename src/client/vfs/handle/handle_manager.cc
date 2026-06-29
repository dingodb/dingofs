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

#include <fcntl.h>
#include <glog/logging.h>

#include <cstdint>
#include <sstream>
#include <utility>
#include <vector>

#include "client/vfs/data/reader/file_reader.h"
#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/data/writer_table.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_fh.h"
#include "common/const.h"
#include "fmt/format.h"

namespace dingofs {
namespace client {
namespace vfs {

std::string Handle::ToString() const {
  std::ostringstream oss;
  oss << "Handle{ino: " << ino << ", fh: " << fh << ", flags: " << std::oct
      << flags << ", has_reader: " << (resources.reader ? "true" : "false")
      << ", has_writer: " << (resources.writer ? "true" : "false") << "}";
  return oss.str();
}

HandleManager::~HandleManager() {
  Stop();
  std::vector<Handle*> handles;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    handles.reserve(handles_.size());
    for (auto& [fh, handle] : handles_) {
      handles.push_back(handle);
    }
    handles_.clear();
  }

  for (auto* handle : handles) {
    ReleaseRefHandle(handle);
  }
}

HandleGuard::~HandleGuard() { Reset(); }

HandleGuard::HandleGuard(HandleGuard&& other) noexcept
    : manager_(std::exchange(other.manager_, nullptr)),
      handle_(std::exchange(other.handle_, nullptr)) {}

HandleGuard& HandleGuard::operator=(HandleGuard&& other) noexcept {
  if (this != &other) {
    Reset();
    manager_ = std::exchange(other.manager_, nullptr);
    handle_ = std::exchange(other.handle_, nullptr);
  }
  return *this;
}

void HandleGuard::Reset() {
  if (manager_ != nullptr && handle_ != nullptr) {
    manager_->ReleaseGuard(handle_);
    manager_ = nullptr;
    handle_ = nullptr;
  }
}

void HandleManager::AcquireRefHandle(Handle* h) {
  int64_t orgin = h->refs.fetch_add(1);
  VLOG(12) << fmt::format("handle-{} AcquireRef origin refs: {}", h->fh, orgin);
  CHECK_GE(orgin, 0);
}

void HandleManager::ReleaseRefHandle(Handle* h) {
  int64_t orgin = h->refs.fetch_sub(1);
  VLOG(12) << fmt::format("handle-{} ReleaseRef origin refs: {}", h->fh, orgin);
  CHECK_GT(orgin, 0);
  if (orgin == 1) {
    DestroyHandle(h);
  }
  cv_.notify_all();
}

void HandleManager::DestroyHandle(Handle* h) {
  HandleResources resources;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    resources = DetachHandleResourcesLocked(h);
  }
  ReleaseHandleResources(resources);
  delete h;
}

void HandleManager::ReleaseGuard(Handle* h) { ReleaseRefHandle(h); }

HandleResources HandleManager::DetachHandleResourcesLocked(Handle* h) {
  return std::exchange(h->resources, {});
}

void HandleManager::ReleaseHandleResources(HandleResources resources) {
  if (resources.reader != nullptr) {
    resources.reader->Close();
    resources.reader->ReleaseRef();
  }

  if (resources.writer != nullptr) {
    vfs_hub_->GetWriterTable()->ReleaseWriter(resources.writer);
  }
}

Status HandleManager::Start() { return Status::OK(); }

void HandleManager::Stop() {
  std::vector<HandleResources> resources_to_release;
  std::vector<Handle*> stop_refs;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopped_) {
      LOG(INFO) << "HandleManager already stopped";
      return;
    }

    stopped_ = true;

    cv_.wait(lock, [&]() {
      for (auto& [fh, handle] : handles_) {
        if (handle->refs.load(std::memory_order_acquire) != 1) {
          return false;
        }
      }
      return true;
    });

    for (auto& [fh, handle] : handles_) {
      if (handle->ino == kStatsIno) {
        continue;
      }
      AcquireRefHandle(handle);
      stop_refs.push_back(handle);

      auto resources = DetachHandleResourcesLocked(handle);
      if (resources.reader != nullptr || resources.writer != nullptr) {
        resources_to_release.push_back(resources);
      }
    }
  }

  // Release resources outside mutex_: FileWriter::Close() can synchronously
  // wait for callbacks that may call back into
  // HandleManager::InvalidateByIno().
  for (auto& resources : resources_to_release) {
    ReleaseHandleResources(resources);
  }

  for (auto* handle : stop_refs) {
    ReleaseRefHandle(handle);
  }
}

Handle* HandleManager::NewHandle(uint64_t fh, Ino ino, int flags) {
  auto* handle = new Handle();
  handle->fh = fh;
  handle->ino = ino;
  handle->flags = flags;

  // Reader is always per-fh.
  handle->resources.reader = new FileReader(vfs_hub_, fh, ino);
  handle->resources.reader->AcquireRef();
  Status s = handle->resources.reader->Open();
  if (!s.ok()) {
    LOG(ERROR) << fmt::format(
        "NewHandle: reader Open failed, fh={}, ino={}, "
        "status={}",
        fh, ino, s.ToString());
    // Reader holds 1 ref from AcquireRef above; release it (reader will
    // delete-this when refs hit 0). Then drop the bare handle.
    handle->resources.reader->ReleaseRef();
    delete handle;
    return nullptr;
  }

  // Writer only for writable opens. Borrowed from WriterTable.
  if ((flags & O_ACCMODE) != O_RDONLY) {
    handle->resources.writer = vfs_hub_->GetWriterTable()->AcquireWriter(ino);
    if (handle->resources.writer == nullptr) {
      LOG(ERROR) << fmt::format(
          "NewHandle: AcquireWriter failed, fh={}, ino={}", fh, ino);
      handle->resources.reader->Close();
      handle->resources.reader->ReleaseRef();
      delete handle;
      return nullptr;
    }
  }

  if (!AddHandle(handle)) {
    DestroyHandle(handle);
    return nullptr;
  }
  return handle;
}

bool HandleManager::AddHandle(Handle* handle) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) {
    LOG(WARNING) << "AddHandle rejected because HandleManager is stopped, fh: "
                 << handle->fh;
    return false;
  }

  AcquireRefHandle(handle);
  handles_[handle->fh] = handle;

  total_count_ << 1;
  return true;
}

void HandleManager::ReleaseHandler(uint64_t fh) {
  Handle* h = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = handles_.find(fh);
    if (iter == handles_.end()) {
      VLOG(1) << "ReleaseHandler ignored, fh not found: " << fh;
      return;
    }
    h = iter->second;
    handles_.erase(iter);
  }
  // Drop the AddHandle-time ref. DestroyHandle (called when refs→0) does
  // reader Close + writer return + delete; running it outside the table
  // mutex avoids deadlocks against WriterTable / FileWriter cleanup.
  ReleaseRefHandle(h);
}

HandleGuard HandleManager::FindHandlerGuard(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) {
    return {};
  }

  auto it = handles_.find(fh);
  if (it == handles_.end()) {
    return {};
  }

  AcquireRefHandle(it->second);
  return HandleGuard(this, it->second);
}

HandleGuard HandleManager::FindHandlerForRelease(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = handles_.find(fh);
  if (it == handles_.end()) {
    return {};
  }

  AcquireRefHandle(it->second);
  return HandleGuard(this, it->second);
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
  if (handle->resources.reader != nullptr) {
    handle->resources.reader->Invalidate(offset, size);
  } else {
    LOG(WARNING) << "Invalidate failed, reader is nullptr, fh:" << fh;
  }
}

Status HandleManager::FlushByIno(Ino ino) {
  // O(1) hash lookup via WriterTable.
  auto* writer = vfs_hub_->GetWriterTable()->PeekWriter(ino);
  if (writer == nullptr) {
    return Status::OK();  // no writer for this ino → nothing to flush
  }
  Status s = writer->Flush();
  vfs_hub_->GetWriterTable()->ReleaseWriter(writer);
  if (!s.ok()) {
    LOG(WARNING) << fmt::format("FlushByIno failed, ino: {}, status: {}", ino,
                                s.ToString());
  }
  return s;
}

void HandleManager::InvalidateByIno(Ino ino, int64_t offset, int64_t size) {
  std::vector<Handle*> handles_to_invalidate;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
      return;
    }
    for (auto& [fh, handle] : handles_) {
      if (handle->ino == ino && handle->resources.reader != nullptr) {
        AcquireRefHandle(handle);
        handles_to_invalidate.push_back(handle);
      }
    }
  }

  for (auto* handle : handles_to_invalidate) {
    handle->resources.reader->Invalidate(offset, size);
    ReleaseRefHandle(handle);
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
    Ino ino = handler["ino"].asUInt64();
    uint64_t fh = handler["fh"].asUInt64();
    uint flags = handler["flags"].asUInt();

    auto* h = NewHandle(fh, ino, flags);
    if (h == nullptr) {
      LOG(ERROR) << fmt::format("Load: NewHandle failed for fh={}, ino={}", fh,
                                ino);
      continue;
    }
    max_fh = std::max(max_fh, fh);
  }

  vfs::FhGenerator::UpdateNextFh(max_fh + 1);

  LOG(INFO) << "successfuly load " << handles_.size()
            << " handlers, next fh is:" << vfs::FhGenerator::GetNextFh();

  return true;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
