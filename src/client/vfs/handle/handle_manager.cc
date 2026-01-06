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

#include "client/vfs/data/file.h"
#include "client/vfs/vfs_fh.h"
#include "common/const.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {

Status HandleManager::Start() {
  bg_executor_ = std::make_unique<ExecutorImpl>(
      "handle_bg", FLAGS_vfs_handle_bg_executor_thread);
  bg_executor_->Start();

  bg_executor_->Schedule([&] { RunPeriodicFlush(); },
                         FLAGS_vfs_periodic_flush_interval_ms);

  bg_executor_->Schedule([&] { RunPeriodicShrinkMem(); },
                         FLAGS_vfs_periodic_trim_mem_ms);

  return Status::OK();
}

// TODO: concurrent flush
void HandleManager::Stop() {
  std::unordered_map<uint64_t, std::unique_ptr<Handle>> to_release;

  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopped_) {
      LOG(INFO) << "HandleManager already stopped";
      return;
    }

    stopped_ = true;

    while (bg_flush_tigger_ > 0 || bg_shrink_mem_tigger_ > 0) {
      LOG(INFO) << "HandleManager waiting bg tasks done, flush_tigger:"
                << bg_flush_tigger_
                << ", shrink_mem_tigger:" << bg_shrink_mem_tigger_;
      cv_.wait(lock);
    }

    handles_.swap(to_release);
  }

  for (auto& [fh, handle] : to_release) {
    if (handle->ino == kStatsIno) {
      continue;
    }

    CHECK_NOTNULL(handle->file);
    handle->file->Close();
    handle.reset();
  }

  if (!bg_executor_->Stop()) {
    LOG(ERROR) << "Failed to stop HandleManager bg executor";
  }
}

void HandleManager::AddHandle(std::unique_ptr<Handle> handle) {
  std::lock_guard<std::mutex> lock(mutex_);
  handles_[handle->fh] = std::move(handle);
}

Handle* HandleManager::FindHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = handles_.find(fh);
  return (it == handles_.end()) ? nullptr : it->second.get();
}

void HandleManager::ReleaseHandler(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  handles_.erase(fh);
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

  auto* handle = it->second.get();
  if (handle->file) {
    handle->file->Invalidate(offset, size);
  } else {
    LOG(WARNING) << "Invalidate failed, file is nullptr, fh:" << fh;
  }
}

void HandleManager::TriggerFlushAllDone(TriggerFlushTask* task, uint64_t fh,
                                        Status s) {
  if (!s.ok()) {
    LOG(ERROR) << "Failed to async flush file handle, fh:" << fh
               << ", error: " << s.ToString();
  }

  int64_t origin = task->inflight_flush.fetch_sub(1);

  if (origin > 1) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    bg_flush_tigger_--;
    if (bg_flush_tigger_ == 0) {
      cv_.notify_all();
    }
  }

  delete task;
}

void HandleManager::TriggerFlushAll() {
  std::unordered_map<uint64_t, Handle*> to_flush_handles;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
      LOG(WARNING) << "HandleManager already stopped";
      return;
    }

    for (auto& [fh, handle] : to_flush_handles) {
      if (handle->ino == kStatsIno) {
        continue;
      }

      CHECK_NOTNULL(handle->file);
      to_flush_handles.emplace(std::make_pair(fh, handle));
    }

    CHECK_GE(bg_flush_tigger_, 0);
    bg_flush_tigger_++;
  }

  auto* task = new TriggerFlushTask();
  task->inflight_flush.store(to_flush_handles.size());

  for (auto& [fh, handle] : to_flush_handles) {
    CHECK_NOTNULL(handle->file);
    uint64_t fh_copy = fh;
    handle->file->AsyncFlush([&, task, fh_copy](const Status& status) {
      TriggerFlushAllDone(task, fh_copy, status);
    });
  }
}

void HandleManager::RunPeriodicFlush() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
      LOG(INFO) << "HandleManager already stopped, stop periodic flush";
      return;
    }
  }

  TriggerFlushAll();

  bg_executor_->Schedule([&] { RunPeriodicFlush(); },
                         FLAGS_vfs_periodic_flush_interval_ms);
}

void HandleManager::RunPeriodicShrinkMem() {
  std::unordered_map<uint64_t, Handle*> to_trim_handles;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
      LOG(INFO) << "HandleManager already stopped, stop periodic shrink mem";
      return;
    }

    for (auto& [fh, handle] : handles_) {
      if (handle->ino == kStatsIno) {
        continue;
      }

      CHECK_NOTNULL(handle->file);
      to_trim_handles.emplace(std::make_pair(fh, handle.get()));
    }

    CHECK_GE(bg_shrink_mem_tigger_, 0);
    bg_shrink_mem_tigger_++;
  }

  for (auto& [fh, handle] : to_trim_handles) {
    CHECK_NOTNULL(handle->file);
    handle->file->ShrinkMem();
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    bg_shrink_mem_tigger_--;
    if (bg_shrink_mem_tigger_ == 0) {
      cv_.notify_all();
    }
  }

  bg_executor_->Schedule([&] { RunPeriodicShrinkMem(); },
                         FLAGS_vfs_periodic_trim_mem_ms);
}

bool HandleManager::Dump(Json::Value& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  Json::Value handlers = Json::arrayValue;

  for (const auto& handle : handles_) {
    auto* fileHandle = handle.second.get();

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

    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto file_handler = std::make_unique<Handle>();
      file_handler->ino = ino;
      file_handler->fh = fh;
      file_handler->flags = flags;
      file_handler->file = std::make_unique<File>(vfs_hub_, fh, ino);

      handles_.emplace(file_handler->fh, std::move(file_handler));
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