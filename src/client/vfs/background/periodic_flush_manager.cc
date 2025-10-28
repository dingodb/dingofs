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

#include "client/vfs/background/periodic_flush_manager.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <mutex>

#include "client/vfs/handle/handle_manager.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/options/client/option.h"

namespace dingofs {
namespace client {
namespace vfs {

static std::atomic<uint64_t> seq_id_gen{1};

void PeriodicFlushManager::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) {
    stopped_ = false;
    LOG(INFO) << "Starting PeriodicFlushManager";
  } else {
    LOG(WARNING) << "PeriodicFlushManager is already running";
    return;
  }
}

void PeriodicFlushManager::Stop() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!stopped_) {
    stopped_ = true;
    LOG(INFO) << "Stopping PeriodicFlushManager";
  } else {
    LOG(WARNING) << "PeriodicFlushManager is already stopped";
  }
}

void PeriodicFlushManager::FlushHandleDone(Status s, uint64_t seq_id,
                                           HandleSPtr handle) {
  if (!s.ok()) {
    LOG(WARNING) << "PeriodicFlushManager::FlushHandleDone failed: "
                 << s.ToString() << " seq_id: " << seq_id
                 << " handle: " << handle->ToString();
  } else {
    VLOG(4) << "PeriodicFlushManager::FlushHandleDone seq_id: " << seq_id
            << ", handle" << handle->ToString() << " status: " << s.ToString();
  }
}

void PeriodicFlushManager::FlushHandle(uint64_t fh) {
  VLOG(4) << "PeriodicFlushManager::FlushHandle fh: " << fh;

  HandleSPtr handle{nullptr};

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
      LOG(WARNING) << "PeriodicFlushManager is stopped, cannot flush handle: "
                   << fh;
      return;
    }

    auto iter = handles_.find(fh);
    CHECK(iter != handles_.end())
        << "Handle not found in PeriodicFlushManager: fh=" << fh;

    handle = iter->second.lock();
    if (handle == nullptr) {
      VLOG(1) << "Handle is released for fh: " << fh;
      handles_.erase(iter);
      return;
    }
  }

  CHECK_NOTNULL(handle);
  CHECK_NOTNULL(handle->file);
  uint64_t seq_id = seq_id_gen.fetch_add(1, std::memory_order_relaxed);
  // make handle is copped to HandleFlushDone
  // to avoid handle is released before flush done
  handle->file->AsyncFlush([this, seq_id, handle](auto&& ph1) {
    FlushHandleDone(std::forward<decltype(ph1)>(ph1), seq_id, handle);
  });

  VLOG(4) << "Submitted flush for handle: " << handle->ToString()
          << " with seq_id: " << seq_id;

  vfs_hub_->GetFlushExecutor()->Schedule(
      [this, fh] { FlushHandle(fh); },
      FLAGS_client_vfs_periodic_flush_interval_ms);
}

void PeriodicFlushManager::SubmitToFlush(HandleSPtr handle) {
  VLOG(4) << "PeriodicFlushManager::SubmitToFlush handle: "
          << handle->ToString();

  uint64_t fh = handle->fh;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK(!stopped_) << "PeriodicFlushManager is stopped";

    auto iter = handles_.find(handle->fh);
    if (iter != handles_.end()) {
      LOG(FATAL) << "Handle already exists in PeriodicFlushManager: "
                 << handle->ToString();
    } else {
      handles_.emplace(handle->fh, handle);
    }
  }

  vfs_hub_->GetFlushExecutor()->Schedule(
      [this, fh] { FlushHandle(fh); },
      FLAGS_client_vfs_periodic_flush_interval_ms);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs