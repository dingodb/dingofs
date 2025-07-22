/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * @Project: dingo
 * @Date: 2021-12-15 10:54:28
 * @Author: chenwei
 */
#include "metaserver/partition_clean_manager.h"

#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace metaserver {

using utils::Thread;

void PartitionCleanManager::Add(
    uint32_t partition_id, const std::shared_ptr<PartitionCleaner>& cleaner,
    copyset::CopysetNode* copyset_node) {
  LOG(INFO) << "Add partition to partition clean mananager, partititonId = "
            << partition_id;
  cleaner->Init(option_, copyset_node);

  {
    dingofs::utils::WriteLockGuard lock_guard(rwLock_);
    partitonCleanerList_.push_back(cleaner);
  }

  partitionCleanerCount << 1;
}

bool PartitionCleanManager::IsAdded(uint32_t partition_id) {
  dingofs::utils::WriteLockGuard lock_guard(rwLock_);

  for (auto& cleaner : partitonCleanerList_) {
    if (cleaner->GetPartitionId() == partition_id) {
      return true;
    }
  }

  return false;
}

void PartitionCleanManager::Remove(uint32_t partition_id) {
  dingofs::utils::WriteLockGuard lock_guard(rwLock_);
  // 1. first check inProcessingCleaner
  if (inProcessingCleaner_ != nullptr &&
      inProcessingCleaner_->GetPartitionId() == partition_id) {
    inProcessingCleaner_->Stop();
    LOG(INFO) << "remove partition from PartitionCleanManager, partition is"
              << " in processing, stop this cleaner, partititonId = "
              << partition_id;
    return;
  }

  // 2. then check partitonCleanerList
  for (auto it = partitonCleanerList_.begin(); it != partitonCleanerList_.end();
       it++) {
    if ((*it)->GetPartitionId() == partition_id) {
      partitonCleanerList_.erase(it);
      partitionCleanerCount << -1;
      LOG(INFO) << "remove partition from PartitionCleanManager, "
                << "partitionId = " << partition_id;
      return;
    }
  }
}

void PartitionCleanManager::Run() {
  if (isStop_.exchange(false)) {
    thread_ = Thread(&PartitionCleanManager::ScanLoop, this);
    LOG(INFO) << "Start PartitionCleanManager thread ok.";
    return;
  }

  LOG(INFO) << "PartitionCleanManager already runned.";
}

void PartitionCleanManager::Fini() {
  if (!isStop_.exchange(true)) {
    LOG(INFO) << "stop PartitionCleanManager manager ...";
    sleeper_.interrupt();
    thread_.join();
    partitonCleanerList_.clear();
    inProcessingCleaner_ = nullptr;
  }
  LOG(INFO) << "stop PartitionCleanManager manager ok.";
}

void PartitionCleanManager::ScanLoop() {
  LOG(INFO) << "PartitionCleanManager start scan thread, scanPeriodSec = "
            << option_.scanPeriodSec;
  while (sleeper_.wait_for(std::chrono::seconds(option_.scanPeriodSec))) {
    {
      dingofs::utils::WriteLockGuard lock_guard(rwLock_);
      if (partitonCleanerList_.empty()) {
        continue;
      }

      inProcessingCleaner_ = partitonCleanerList_.front();
      partitonCleanerList_.pop_front();
    }

    uint32_t partition_id = inProcessingCleaner_->GetPartitionId();
    LOG(INFO) << "scan partition, partitionId = " << partition_id;
    bool delete_record = inProcessingCleaner_->ScanPartition();
    if (delete_record) {
      LOG(INFO) << "scan partition, partition is empty"
                << ", delete record from clean manager, partitionId = "
                << partition_id;
      partitionCleanerCount << -1;
    } else {
      dingofs::utils::WriteLockGuard lock_guard(rwLock_);
      if (!inProcessingCleaner_->IsStop()) {
        partitonCleanerList_.push_back(inProcessingCleaner_);
      } else {
        LOG(INFO) << "scan partition, cleaner is mark stoped, remove it"
                  << ", partitionId = " << partition_id;
      }
    }
    inProcessingCleaner_ = nullptr;
  }
  LOG(INFO) << "PartitionCleanManager stop scan thread.";
}

}  // namespace metaserver
}  // namespace dingofs
