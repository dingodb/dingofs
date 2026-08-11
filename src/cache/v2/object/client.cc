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

#include "cache/v2/object/client.h"

#include <glog/logging.h>

#include <mutex>
#include <utility>

#include "common/blockaccess/prefix_block_accesser.h"
#include "common/config_mapper.h"

namespace dingofs {
namespace cache {
namespace v2 {

StorageClient::StorageClient(MDSClient* mds_client) : mds_client_(mds_client) {}

StorageClient::~StorageClient() { Shutdown(); }

void StorageClient::Start() {
  LOG(INFO) << "StorageClient is starting...";
  running_.store(true, std::memory_order_release);
  LOG(INFO) << "Successfully start StorageClient";
}

void StorageClient::Shutdown() {
  LOG(INFO) << "StorageClient is shutting down...";
  running_.store(false, std::memory_order_release);
  LOG(INFO) << "Successfully shutdown StorageClient";
}

Status StorageClient::GetOrCreate(uint64_t fs_id,
                                  blockaccess::BlockAccesser** accesser) {
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = accessers_.find(fs_id);
    if (it != accessers_.end()) {
      *accesser = it->second.get();
      return Status::OK();
    }
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (!accessers_.contains(fs_id)) {
    Status status = Create(fs_id);
    if (!status.ok()) {
      return status;
    }
  }
  *accesser = accessers_[fs_id].get();
  return Status::OK();
}

Status StorageClient::Create(uint64_t fs_id) {
  pb::mds::FsInfo fs_info;
  Status status = mds_client_->GetFSInfo(fs_id, &fs_info);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to get fs info for fs_id=" << fs_id << ": "
               << status.ToString();
    return status;
  }

  blockaccess::BlockAccessOptions options;
  FillBlockAccessOption(fs_info, &options);
  blockaccess::BlockAccesserUPtr accesser =
      blockaccess::NewPrefixBlockAccesser(fs_info.fs_name(), options);
  status = accesser->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to init block accesser for fs=" << fs_info.fs_name()
               << ": " << status.ToString();
    return status;
  }

  LOG(INFO) << "Successfully create block accesser for fs="
            << fs_info.fs_name();
  accessers_[fs_id] = std::move(accesser);
  return Status::OK();
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
