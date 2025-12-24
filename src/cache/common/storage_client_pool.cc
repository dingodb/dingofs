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

/*
 * Project: DingoFS
 * Created Date: 2025-03-18
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/storage_client_pool.h"

#include <bthread/rwlock.h>
#include <butil/time.h>
#include <glog/logging.h>

#include <memory>

#include "cache/common/mds_client.h"
#include "cache/common/storage_client.h"
#include "common/config_mapper.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

StorageClientPoolImpl::StorageClientPoolImpl(MDSClientSPtr mds_client)
    : mds_client_(mds_client) {
  CHECK_NOTNULL(mds_client);
}

Status StorageClientPoolImpl::GetStorageClient(uint32_t fs_id,
                                               StorageClient** storage_client) {
  {
    bthread::RWLockRdGuard guard(rwlock_);
    auto iter = clients_.find(fs_id);
    if (iter != clients_.end()) {
      *storage_client = iter->second.get();
      return Status::OK();
    }
  }

  {
    bthread::RWLockWrGuard guard(rwlock_);
    auto iter = clients_.find(fs_id);
    if (iter != clients_.end()) {
      *storage_client = iter->second.get();
      return Status::OK();
    }
    return CreateStorageClient(fs_id, storage_client);
  }
}

Status StorageClientPoolImpl::CreateStorageClient(
    uint32_t fs_id, StorageClient** storage_client) {
  pb::mds::FsInfo fs_info;
  auto status = mds_client_->GetFSInfo(fs_id, &fs_info);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to get FsInfo for fsid=" << fs_id;
    return status;
  }

  // New block accesser
  blockaccess::BlockAccessOptions block_access_opt;
  FillBlockAccessOption(fs_info, &block_access_opt);
  accesseres_[fs_id] = blockaccess::NewBlockAccesser(block_access_opt);
  auto* block_accesser = accesseres_[fs_id].get();
  status = block_accesser->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to init BlockAccesser for fsid=" << fs_id;
    return status;
  }

  // New storage and init it
  auto client = std::make_unique<StorageClient>(block_accesser);
  status = client->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start StorageClient for fsid=" << fs_id;
    return status;
  }

  *storage_client = client.get();
  clients_.emplace(fs_id, std::move(client));
  LOG(INFO) << "Created StorageClient for fsid=" << fs_id;
  return status;
}

}  // namespace cache
}  // namespace dingofs
