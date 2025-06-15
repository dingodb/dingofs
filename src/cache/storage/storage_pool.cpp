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

#include "cache/storage/storage_pool.h"

#include <memory>
#include <mutex>

#include "cache/common/type.h"
#include "cache/storage/storage_impl.h"
#include "common/config_mapper.h"

namespace dingofs {
namespace cache {

SingleStorage::SingleStorage(StorageSPtr storage) : storage_(storage) {}

Status SingleStorage::GetStorage(uint32_t /*fs_id*/, StorageSPtr& storage) {
  if (storage_ != nullptr) {
    storage = storage_;
    return Status::OK();
  }
  return Status::Internal("no storage available");
}

StoragePoolImpl::StoragePoolImpl(
    std::shared_ptr<stub::rpcclient::MdsClient> mds_client)
    : mds_client_(mds_client) {}

Status StoragePoolImpl::GetStorage(uint32_t fs_id, StorageSPtr& storage) {
  std::unique_lock<BthreadMutex> lk(mutex_);
  if (Get(fs_id, storage)) {
    return Status::OK();
  }

  auto status = Create(fs_id, storage);
  if (status.ok()) {
    Insert(fs_id, storage);
    return Status::OK();
  }
  return Status::NotFound("new storage failed");
}

bool StoragePoolImpl::Get(uint32_t fs_id, StorageSPtr& storage) {
  auto iter = storages_.find(fs_id);
  if (iter != storages_.end()) {
    storage = iter->second;
    return true;
  }
  return false;
}

void StoragePoolImpl::Insert(uint32_t fs_id, StorageSPtr storage) {
  storages_.emplace(fs_id, storage);
}

Status StoragePoolImpl::Create(uint32_t fs_id, StorageSPtr& storage) {
  // get filesyste information
  PBFsInfo fs_info;
  PBFSStatusCode code = mds_client_->GetFsInfo(fs_id, &fs_info);
  if (code != PBFSStatusCode::OK) {
    LOG(ERROR) << "Get filesystem information failed: fs_id = " << fs_id
               << ", rc = " << FSStatusCode_Name(code);
    return Status::Internal("get filesystem information failed");
  } else if (!fs_info.has_storage_info()) {
    LOG(ERROR) << "The filesystem missing storage_info: fs_id = " << fs_id;
    return Status::Internal("filesystem missing storage_info");
  }

  // new block accesser
  blockaccess::BlockAccessOptions block_access_opt;
  FillBlockAccessOption(fs_info.storage_info(), &block_access_opt);
  block_accesseres_[fs_id] =
      std::make_unique<blockaccess::BlockAccesserImpl>(block_access_opt);
  auto* block_accesser = block_accesseres_[fs_id].get();
  auto status = block_accesser->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Init block accesser for filesystem (fs_id=" << fs_id
               << ") failed: " << status.ToString();
    return status;
  }

  // new storage and init it
  storage = std::make_shared<StorageImpl>(block_accesser);
  status = storage->Init();
  if (status.ok()) {
    LOG(INFO) << "New storage for filesystem (fs_id=" << fs_id << ") success.";
  } else {
    LOG(ERROR) << "New storage for filesystem (fs_id=" << fs_id
               << ") failed: " << status.ToString();
  }
  return status;
}

}  // namespace cache
}  // namespace dingofs
