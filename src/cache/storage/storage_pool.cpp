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

#include <butil/time.h>
#include <glog/logging.h>

#include <memory>
#include <mutex>

#include "cache/common/proto.h"
#include "cache/common/type.h"
#include "cache/storage/storage_impl.h"
#include "common/config_mapper.h"

namespace dingofs {
namespace cache {

SingleStorage::SingleStorage(StorageSPtr storage) : storage_(storage) {
  CHECK_NOTNULL(storage_);
}

Status SingleStorage::GetStorage(uint32_t /*fs_id*/, StorageSPtr& storage) {
  storage = storage_;
  return Status::OK();
}

StoragePoolImpl::StoragePoolImpl(GetStorageInfoFunc get_storage_info_func)
    : get_storage_info_func_(get_storage_info_func) {
  CHECK_NOTNULL(get_storage_info_func_);
}

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

Status StoragePoolImpl::Create(uint32_t fs_id, StorageSPtr& storage) {
  // Get storage information
  PBStorageInfo storage_info;
  auto status = get_storage_info_func_(fs_id, &storage_info);
  if (!status.ok()) {
    LOG(ERROR) << "Get filesystem storage information failed: fs_id = " << fs_id
               << ", status = " << status.ToString();
    return status;
  }

  // New block accesser
  blockaccess::BlockAccessOptions block_access_opt;
  FillBlockAccessOption(storage_info, &block_access_opt);
  block_accesseres_[fs_id] =
      std::make_unique<blockaccess::BlockAccesserImpl>(block_access_opt);
  auto* block_accesser = block_accesseres_[fs_id].get();
  status = block_accesser->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Init block accesser for filesystem failed: fs_id = " << fs_id
               << ", status = " << status.ToString();
    return status;
  }

  // New storage and init it
  storage = std::make_shared<StorageImpl>(block_accesser);
  status = storage->Start();
  if (!status.ok()) {
    LOG(ERROR) << "New storage for filesystem failed: fs_id = " << fs_id
               << ", status = " << status.ToString();
    return status;
  }

  LOG(INFO) << "New storage for filesystem (fs_id=" << fs_id << ") success.";
  return status;
}

void StoragePoolImpl::Insert(uint32_t fs_id, StorageSPtr storage) {
  storages_.emplace(fs_id, storage);
}

}  // namespace cache
}  // namespace dingofs
