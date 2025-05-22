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

#include "cache/utils/block_accesser_pool.h"

#include <glog/logging.h>
#include <sys/select.h>

#include <memory>
#include <ostream>

#include "common/config_mapper.h"
#include "blockaccess/block_accesser.h"
#include "dingofs/mds.pb.h"
#include "stub/metric/metric.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace utils {

using dingofs::blockaccess::BlockAccesserSPtr;
using dingofs::pb::mds::FSStatusCode;
using dingofs::stub::rpcclient::MdsClient;
using dingofs::utils::ReadLockGuard;
using dingofs::utils::RWLock;
using dingofs::utils::WriteLockGuard;

BlockAccesserPoolImpl::BlockAccesserPoolImpl(
    std::shared_ptr<MdsClient> mds_client)
    : mds_client_(mds_client) {}

Status BlockAccesserPoolImpl::Get(uint32_t fs_id,
                                  BlockAccesserSPtr& block_accesser) {
  auto status = DoGet(fs_id, block_accesser);
  if (status.ok()) {
    return status;
  }

  if (NewBlockAccesser(fs_id, block_accesser)) {
    DoInsert(fs_id, block_accesser);
    return Status::OK();
  }
  return Status::NotFound("new data accesser failed");
}

Status BlockAccesserPoolImpl::DoGet(uint32_t fs_id,
                                    BlockAccesserSPtr& block_accesser) {
  ReadLockGuard lk(rwlock_);
  auto iter = block_accessers_.find(fs_id);
  if (iter != block_accessers_.end()) {
    block_accesser = iter->second;
    return Status::OK();
  }
  return Status::NotFound("not found data accesser");
}

void BlockAccesserPoolImpl::DoInsert(uint32_t fs_id,
                                     BlockAccesserSPtr data_accesser) {
  WriteLockGuard lk(rwlock_);
  block_accessers_.emplace(fs_id, data_accesser);
}

bool BlockAccesserPoolImpl::NewBlockAccesser(
    uint32_t fs_id, BlockAccesserSPtr& block_accesser) {
  pb::mds::FsInfo fs_info;
  FSStatusCode code = mds_client_->GetFsInfo(fs_id, &fs_info);
  if (code != FSStatusCode::OK) {
    LOG(ERROR) << "Get filesystem information failed: fs_id = " << fs_id
               << ", rc = " << FSStatusCode_Name(code);
    return false;
  }

  if (!fs_info.has_storage_info()) {
    LOG(ERROR) << "The filesystem missing storage_info: fs_id = " << fs_id;
    return false;
  }

  blockaccess::BlockAccessOptions block_access_opt;
  const auto& storage_info = fs_info.storage_info();
  FillBlockAccessOption(storage_info, &block_access_opt);

  auto accesser =
      std::make_unique<blockaccess::BlockAccesserImpl>(block_access_opt);
  Status s = accesser->Init();
  if (s.ok()) {
    block_accesser = std::move(accesser);
    LOG(INFO) << "New block accesser for filesystem(fs_id=" << fs_id
              << ") success.";
  } else {
    LOG(ERROR) << "New block accesser for filesystem(fs_id=" << fs_id
               << ") failed.";
  }
  return s.ok();
}

}  // namespace utils
}  // namespace cache
}  // namespace dingofs
