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

#include "cache/utils/data_accesser_pool.h"

#include <glog/logging.h>
#include <sys/select.h>

#include <memory>
#include <ostream>

#include "dataaccess/s3/aws/aws_s3_common.h"
#include "dataaccess/s3/s3_accesser.h"
#include "dingofs/mds.pb.h"
#include "stub/metric/metric.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace utils {

using dingofs::dataaccess::aws::S3AdapterOption;
using dingofs::pb::mds::FSStatusCode;
using dingofs::utils::ReadLockGuard;
using dingofs::utils::WriteLockGuard;

DataAccesserPoolImpl::DataAccesserPoolImpl(
    std::shared_ptr<MdsClient> mds_client)
    : mds_client_(mds_client) {}

Status DataAccesserPoolImpl::Get(uint32_t fs_id,
                                 DataAccesserPtr& data_accesser) {
  auto status = DoGet(fs_id, data_accesser);
  if (status.ok()) {
    return status;
  }

  if (NewDataAccesser(fs_id, data_accesser)) {
    DoInsert(fs_id, data_accesser);
    return Status::OK();
  }
  return Status::NotFound("new data accesser failed");
}

Status DataAccesserPoolImpl::DoGet(uint32_t fs_id,
                                   DataAccesserPtr& data_accesser) {
  ReadLockGuard lk(rwlock_);
  auto iter = data_accessers_.find(fs_id);
  if (iter != data_accessers_.end()) {
    data_accesser = iter->second;
    return Status::OK();
  }
  return Status::NotFound("not found data accesser");
}

void DataAccesserPoolImpl::DoInsert(uint32_t fs_id,
                                    DataAccesserPtr data_accesser) {
  WriteLockGuard lk(rwlock_);
  data_accessers_.emplace(fs_id, data_accesser);
}

bool DataAccesserPoolImpl::NewDataAccesser(uint32_t fs_id,
                                           DataAccesserPtr& data_accesser) {
  pb::mds::FsInfo fs_info;
  FSStatusCode code = mds_client_->GetFsInfo(fs_id, &fs_info);
  if (code != FSStatusCode::OK) {
    LOG(ERROR) << "Get filesystem information failed: fs_id = " << fs_id
               << ", rc = " << FSStatusCode_Name(code);
    return false;
  } else if (!fs_info.detail().has_s3info()) {
    LOG(ERROR) << "The filesystem missing s3 information: fs_id = " << fs_id;
    return false;
  }

  // TODO(Wine93): set more s3 option
  S3AdapterOption option;
  auto s3_info = fs_info.detail().s3info();
  option.ak = s3_info.ak();
  option.sk = s3_info.sk();
  option.s3Address = s3_info.endpoint();
  option.bucketName = s3_info.bucketname();

  data_accesser = dataaccess::S3Accesser::New(option);
  bool succ = data_accesser->Init();
  if (succ) {
    LOG(INFO) << "New data accesser for filesystem(fs_id=" << fs_id
              << ") success.";
  } else {
    LOG(ERROR) << "New data accesser for filesystem(fs_id=" << fs_id
               << ") failed.";
  }
  return succ;
}

}  // namespace utils
}  // namespace cache
}  // namespace dingofs
