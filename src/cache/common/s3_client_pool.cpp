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

#include "cache/common/s3_client_pool.h"

#include <ostream>

#include "cache/common/errno.h"
#include "pb/mds/mds.h"
#include "stub/metric/metric.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace common {

using pb::mds::FsInfo;

using utils::ReadLockGuard;
using utils::WriteLockGuard;

Errno S3ClientPoolImpl::GetS3Client(uint32_t fs_id,
                                    std::shared_ptr<S3Client>& s3_client) {
  {
    ReadLockGuard lk(rwlock_);
    auto iter = clients_.find(fs_id);
    if (iter != clients_.end()) {
      s3_client = iter->second;
      return Errno::OK;
    }
  }

  auto rc = NewS3Client(fs_id, s3_client);
  if (rc == Errno::OK) {
    WriteLockGuard lk(rwlock_);
    clients_[fs_id] = s3_client;
  }
  return rc;
}

Errno S3ClientPoolImpl::NewS3Client(uint32_t fs_id,
                                    std::shared_ptr<S3Client>& s3_client) {
  FsInfo fs_info;
  FSStatusCode code = mds_client->GetFsInfo(fs_id, fs_info);
  if (code == FSStatusCode::OK) {
  }

  aws::S3AdapterOption option;
}

Errno S3ClientPoolImpl::GetFsInfo(uint32_t fs_id) {
  std::string fn = (fs_name == nullptr) ? "" : fs_name;
  FSStatusCode ret = mds_client.GetFsInfo(fn, fs_info);
  if (ret != FSStatusCode::OK) {
    if (FSStatusCode::NOT_FOUND == ret) {
      LOG(ERROR) << "The fsName not exist, fsName = " << fs_name;
      return -1;
    } else {
      LOG(ERROR) << "GetFsInfo failed, FSStatusCode = " << ret
                 << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                 << ", fsName = " << fs_name;
      return -1;
    }
  }
  return 0;
}

}  // namespace common
}  // namespace cache
}  // namespace dingofs
