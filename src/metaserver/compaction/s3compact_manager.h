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
 * @Date: 2021-09-09
 * @Author: majie1
 */

#ifndef DINGOFS_SRC_METASERVER_S3COMPACT_MANAGER_H_
#define DINGOFS_SRC_METASERVER_S3COMPACT_MANAGER_H_

#include <memory>
#include <string>
#include <vector>

#include "dataaccess/accesser_common.h"
#include "dataaccess/block_accesser_factory.h"
#include "metaserver/compaction/s3compact.h"
#include "metaserver/compaction/s3compact_worker.h"
#include "metaserver/compaction/fs_info_cache.h"
#include "utils/configuration.h"

namespace dingofs {
namespace metaserver {

struct S3CompactWorkQueueOption {
  dataaccess::BlockAccessOptions block_access_opts;

  bool enable;
  uint64_t threadNum;
  uint64_t fragmentThreshold;
  uint64_t maxChunksPerCompact;
  uint64_t enqueueSleepMS;
  std::vector<std::string> mdsAddrs;
  std::string metaserverIpStr;
  uint64_t metaserverPort;
  uint64_t fs_info_cache_size;
  uint64_t s3ReadMaxRetry;
  uint64_t s3ReadRetryInterval;

  void Init(std::shared_ptr<utils::Configuration> conf);
};

class S3CompactManager {
 public:
  static S3CompactManager& GetInstance() {
    static S3CompactManager instance;
    return instance;
  }

  void Init(std::shared_ptr<utils::Configuration> conf);

  void Register(S3Compact s3compact);

  void Cancel(uint32_t partition_id);

  int Run();

  void Stop();

 private:
  S3CompactManager() = default;
  ~S3CompactManager() = default;

  S3CompactWorkQueueOption opts_;
  std::unique_ptr<FsInfoCache> fs_info_cache_;

  S3CompactWorkerContext workerContext_;
  S3CompactWorkerOptions workerOptions_;

  std::shared_ptr<dataaccess::BlockAccesserFactory> block_accesser_factory_; 

  std::vector<std::unique_ptr<S3CompactWorker>> workers_;

  bool inited_{false};
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_S3COMPACT_MANAGER_H_
