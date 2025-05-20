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
 * @Date: 2021-09-07
 * @Author: majie1
 */

#include "metaserver/compaction/s3compact_manager.h"

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>

#include "absl/memory/memory.h"
#include "dataaccess/block_accesser_factory.h"
#include "dataaccess/s3/s3_common.h"
#include "metaserver/compaction/fs_info_cache.h"
#include "metaserver/compaction/s3compact_worker.h"
#include "utils/string_util.h"

namespace dingofs {
namespace metaserver {

using utils::Configuration;
using utils::ReadLockGuard;
using utils::RWLock;
using utils::WriteLockGuard;

void S3CompactWorkQueueOption::Init(std::shared_ptr<Configuration> conf) {
  std::string mds_addrs_str;
  conf->GetValueFatalIfFail("mds.listen.addr", &mds_addrs_str);
  dingofs::utils::SplitString(mds_addrs_str, ",", &mdsAddrs);
  conf->GetValueFatalIfFail("global.ip", &metaserverIpStr);
  conf->GetValueFatalIfFail("global.port", &metaserverPort);

  dataaccess::InitAwsSdkConfig(conf.get(),
                               &block_access_opts.s3_options.aws_sdk_config);
  dataaccess::InitBlockAccesserThrottleOptions(
      conf.get(), &block_access_opts.throttle_options);

  conf->GetValueFatalIfFail("s3compactwq.enable", &enable);
  conf->GetValueFatalIfFail("s3compactwq.thread_num", &threadNum);
  conf->GetValueFatalIfFail("s3compactwq.fragment_threshold",
                            &fragmentThreshold);
  conf->GetValueFatalIfFail("s3compactwq.max_chunks_per_compact",
                            &maxChunksPerCompact);
  conf->GetValueFatalIfFail("s3compactwq.enqueue_sleep_ms", &enqueueSleepMS);

  if (!conf->GetUInt64Value("s3compactwq.fs_info_cache_size",
                            &fs_info_cache_size)) {
    fs_info_cache_size = 100;
    LOG(INFO)
        << "Not found `s3compactwq.fs_info_cache_size` in conf, default to 100";
  }

  conf->GetValueFatalIfFail("s3compactwq.s3_read_max_retry", &s3ReadMaxRetry);
  conf->GetValueFatalIfFail("s3compactwq.s3_read_retry_interval",
                            &s3ReadRetryInterval);
}

void S3CompactManager::Init(std::shared_ptr<Configuration> conf) {
  opts_.Init(conf);
  if (opts_.enable) {
    LOG(INFO) << "s3compact: enabled.";

    butil::ip_t metaserver_ip;
    if (butil::str2ip(opts_.metaserverIpStr.c_str(), &metaserver_ip) < 0) {
      LOG(FATAL) << "Invalid Metaserver IP provided: " << opts_.metaserverIpStr;
    }

    butil::EndPoint metaserver_addr(metaserver_ip, opts_.metaserverPort);
    LOG(INFO) << "Metaserver address: " << opts_.metaserverIpStr << ":"
              << opts_.metaserverPort;

    block_accesser_factory_ =
        std::make_shared<dataaccess::BlockAccesserFactory>();
    fs_info_cache_ = std::make_unique<FsInfoCache>(
        opts_.fs_info_cache_size, opts_.mdsAddrs, metaserver_addr);

    workerOptions_.block_accesser_factory = block_accesser_factory_;
    workerOptions_.block_access_opts = opts_.block_access_opts;
    workerOptions_.fs_info_cache = fs_info_cache_.get();
    workerOptions_.maxChunksPerCompact = opts_.maxChunksPerCompact;
    workerOptions_.fragmentThreshold = opts_.fragmentThreshold;
    workerOptions_.s3ReadMaxRetry = opts_.s3ReadMaxRetry;
    workerOptions_.s3ReadRetryInterval = opts_.s3ReadRetryInterval;
    workerOptions_.sleepMS = opts_.enqueueSleepMS;

    inited_ = true;
  } else {
    LOG(INFO) << "s3compact: not enabled";
  }
}

void S3CompactManager::Register(S3Compact s3compact) {
  {
    std::lock_guard<std::mutex> lock(workerContext_.mtx);
    workerContext_.s3compacts.push_back(std::move(s3compact));
  }

  workerContext_.cond.notify_one();
}

void S3CompactManager::Cancel(uint32_t partition_id) {
  std::lock_guard<std::mutex> lock(workerContext_.mtx);
  auto it = workerContext_.compacting.find(partition_id);
  if (it != workerContext_.compacting.end()) {
    it->second->Cancel(partition_id);
    LOG(INFO) << "Canceled s3 compaction, partition: " << partition_id;
    return;
  }

  for (auto comp = workerContext_.s3compacts.begin();
       comp != workerContext_.s3compacts.end(); ++comp) {
    if (comp->partitionInfo.partitionid() == partition_id) {
      workerContext_.s3compacts.erase(comp);
      LOG(INFO) << "Canceled s3 compaction, partitionid: " << partition_id;
      return;
    }
  }

  LOG(WARNING) << "Fail to find s3 compaction for partition: " << partition_id;
}

int S3CompactManager::Run() {
  if (!inited_) {
    LOG(WARNING) << "s3compact: not inited, wont't run";
    return 0;
  }

  if (!workerContext_.running.exchange(true)) {
    for (uint64_t i = 0; i < opts_.threadNum; ++i) {
      workers_.push_back(absl::make_unique<S3CompactWorker>(
          this, &workerContext_, &workerOptions_));
      workers_.back()->Run();
    }
  }

  return 0;
}

void S3CompactManager::Stop() {
  if (!inited_ || !workerContext_.running.exchange(false)) {
    return;
  }

  workerContext_.cond.notify_all();
  for (auto& worker : workers_) {
    worker->Stop();
  }
}

}  // namespace metaserver
}  // namespace dingofs
