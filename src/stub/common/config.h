/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: dingo
 * File Created: Saturday, 29th December 2018 3:50:45 pm
 * Author: tongguangxun
 */

#ifndef DINGOFS_SRC_STUB_COMMON_CONFIG_H_
#define DINGOFS_SRC_STUB_COMMON_CONFIG_H_

#include <cstdint>
#include <string>
#include <vector>

#include "utils/configuration.h"

namespace dingofs {
namespace stub {
namespace common {

struct MetaCacheOpt {
  int metacacheGetLeaderRetry = 3;
  int metacacheRPCRetryIntervalUS = 500;
  int metacacheGetLeaderRPCTimeOutMS = 1000;

  uint16_t getPartitionCountOnce = 3;
  uint16_t createPartitionOnce = 3;
};

struct ExcutorOpt {
  uint32_t maxRetry = 3;
  uint64_t retryIntervalUS = 200;
  uint64_t rpcTimeoutMS = 1000;
  uint64_t rpcStreamIdleTimeoutMS = 500;
  uint64_t maxRPCTimeoutMS = 64000;
  uint64_t maxRetrySleepIntervalUS = 64ull * 1000 * 1000;
  uint64_t minRetryTimesForceTimeoutBackoff = 5;
  uint64_t maxRetryTimesBeforeConsiderSuspend = 20;
  uint32_t batchInodeAttrLimit = 10000;
  bool enableRenameParallel = false;
};

struct MdsOption {
  uint64_t mdsMaxRetryMS = 8000;
  struct RpcRetryOption {
    // rpc max timeout
    uint64_t maxRPCTimeoutMS = 2000;
    // rpc normal timeout
    uint64_t rpcTimeoutMs = 500;
    // rpc retry interval
    uint32_t rpcRetryIntervalUS = 50000;
    // retry maxFailedTimesBeforeChangeAddr at a server
    uint32_t maxFailedTimesBeforeChangeAddr = 5;

    /**
     * When the failed times except RPC error
     * greater than mdsNormalRetryTimesBeforeTriggerWait,
     * it will trigger wait strategy, and sleep long time before retry
     */
    uint64_t maxRetryMsInIOPath = 86400000;  // 1 day

    // if the overall timeout passed by the user is 0, that means retry
    // until success. First try normalRetryTimesBeforeTriggerWait times,
    // then repeatedly sleep waitSleepMs and try once
    uint64_t normalRetryTimesBeforeTriggerWait = 3;  // 3 times
    uint64_t waitSleepMs = 10000;                    // 10 seconds

    std::vector<std::string> addrs;
  } rpcRetryOpt;
};

void InitMdsOption(utils::Configuration* conf, MdsOption* mds_opt);

}  // namespace common
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_SRC_STUB_COMMON_CONFIG_H_
