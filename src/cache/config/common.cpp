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
 * Created Date: 2025-06-02
 * Author: Jingli Chen (Wine93)
 */

#include "cache/config/common.h"

#include <absl/strings/str_split.h>
#include <gflags/gflags.h>

namespace dingofs {
namespace cache {

DEFINE_string(logdir, "/tmp", "");
DEFINE_int32(loglevel, 0, "");

DEFINE_string(mds_rpc_addrs, "127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702",
              "");
DEFINE_uint64(mds_rpc_retry_total_ms, 16000, "");
DEFINE_uint64(mds_rpc_max_timeout_ms, 2000, "");
DEFINE_uint64(mds_rpc_timeout_ms, 500, "");
DEFINE_uint64(mds_rpc_retry_interval_us, 50000, "");
DEFINE_uint64(mds_rpc_max_failed_times_before_change_addr, 2, "");
DEFINE_uint64(mds_rpc_normal_retry_times_before_trigger_wait, 3, "");
DEFINE_uint64(mds_rpc_wait_sleep_ms, 1000, "");

stub::common::MdsOption NewMdsOption() {
  stub::common::MdsOption option;
  auto& retry = option.rpcRetryOpt;

  option.mdsMaxRetryMS = FLAGS_mds_rpc_retry_total_ms;
  retry.addrs = absl::StrSplit(FLAGS_mds_rpc_addrs, ',', absl::SkipEmpty());
  retry.maxRPCTimeoutMS = FLAGS_mds_rpc_max_timeout_ms;
  retry.rpcTimeoutMs = FLAGS_mds_rpc_timeout_ms;
  retry.rpcRetryIntervalUS = FLAGS_mds_rpc_retry_interval_us;
  retry.maxFailedTimesBeforeChangeAddr =
      FLAGS_mds_rpc_max_failed_times_before_change_addr;
  retry.normalRetryTimesBeforeTriggerWait =
      FLAGS_mds_rpc_normal_retry_times_before_trigger_wait;
  retry.waitSleepMs = FLAGS_mds_rpc_wait_sleep_ms;

  return option;
}

}  // namespace cache
}  // namespace dingofs
