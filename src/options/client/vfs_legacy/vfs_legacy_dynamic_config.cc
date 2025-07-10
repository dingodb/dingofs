/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-07
 * Author: Jingli Chen (Wine93)
 */

#include "options/client/vfs_legacy/vfs_legacy_dynamic_config.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace dingofs {
namespace client {

namespace {
bool PassDouble(const char*, double) { return true; }
bool PassUint64(const char*, uint64_t) { return true; }
bool PassInt32(const char*, int32_t) { return true; }
bool PassUint32(const char*, uint32_t) { return true; }
bool PassBool(const char*, bool) { return true; }
};  // namespace

// thread num or bthread num
DEFINE_uint32(stat_timer_thread_num, 8, "stat timer thread num");
DEFINE_validator(stat_timer_thread_num, &PassUint32);

DEFINE_uint32(fs_usage_flush_interval_second, 20,
              "fs usage flush interval in second");
DEFINE_validator(fs_usage_flush_interval_second, &PassUint32);

DEFINE_uint32(flush_quota_interval_second, 30,
              "flush quota interval in second");
DEFINE_validator(flush_quota_interval_second, &PassUint32);

DEFINE_uint32(load_quota_interval_second, 30, "flush quota interval in second");
DEFINE_validator(load_quota_interval_second, &PassUint32);

DEFINE_uint32(push_metric_interval_millsecond, 500,
              "push client metrics interval in millsecond");
DEFINE_validator(push_metric_interval_millsecond, &PassUint32);

DEFINE_uint32(max_parent_depth_for_quota_check, 2048,
              "max parent depth for quota check");
DEFINE_validator(max_parent_depth_for_quota_check, &PassUint32);

// fuse
// kernal will retry when read fail
DEFINE_uint32(fuse_read_max_retry_s3_not_exist, 60,
              "fuse read max retry when s3 object not exist");
DEFINE_validator(fuse_read_max_retry_s3_not_exist, &PassUint32);

DEFINE_bool(s3_prefetch, true, "enable s3 prefetch when read miss");
DEFINE_validator(s3_prefetch, &PassBool);

// in time warmup
DEFINE_bool(in_time_warmup, false, "in time warmup inode when enable");
DEFINE_validator(in_time_warmup, &PassBool);

// from config.h and config.cpp
DEFINE_bool(enableCto, true, "acheieve cto consistency");
DEFINE_bool(supportKVcache, false, "use kvcache to speed up sharing");

/**
 * use curl -L fuseclient:port/flags/fuseClientAvgWriteBytes?setvalue=true
 * for dynamic parameter configuration
 */
DEFINE_uint64(fuseClientAvgWriteBytes, 0,
              "the write throttle bps of fuse client");
DEFINE_validator(fuseClientAvgWriteBytes, &PassUint64);
DEFINE_uint64(fuseClientBurstWriteBytes, 0,
              "the write burst bps of fuse client");
DEFINE_validator(fuseClientBurstWriteBytes, &PassUint64);
DEFINE_uint64(fuseClientBurstWriteBytesSecs, 180,
              "the times that write burst bps can continue");
DEFINE_validator(fuseClientBurstWriteBytesSecs, &PassUint64);

DEFINE_uint64(fuseClientAvgWriteIops, 0,
              "the write throttle iops of fuse client");
DEFINE_validator(fuseClientAvgWriteIops, &PassUint64);
DEFINE_uint64(fuseClientBurstWriteIops, 0,
              "the write burst iops of fuse client");
DEFINE_validator(fuseClientBurstWriteIops, &PassUint64);
DEFINE_uint64(fuseClientBurstWriteIopsSecs, 180,
              "the times that write burst iops can continue");
DEFINE_validator(fuseClientBurstWriteIopsSecs, &PassUint64);

DEFINE_uint64(fuseClientAvgReadBytes, 0,
              "the Read throttle bps of fuse client");
DEFINE_validator(fuseClientAvgReadBytes, &PassUint64);
DEFINE_uint64(fuseClientBurstReadBytes, 0, "the Read burst bps of fuse client");
DEFINE_validator(fuseClientBurstReadBytes, &PassUint64);
DEFINE_uint64(fuseClientBurstReadBytesSecs, 180,
              "the times that Read burst bps can continue");
DEFINE_validator(fuseClientBurstReadBytesSecs, &PassUint64);

DEFINE_uint64(fuseClientAvgReadIops, 0,
              "the Read throttle iops of fuse client");
DEFINE_validator(fuseClientAvgReadIops, &PassUint64);
DEFINE_uint64(fuseClientBurstReadIops, 0, "the Read burst iops of fuse client");
DEFINE_validator(fuseClientBurstReadIops, &PassUint64);
DEFINE_uint64(fuseClientBurstReadIopsSecs, 180,
              "the times that Read burst iops can continue");
DEFINE_validator(fuseClientBurstReadIopsSecs, &PassUint64);

}  // namespace client
}  // namespace dingofs
