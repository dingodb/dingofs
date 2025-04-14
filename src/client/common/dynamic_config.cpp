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

#include "client/common/dynamic_config.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace dingofs {
namespace client {
namespace common {

namespace {
bool PassDouble(const char*, double) { return true; }
bool PassUint64(const char*, uint64_t) { return true; }
bool PassInt32(const char*, int32_t) { return true; }
bool PassUint32(const char*, uint32_t) { return true; }
bool PassBool(const char*, bool) { return true; }
};  // namespace

// access log
DEFINE_bool(access_logging, true, "enable access log");
DEFINE_validator(access_logging, &PassBool);

// fuse module
DEFINE_bool(fuse_file_info_direct_io, false, "use direct io for file");
DEFINE_bool(fuse_file_info_keep_cache, false, "keep file page cache");

DEFINE_validator(fuse_file_info_direct_io, &PassBool);
DEFINE_validator(fuse_file_info_keep_cache, &PassBool);

// block cache
DEFINE_bool(block_cache_logging, true, "enable block cache logging");
DEFINE_bool(block_cache_stage_bandwidth_throttle_enable, false,
            "enable block cache stage bandwidth throttle");
DEFINE_uint64(block_cache_stage_bandwidth_throttle_mb, 102400,
              "block cache stage bandwidth throttle");

DEFINE_validator(block_cache_logging, &PassBool);
DEFINE_validator(block_cache_stage_bandwidth_throttle_enable, &PassBool);
DEFINE_validator(block_cache_stage_bandwidth_throttle_mb, &PassUint64);

// disk cache
DEFINE_bool(drop_page_cache, true, "drop page cache for disk cache");

DEFINE_validator(drop_page_cache, &PassBool);

// disk_cache_cleanup_expire_interval_millsecond

// disk cache manager
DEFINE_uint64(disk_cache_expire_second, 0,
              "cache expire time, 0 means never expired");
DEFINE_uint64(disk_cache_cleanup_expire_interval_millsecond, 1000,
              "cleanup expire blocks interval in millsecond");
DEFINE_double(disk_cache_free_space_ratio, 0.1, "disk free space ratio");

DEFINE_validator(disk_cache_expire_second, &PassUint64);
DEFINE_validator(disk_cache_cleanup_expire_interval_millsecond, &PassUint64);
DEFINE_validator(disk_cache_free_space_ratio, &PassDouble);

// disk state machine
DEFINE_int32(disk_state_tick_duration_second, 60,
             "tick duration in seconds for disk state machine");
DEFINE_int32(disk_state_normal2unstable_io_error_num, 3,
             "io error number to transit from normal to unstable");
DEFINE_int32(disk_state_unstable2normal_io_succ_num, 10,
             "io success number to transit from unstable to normal");
DEFINE_int32(disk_state_unstable2down_second, 30 * 60,
             "second to transit from unstable to down");

DEFINE_validator(disk_state_tick_duration_second, &PassInt32);
DEFINE_validator(disk_state_normal2unstable_io_error_num, &PassInt32);
DEFINE_validator(disk_state_unstable2normal_io_succ_num, &PassInt32);
DEFINE_validator(disk_state_unstable2down_second, &PassInt32);

// disk state health checker
DEFINE_int32(disk_check_duration_millsecond, 1 * 1000,
             "disk health check duration in millsecond");
DEFINE_validator(disk_check_duration_millsecond, &PassInt32);

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

DEFINE_int32(bthread_worker_num, 0, "bthread worker num");

// from config.h and config.cpp
DEFINE_bool(enableCto, true, "acheieve cto consistency");
DEFINE_bool(useFakeS3, false,
            "Use fake s3 to inject more metadata for testing metaserver");
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

}  // namespace common
}  // namespace client
}  // namespace dingofs
