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

#ifndef DINGOFS_OPTIONS_MDSV2_H_
#define DINGOFS_OPTIONS_MDSV2_H_

#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_string(mds_monitor_lock_name);
DECLARE_string(mds_gc_lock_name);

DECLARE_string(mds_pid_file_name);

DECLARE_string(mds_id_generator_type);

// quota config
DECLARE_uint32(mds_quota_worker_num);
DECLARE_uint32(mds_quota_worker_max_pending_num);

// crontab config
DECLARE_uint32(mds_crontab_heartbeat_interval_s);
DECLARE_uint32(mds_crontab_fsinfosync_interval_s);
DECLARE_uint32(mds_crontab_mdsmonitor_interval_s);
DECLARE_uint32(mds_crontab_quota_sync_interval_s);
DECLARE_uint32(mds_crontab_gc_interval_s);

// log config
DECLARE_string(mds_log_level);
DECLARE_string(mds_log_path);

// server config
DECLARE_uint32(mds_server_id);
DECLARE_string(mds_server_host);
DECLARE_string(mds_server_listen_host);
DECLARE_uint32(mds_server_port);

// service config
DECLARE_string(mds_service_worker_set_type);

DECLARE_uint32(mds_service_read_worker_num);
DECLARE_uint32(mds_service_read_worker_max_pending_num);
DECLARE_bool(mds_service_read_worker_use_pthread);

DECLARE_uint32(mds_service_write_worker_num);
DECLARE_uint32(mds_service_write_worker_max_pending_num);
DECLARE_bool(mds_service_write_worker_use_pthread);

DECLARE_uint64(mds_service_log_threshold_time_us);
DECLARE_int32(mds_service_log_print_max_length);

DECLARE_uint32(mds_scan_batch_size);
DECLARE_uint32(mds_txn_max_retry_times);

DECLARE_bool(mds_cache_member_enable_cache);

DECLARE_uint32(mds_transfer_max_slice_num);

// gc config
DECLARE_uint32(mds_gc_worker_num);
DECLARE_uint32(mds_gc_max_pending_task_count);
DECLARE_bool(mds_gc_delslice_enable);
DECLARE_bool(mds_gc_delfile_enable);
DECLARE_bool(mds_gc_filesession_enable);
DECLARE_uint32(mds_gc_delfile_reserve_time_s);
DECLARE_uint32(mds_gc_filesession_reserve_time_s);

// heartbeat config
DECLARE_uint32(mds_heartbeat_mds_offline_period_time_ms);
DECLARE_uint32(mds_heartbeat_client_offline_period_ms);

// monitor config
DECLARE_uint32(mds_monitor_client_clean_period_time_s);

// distribution lock config
DECLARE_uint64(mds_distribution_lock_lease_ttl_ms);
DECLARE_uint32(mds_distribution_lock_scan_size);

// filesystem config
DECLARE_uint32(mds_filesystem_name_max_size);
DECLARE_uint32(mds_filesystem_hash_bucket_num);

// compaction config
DECLARE_bool(mds_compact_chunk_enable);
DECLARE_bool(mds_compact_chunk_detail_log_enable);
DECLARE_uint32(mds_compact_chunk_threshold_num);
DECLARE_uint32(mds_compact_chunk_interval_ms);

// notify buddy config
DECLARE_uint32(mds_notify_message_batch_size);
DECLARE_uint32(mds_wait_message_delay_us);

DECLARE_uint32(mds_inode_cache_max_count);
DECLARE_uint32(mds_partition_cache_max_count);

// store operation config
DECLARE_uint32(mds_store_operation_batch_size);
DECLARE_uint32(mds_store_operation_merge_delay_us);

DECLARE_int32(mds_storage_dingodb_replica_num);

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_OPTIONS_MDSV2_H_