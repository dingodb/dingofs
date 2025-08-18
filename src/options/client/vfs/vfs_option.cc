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

#include "options/client/vfs/vfs_option.h"

#include "options/client/client_dynamic_option.h"
#include "options/client/common_option.h"
#include "options/client/memory/page_option.h"
#include "options/client/vfs/vfs_dynamic_option.h"
#include "options/trace/trace_dynamic_option.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {
namespace vfs {

DEFINE_bool(data_single_thread_read, false,
            "Use single thread read to chunk, If true, use single thread read "
            "to block cache, otherwise use executor read with async");

void InitVFSOption(utils::Configuration* conf, VFSOption* option) {
  blockaccess::InitAwsSdkConfig(
      conf, &option->block_access_opt.s3_options.aws_sdk_config);
  blockaccess::InitBlockAccesserThrottleOptions(
      conf, &option->block_access_opt.throttle_options);

  InitMemoryPageOption(conf, &option->page_option);

  InitBlockCacheOption(conf, &option->block_cache_option);
  InitRemoteBlockCacheOption(conf, &option->remote_block_cache_option);

  InitFuseOption(conf, &option->fuse_option);
  InitPrefetchOption(conf);

  // vfs data related
  if (!conf->GetBoolValue("vfs.data.writeback",
                          &option->data_option.writeback)) {
    LOG(INFO) << "Not found `vfs.data.writeback` in conf, default:"
              << (option->data_option.writeback ? "true" : "false");
  }

  if(!conf->GetStringValue("vfs.data.writeback_suffix",
                           &option->data_option.writeback_suffix)) {
    LOG(INFO) << "Not found `vfs.data.writeback_suffix` in conf, "
                 "default to: " << option->data_option.writeback_suffix;
  }

  if (!conf->GetIntValue("vfs.data.flush_bg_thread",
                         &FLAGS_vfs_flush_bg_thread)) {
    LOG(INFO) << "Not found `vfs.data.flush_bg_thread` in conf, "
                 "default to "
              << FLAGS_vfs_flush_bg_thread;
  }

  if (!conf->GetBoolValue("vfs.data.single_tread_read",
                          &FLAGS_data_single_thread_read)) {
    LOG(INFO) << "Not found `vfs.data.single_tread_read` in conf, "
                 "default to "
              << (FLAGS_data_single_thread_read ? "true" : "false");
  }

  if (!conf->GetIntValue("vfs.data.read_executor_thread",
                         &FLAGS_vfs_read_executor_thread)) {
    LOG(INFO) << "Not found `vfs.data.read_executor_thread` in conf, "
                 "default to "
              << FLAGS_vfs_read_executor_thread;
  }

  // vfs meta related
  if (!conf->GetUInt32Value("vfs.meta.max_name_length",
                            &option->meta_option.max_name_length)) {
    LOG(INFO) << "Not found `vfs.meta.max_name_length` in conf, default to "
              << option->meta_option.max_name_length;
  }

  if (!conf->GetUInt32Value("vfs.dummy_server.port",
                            &option->dummy_server_port)) {
    LOG(INFO) << "Not found `vfs.dummy_server.port` in conf, default to "
              << option->dummy_server_port;
  }

  if (!conf->GetIntValue("vfs.bthread_worker_num", &FLAGS_bthread_worker_num)) {
    FLAGS_bthread_worker_num = 0;
    LOG(INFO) << "Not found `vfs.bthread_worker_num` in conf, "
                 "default to 0";
  }

  if (!conf->GetBoolValue("vfs.access_logging", &FLAGS_access_logging)) {
    LOG(INFO) << "Not found `vfs.access_logging` in conf, default: "
              << FLAGS_access_logging;
  }
  if (!conf->GetInt64Value("vfs.access_log_threshold_us",
                           &FLAGS_access_log_threshold_us)) {
    LOG(INFO) << "Not found `vfs.access_log_threshold_us` in conf, "
                 "default: "
              << FLAGS_access_log_threshold_us;
  }

  if (!conf->GetBoolValue("vfs.vfs_meta_logging", &FLAGS_vfs_meta_logging)) {
    LOG(INFO) << "Not found `vfs.vfs_meta_logging` in conf, default: "
              << FLAGS_vfs_meta_logging;
  }
  if (!conf->GetInt64Value("vfs.vfs_meta_log_threshold_u",
                           &FLAGS_vfs_meta_log_threshold_us)) {
    LOG(INFO) << "Not found `vfs.vfs_meta_log_threshold_u` in conf, "
                 "default: "
              << FLAGS_vfs_meta_log_threshold_us;
  }

  if (!conf->GetBoolValue("vfs.trace_logging", &FLAGS_trace_logging)) {
    LOG(INFO) << "Not found `vfs.trace_logging` in conf, default: "
              << FLAGS_trace_logging;
  }

  SetBrpcOpt(conf);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
