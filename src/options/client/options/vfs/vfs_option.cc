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

#include "options/client/options/vfs/vfs_option.h"

#include "options/client/options/common_option.h"
#include "options/client/options/memory/page_option.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {
namespace vfs {

DEFINE_bool(data_use_direct_write, true,
            "Use direct write to chunk, If true, use direct write to block "
            "cache, otherwise use buffer write");

void InitVFSOption(utils::Configuration* conf, VFSOption* option) {
  blockaccess::InitAwsSdkConfig(
      conf, &option->block_access_opt.s3_options.aws_sdk_config);
  blockaccess::InitBlockAccesserThrottleOptions(
      conf, &option->block_access_opt.throttle_options);

  InitMemoryPageOption(conf, &option->page_option);

  InitBlockCacheOption(conf, &option->block_cache_option);
  InitRemoteBlockCacheOption(conf, &option->remote_block_cache_option);

  InitFuseOption(conf, &option->fuse_option);

  // vfs data related
  if (!conf->GetBoolValue("vfs.data.use_direct_write",
                          &vfs::FLAGS_data_use_direct_write)) {
    vfs::FLAGS_data_use_direct_write = true;
    LOG(INFO) << "Not found `vfs.data.use_direct_write` in conf, "
                 "default to true";
  }

  if (!conf->GetBoolValue("vfs.data.writeback",
                          &option->data_option.writeback)) {
    LOG(INFO) << "Not found `vfs.data.writeback` in conf, default:"
              << (option->data_option.writeback ? "true" : "false");
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
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
