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

#ifndef DINGOFS_SRC_CLIENT_OPTIONS_VFS_OPTION_H_
#define DINGOFS_SRC_CLIENT_OPTIONS_VFS_OPTION_H_

#include <cstdint>

#include "blockaccess/accesser_common.h"
#include "options/cache/blockcache.h"
#include "options/cache/tiercache.h"
#include "options/client/fuse/fuse_option.h"
#include "options/client/memory/page_option.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {
namespace vfs {

DECLARE_bool(data_use_direct_write);
DECLARE_bool(data_single_thread_read);

struct VFSMetaOption {
  uint32_t max_name_length{255};  // max length of file name
};

struct VFSDataOption {
  bool writeback{false};  // whether to use writeback
  std::string writeback_suffix;
};

struct VFSOption {
  blockaccess::BlockAccessOptions block_access_opt;  // from config
  PageOption page_option;
  cache::BlockCacheOption block_cache_option;
  cache::RemoteBlockCacheOption remote_block_cache_option;
  FuseOption fuse_option;

  VFSMetaOption meta_option;
  VFSDataOption data_option;

  uint32_t dummy_server_port{10000};
};

void InitVFSOption(utils::Configuration* conf, VFSOption* option);

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_OPTIONS_VFS_OPTION_H_