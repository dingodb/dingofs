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

#ifndef DINGOFS_CLIENT_COMMON_CLIENT_OPTION_H_
#define DINGOFS_CLIENT_COMMON_CLIENT_OPTION_H_

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>

#include <string>

#include "common/blockaccess/accesser_common.h"
#include "common/const.h"
#include "common/options/client.h"

namespace dingofs {
namespace client {

// fuse option
struct FuseConnInfo {
  bool want_splice_move;
  bool want_splice_read;
  bool want_splice_write;
  bool want_auto_inval_data;
};

struct FuseFileInfo {
  bool keep_cache;
};

struct FuseOption {
  FuseConnInfo conn_info;
  FuseFileInfo file_info;
};

// memory option
struct PageOption {
  uint32_t page_size;
  uint64_t total_size;
  bool use_pool;
};

// vfs option
struct VFSMetaOption {
  uint32_t max_name_length;  // max length of file name
};

struct VFSDataOption {
  bool writeback{false};  // whether to use writeback
  std::string writeback_suffix;
};

struct VFSOption {
  blockaccess::BlockAccessOptions block_access_opt;  // from gflags
  PageOption page_option;
  FuseOption fuse_option;

  VFSMetaOption meta_option;
  VFSDataOption data_option;

  uint32_t dummy_server_port{10000};
};

static void InitFuseOption(FuseOption* option) {
  {  // fuse conn info
    auto* fuse_conn_info = &option->conn_info;
    fuse_conn_info->want_splice_move = FLAGS_fuse_conn_info_want_splice_move;
    fuse_conn_info->want_splice_read = FLAGS_fuse_conn_info_want_splice_read;
    fuse_conn_info->want_splice_write = FLAGS_fuse_conn_info_want_splice_write;
    fuse_conn_info->want_auto_inval_data =
        FLAGS_fuse_conn_info_want_auto_inval_data;
  }

  {  // fuse file info
    auto* fuse_file_info = &option->file_info;
    fuse_file_info->keep_cache = FLAGS_fuse_file_info_keep_cache;
  }
}

static void InitMemoryPageOption(PageOption* option) {
  // page option
  option->page_size = FLAGS_data_stream_page_size;
  option->total_size = FLAGS_data_stream_page_total_size_mb;
  option->use_pool = FLAGS_data_stream_page_use_pool;

  if (option->page_size == 0) {
    CHECK(false) << "page size must greater than 0.";
  }

  option->total_size = option->total_size * kMiB;
  if (option->total_size < 64 * kMiB) {
    CHECK(false) << "page total size must greater than 64MB.";
  }
}

static void InitVFSMetaOption(VFSMetaOption* option) {
  option->max_name_length = FLAGS_vfs_meta_max_name_length;
  if (option->max_name_length < 1) {
    CHECK(false) << "file name length must greater than 0.";
  }
}

static void InitVFSDataOption(VFSDataOption* option) {
  option->writeback = FLAGS_vfs_data_writeback;
  option->writeback_suffix = FLAGS_vfs_data_writeback_suffix;
}

static void InitVFSOption(VFSOption* option) {
  blockaccess::InitAwsSdkConfig(
      &option->block_access_opt.s3_options.aws_sdk_config);
  blockaccess::InitBlockAccesserThrottleOptions(
      &option->block_access_opt.throttle_options);

  InitMemoryPageOption(&option->page_option);

  InitFuseOption(&option->fuse_option);

  InitVFSMetaOption(&option->meta_option);

  InitVFSDataOption(&option->data_option);

  option->dummy_server_port = FLAGS_vfs_dummy_server_port;
}

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_COMMON_CLIENT_OPTION_H_
