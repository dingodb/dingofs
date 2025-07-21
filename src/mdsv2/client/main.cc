// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mdsv2/client/br.h"
#include "mdsv2/client/integration_test.h"
#include "mdsv2/client/mds.h"
#include "mdsv2/client/store.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/helper.h"

DEFINE_string(coor_addr, "", "coordinator address, etc: list://127.0.0.1:22001 or file://./coor_list");
DEFINE_string(mds_addr, "", "mds address");

DEFINE_string(cmd, "", "command");

DEFINE_string(s3_endpoint, "", "s3 endpoint");
DEFINE_string(s3_ak, "", "s3 ak");
DEFINE_string(s3_sk, "", "s3 sk");
DEFINE_string(s3_bucketname, "", "s3 bucket name");
DEFINE_string(s3_objectname, "", "s3 object name");

DEFINE_string(fs_name, "", "fs name");
DEFINE_uint32(fs_id, 0, "fs id");
DEFINE_string(fs_partition_type, "mono", "fs partition type");

DEFINE_uint32(chunk_size, 64 * 1024 * 1024, "chunk size");
DEFINE_uint32(block_size, 4 * 1024 * 1024, "block size");

DEFINE_string(name, "", "name");
DEFINE_string(prefix, "", "prefix");

DEFINE_uint64(ino, 0, "ino");
DEFINE_uint64(parent, 0, "parent");
DEFINE_string(parents, "", "parents");
DEFINE_uint32(num, 1, "num");

DEFINE_uint32(max_bytes, 1024 * 1024 * 1024, "max bytes");
DEFINE_uint32(max_inodes, 1000000, "max inodes");

DEFINE_bool(is_force, false, "is force");

DEFINE_string(type, "", "type backup[meta|fsmeta]");
DEFINE_string(output_type, "stdout", "output type[stdout|file|s3]");
DEFINE_string(out, "./output", "output file path");
DEFINE_bool(is_binary, false, "is binary");

static std::string GetDefaultCoorAddrPath() {
  if (!FLAGS_coor_addr.empty()) {
    return FLAGS_coor_addr;
  }

  std::vector<std::string> paths = {"./coor_list", "./conf/coor_list", "./bin/coor_list"};
  for (const auto& path : paths) {
    if (dingofs::mdsv2::Helper::IsExistPath(path)) {
      return "file://" + path;
    }
  }

  return "";
}

// get the last name from the path
// e.g. /path/to/file.txt -> file.txt
static std::string GetLastName(const std::string& name) {
  size_t pos = name.find_last_of('/');
  if (pos == std::string::npos) {
    return name;
  }
  return name.substr(pos + 1);
}

int main(int argc, char* argv[]) {
  using Helper = dingofs::mdsv2::Helper;

  google::ParseCommandLineFlags(&argc, &argv, true);

  std::string program_name = GetLastName(std::string(argv[0]));
  dingofs::mdsv2::DingoLogger::InitLogger("./log", program_name, dingofs::mdsv2::LogLevel::kINFO);

  std::string lower_cmd = Helper::ToLowerCase(FLAGS_cmd);

  // run integration test command
  {
    if (dingofs::mdsv2::client::IntegrationTestCommandRunner::Run(lower_cmd)) {
      return 0;
    }
  }

  // run backup command
  {
    dingofs::mdsv2::br::BackupCommandRunner::Options options;
    options.type = Helper::ToLowerCase(FLAGS_type);
    options.output_type = Helper::ToLowerCase(FLAGS_output_type);
    options.fs_id = FLAGS_fs_id;
    options.fs_name = FLAGS_fs_name;
    options.file_path = FLAGS_out;
    options.is_binary = FLAGS_is_binary;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    if (dingofs::mdsv2::br::BackupCommandRunner::Run(options, GetDefaultCoorAddrPath(), lower_cmd)) {
      return 0;
    }
  }

  // run restore command
  {
    dingofs::mdsv2::br::RestoreCommandRunner::Options options;
    options.type = Helper::ToLowerCase(FLAGS_type);
    options.output_type = Helper::ToLowerCase(FLAGS_output_type);
    options.fs_id = FLAGS_fs_id;
    options.fs_name = FLAGS_fs_name;
    options.file_path = FLAGS_out;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    if (dingofs::mdsv2::br::RestoreCommandRunner::Run(options, GetDefaultCoorAddrPath(), lower_cmd)) {
      return 0;
    }
  }

  // run mds command
  {
    dingofs::mdsv2::client::MdsCommandRunner::Options options;
    options.ino = FLAGS_ino;
    options.parent = FLAGS_parent;
    options.parents = FLAGS_parents;
    options.name = FLAGS_name;
    options.fs_name = FLAGS_fs_name;
    options.prefix = FLAGS_prefix;
    options.num = FLAGS_num;
    options.max_bytes = FLAGS_max_bytes;
    options.max_inodes = FLAGS_max_inodes;
    options.fs_partition_type = FLAGS_fs_partition_type;
    options.chunk_size = FLAGS_chunk_size;
    options.block_size = FLAGS_block_size;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    if (dingofs::mdsv2::client::MdsCommandRunner::Run(options, FLAGS_mds_addr, lower_cmd, FLAGS_fs_id)) {
      return 0;
    }
  }

  // run store command
  {
    dingofs::mdsv2::client::StoreCommandRunner::Options options;
    options.fs_id = FLAGS_fs_id;
    options.fs_name = FLAGS_fs_name;
    options.meta_table_name = dingofs::mdsv2::kMetaTableName;
    options.fsstats_table_name = dingofs::mdsv2::kFsStatsTableName;
    dingofs::mdsv2::client::StoreCommandRunner::Run(options, GetDefaultCoorAddrPath(), lower_cmd);
  }

  return 0;
}
