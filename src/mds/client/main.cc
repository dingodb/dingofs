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

#include <glog/logging.h>

#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "mds/client/br.h"
#include "mds/client/mds.h"
#include "mds/client/store.h"
#include "mds/common/constant.h"
#include "mds/common/helper.h"

DEFINE_string(coor_addr, "", "coordinator address, etc: list://127.0.0.1:22001 or file://./coor_list");
DEFINE_string(mds_addr, "", "mds address");

DEFINE_string(cmd, "", "command");

DEFINE_string(s3_endpoint, "", "s3 endpoint");
DEFINE_string(s3_ak, "", "s3 ak");
DEFINE_string(s3_sk, "", "s3 sk");
DEFINE_string(s3_bucketname, "", "s3 bucket name");
DEFINE_string(s3_objectname, "", "s3 object name");

DEFINE_string(rados_user_name, "", "s3 user name");
DEFINE_string(rados_pool_name, "", "s3 pool name");
DEFINE_string(rados_mon_host, "", "rados mon host");
DEFINE_string(rados_key, "", "rados key");
DEFINE_string(rados_cluster_name, "", "rados cluster name");

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

DEFINE_uint64(max_bytes, 1024 * 1024 * 1024, "max bytes");
DEFINE_uint64(max_inodes, 1000000, "max inodes");

DEFINE_bool(is_force, false, "is force");

DEFINE_string(type, "", "type backup[meta|fsmeta]");
DEFINE_string(output_type, "stdout", "output type[stdout|file|s3]");
DEFINE_string(input_type, "stdout", "input type[stdout|file|s3]");
DEFINE_string(out, "./output", "output file path");
DEFINE_string(in, "./input", "input file path");
DEFINE_bool(is_binary, false, "is binary");

DEFINE_string(mds_id_list, "", "mds id list for joinfs or quitfs, e.g. 1,2,3");

DEFINE_string(member_id, "", "cache member id must be uuid");
DEFINE_string(cache_member_ip, "", "cache member ip");
DEFINE_uint32(cache_member_port, 0, "cache member port");
DEFINE_string(group_name, "", "cache group name");
DEFINE_uint32(weight, 0, "cache member weight");

static std::string GetDefaultCoorAddrPath() {
  if (!FLAGS_coor_addr.empty()) {
    return FLAGS_coor_addr;
  }

  std::vector<std::string> paths = {"./coor_list", "./conf/coor_list", "./bin/coor_list"};
  for (const auto& path : paths) {
    if (dingofs::mds::Helper::IsExistPath(path)) {
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
  using Helper = dingofs::mds::Helper;

  google::ParseCommandLineFlags(&argc, &argv, true);

  std::string program_name = GetLastName(std::string(argv[0]));
  ::FLAGS_log_dir = "./log/";
  dingofs::Logger::Init(program_name);

  std::string lower_cmd = Helper::ToLowerCase(FLAGS_cmd);

  // run backup command
  {
    dingofs::mds::br::BackupCommandRunner::Options options;
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

    if (dingofs::mds::br::BackupCommandRunner::Run(options, GetDefaultCoorAddrPath(), lower_cmd)) {
      return 0;
    }
  }

  // run restore command
  {
    dingofs::mds::br::RestoreCommandRunner::Options options;
    options.type = Helper::ToLowerCase(FLAGS_type);
    options.input_type = Helper::ToLowerCase(FLAGS_input_type);
    options.fs_id = FLAGS_fs_id;
    options.fs_name = FLAGS_fs_name;
    options.file_path = FLAGS_in;
    options.is_force = FLAGS_is_force;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    if (dingofs::mds::br::RestoreCommandRunner::Run(options, GetDefaultCoorAddrPath(), lower_cmd)) {
      return 0;
    }
  }

  // run mds command
  {
    dingofs::mds::client::MdsCommandRunner::Options options;
    options.fs_id = FLAGS_fs_id;
    options.ino = FLAGS_ino;
    options.parent = FLAGS_parent;
    options.parents = FLAGS_parents;
    options.name = FLAGS_name;
    options.fs_name = FLAGS_fs_name;
    options.mds_id_list = FLAGS_mds_id_list;
    options.prefix = FLAGS_prefix;
    options.num = FLAGS_num;
    options.max_bytes = FLAGS_max_bytes;
    options.max_inodes = FLAGS_max_inodes;
    options.fs_partition_type = FLAGS_fs_partition_type;
    options.chunk_size = FLAGS_chunk_size;
    options.block_size = FLAGS_block_size;

    // cache member
    options.member_id = FLAGS_member_id;
    options.ip = FLAGS_cache_member_ip;
    options.port = FLAGS_cache_member_port;
    options.group_name = FLAGS_group_name;
    options.weight = FLAGS_weight;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    auto& rados_info = options.rados_info;
    rados_info.user_name = FLAGS_rados_user_name;
    rados_info.pool_name = FLAGS_rados_pool_name;
    rados_info.mon_host = FLAGS_rados_mon_host;
    rados_info.key = FLAGS_rados_key;
    rados_info.cluster_name = FLAGS_rados_cluster_name;

    if (dingofs::mds::client::MdsCommandRunner::Run(options, FLAGS_mds_addr, lower_cmd, FLAGS_fs_id)) {
      return 0;
    }
  }

  // run store command
  {
    dingofs::mds::client::StoreCommandRunner::Options options;
    options.fs_id = FLAGS_fs_id;
    options.fs_name = FLAGS_fs_name;
    options.meta_table_name = dingofs::mds::kMetaTableName;
    options.fsstats_table_name = dingofs::mds::kFsStatsTableName;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    auto& rados_info = options.rados_info;
    rados_info.user_name = FLAGS_rados_user_name;
    rados_info.pool_name = FLAGS_rados_pool_name;
    rados_info.mon_host = FLAGS_rados_mon_host;
    rados_info.key = FLAGS_rados_key;
    rados_info.cluster_name = FLAGS_rados_cluster_name;

    dingofs::mds::client::StoreCommandRunner::Run(options, GetDefaultCoorAddrPath(), lower_cmd);
  }

  return 0;
}
