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
#include "mdsv2/client/integration_test.h"
#include "mdsv2/client/mds.h"
#include "mdsv2/client/store.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/type.h"

DEFINE_string(coor_addr, "", "coordinator address, etc: list://127.0.0.1:22001 or file://./coor_list");
DEFINE_string(mds_addr, "", "mds address");

DEFINE_string(cmd, "", "command");

DEFINE_string(s3_endpoint, "", "s3 endpoint");
DEFINE_string(s3_ak, "", "s3 ak");
DEFINE_string(s3_sk, "", "s3 sk");
DEFINE_string(s3_bucketname, "", "s3 bucket name");

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

DEFINE_string(lock_table_name, "dingofs-lock", "lock table name");
DEFINE_string(autoincrement_table_name, "dingofs-autoincrement", "autoincrement table name");
DEFINE_string(heartbeat_table_name, "dingofs-heartbeat", "heartbeat table name");
DEFINE_string(fs_table_name, "dingofs-fs", "fs table name");
DEFINE_string(quota_table_name, "dingofs-quota", "quota table name");
DEFINE_string(stats_table_name, "dingofs-stats", "stats table name");
DEFINE_string(filesession_table_name, "dingofs-filesession", "file session table name");
DEFINE_string(del_slice_table_name, "dingofs-delslice", "del slice table name");
DEFINE_string(del_file_table_name, "dingofs-delfile", "del file table name");

DEFINE_uint32(max_bytes, 1024 * 1024 * 1024, "max bytes");
DEFINE_uint32(max_inodes, 1000000, "max inodes");

DEFINE_bool(is_force, false, "is force");

static std::set<std::string> g_mds_cmd = {
    "integrationtest", "getmdslist",
    "createfs",        "deletefs",
    "updatefs",        "getfs",
    "listfs",          "mkdir",
    "batchmkdir",      "mknod",
    "batchmknod",      "getdentry",
    "listdentry",      "getinode",
    "batchgetinode",   "batchgetxattr",
    "setfsstats",      "continuesetfsstats",
    "getfsstats",      "getfspersecondstats",
    "setfsquota",      "getfsquota",
    "setdirquota",     "getdirquota",
    "deletedirquota",
};

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

int main(int argc, char* argv[]) {
  using Helper = dingofs::mdsv2::Helper;

  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  google::InitGoogleLogging(argv[0]);

  google::ParseCommandLineFlags(&argc, &argv, true);

  // dingofs::mdsv2::DingoLogger::InitLogger("./log", "mdsv2_client", dingofs::mdsv2::LogLevel::kINFO);

  std::string lower_cmd = Helper::ToLowerCase(FLAGS_cmd);

  if (lower_cmd == Helper::ToLowerCase("IntegrationTest")) {
    dingofs::mdsv2::client::RunIntegrationTests();
    return 0;
  }

  if (g_mds_cmd.count(lower_cmd) > 0) {
    if (FLAGS_mds_addr.empty()) {
      std::cout << "mds address is empty." << '\n';
      return -1;
    }

    dingofs::mdsv2::client::MDSClient mds_client(FLAGS_fs_id);
    if (!mds_client.Init(FLAGS_mds_addr)) {
      std::cout << "init interaction fail." << '\n';
      return -1;
    }

    if (lower_cmd == Helper::ToLowerCase("GetMdsList")) {
      mds_client.GetMdsList();

    } else if (lower_cmd == Helper::ToLowerCase("CreateFs")) {
      dingofs::mdsv2::client::MDSClient::CreateFsParams params;
      params.partition_type = FLAGS_fs_partition_type;
      params.chunk_size = FLAGS_chunk_size;
      params.block_size = FLAGS_block_size;
      params.s3_endpoint = FLAGS_s3_endpoint;
      params.s3_ak = FLAGS_s3_ak;
      params.s3_sk = FLAGS_s3_sk;
      params.s3_bucketname = FLAGS_s3_bucketname;

      mds_client.CreateFs(FLAGS_fs_name, params);

    } else if (lower_cmd == Helper::ToLowerCase("DeleteFs")) {
      mds_client.DeleteFs(FLAGS_fs_name, FLAGS_is_force);

    } else if (lower_cmd == Helper::ToLowerCase("UpdateFs")) {
      mds_client.UpdateFs(FLAGS_fs_name);

    } else if (lower_cmd == Helper::ToLowerCase("GetFs")) {
      mds_client.GetFs(FLAGS_fs_name);

    } else if (lower_cmd == Helper::ToLowerCase("ListFs")) {
      mds_client.ListFs();

    } else if (lower_cmd == Helper::ToLowerCase("MkDir")) {
      mds_client.MkDir(FLAGS_parent, FLAGS_name);

    } else if (lower_cmd == Helper::ToLowerCase("BatchMkDir")) {
      std::vector<int64_t> parents;
      dingofs::mdsv2::Helper::SplitString(FLAGS_parents, ',', parents);
      mds_client.BatchMkDir(parents, FLAGS_prefix, FLAGS_num);

    } else if (lower_cmd == Helper::ToLowerCase("MkNod")) {
      mds_client.MkNod(FLAGS_parent, FLAGS_name);

    } else if (lower_cmd == Helper::ToLowerCase("BatchMkNod")) {
      std::vector<int64_t> parents;
      dingofs::mdsv2::Helper::SplitString(FLAGS_parents, ',', parents);
      mds_client.BatchMkNod(parents, FLAGS_prefix, FLAGS_num);

    } else if (lower_cmd == Helper::ToLowerCase("GetDentry")) {
      mds_client.GetDentry(FLAGS_parent, FLAGS_name);

    } else if (lower_cmd == Helper::ToLowerCase("ListDentry")) {
      mds_client.ListDentry(FLAGS_parent, false);

    } else if (lower_cmd == Helper::ToLowerCase("GetInode")) {
      mds_client.GetInode(FLAGS_parent);

    } else if (lower_cmd == Helper::ToLowerCase("BatchGetInode")) {
      std::vector<int64_t> inos;
      dingofs::mdsv2::Helper::SplitString(FLAGS_parents, ',', inos);
      mds_client.BatchGetInode(inos);

    } else if (lower_cmd == Helper::ToLowerCase("BatchGetXattr")) {
      std::vector<int64_t> inos;
      dingofs::mdsv2::Helper::SplitString(FLAGS_parents, ',', inos);
      mds_client.BatchGetXattr(inos);

    } else if (lower_cmd == Helper::ToLowerCase("SetFsStats")) {
      mds_client.SetFsStats(FLAGS_fs_name);

    } else if (lower_cmd == Helper::ToLowerCase("ContinueSetFsStats")) {
      mds_client.ContinueSetFsStats(FLAGS_fs_name);

    } else if (lower_cmd == Helper::ToLowerCase("GetFsStats")) {
      mds_client.GetFsStats(FLAGS_fs_name);

    } else if (lower_cmd == Helper::ToLowerCase("GetFsPerSecondStats")) {
      mds_client.GetFsPerSecondStats(FLAGS_fs_name);

    } else if (lower_cmd == Helper::ToLowerCase("SetFsQuota")) {
      dingofs::mdsv2::QuotaEntry quota;
      quota.set_max_bytes(FLAGS_max_bytes);
      quota.set_max_inodes(FLAGS_max_inodes);

      mds_client.SetFsQuota(quota);

    } else if (lower_cmd == Helper::ToLowerCase("GetFsQuota")) {
      auto response = mds_client.GetFsQuota();
      std::cout << "fs quota: " << response.quota().ShortDebugString() << '\n';

    } else if (lower_cmd == Helper::ToLowerCase("SetDirQuota")) {
      if (FLAGS_ino == 0) {
        std::cout << "ino is empty." << '\n';
        return -1;
      }

      dingofs::mdsv2::QuotaEntry quota;
      quota.set_max_bytes(FLAGS_max_bytes);
      quota.set_max_inodes(FLAGS_max_inodes);

      mds_client.SetDirQuota(FLAGS_ino, quota);

    } else if (lower_cmd == Helper::ToLowerCase("GetDirQuota")) {
      if (FLAGS_ino == 0) {
        std::cout << "ino is empty." << '\n';
        return -1;
      }

      auto response = mds_client.GetDirQuota(FLAGS_ino);
      std::cout << "dir quota: " << response.quota().ShortDebugString() << '\n';

    } else if (lower_cmd == Helper::ToLowerCase("DeleteDirQuota")) {
      if (FLAGS_ino == 0) {
        std::cout << "ino is empty." << '\n';
        return -1;
      }
      mds_client.DeleteDirQuota(FLAGS_ino);

    } else {
      std::cout << "Invalid command: " << lower_cmd;
      return -1;
    }
  } else {
    std::string coor_addr = GetDefaultCoorAddrPath();
    if (coor_addr.empty()) {
      std::cout << "coordinator address is empty." << '\n';
      return -1;
    }

    DINGO_LOG(INFO) << "coor_addr: " << coor_addr;

    dingofs::mdsv2::client::StoreClient store_client;
    if (!store_client.Init(coor_addr)) {
      std::cout << "init store client fail." << '\n';
      return -1;
    }

    if (lower_cmd == Helper::ToLowerCase("CreateLockTable")) {
      store_client.CreateLockTable(FLAGS_lock_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("CreateAutoIncrementTable")) {
      store_client.CreateAutoIncrementTable(FLAGS_autoincrement_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("CreateHeartbeatTable")) {
      store_client.CreateHeartbeatTable(FLAGS_heartbeat_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("CreateFsTable")) {
      store_client.CreateFsTable(FLAGS_fs_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("CreateFsQuotaTable")) {
      store_client.CreateFsQuotaTable(FLAGS_quota_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("CreateFsStatsTable")) {
      store_client.CreateFsStatsTable(FLAGS_stats_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("CreateFileSessionTable")) {
      store_client.CreateFileSessionTable(FLAGS_filesession_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("CreateDelSliceTable")) {
      store_client.CreateDelSliceTable(FLAGS_del_slice_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("CreateDelFileTable")) {
      store_client.CreateDelFileTable(FLAGS_del_file_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("CreateAllTable")) {
      store_client.CreateLockTable(FLAGS_lock_table_name);
      store_client.CreateAutoIncrementTable(FLAGS_autoincrement_table_name);
      store_client.CreateHeartbeatTable(FLAGS_heartbeat_table_name);
      store_client.CreateFsTable(FLAGS_fs_table_name);
      store_client.CreateFsQuotaTable(FLAGS_quota_table_name);
      store_client.CreateFsStatsTable(FLAGS_stats_table_name);
      store_client.CreateFileSessionTable(FLAGS_filesession_table_name);
      store_client.CreateDelSliceTable(FLAGS_del_slice_table_name);
      store_client.CreateDelFileTable(FLAGS_del_file_table_name);

    } else if (lower_cmd == Helper::ToLowerCase("tree")) {
      store_client.PrintDentryTree(FLAGS_fs_id, true);

    } else {
      std::cout << "invalid command: " << FLAGS_cmd;
      return -1;
    }
  }

  return 0;
}
