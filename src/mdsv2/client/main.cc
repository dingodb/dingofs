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
#include "mdsv2/client/mds.h"
#include "mdsv2/client/store.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"

DEFINE_string(coor_addr, "", "coordinator address, etc: list://127.0.0.1:22001 or file://./coor_list");
DEFINE_string(mds_addr, "", "mds address");

DEFINE_string(cmd, "", "command");

DEFINE_string(fs_name, "", "fs name");
DEFINE_uint32(fs_id, 0, "fs id");
DEFINE_string(fs_partition_type, "mono", "fs partition type");

DEFINE_string(name, "", "name");
DEFINE_string(prefix, "", "prefix");

DEFINE_uint64(parent, 0, "parent");
DEFINE_string(parents, "", "parents");
DEFINE_uint32(num, 1, "num");

DEFINE_string(lock_table_name, "dingofs-lock", "lock table name");
DEFINE_string(mds_table_name, "dingofs-heartbeat", "mds table name");
DEFINE_string(fs_table_name, "dingofs-fs", "fs table name");
DEFINE_string(quota_table_name, "dingofs-quota", "quota table name");
DEFINE_string(stats_table_name, "dingofs-stats", "stats table name");

std::set<std::string> g_mds_cmd = {"create_fs", "delete_fs", "get_fs", "mkdir", "batch_mkdir", "mknod", "batch_mknod"};

std::string GetDefaultCoorAddrPath() {
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
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  google::InitGoogleLogging(argv[0]);

  google::ParseCommandLineFlags(&argc, &argv, true);

  // dingofs::mdsv2::DingoLogger::InitLogger("./log", "mdsv2_client", dingofs::mdsv2::LogLevel::kINFO);

  if (g_mds_cmd.count(FLAGS_cmd) > 0) {
    if (FLAGS_mds_addr.empty()) {
      std::cout << "mds address is empty." << '\n';
      return -1;
    }

    dingofs::mdsv2::client::MDSClient mds_client;
    if (!mds_client.Init(FLAGS_mds_addr)) {
      std::cout << "init interaction fail." << '\n';
      return -1;
    }

    if (FLAGS_cmd == "create_fs") {
      mds_client.CreateFs(FLAGS_fs_name, FLAGS_fs_partition_type);

    } else if (FLAGS_cmd == "delete_fs") {
      mds_client.DeleteFs(FLAGS_fs_name);

    } else if (FLAGS_cmd == "get_fs") {
      mds_client.GetFs(FLAGS_fs_name);

    } else if (FLAGS_cmd == "mkdir") {
      mds_client.MkDir(FLAGS_fs_id, FLAGS_parent, FLAGS_name);

    } else if (FLAGS_cmd == "batch_mkdir") {
      std::vector<int64_t> parents;
      dingofs::mdsv2::Helper::SplitString(FLAGS_parents, ',', parents);
      mds_client.BatchMkDir(FLAGS_fs_id, parents, FLAGS_prefix, FLAGS_num);

    } else if (FLAGS_cmd == "mknod") {
      mds_client.MkNod(FLAGS_fs_id, FLAGS_parent, FLAGS_name);

    } else if (FLAGS_cmd == "batch_mknod") {
      std::vector<int64_t> parents;
      dingofs::mdsv2::Helper::SplitString(FLAGS_parents, ',', parents);
      mds_client.BatchMkNod(FLAGS_fs_id, parents, FLAGS_prefix, FLAGS_num);

    } else {
      std::cout << "Invalid command: " << FLAGS_cmd;
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

    if (FLAGS_cmd == "create_lock_table") {
      store_client.CreateLockTable(FLAGS_lock_table_name);

    } else if (FLAGS_cmd == "create_heartbeat_table") {
      store_client.CreateHeartbeatTable(FLAGS_mds_table_name);

    } else if (FLAGS_cmd == "create_fs_table") {
      store_client.CreateFsTable(FLAGS_fs_table_name);

    } else if (FLAGS_cmd == "create_quota_table") {
      store_client.CreateFsQuotaTable(FLAGS_quota_table_name);

    } else if (FLAGS_cmd == "create_stats_table") {
      store_client.CreateFsStatsTable(FLAGS_stats_table_name);

    } else if (FLAGS_cmd == "create_all_table") {
      store_client.CreateLockTable(FLAGS_lock_table_name);
      store_client.CreateHeartbeatTable(FLAGS_mds_table_name);
      store_client.CreateFsTable(FLAGS_fs_table_name);
      store_client.CreateFsQuotaTable(FLAGS_quota_table_name);
      store_client.CreateFsStatsTable(FLAGS_stats_table_name);

    } else if (FLAGS_cmd == "tree") {
      store_client.PrintDentryTree(FLAGS_fs_id, true);

    } else {
      std::cout << "Invalid command: " << FLAGS_cmd;
      return -1;
    }
  }

  return 0;
}
