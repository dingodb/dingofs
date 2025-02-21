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

#include <gflags/gflags.h>

#include "mdsv2/client/mds.h"
#include "mdsv2/client/store.h"
#include "mdsv2/common/logging.h"

DEFINE_string(coor_addr, "127.0.0.1:7801", "coordinator address");
DEFINE_string(addr, "127.0.0.1:7801", "mds address");

DEFINE_string(cmd, "", "command");

DEFINE_string(fs_name, "", "fs name");
DEFINE_uint32(fs_id, 0, "fs id");
DEFINE_string(fs_partition_type, "mono", "fs partition type");

std::set<std::string> g_mds_cmd = {"create_fs", "delete_fs", "get_fs"};

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  google::InitGoogleLogging(argv[0]);

  google::ParseCommandLineFlags(&argc, &argv, true);

  // dingofs::mdsv2::DingoLogger::InitLogger("./log", "mdsv2_client", dingofs::mdsv2::LogLevel::kINFO);

  if (g_mds_cmd.count(FLAGS_cmd) > 0) {
    dingofs::mdsv2::client::MDSClient mds_client;
    if (!mds_client.Init(FLAGS_addr)) {
      std::cout << "init interaction fail." << std::endl;
      return -1;
    }

    if (FLAGS_cmd == "create_fs") {
      mds_client.CreateFs(FLAGS_fs_name, FLAGS_fs_partition_type);

    } else if (FLAGS_cmd == "delete_fs") {
      mds_client.DeleteFs(FLAGS_fs_name);

    } else if (FLAGS_cmd == "get_fs") {
      mds_client.GetFs(FLAGS_fs_name);

    } else {
      std::cout << "Invalid command: " << FLAGS_cmd;
      return -1;
    }
  } else {
    dingofs::mdsv2::client::StoreClient store_client;
    store_client.Init(FLAGS_coor_addr);

    if (FLAGS_cmd == "tree") {
      store_client.PrintDentryTree(FLAGS_fs_id, true);

    } else {
      std::cout << "Invalid command: " << FLAGS_cmd;
      return -1;
    }
  }

  return 0;
}
