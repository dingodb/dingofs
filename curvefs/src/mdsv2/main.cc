// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "curvefs/src/mdsv2/common/helper.h"
#include "curvefs/src/mdsv2/common/version.h"
#include "curvefs/src/mdsv2/server.h"
#include "fmt/core.h"
#include "glog/logging.h"

DEFINE_string(conf, "./conf/mdsv2.conf", "mdsv2 config path");
DEFINE_string(coor_url, "file://./conf/coor_list", "coor service url, e.g. file://<path> or list://<addr1>,<addr2>");

int main(int argc, char* argv[]) {
  if (dingofs::mdsv2::Helper::IsExistPath("conf/gflags.conf")) {
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
  }

  google::ParseCommandLineFlags(&argc, &argv, true);

  if (dingofs::mdsv2::FLAGS_show_version) {
    dingofs::mdsv2::DingoShowVerion();

    printf("Usage: %s --conf ./conf/mdsv2.conf --coor_url=[file://./conf/coor_list]\n", argv[0]);
    printf("Example: \n");
    printf("         bin/dingofs_mdsv2\n");
    printf(
        "         bin/dingofs_mdsv2 --conf ./conf/mdsv2.yaml "
        "--coor_url=file://./conf/coor_list\n");
    exit(-1);
  }

  dingofs::mdsv2::Server& server = dingofs::mdsv2::Server::GetInstance();

  CHECK(server.InitConfig(FLAGS_conf)) << fmt::format("init config({}) error.", FLAGS_conf);
  CHECK(server.InitLog()) << "init log error.";
  CHECK(server.InitMDSMeta()) << "init mds meta error.";
  CHECK(server.InitCoordinatorClient(FLAGS_coor_url)) << "init coordinator client error.";
  CHECK(server.InitFsIdGenerator()) << "init fs id generator error.";
  CHECK(server.InitStorage(FLAGS_coor_url)) << "init storage error.";
  CHECK(server.InitFileSystem()) << "init file system error.";
  CHECK(server.InitWorkerSet()) << "init worker set error.";
  CHECK(server.InitHeartbeat()) << "init heartbeat error.";
  CHECK(server.InitCrontab()) << "init crontab error.";

  server.Run();

  server.Stop();

  return 0;
}