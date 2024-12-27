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

#include "dingofs/src/mdsv2/client/mds.h"
#include "dingofs/src/mdsv2/common/logging.h"

DEFINE_string(addr, "127.0.0.1:7800", "mds address");
DEFINE_string(cmd, "", "command");

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  google::InitGoogleLogging(argv[0]);

  google::ParseCommandLineFlags(&argc, &argv, true);

  // dingofs::mdsv2::DingoLogger::InitLogger("./log", "mdsv2_client", dingofs::mdsv2::LogLevel::kINFO);

  dingofs::mdsv2::client::Interaction interaction;
  interaction.Init(FLAGS_addr);

  if (FLAGS_cmd == "create_fs") {
    dingofs::mdsv2::client::MDSClient::CreateFs(interaction);

  } else {
    std::cout << "Invalid command: " << FLAGS_cmd;
    return -1;
  }

  return 0;
}
