// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <brpc/channel.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <gflags/gflags.h>

#include "base/math/math.h"
#include "base/string/string.h"
#include "dingofs/blockcache.pb.h"

using ::dingofs::base::math::kMiB;
using ::dingofs::base::string::StrFormat;

DEFINE_string(server, "127.0.0.1:9301", "IP Address of server");
DEFINE_int64(block_id, 0, "Block key id");

void TestMemcpy() {
  auto* in = new char[4 * kMiB];
  auto* out = new char[4 * kMiB];
  for (int i = 0; i < 4 * kMiB; i++) {
    in[i] = 'a' + i % 26;
    out[i] = 'b' + i % 25;
  }

  for (int i = 0; i < 4 * kMiB; i++) {
    out[i] = (in[i] + i) % 26;
    in[i] = (out[i] + i) % 26;
  }

  ::butil::Timer timer;
  timer.start();
  memcpy(out, in, 4 * kMiB);

  // for (int i = 0; i < 4 * kMiB; i++) {
  //   out[i] = in[i];
  // }

  timer.stop();
  LOG(INFO) << "Cost " << timer.u_elapsed() * 1.0 / 1e6 << " seconds.";
}

void TestMemcpy2() {
  auto* in = new char[4 * kMiB];

  ::butil::Timer timer;
  timer.start();
  memset(in, 0, 4 * kMiB);
  timer.stop();
  LOG(INFO) << "first cost " << timer.u_elapsed() * 1.0 / 1e6 << " seconds.";

  in[100] = 'a';

  timer.start();
  memset(in, 0, 4 * kMiB);
  timer.stop();
  LOG(INFO) << "second cost " << timer.u_elapsed() * 1.0 / 1e6 << " seconds.";
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  // TestMemcpy();
  TestMemcpy2();

  ::brpc::Channel channel;
  ::brpc::ChannelOptions options;
  int rc = channel.Init(FLAGS_server.c_str(), &options);
  if (rc != 0) {
    LOG(ERROR) << "Init channel failed, rc=" << rc;
    return -1;
  }

  // cntl
  ::brpc::Controller cntl;
  cntl.set_log_id(1);

  // request
  ::dingofs::pb::client::blockcache::BlockKey pb;
  ::dingofs::pb::client::blockcache::RangeRequest request;
  pb.set_fs_id(0);
  pb.set_ino(0);
  pb.set_id(FLAGS_block_id);
  pb.set_index(0);
  pb.set_version(0);
  *request.mutable_block_key() = pb;
  request.set_block_size(4 * kMiB);
  request.set_offset(0);
  request.set_length(4 * kMiB);

  // response
  ::dingofs::pb::client::blockcache::RangeResponse response;

  // stub
  ::dingofs::pb::client::blockcache::BlockCacheService_Stub stub(&channel);

  ::butil::Timer timer;
  timer.start();
  stub.Range(&cntl, &request, &response, nullptr);
  if (!cntl.Failed()) {
    LOG(INFO) << "Received response from " << cntl.remote_side() << " to "
              << cntl.local_side() << ": " << "latency=" << cntl.latency_us()
              << "us";
  } else {
    LOG(WARNING) << cntl.ErrorText();
  }
  timer.stop();
  LOG(INFO) << "rpc cost " << StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6)
            << " seconds.";

  return 0;
}
