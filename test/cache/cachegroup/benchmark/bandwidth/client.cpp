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
#include "cache/blockcache/block_cache.h"
#include "dingofs/blockcache.pb.h"

using ::dingofs::base::math::kMiB;
using ::dingofs::base::string::StrFormat;
using ::dingofs::client::blockcache::BlockKey;

DEFINE_string(server, "127.0.0.1:9301", "IP Address of server");
DEFINE_int64(block_ino, 0, "Block inode number");
DEFINE_int64(first_block_id, 0, "First block key id");
DEFINE_int64(last_block_id, 1, "Last block key id");

static dingofs::pb::client::blockcache::RangeRequest NewRequest(
    uint64_t ino, uint64_t block_id) {
  dingofs::pb::client::blockcache::BlockKey pb;
  pb.set_fs_id(0);
  pb.set_ino(ino);
  pb.set_id(block_id);
  pb.set_index(0);
  pb.set_version(0);

  dingofs::pb::client::blockcache::RangeRequest request;
  *request.mutable_block_key() = pb;
  request.set_block_size(4 * kMiB);
  request.set_offset(0);
  request.set_length(4 * kMiB);
  return request;
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  brpc::Channel channel;
  brpc::ChannelOptions options;
  int rc = channel.Init(FLAGS_server.c_str(), &options);
  if (rc != 0) {
    LOG(ERROR) << "Init channel failed, rc=" << rc;
    return -1;
  }

  for (auto block_id = FLAGS_first_block_id; block_id <= FLAGS_last_block_id;
       block_id++) {
    brpc::Controller cntl;
    dingofs::pb::client::blockcache::RangeResponse response;
    dingofs::pb::client::blockcache::BlockCacheService_Stub stub(&channel);
    auto request = NewRequest(FLAGS_block_ino, block_id);

    butil::Timer timer;
    timer.start();
    stub.Range(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
      LOG(ERROR) << "Received response from " << cntl.remote_side() << " to "
                 << cntl.local_side() << ": " << "latency=" << cntl.latency_us()
                 << "us, error=" << cntl.ErrorText();
    }
    timer.stop();

    std::string filename = BlockKey(request.block_key()).Filename();
    LOG(INFO) << "range(" << filename << "): OK <"
              << StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6) << ">";
  }

  return 0;
}
