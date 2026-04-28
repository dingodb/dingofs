/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

//
// brpc/TCP counterpart of rdma_client.cc. Sends RangeBench.Range RPCs,
// verifies that the response attachment's head/tail uint64s equal num+1,
// and logs per-call latency. Use this side-by-side with rdma_client to
// compare end-to-end latency for 4MB payloads on the same network.
//

#include <absl/strings/str_format.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <butil/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstdint>
#include <random>

#include "dingofs/range_bench.pb.h"

DEFINE_string(server_address, "127.0.0.1:8889",
              "brpc server address (host:port)");
DEFINE_int32(rounds, 1, "Number of rounds; -1 means infinite");
DEFINE_int32(timeout_ms, 30000, "Per-RPC timeout in ms");
DEFINE_string(protocol, "baidu_std", "brpc protocol");
DEFINE_string(connection_type, "single", "single | pooled | short");
DEFINE_bool(use_rdma, false,
            "If true, brpc connects to the server via its built-in RDMA "
            "backend (after the TCP HelloMessage handshake). Server must "
            "also be started with --use_rdma=true.");

namespace pb_rb = dingofs::pb::range_bench;

int main(int argc, char** argv) {
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;
  FLAGS_stderrthreshold = google::WARNING;
  FLAGS_logbuflevel = google::INFO;
  FLAGS_logbufsecs = 1;
  FLAGS_max_log_size = 256;

  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  brpc::Channel channel;
  brpc::ChannelOptions options;
  options.protocol = FLAGS_protocol;
  options.connection_type = FLAGS_connection_type;
  options.timeout_ms = FLAGS_timeout_ms;
  options.max_retry = 0;
  options.use_rdma = FLAGS_use_rdma;
  if (channel.Init(FLAGS_server_address.c_str(), &options) != 0) {
    LOG(ERROR) << "Fail to init channel: server=" << FLAGS_server_address
               << " use_rdma=" << FLAGS_use_rdma;
    return 1;
  }
  LOG(INFO) << "Connected to brpc server=" << FLAGS_server_address
            << " use_rdma=" << FLAGS_use_rdma;

  pb_rb::RangeBenchService_Stub stub(&channel);
  std::mt19937_64 rng(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dist(0, (1ULL << 32) - 1);

  int success = 0;
  int fail = 0;
  for (int i = 0; FLAGS_rounds < 0 || i < FLAGS_rounds; ++i) {
    butil::Timer timer;
    timer.start();

    uint64_t num = dist(rng);
    pb_rb::RangeBenchRequest request;
    request.set_num(num);

    pb_rb::RangeBenchResponse response;
    brpc::Controller cntl;
    stub.Range(&cntl, &request, &response, nullptr);

    timer.stop();

    if (cntl.Failed()) {
      ++fail;
      LOG(ERROR) << "Round[" << i << "] Call failed: req.num=" << num
                 << " err=" << cntl.ErrorText();
      continue;
    }
    if (response.num() != num + 1) {
      ++fail;
      LOG(ERROR) << "Round[" << i << "] Mismatch: req.num=" << num
                 << " resp.num=" << response.num() << " expected=" << (num + 1);
      continue;
    }

    // Pull head/tail markers out of the attachment (matches rdma_client's
    // verification step on the server-written 4MB region).
    const auto& body = cntl.response_attachment();
    uint64_t head = 0, tail = 0;
    if (body.size() >= 2 * sizeof(uint64_t)) {
      body.copy_to(&head, sizeof(uint64_t), 0);
      body.copy_to(&tail, sizeof(uint64_t), body.size() - sizeof(uint64_t));
    }

    ++success;
    LOG(INFO) << "Round[" << i << "] OK: req.num=" << num
              << " resp.num=" << response.num()
              << " attachment_size=" << body.size() << " head=" << head
              << " tail=" << tail << ", cost "
              << absl::StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6)
              << " seconds.";
  }

  LOG(INFO) << "Done: success=" << success << " fail=" << fail;
  return fail == 0 ? 0 : 1;
}
