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
// brpc/TCP counterpart of rdma_server.cc — used for end-to-end latency
// comparison against the RDMA path. Handles RangeBench.Range: returns
// num+1 plus a configurable-size response attachment whose first and last
// 8 bytes are set to `out`, mirroring the RDMA example's WRITE payload.
//
// The attachment is shipped via brpc's response_attachment (IOBuf side
// channel), which is the closest fair comparison to RDMA's "small SEND
// control + bulk WRITE" pattern: no protobuf serialization overhead for
// the bulk bytes, only the small RangeBenchResponse is protobuf-encoded.
//

// Must match the define used to build libbrpc.a (we rebuilt eureka brpc
// with -DWITH_RDMA=ON, which sets BRPC_WITH_RDMA=1). Without this, the
// rdma_helper.h API surface (RegisterMemoryForRdma, etc.) is hidden behind
// an #if BRPC_WITH_RDMA guard.
#define BRPC_WITH_RDMA 1

#include <brpc/controller.h>
#include <brpc/rdma/rdma_helper.h>
#include <brpc/server.h>
#include <butil/iobuf.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstdint>
#include <mutex>

#include "dingofs/range_bench.pb.h"

DEFINE_int32(port, 8889, "Server TCP port");
DEFINE_int32(attachment_bytes, 4194304,
             "Response attachment size in bytes (default: 4MB)");
DEFINE_bool(use_rdma, false,
            "If true, brpc switches to its built-in RDMA backend (still "
            "negotiates over TCP on `--port`, upgrades to RDMA after the "
            "HelloMessage handshake). Use this to compare brpc-over-TCP vs "
            "brpc-over-RDMA without changing any other code.");

namespace pb_rb = dingofs::pb::range_bench;

class RangeBenchServiceImpl : public pb_rb::RangeBenchService {
 public:
  // Full zero-copy on the send side:
  //   --use_rdma=true  : pre-register one big buffer with the RDMA PD, hand
  //                      every response off via
  //                      append_user_data_with_meta(lkey). No memcpy from user
  //                      buf → block_pool.
  //   --use_rdma=false : same single buffer, append_user_data (TCP doesn't
  //                      need registration; brpc only keeps a reference).
  // Safe to share one buffer because the client is strictly serial in this
  // benchmark — the next request can't arrive until the previous response's
  // HCA send has completed and brpc has called the deleter. The mutex is
  // defensive against rare parallelism (e.g. brpc internal retry).
  RangeBenchServiceImpl(size_t bytes, bool use_rdma)
      : bytes_(bytes), use_rdma_(use_rdma) {
    buf_ = new char[bytes_];
    std::memset(buf_, 0, bytes_);
    if (use_rdma_) {
      lkey_ = brpc::rdma::RegisterMemoryForRdma(buf_, bytes_);
      CHECK_NE(lkey_, 0u) << "RegisterMemoryForRdma failed";
      LOG(INFO) << "Pre-registered server attachment buffer: addr=" << buf_
                << " size=" << bytes_ << " lkey=" << lkey_;
    }
  }

  ~RangeBenchServiceImpl() override {
    if (use_rdma_) {
      brpc::rdma::DeregisterMemoryForRdma(buf_);
    }
    delete[] buf_;
  }

  void Range(google::protobuf::RpcController* controller,
             const pb_rb::RangeBenchRequest* request,
             pb_rb::RangeBenchResponse* response,
             google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
    auto* cntl = static_cast<brpc::Controller*>(controller);

    uint64_t in = request->num();
    uint64_t out = in + 1;
    response->set_num(out);

    std::lock_guard<std::mutex> lk(mu_);
    *reinterpret_cast<uint64_t*>(buf_) = out;
    *reinterpret_cast<uint64_t*>(buf_ + bytes_ - sizeof(uint64_t)) = out;
    if (use_rdma_) {
      cntl->response_attachment().append_user_data_with_meta(
          buf_, bytes_, [](void*) {}, lkey_);
    } else {
      cntl->response_attachment().append_user_data(buf_, bytes_, [](void*) {});
    }
  }

 private:
  size_t bytes_;
  bool use_rdma_;
  char* buf_{nullptr};
  uint32_t lkey_{0};
  std::mutex mu_;
};

int main(int argc, char** argv) {
  // Same perf-friendly logging defaults as rdma_server.cc.
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;
  FLAGS_stderrthreshold = google::WARNING;
  FLAGS_logbuflevel = google::INFO;
  FLAGS_logbufsecs = 1;
  FLAGS_max_log_size = 256;

  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // GlobalRdmaInitializeOrDie must run before RegisterMemoryForRdma; doing it
  // explicitly (instead of relying on Server::Start to trigger it) lets the
  // service register its pre-allocated send buffer in the constructor.
  if (FLAGS_use_rdma) {
    brpc::rdma::GlobalRdmaInitializeOrDie();
  }

  brpc::Server server;
  RangeBenchServiceImpl service(FLAGS_attachment_bytes, FLAGS_use_rdma);
  if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add RangeBenchService";
    return 1;
  }

  brpc::ServerOptions options;
  options.use_rdma = FLAGS_use_rdma;
  if (server.Start(FLAGS_port, &options) != 0) {
    LOG(ERROR) << "Fail to start brpc server on port=" << FLAGS_port
               << " use_rdma=" << FLAGS_use_rdma;
    return 1;
  }

  LOG(INFO) << "brpc server up: port=" << FLAGS_port
            << " attachment_bytes=" << FLAGS_attachment_bytes
            << " use_rdma=" << FLAGS_use_rdma
            << " (waiting for incoming RPCs...)";

  server.RunUntilAskedToQuit();
  return 0;
}
