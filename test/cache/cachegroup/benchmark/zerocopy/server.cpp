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

// A server to receive EchoRequest and send back EchoResponse.

#include <brpc/server.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <malloc.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "absl/synchronization/blocking_counter.h"
#include "base/filepath/filepath.h"
#include "base/string/string.h"
#include "cache/blockcache/aio.h"
#include "cache/blockcache/cache_store.h"
#include "dingofs/blockcache.pb.h"

DEFINE_string(listen_addr, "", "Server listen address");
DEFINE_int32(listen_port, 9301, "Server listen port");
DEFINE_string(cache_dir, "./", "Block cache root directory");
DEFINE_bool(use_direct, true, "Use direct io to read block");

using ::absl::BlockingCounter;
using ::dingofs::base::filepath::PathJoin;
using ::dingofs::base::string::StrFormat;
using ::dingofs::client::blockcache::AioContext;
using ::dingofs::client::blockcache::AioQueue;
using ::dingofs::client::blockcache::AioQueueImpl;
using ::dingofs::client::blockcache::AioType;
using ::dingofs::client::blockcache::BlockKey;
using ::dingofs::pb::client::blockcache::BlockCacheErrFailure;
using ::dingofs::pb::client::blockcache::BlockCacheOk;

class BlockCacheNode {
 public:
  BlockCacheNode() : aio_queue_(std::make_unique<AioQueueImpl>()) {}

  bool Init() { return aio_queue_->Init(128); }

  bool Range(const BlockKey& block_key, uint64_t /*block_size*/,
             uint64_t offset, uint64_t length, ::butil::IOBuf* buffer) {
    LOG(INFO) << "Range()";
    std::string filename = block_key.Filename();

    int flag = O_RDONLY;
    if (FLAGS_use_direct) {
      flag |= O_DIRECT;
    }
    // int flag = O_RDONLY | O_DIRECT;
    std::string filepath = PathJoin(
        std::vector<std::string>{FLAGS_cache_dir, block_key.Filename()});
    int fd = open(filepath.c_str(), flag);
    if (fd < 0) {
      LOG(ERROR) << "open(" << filepath << ") failed.";
      return false;
    }

    char* data;

    {
      ::butil::Timer timer;
      timer.start();
      // int flags = MAP_PRIVATE | MAP_LOCKED;
      // int flags = MAP_PRIVATE | MAP_NONBLOCK | MAP_POPULATE;
      int flags = MAP_PRIVATE;
      // auto* addr = (char*)::memalign(4096, length);
      data = (char*)mmap(nullptr, length, PROT_READ, flags, fd, offset);
      if (data == MAP_FAILED) {
        LOG(ERROR) << "mmap() failed: " << strerror(errno);
        return false;
      }
      timer.stop();
      LOG(INFO) << "mmap cost "
                << StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6)
                << " seconds.";
    }

    //{
    //  ::butil::Timer timer;
    //  timer.start();
    //  data = (char*)::memalign(4096, length);
    //  // std::memset(data, 0, length);
    //  timer.stop();
    //  LOG(INFO) << "memset cost "
    //            << StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6)
    //            << " seconds.";
    //}

    //{
    //  ::butil::Timer timer;
    //  timer.start();
    //  int n = read(fd, data, length);
    //  LOG_IF(FATAL, n != length) << "read() failed";
    //  timer.stop();
    //  LOG(INFO) << "read cost "
    //            << StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6)
    //            << " seconds.";
    //}

    // close(fd);
    //{
    //  ::butil::Timer timer;
    //  timer.start();
    //  BlockingCounter count(1);
    //  auto* aio = new AioContext(AioType::kRead, fd, offset, length, data,
    //                             [&](AioContext* aio, int /*rc*/) {
    //                               count.DecrementCount();
    //                               delete aio;
    //                             });

    //  aio_queue_->Submit(std::vector<AioContext*>{aio});
    //  count.Wait();
    //  timer.stop();
    //  LOG(INFO) << "read block cost "
    //            << StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6)
    //            << " seconds.";
    //}

    buffer->append_user_data(data, length, [length, fd](void* ptr) {
      // delete[] (char*)(ptr);
      int rc = ::munmap((char*)ptr, length);
      if (rc == 0) {
        LOG(INFO) << "munmap()";
      }

      //::butil::Timer timer;
      // timer.start();
      // rc = ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
      // if (rc == 0) {
      //   LOG(INFO) << "posix_fadvise()";
      // }
      // timer.stop();

      // LOG(INFO) << "posix_fadvise cost "
      //           << StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6)
      //           << " seconds.";
    });
    return true;
  }

 private:
  std::unique_ptr<AioQueueImpl> aio_queue_;
};

class BlockCacheServiceImpl
    : public ::dingofs::pb::client::blockcache::BlockCacheService {
 public:
  BlockCacheServiceImpl(std::shared_ptr<BlockCacheNode> node) : node_(node) {}

  void Range(::google::protobuf::RpcController* cntl_base,
             const ::dingofs::pb::client::blockcache::RangeRequest* request,
             ::dingofs::pb::client::blockcache::RangeResponse* response,
             ::google::protobuf::Closure* done) override {
    ::brpc::ClosureGuard done_guard(done);
    ::brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side() << " to " << cntl->local_side();

    ::butil::IOBuf buffer;
    bool succ = node_->Range(request->block_key(), request->block_size(),
                             request->offset(), request->length(), &buffer);
    if (!succ) {
      response->set_status(BlockCacheErrFailure);
      return;
    }

    ::butil::Timer timer;
    timer.start();
    int start_time = 1;
    response->set_status(BlockCacheOk);
    cntl->response_attachment().append(buffer);
    cntl->set_after_rpc_resp_fn(
        [&timer](::google::protobuf::RpcController* /*cntl_base*/,
                 const ::google::protobuf::Message* /*request*/,
                 const ::google::protobuf::Message* /*response*/) {
          timer.stop();
          LOG(INFO) << "1 block sent, cost "
                    << StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6)
                    << " seconds.";
          return;
        });
  }

 private:
  std::shared_ptr<BlockCacheNode> node_;
};

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  auto node = std::make_shared<BlockCacheNode>();
  if (!node->Init()) {
    LOG(ERROR) << "Init block cache node failed.";
    return -1;
  }

  ::brpc::Server server;
  BlockCacheServiceImpl block_cache_service_impl(node);
  if (server.AddService(&block_cache_service_impl,
                        brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add service";
    return -1;
  }

  butil::EndPoint point;
  if (!FLAGS_listen_addr.empty()) {
    if (butil::str2endpoint(FLAGS_listen_addr.c_str(), &point) < 0) {
      LOG(ERROR) << "Invalid listen address:" << FLAGS_listen_addr;
      return -1;
    }
  } else {
    point = butil::EndPoint(butil::IP_ANY, FLAGS_listen_port);
  }

  // Start the server.
  brpc::ServerOptions options;
  if (server.Start(point, &options) != 0) {
    LOG(ERROR) << "Fail to start block cache server.";
    return -1;
  }

  // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
  server.RunUntilAskedToQuit();
  return 0;
}
