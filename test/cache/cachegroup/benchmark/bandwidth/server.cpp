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
#include <bthread/execution_queue.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <malloc.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "base/filepath/filepath.h"
#include "cache/blockcache/cache_store.h"
#include "dingofs/blockcache.pb.h"

DEFINE_string(listen_addr, "", "Server listen address");
DEFINE_int32(listen_port, 9301, "Server listen port");
DEFINE_string(cache_dir, "./", "Block cache root directory");

using ::dingofs::base::filepath::PathJoin;
using ::dingofs::base::string::StrFormat;
using ::dingofs::client::blockcache::BlockKey;
using ::dingofs::pb::client::blockcache::BlockCacheErrFailure;
using ::dingofs::pb::client::blockcache::BlockCacheOk;

class BlockCacheNode {
 public:
  BlockCacheNode() : drop_page_cache_queue_id_({0}) {}

  bool Init() {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
    queue_options.use_pthread = true;
    int rc = bthread::execution_queue_start(
        &drop_page_cache_queue_id_, &queue_options, DropPageCache, this);
    if (rc != 0) {
      LOG(ERROR) << "execution_queue_start() failed, rc=" << rc;
    }
    return rc == 0;
  }

  static int DropPageCache(void* meta, bthread::TaskIterator<int>& iter) {
    BlockCacheNode* node = static_cast<BlockCacheNode*>(meta);
    for (; iter; iter++) {
      int fd = *iter;
      int rc = ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
      if (rc != 0) {
        LOG(ERROR) << "posix_fadvise(" << fd << ") failed: " << strerror(errno);
      }
    }
    return 0;
  }

  bool Range(const BlockKey& block_key, uint64_t /*block_size*/,
             uint64_t offset, uint64_t length, ::butil::IOBuf* buffer) {
    auto subpath =
        std::vector<std::string>{FLAGS_cache_dir, block_key.Filename()};
    std::string filepath = PathJoin(subpath);
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
      LOG(ERROR) << "open(" << filepath << ") failed: " << strerror(errno);
      return false;
    }

    char* data = static_cast<char*>(
        mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset));
    if (data == MAP_FAILED) {
      LOG(ERROR) << "mmap() failed: " << strerror(errno);
      return false;
    }

    buffer->append_user_data(data, length, [this, fd, length](void* ptr) {
      int rc = ::munmap(static_cast<char*>(ptr), length);
      if (rc != 0) {
        LOG(ERROR) << "munmap() failed: " << strerror(errno);
      }

      CHECK_EQ(0,
               bthread::execution_queue_execute(drop_page_cache_queue_id_, fd));
    });
    return true;
  }

 private:
  bthread::ExecutionQueueId<int> drop_page_cache_queue_id_;
};

class BlockCacheServiceImpl
    : public ::dingofs::pb::client::blockcache::BlockCacheService {
 public:
  BlockCacheServiceImpl(std::shared_ptr<BlockCacheNode> node) : node_(node) {}

  void Range(::google::protobuf::RpcController* cntl_base,
             const ::dingofs::pb::client::blockcache::RangeRequest* request,
             ::dingofs::pb::client::blockcache::RangeResponse* response,
             ::google::protobuf::Closure* done) override {
    butil::Timer timer;
    timer.start();

    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    butil::IOBuf buffer;
    bool succ = node_->Range(request->block_key(), request->block_size(),
                             request->offset(), request->length(), &buffer);
    if (!succ) {
      response->set_status(BlockCacheErrFailure);
      return;
    }

    response->set_status(BlockCacheOk);
    cntl->response_attachment().append(buffer);

    std::string filename = BlockKey(request->block_key()).Filename();
    cntl->set_after_rpc_resp_fn(
        [&timer, filename](::google::protobuf::RpcController* /*cntl_base*/,
                           const ::google::protobuf::Message* /*request*/,
                           const ::google::protobuf::Message* /*response*/) {
          timer.stop();
          LOG(INFO) << "range(" << filename << "): OK <"
                    << StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6) << ">";
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
