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

#ifndef DINGOFS_BLOCKCACHE_NET_BRPC_BRPC_SERVER_H_
#define DINGOFS_BLOCKCACHE_NET_BRPC_BRPC_SERVER_H_

#include <google/protobuf/service.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"

namespace brpc {
class Server;
}

namespace dingofs {
namespace blockcache {

inline void DrainInflight(const std::atomic<int64_t>& inflight) {
  while (inflight.load(std::memory_order_acquire) > 0) {
    std::this_thread::yield();
  }
}

class BrpcServer {
 public:
  struct Option {
    std::string listen_ip = "0.0.0.0";
    uint16_t listen_port = 0;
  };

  explicit BrpcServer(Option option);
  ~BrpcServer();
  BrpcServer(const BrpcServer&) = delete;
  BrpcServer& operator=(const BrpcServer&) = delete;

  void AddService(std::unique_ptr<google::protobuf::Service> service);

  Status Start();
  void Shutdown();

  void CallStarted() { inflight_.fetch_add(1, std::memory_order_relaxed); }
  void CallFinished() { inflight_.fetch_sub(1, std::memory_order_release); }

  static bool reply_on_bthread();

 private:
  Option option_;

  std::unique_ptr<::brpc::Server> server_;
  std::vector<std::unique_ptr<google::protobuf::Service>> services_;
  bool started_ = false;
  std::atomic<int64_t> inflight_{0};
};

}  // namespace blockcache
}  // namespace dingofs

#endif
