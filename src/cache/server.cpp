/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2025-06-08
 * Author: Jingli Chen (Wine93)
 */

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include "cache/cachegroup/cache_group_node_server.h"
#include "cache/utils/logging.h"
#include "cache/utils/offload_thread_pool.h"

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
DECLARE_int32(max_connection_pool_size);
}  // namespace brpc

namespace dingofs {
namespace cache {

static void InitBrpcFlags() {
  brpc::FLAGS_graceful_quit_on_sigterm = true;
  brpc::FLAGS_max_connection_pool_size = 256;
}

static void GlobalInitOrDie() {
  InitLogging("dingo-cache");
  InitBrpcFlags();
  OffloadThreadPool::GetInstance().Start();
}

static Status StartServer() {
  CacheGroupNodeServerImpl server;
  auto status = server.Start();
  if (!status.ok()) {
    return status;
  }

  server.Shutdown();
  return Status::OK();
}

int Run() {
  GlobalInitOrDie();
  return StartServer().ok() ? 0 : -1;
}

}  // namespace cache
}  // namespace dingofs
