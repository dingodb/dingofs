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

/*
 * Project: DingoFS
 * Created Date: 2026-06-22
 * Author: AI
 */

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_MDS_SERVER_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_MDS_SERVER_H_

#include <cstdint>
#include <string>

#include "common/status.h"
#include "test/integration/cache/deploy/process_guard.h"

namespace dingofs {
namespace cache {
namespace integration {

// Spins up a real dingo-mds process backed by the in-memory ("dummy") storage
// engine -- no TiKV / dingo-store cluster required. The dummy coordinator makes
// the single MDS self-register on its first heartbeat, so MDS discovery and
// cache-group registration work end to end.
class MdsServer {
 public:
  MdsServer() = default;
  ~MdsServer() { Stop(); }

  MdsServer(const MdsServer&) = delete;
  MdsServer& operator=(const MdsServer&) = delete;

  // Renders a minimal dummy-engine config under `workdir`, spawns dingo-mds on
  // a free port, and waits until the RPC port accepts connections.
  Status Start(const std::string& workdir);

  void Stop() { proc_.Stop(); }

  bool Running() const { return proc_.Running(); }

  // Creates a LOCALFILE-backed filesystem (data persisted under `storage_path`)
  // via a direct CreateFs RPC, and returns its generated fs id. A pre-existing
  // filesystem of the same name is reused.
  Status CreateLocalFs(const std::string& fs_name,
                       const std::string& storage_path, uint32_t* fs_id);

  std::string Addr() const { return "127.0.0.1:" + std::to_string(port_); }
  int Port() const { return port_; }

 private:
  ProcessGuard proc_;
  std::string workdir_;
  int port_{0};
};

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_MDS_SERVER_H_
