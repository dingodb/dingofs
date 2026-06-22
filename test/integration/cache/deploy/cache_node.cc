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

#include "test/integration/cache/deploy/cache_node.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <filesystem>

#include "common/options/cache.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {
namespace integration {

DEFINE_string(dingo_cache_bin, "",
              "path to the dingo-cache binary; empty resolves to the binary "
              "next to the test executable");

static std::string CacheBinary() {
  if (!FLAGS_dingo_cache_bin.empty()) return FLAGS_dingo_cache_bin;
  return ExeDir() + "/../dingo-cache";
}

Status CacheNode::Start(const std::string& workdir, const std::string& mds_addr,
                        const std::string& group, const std::string& member_id,
                        uint64_t cache_size_mb, int fixed_port) {
  id_ = member_id;
  port_ = fixed_port != 0 ? fixed_port : PickFreePort();

  namespace fs = std::filesystem;
  std::error_code ec;
  const std::string cache_dir = workdir + "/cache";
  const std::string log_dir = workdir + "/log";
  fs::create_directories(cache_dir, ec);
  fs::create_directories(log_dir, ec);

  const std::string bin = CacheBinary();
  if (!fs::exists(bin, ec)) {
    return Status::Internal("dingo-cache binary not found: " + bin +
                            " (pass --dingo_cache_bin)");
  }

  std::vector<std::string> argv = {
      bin,
      "--id=" + id_,
      "--mds_addrs=" + mds_addr,
      "--listen_ip=127.0.0.1",
      "--listen_port=" + std::to_string(port_),
      "--group_name=" + group,
      "--group_weight=100",
      "--cache_store=disk",
      "--enable_stage=true",
      "--enable_cache=true",
      "--cache_dir=" + cache_dir,
      "--cache_size_mb=" + std::to_string(cache_size_mb),
      "--use_rdma=false",
      "--public_address=false",
      "--daemonize=false",
      "--log_dir=" + log_dir,
  };
  // Forward the RDMA device the test process detected so the node's slab pool
  // initializes on the same HCA (the on-disk cache requires it).
  if (!FLAGS_cache_rdma_device.empty()) {
    argv.push_back("--cache_rdma_device=" + FLAGS_cache_rdma_device);
    argv.push_back("--cache_rdma_port_num=" +
                   std::to_string(FLAGS_cache_rdma_port_num));
  }

  if (!proc_.Start(argv, workdir + "/cache.out")) {
    return Status::Internal("spawn dingo-cache failed");
  }

  if (!WaitPort("127.0.0.1", port_)) {
    return Status::Internal("dingo-cache did not open port " +
                            std::to_string(port_));
  }
  return Status::OK();
}

bool PingNode(const std::string& ip, int port, int timeout_ms) {
  brpc::Channel channel;
  brpc::ChannelOptions opt;
  opt.timeout_ms = timeout_ms;
  if (channel.Init((ip + ":" + std::to_string(port)).c_str(), &opt) != 0) {
    return false;
  }
  pb::cache::BlockCacheService_Stub stub(&channel);
  pb::cache::PingRequest request;
  pb::cache::PingResponse response;
  brpc::Controller cntl;
  cntl.set_timeout_ms(timeout_ms);
  stub.Ping(&cntl, &request, &response, nullptr);
  return !cntl.Failed();
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
