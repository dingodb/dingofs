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

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_RDMA_ENV_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_RDMA_ENV_H_

#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "cache/infiniband/slab_pool.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {
namespace integration {

// Whether the process-wide RDMA slab pool was successfully initialized.
//
// The whole integration suite needs RDMA: the on-disk cache (in this test
// process AND in the spawned dingo-cache) allocates its O_DIRECT/io_uring
// buffers from the global RDMA slab pool, which only exists after
// InitializeGlobalSlabPool() succeeds on a real, ACTIVE InfiniBand/RoCE device.
inline bool& SlabPoolReady() {
  static bool ready = false;
  return ready;
}

// Scan sysfs for an RDMA device whose port is ACTIVE. Prefer an InfiniBand
// link-layer port, otherwise fall back to the first active port of any link
// layer. Returns "" when none is found.
inline std::string DetectActiveRdmaDevice(uint32_t* port_out) {
  namespace fs = std::filesystem;
  const std::string base = "/sys/class/infiniband";
  std::error_code ec;
  if (!fs::exists(base, ec)) {
    return "";
  }

  auto read_small = [](const std::string& path) -> std::string {
    std::ifstream in(path);
    std::stringstream ss;
    ss << in.rdbuf();
    auto s = ss.str();
    while (!s.empty() && (s.back() == '\n' || s.back() == ' ')) s.pop_back();
    return s;
  };

  std::string fallback_dev;
  uint32_t fallback_port = 0;
  for (const auto& dev : fs::directory_iterator(base, ec)) {
    const auto ports_dir = dev.path() / "ports";
    if (!fs::exists(ports_dir, ec)) {
      continue;
    }
    for (const auto& port : fs::directory_iterator(ports_dir, ec)) {
      const auto state = read_small((port.path() / "state").string());
      if (state.find("ACTIVE") == std::string::npos) {
        continue;
      }
      const auto link = read_small((port.path() / "link_layer").string());
      const auto dev_name = dev.path().filename().string();
      const uint32_t port_num =
          static_cast<uint32_t>(std::stoul(port.path().filename().string()));
      if (link == "InfiniBand") {
        *port_out = port_num;
        return dev_name;
      }
      if (fallback_dev.empty()) {
        fallback_dev = dev_name;
        fallback_port = port_num;
      }
    }
  }

  if (!fallback_dev.empty()) {
    *port_out = fallback_port;
  }
  return fallback_dev;
}

// Detect a device, point the cache RDMA flags at it (so both this process and
// the spawned dingo-cache use the same HCA), and initialize the global slab
// pool. Returns false (a safe no-op) when no usable device exists -- the whole
// integration suite then GTEST_SKIPs. Call once after ParseCommandLineFlags.
inline bool InitSlabPoolForTests() {
  if (FLAGS_cache_rdma_device.empty()) {
    uint32_t port = 1;
    auto dev = DetectActiveRdmaDevice(&port);
    if (dev.empty()) {
      return false;
    }
    FLAGS_cache_rdma_device = dev;
    FLAGS_cache_rdma_port_num = port;
  }

  // The slab pool pins kSlabBufferSize(4MiB) * iodepth * 2 of registered
  // memory. Keep the test footprint modest (default iodepth is 128 -> ~1GiB).
  if (FLAGS_iodepth > 16) {
    FLAGS_iodepth = 16;
  }

  SlabPoolReady() = infiniband::InitializeGlobalSlabPool().ok();
  return SlabPoolReady();
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_RDMA_ENV_H_
