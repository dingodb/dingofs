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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_OPTION_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_OPTION_H_

#include <cstddef>
#include <cstdint>
#include <string>

namespace dingofs {
namespace cache {
namespace v2 {

// Bulk lanes a connection may open.
inline constexpr uint8_t kMaxBulkLanes = 16;

// One struct for the whole transport.
struct RdmaOption {
  // Serve rdma on this domain's shard; only the server transport reads it.
  bool enabled = false;

  std::string device;  // empty: first device with an active port
  uint8_t port_num = 1;
  int gid_index = -1;  // RoCE only; -1 auto-selects RoCEv2/IPv4

  // Must cover all outstanding WRs; grows via verbs::CompletionQueue::Reserve.
  uint32_t cq_entries = 4096;

  // THE knob: everything the small lane needs derives from it (frame_budget).
  uint16_t max_inflight_rpcs = 128;

  uint32_t recv_buf_bytes = 16 << 10;
  uint32_t max_inline = 192;

  // >1 bulk lane: an RC QP is strictly ordered; one lane stalls small READs.
  uint32_t bulk_send_wr = 256;
  uint8_t bulk_lanes = 2;

  // Connections one shard may hold. Both small-lane rings are carved per
  // connection, so this is what the pool has to be sized for -- a fixed pool
  // silently capped a shard at however many connections happened to fit, and
  // the ones past that failed to hand out a frame.
  uint32_t max_connections = 64;

  // RDMA has no FIN: quiet initiators ping; the server reaps silent conns.
  uint64_t ping_interval_ns = 2ull * 1000 * 1000 * 1000;
  uint64_t idle_timeout_ns = 10ull * 1000 * 1000 * 1000;
  uint64_t sweep_period_ns() const { return idle_timeout_ns / 4; }

  uint8_t total_lanes() const { return static_cast<uint8_t>(1 + bulk_lanes); }

  // Deriving send/recv/frame-pool from one number makes ENOBUFS unreachable.
  uint32_t frame_budget() const {
    return (2u * static_cast<uint32_t>(max_inflight_rpcs)) + 2u;
  }
  uint32_t small_send_wr() const { return frame_budget(); }
  uint32_t small_recv_wr() const { return frame_budget(); }
  uint32_t frame_pool_size() const { return frame_budget(); }

  // Registered buffer pool per shard: 4 MiB superblocks, one MR. It holds a
  // frame pool and a receive ring for every connection, so it derives from
  // the same budget -- sizing it by hand is what made ENOBUFS reachable.
  size_t pool_superblocks() const {
    constexpr size_t kSuperblock = size_t{1} << 22;
    const size_t per_connection =
        2 * size_t{frame_budget()} * size_t{recv_buf_bytes};
    const size_t bytes = per_connection * max_connections;
    return ((bytes + kSuperblock - 1) / kSuperblock) + 1;  // +1 for the rest
  }
};

inline const char* CheckOption(const RdmaOption& option) {
  if (option.bulk_lanes == 0 || option.bulk_lanes > kMaxBulkLanes) {
    return "bulk_lanes must be in [1, kMaxBulkLanes]";
  }
  if (option.max_inflight_rpcs == 0) {
    return "max_inflight_rpcs must be positive";
  }
  if (option.recv_buf_bytes == 0) {
    return "recv_buf_bytes must be positive";
  }
  if (option.ping_interval_ns == 0 ||
      option.idle_timeout_ns < 3 * option.ping_interval_ns) {
    return "idle_timeout_ns must be at least 3x a positive ping_interval_ns";
  }
  return nullptr;
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_OPTION_H_
