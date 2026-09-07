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

#include "blockcache/common/tombstone.h"

#include <brpc/reloadable_flags.h>
#include <glog/logging.h>

#include <algorithm>
#include <vector>

namespace dingofs {
namespace blockcache {

struct LegacyFlag {
  const char* name;
  const char* note;
};

static std::vector<LegacyFlag>& Registry() {
  static std::vector<LegacyFlag> flags;
  return flags;
}

static bool RegisterLegacyFlag(const char* name, const char* note) {
  Registry().push_back(LegacyFlag{.name = name, .note = note});
  return true;
}

bool IsLegacyFlag(const gflags::CommandLineFlagInfo& flag) {
  return std::ranges::any_of(Registry(), [&](const LegacyFlag& legacy) {
    return flag.name == legacy.name;
  });
}

void LogLegacyFlagsInUse() {
  for (const LegacyFlag& flag : Registry()) {
    gflags::CommandLineFlagInfo info;
    if (!gflags::GetCommandLineFlagInfo(flag.name, &info) || info.is_default) {
      continue;
    }
    LOG(WARNING) << "Ignoring legacy flag --" << flag.name << "="
                 << info.current_value << ": " << flag.note;
  }
}

#define LEGACY_FLAG(type, name, def, note)                 \
  DEFINE_##type(name, def, "legacy flag, ignored: " note); \
  static const bool kLegacyFlagRegistered_##name =         \
      RegisterLegacyFlag(#name, note)

// Shared.
LEGACY_FLAG(int32, connections, 16,
            "the brpc connection pool is gone; connections follow the shards");
LEGACY_FLAG(bool, brpc_log_idle_connection_close, true, "removed");
LEGACY_FLAG(uint64, storage_upload_thread_pool_size, 4,
            "uploads run on the shard reactors, there is no thread pool");

// Client side.
LEGACY_FLAG(uint32, prefetch_max_inflights, 16,
            "prefetch concurrency follows --queue_depth and --shards");
// Per-operation rpc timeouts collapsed into --cache_rpc_timeout_ms.
LEGACY_FLAG(uint32, cache_put_rpc_timeout_ms, 30000,
            "use --cache_rpc_timeout_ms");
LEGACY_FLAG(uint32, cache_range_rpc_timeout_ms, 30000,
            "use --cache_rpc_timeout_ms");
LEGACY_FLAG(uint32, cache_prefetch_rpc_timeout_ms, 3000,
            "use --cache_rpc_timeout_ms");
LEGACY_FLAG(uint32, cache_delete_rpc_timeout_ms, 3000,
            "use --cache_rpc_timeout_ms");
LEGACY_FLAG(uint32, cache_rpc_max_timeout_ms, 60000, "removed");
// Active node pinging and its state machine were replaced by the breaker
// (--remote_breaker_failures / --remote_breaker_open_ms /
// --remote_breaker_max_open_ms).
LEGACY_FLAG(uint32, cache_ping_rpc_timeout_ms, 1000,
            "no active ping, see --remote_breaker_*");
LEGACY_FLAG(uint32, cache_node_state_check_duration_ms, 3000,
            "no node state machine, see --remote_breaker_*");
LEGACY_FLAG(uint32, cache_node_state_tick_duration_s, 30,
            "no node state machine, see --remote_breaker_*");
LEGACY_FLAG(uint32, cache_node_state_unstable2normal_succ_num, 3,
            "no node state machine, see --remote_breaker_*");

// Node side.
LEGACY_FLAG(bool, retrieve_storage_lock, true,
            "storage retrieval is always single-flight");
LEGACY_FLAG(uint32, retrieve_storage_lock_timeout_ms, 10000, "removed");
// RDMA geometry is sized by --rdma_message_bytes / --rdma_bulk_send_wr /
// --buffer_pool_mb now; the old byte counts do not map onto them.
LEGACY_FLAG(int32, rdma_send_buffer_size, 4096, "see --rdma_message_bytes");
LEGACY_FLAG(int32, rdma_send_queue_size, 4096, "see --rdma_bulk_send_wr");
LEGACY_FLAG(int32, rdma_recv_buffer_size, 4096, "see --buffer_pool_mb");
LEGACY_FLAG(int32, rdma_event_dispatcher_num, 1, "one poller per shard");
LEGACY_FLAG(int32, rdma_rpc_timeout_ms, 3000, "removed on the node");
LEGACY_FLAG(uint32, rdma_client_signal_request_send_every, 1024, "removed");
LEGACY_FLAG(uint32, rdma_server_keepalive_interval_s, 10,
            "see --rdma_heartbeat_interval_s / --rdma_idle_timeout_s");
#undef LEGACY_FLAG

// Not legacy: the node's brpc server reads it. It is defined here rather than
// in brpc_server.cc because the client never links that object (it has no
// blockcache brpc server) yet old client configurations still carry the flag.
DEFINE_int32(brpc_idle_timeout_second, -1,
             "seconds an idle connection is kept; -1 never reaps");
DEFINE_validator(brpc_idle_timeout_second, brpc::PassValidate);

}  // namespace blockcache
}  // namespace dingofs
