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

#include "blockcache/common/metrics.h"

#include <bvar/passive_status.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "blockcache/common/mds_client.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/core/runtime/bootstrap.h"
#include "blockcache/core/runtime/smp.h"

namespace dingofs {
namespace blockcache {

namespace {

using Int64Var = bvar::PassiveStatus<int64_t>;
using StringVar = bvar::PassiveStatus<std::string>;
using DiskSlots = std::vector<DiskCacheVars*>;

struct Registry {
  std::vector<LocalCacheVars*> local;                 // [shard]
  std::vector<RemoteCacheVars*> remote;               // [shard]
  std::vector<DiskSlots> disks;                       // [index][shard]
  std::vector<std::unique_ptr<bvar::Variable>> vars;  // last: destroyed first
};

Registry g_registry;

thread_local DiskSlots tls_disk_cache_vars;

template <typename Vars, Counter Vars::* Field>
int64_t Sum(void* arg) {
  int64_t total = 0;
  for (const Vars* vars : *static_cast<std::vector<Vars*>*>(arg)) {
    total += static_cast<int64_t>((vars->*Field).Get());
  }
  return total;
}

template <Counter DiskCacheVars::* Field>
int64_t Any(void* arg) {
  for (const DiskCacheVars* vars : *static_cast<DiskSlots*>(arg)) {
    if ((vars->*Field).Get() != 0) {
      return 1;
    }
  }
  return 0;
}

int64_t Zero(void*) { return 0; }

DiskHealth WorstHealth(const DiskSlots& slots) {
  uint64_t worst = 0;
  for (const DiskCacheVars* vars : slots) {
    worst = std::max(worst, vars->health.Get());
  }
  return static_cast<DiskHealth>(worst);
}

void Running(std::ostream& os, void* arg) {
  const DiskSlots& slots = *static_cast<DiskSlots*>(arg);
  const bool up =
      WorstHealth(slots) == DiskHealth::kNormal &&
      std::all_of(slots.begin(), slots.end(), [](const DiskCacheVars* vars) {
        return vars->running.Get() != 0;
      });
  os << (up ? "up" : "down");
}

void Health(std::ostream& os, void* arg) {
  switch (WorstHealth(*static_cast<DiskSlots*>(arg))) {
    case DiskHealth::kNormal:
      os << "normal";
      return;
    case DiskHealth::kUnstable:
      os << "unstable";
      return;
    case DiskHealth::kDown:
      os << "down";
      return;
  }
}

void Dir(std::ostream& os, void* arg) {
  os << static_cast<DiskSlots*>(arg)->front()->dir;
}

void Uuid(std::ostream& os, void* arg) {
  os << static_cast<DiskSlots*>(arg)->front()->uuid;
}

void Expose(const std::string& name, int64_t (*getter)(void*), void* arg) {
  g_registry.vars.push_back(std::make_unique<Int64Var>(name, getter, arg));
}

void Expose(const std::string& name, void (*print)(std::ostream&, void*),
            void* arg) {
  g_registry.vars.push_back(std::make_unique<StringVar>(name, print, arg));
}

void ExposeDisk(DiskSlots* slots) {
  const std::string prefix =
      "dingofs_disk_cache_" + std::to_string(slots->front()->index);
  Expose(prefix + "_capacity",
         &Sum<DiskCacheVars, &DiskCacheVars::capacity_bytes>, slots);
  Expose(prefix + "_used_bytes",
         &Sum<DiskCacheVars, &DiskCacheVars::used_bytes>, slots);
  Expose(prefix + "_cache_blocks",
         &Sum<DiskCacheVars, &DiskCacheVars::cached_blocks>, slots);
  Expose(prefix + "_stage_blocks",
         &Sum<DiskCacheVars, &DiskCacheVars::staged_blocks>, slots);
  Expose(prefix + "_cache_hits", &Sum<DiskCacheVars, &DiskCacheVars::hits>,
         slots);
  Expose(prefix + "_cache_misses", &Sum<DiskCacheVars, &DiskCacheVars::misses>,
         slots);
  Expose(prefix + "_stage_skips", &Zero, slots);
  Expose(prefix + "_stage_full", &Any<&DiskCacheVars::stage_full>, slots);
  Expose(prefix + "_cache_full", &Any<&DiskCacheVars::cache_full>, slots);
  Expose(prefix + "_running_status", &Running, slots);
  Expose(prefix + "_healthy_status", &Health, slots);
  Expose(prefix + "_dir", &Dir, slots);
  Expose(prefix + "_uuid", &Uuid, slots);
}

DiskStats ToStats(const DiskCacheVars& vars) {
  return DiskStats{.index = vars.index,
                   .uuid = vars.uuid,
                   .dir = vars.dir,
                   .capacity_bytes = vars.capacity_bytes.Get(),
                   .used_bytes = vars.used_bytes.Get(),
                   .cached_blocks = vars.cached_blocks.Get(),
                   .staged_blocks = vars.staged_blocks.Get(),
                   .hits = vars.hits.Get(),
                   .misses = vars.misses.Get(),
                   .health = static_cast<DiskHealth>(vars.health.Get()),
                   .stage_full = vars.stage_full.Get() != 0,
                   .cache_full = vars.cache_full.Get() != 0,
                   .running = vars.running.Get() != 0};
}

}  // namespace

void RegisterDiskCacheVars(DiskCacheVars* vars) {
  DCHECK(HasReactor()) << "RegisterDiskCacheVars off a shard thread";
  tls_disk_cache_vars.push_back(vars);
}

void UnregisterDiskCacheVars(DiskCacheVars* vars) {
  DCHECK(HasReactor()) << "UnregisterDiskCacheVars off a shard thread";
  std::erase(tls_disk_cache_vars, vars);
}

void ExposeMetrics() {
  CHECK(!HasReactor()) << "ExposeMetrics on a shard thread";
  Registry& r = g_registry;
  CHECK(r.vars.empty()) << "metrics exposed twice";

  // Each shard writes its own slot; the latch inside RunOnAllAndWait publishes
  // them to this thread.
  const unsigned shards = ShardCount();
  r.local.assign(shards, nullptr);
  r.remote.assign(shards, nullptr);
  std::vector<DiskSlots> per_shard(shards);
  RunOnAllAndWait([&](unsigned shard) -> Future<> {
    r.local[shard] = &tls_local_cache_vars;
    r.remote[shard] = &tls_remote_cache_vars;
    per_shard[shard] = tls_disk_cache_vars;
    return MakeReadyFuture<>();
  });
  for (const DiskSlots& slots : per_shard) {
    for (DiskCacheVars* vars : slots) {
      if (vars->index >= r.disks.size()) {
        r.disks.resize(vars->index + 1);
      }
      r.disks[vars->index].push_back(vars);
    }
  }

  Expose("dingofs_disk_cache_group_load_total_bytes",
         &Sum<LocalCacheVars, &LocalCacheVars::load_bytes>, &r.local);
  Expose("dingofs_disk_cache_group_stage_total_bytes",
         &Sum<LocalCacheVars, &LocalCacheVars::stage_bytes>, &r.local);
  Expose("dingofs_disk_cache_group_cache_total_bytes",
         &Sum<LocalCacheVars, &LocalCacheVars::cache_bytes>, &r.local);
  // Older `dingo fs stats` builds read the remote tier as `remote_node_group`.
  for (const std::string prefix :
       {"dingofs_remote_cache_cluster_", "dingofs_remote_node_group_"}) {
    Expose(prefix + "range_total_bytes",
           &Sum<RemoteCacheVars, &RemoteCacheVars::range_bytes>, &r.remote);
    Expose(prefix + "put_total_bytes",
           &Sum<RemoteCacheVars, &RemoteCacheVars::put_bytes>, &r.remote);
    Expose(prefix + "cache_total_bytes",
           &Sum<RemoteCacheVars, &RemoteCacheVars::cache_bytes>, &r.remote);
  }
  Expose("dingofs_remote_cache_hit_count",
         &Sum<RemoteCacheVars, &RemoteCacheVars::hits>, &r.remote);
  Expose("dingofs_remote_cache_miss_count",
         &Sum<RemoteCacheVars, &RemoteCacheVars::misses>, &r.remote);
  for (DiskSlots& slots : r.disks) {
    if (!slots.empty()) {
      ExposeDisk(&slots);
    }
  }
}

void HideMetrics() {
  CHECK(!HasReactor()) << "HideMetrics on a shard thread";
  Registry& r = g_registry;
  r.vars.clear();
  r.local.clear();
  r.remote.clear();
  r.disks.clear();
}

std::vector<DiskStats> SnapshotDisks() {
  CHECK(!HasReactor()) << "SnapshotDisks on a shard thread";
  if (!ProcessRuntimeStarted()) {
    return {};
  }

  std::vector<CacheStats> parts(ShardCount());
  RunOnAllAndWait([&parts](unsigned shard) -> Future<> {
    for (const DiskCacheVars* vars : tls_disk_cache_vars) {
      parts[shard].disks.push_back(ToStats(*vars));
    }
    return MakeReadyFuture<>();
  });

  CacheStats all;
  for (const CacheStats& part : parts) {
    all.Merge(part);
  }
  return std::move(all.disks);
}

Members SnapshotMembers() {
  CHECK(!HasReactor()) << "SnapshotMembers on a shard thread";
  if (!ProcessRuntimeStarted()) {
    return {};
  }

  return RunOnAndWait(0, []() -> Future<Members> {
    const Members* members = ThisRemoteCacheVars().members;
    co_return members != nullptr ? *members : Members{};
  });
}

}  // namespace blockcache
}  // namespace dingofs
