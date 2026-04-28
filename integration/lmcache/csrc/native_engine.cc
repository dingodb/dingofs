// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "native_engine.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "cache/blockcache/block_cache.h"
#include "cache/remotecache/rdma_buffer_manager.h"
#include "cache/remotecache/remote_block_cache.h"
#include "common/block/block_handle.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace integration {
namespace lmcache {

namespace {

// One engine per process: gflags storage is global, and so is the brpc /
// bthread / glog runtime. Subsequent constructions would silently corrupt
// the first engine's configuration.
std::atomic<bool> g_engine_alive{false};

std::once_flag g_glog_once;

void InitGlogOnce() {
  std::call_once(g_glog_once, [] {
    google::InitGoogleLogging("dingofs_lmcache_connector");
  });
}

// Translate a dingofs Status into a (ok, err) pair.
//   - ok / IsExist (key already cached or in-flight by another writer) →
//   ok=true
//   - NotFound: normal cache-miss outcome → ok=false, err=""
//   - anything else: ok=false, err=<status string> (surfaces as RuntimeError)
struct StatusReport {
  bool ok;
  std::string err;
};

StatusReport ReportFromStatus(const Status& s) {
  if (s.ok() || s.IsExist()) return {true, {}};
  if (s.IsNotFound()) return {false, {}};
  return {false, s.ToString()};
}

}  // namespace

// ---------- NativeEngine ----------

NativeEngine::NativeEngine() = default;

NativeEngine::~NativeEngine() { Shutdown(); }

std::unique_ptr<NativeEngine> NativeEngine::Create(const InitOptions& opts) {
  bool expected = false;
  if (!g_engine_alive.compare_exchange_strong(expected, true)) {
    throw std::runtime_error(
        "dingofs lmcache NativeEngine: only one instance allowed per process "
        "(gflags are process-global)");
  }

  if (opts.mds_addrs.empty()) {
    g_engine_alive.store(false);
    throw std::invalid_argument("mds_addrs is required");
  }
  if (opts.cache_group.empty()) {
    g_engine_alive.store(false);
    throw std::invalid_argument("cache_group is required");
  }

  std::unique_ptr<NativeEngine> engine(new NativeEngine());
  engine->rdma_pools_ = opts.rdma_pools;
  try {
    engine->InstallFlags(opts);
    InitGlogOnce();
    engine->StartCache();
    engine->running_.store(true, std::memory_order_release);
    engine->RegisterRdmaPools();
  } catch (...) {
    engine->Shutdown();
    g_engine_alive.store(false);
    throw;
  }

  return engine;
}

void NativeEngine::InstallFlags(const InitOptions& opts) {
  // LMCache hands us buffers from a pinned CPU arena. When that arena is
  // present, prefer the custom cache RDMA transport by default; an explicit
  // conf file can still turn it off for diagnostics with --use_rdma=false.
  dingofs::cache::FLAGS_use_rdma = !opts.rdma_pools.empty();

  // 1) Conf file (if any) goes first: same parser dingofs's own binaries use
  //    (src/common/flag.cc:98). errors_are_fatal=false so a typo'd flag or a
  //    missing file surfaces as a C++ exception instead of calling exit(1).
  //    That kept-process behavior is essential for testability.
  if (!opts.conf_file.empty()) {
    bool ok =
        ::gflags::ReadFromFlagsFile(opts.conf_file, "dingofs-lmcache-connector",
                                    /*errors_are_fatal=*/false);
    if (!ok) {
      throw std::runtime_error(
          "Failed to load dingofs gflags from '" + opts.conf_file +
          "' (unreadable file, syntax error, or unknown flag inside)");
    }
  }
  // 2) URL fields are authoritative — apply last so they win over the conf
  //    file. FLAGS_mds_addrs / FLAGS_cache_group live in dingofs::cache;
  //    direct assignment matches src/client/main.cc:174.
  dingofs::cache::FLAGS_mds_addrs = opts.mds_addrs;
  dingofs::cache::FLAGS_cache_group = opts.cache_group;
}

void NativeEngine::StartCache() {
  // storage_client is unused on the cache-group path (see
  // RemoteBlockCacheImpl ctor at
  // src/cache/remotecache/remote_block_cache.cc:55).
  block_cache_ =
      std::make_unique<dingofs::cache::RemoteBlockCacheImpl>(nullptr);
  Status s = block_cache_->Start();
  if (!s.ok()) {
    block_cache_.reset();
    throw std::runtime_error("RemoteBlockCache::Start() failed: " +
                             s.ToString());
  }
}

void NativeEngine::RegisterRdmaPools() {
  if (rdma_pools_.empty()) {
    LOG(INFO) << "DingoFS LMCache RDMA: no CPU memory pool provided; "
                 "RDMA data path uses the cache client's fallback pool";
    return;
  }

  rdma_registry_ = RdmaMemoryRegistry::Create(rdma_pools_);
}

void NativeEngine::Shutdown() {
  bool was_running = running_.exchange(false, std::memory_order_acq_rel);
  if (!was_running) {
    rdma_registry_.reset();
    return;
  }

  if (block_cache_) {
    block_cache_->Shutdown();
    block_cache_.reset();
  }
  rdma_registry_.reset();
  g_engine_alive.store(false);
}

const std::vector<RegisteredMemoryRegion>&
NativeEngine::registered_rdma_regions() const {
  static const std::vector<RegisteredMemoryRegion> empty;
  if (rdma_registry_ == nullptr) {
    return empty;
  }
  return rdma_registry_->registered_regions();
}

std::string NativeEngine::rdma_device_name() const {
  if (rdma_registry_ == nullptr) {
    return {};
  }
  return rdma_registry_->device_name();
}

// ---------- sync ops ----------

bool NativeEngine::ExistsSync(const TensorKey& key, std::string* err) {
  if (!running_.load(std::memory_order_acquire)) {
    if (err) *err = "engine shut down";
    return false;
  }
  IOBuffer tmp;
  // retrieve_storage=false: ask the cache node to return NotFound rather
  // than falling through to the underlying object store. ~1 byte over wire.
  cache::RangeOption opt;
  opt.retrieve_storage = false;
  Status s = block_cache_->Range(BlockHandle(key), 0, 1, &tmp, opt);
  if (s.ok()) return true;
  if (s.IsNotFound()) return false;
  if (err) *err = s.ToString();
  return false;
}

bool NativeEngine::Ping(std::string* err) {
  // Connectivity is established at Create() time: RemoteBlockCacheImpl::Start
  // talks to the MDS and populates the cache-group member list before
  // returning. A live `running_` flag therefore implies the cluster is
  // reachable; we deliberately avoid issuing a synthetic Range here because
  // dingo-cache Range falls through to storage on miss, which would fail on
  // an empty sentinel key for reasons unrelated to liveness.
  if (!running_.load(std::memory_order_acquire)) {
    if (err) *err = "engine shut down";
    return false;
  }
  return true;
}

// ---------- async ops ----------

uint64_t NativeEngine::SubmitBatchSet(std::vector<SetItem> items) {
  if (!running_.load(std::memory_order_acquire)) {
    throw std::runtime_error("engine shut down");
  }
  if (items.empty()) return queue_.PushEmpty(OpType::kSet);

  FanIn* fan = queue_.NewFanIn(OpType::kSet, items.size());
  uint64_t fid = fan->future_id();

  // Only stamp the rkey when the cache will actually take its RDMA path
  // (same condition it uses to pick the transport); otherwise the meta is
  // dead weight and the TCP path copies as usual.
  const bool rdma_on = cache::RdmaBufferManager::GetInstance().Enabled();

  for (size_t i = 0; i < items.size(); ++i) {
    auto& it = items[i];
    // Zero-copy handoff from Python into IOBuffer. When this memory lives in a
    // registered LMCache pool we tag the block with its rkey, so EnsureRegistered
    // keeps it as-is and the server RDMA_READs the arena directly; otherwise the
    // cache layer stages it into its registered client pool (single copy).
    uint32_t rkey = (rdma_on && rdma_registry_ != nullptr)
                        ? rdma_registry_->RkeyFor(it.data, it.size)
                        : 0;
    IOBuffer io;
    if (rkey != 0) {
      io.AppendUserDataWithMeta(const_cast<void*>(it.data), it.size,
                                [](void*) {}, rkey);
    } else {
      io.AppendUserData(const_cast<void*>(it.data), it.size, [](void*) {});
    }

    // AsyncCache, not AsyncPut: LMCache chunks are computed in-process and
    // have no storage-backend origin. AsyncPut would trigger server-side
    // writeback to S3/OSS, which is both wasted I/O and would fail when
    // fs_id is unset (LMCache TensorKey leaves fs_id=0). AsyncCache only
    // populates the cache-group node's local cache, which is exactly what
    // we want.
    block_cache_->AsyncCache(
        BlockHandle(it.key), std::move(io),
        [fan, i](Status s) {
          auto r = ReportFromStatus(s);
          fan->Report(i, r.ok, std::move(r.err));
        },
        cache::CacheOption{});
  }
  return fid;
}

uint64_t NativeEngine::SubmitBatchGet(std::vector<GetItem> items) {
  if (!running_.load(std::memory_order_acquire)) {
    throw std::runtime_error("engine shut down");
  }
  if (items.empty()) return queue_.PushEmpty(OpType::kGet);

  FanIn* fan = queue_.NewFanIn(OpType::kGet, items.size());
  uint64_t fid = fan->future_id();

  // One shared response-buffer vector for the whole batch — 1 heap alloc
  // instead of N. Each callback captures the shared_ptr by value; the vector
  // dies when the last callback runs.
  auto response_bufs = std::make_shared<std::vector<IOBuffer>>(items.size());
  auto direct_rdma_get =
      std::make_shared<std::vector<uint8_t>>(items.size(), 0);

  // retrieve_storage=false: cache-miss returns NotFound directly instead of
  // falling through to object storage. LMCache writes via AsyncCache only
  // (no storage origin), so a storage fallback can only fail with
  // InvalidParam (fs_id=0) — translating that to a clean miss lets the
  // caller treat it as a normal cache miss instead of retrying RuntimeError.
  cache::RangeOption opt;
  opt.retrieve_storage = false;

  // Only advertise the arena destination for the server to RDMA-write into
  // when the cache will actually take its RDMA path (same condition it uses to
  // pick the transport); otherwise direct_rdma_get must stay 0 so the callback
  // copies the brpc response into dst.
  const bool rdma_on = cache::RdmaBufferManager::GetInstance().Enabled();

  for (size_t i = 0; i < items.size(); ++i) {
    auto& it = items[i];
    void* dst = it.dst;
    size_t size = it.size;
    uint32_t rkey = (rdma_on && rdma_registry_ != nullptr)
                        ? rdma_registry_->RkeyFor(dst, size)
                        : 0;
    if (rkey != 0) {
      // Pre-registered destination: the server RDMA_WRITEs the block straight
      // into the arena slice. direct_rdma_get skips the post-copy in the cb.
      (*response_bufs)[i].AppendUserDataWithMeta(dst, size, [](void*) {}, rkey);
      (*direct_rdma_get)[i] = 1;
    }
    block_cache_->AsyncRange(
        BlockHandle(it.key), 0, size, &(*response_bufs)[i],
        [fan, i, response_bufs, direct_rdma_get, dst, size](Status s) {
          if (s.ok()) {
            // Inert on the direct path (the buffer length is pre-set to size);
            // still guards the TCP/pooled fallback where the response length is
            // whatever the server returned.
            const size_t actual = (*response_bufs)[i].Size();
            if (actual != size) {
              fan->Report(i, false,
                          "response size mismatch: expected " +
                              std::to_string(size) + ", got " +
                              std::to_string(actual));
              return;
            }
            // direct_rdma_get == 1: server already RDMA-wrote into dst (arena),
            // no copy needed. Otherwise pull the brpc/pooled response into dst.
            if ((*direct_rdma_get)[i] == 0) {
              (*response_bufs)[i].CopyTo(static_cast<char*>(dst), size);
            }
            fan->Report(i, true);
          } else if (s.IsNotFound()) {
            fan->Report(i, false);
          } else {
            fan->Report(i, false, s.ToString());
          }
        },
        opt);
  }
  return fid;
}

uint64_t NativeEngine::SubmitBatchExists(std::vector<TensorKey> keys) {
  if (!running_.load(std::memory_order_acquire)) {
    throw std::runtime_error("engine shut down");
  }
  if (keys.empty()) return queue_.PushEmpty(OpType::kExists);

  FanIn* fan = queue_.NewFanIn(OpType::kExists, keys.size());
  uint64_t fid = fan->future_id();

  auto response_bufs = std::make_shared<std::vector<IOBuffer>>(keys.size());

  cache::RangeOption opt;
  opt.retrieve_storage = false;

  for (size_t i = 0; i < keys.size(); ++i) {
    block_cache_->AsyncRange(
        BlockHandle(keys[i]), 0, 1, &(*response_bufs)[i],
        [fan, i, response_bufs](Status s) {
          if (s.ok()) {
            fan->Report(i, true);
          } else if (s.IsNotFound()) {
            fan->Report(i, false);
          } else {
            fan->Report(i, false, s.ToString());
          }
        },
        opt);
  }
  return fid;
}

}  // namespace lmcache
}  // namespace integration
}  // namespace dingofs
