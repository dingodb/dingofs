// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef DINGOFS_INTEGRATION_LMCACHE_NATIVE_ENGINE_H_
#define DINGOFS_INTEGRATION_LMCACHE_NATIVE_ENGINE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/block/tensor_key.h"
#include "completion_queue.h"

namespace dingofs {

namespace cache {
class RemoteBlockCacheImpl;
}  // namespace cache

namespace integration {
namespace lmcache {

// SetItem / GetItem describe one slot of a batched async I/O. The underlying
// Python buffer is pinned for the op's lifetime by DingoFSNativeClient's
// `_pending` keepalive — native side only borrows the raw pointer.
struct SetItem {
  TensorKey key;
  const void* data;
  size_t size;
};

struct GetItem {
  TensorKey key;
  void* dst;
  size_t size;
};

// A CPU memory range to be registered with the RDMA NIC (via ibv_reg_mr or
// equivalent) when the RDMA data path comes online. Populated from LMCache
// LocalCPUBackend's pinned arena on the Python side at construction time;
// the engine just stores it for the later RDMA bring-up to consume.
struct MemoryRegion {
  std::uintptr_t addr;
  std::size_t length;
};

class NativeEngine {
 public:
  struct InitOptions {
    std::string mds_addrs;  // comma-separated MDS endpoints
    std::string cache_group;
    // Optional path to a gflags-format conf file (one `--flag=value` per line,
    // # comments allowed). Loaded via gflags::ReadFromFlagsFile before
    // mds_addrs / cache_group are applied, so URL fields stay authoritative.
    std::string conf_file;
    // CPU memory regions to RDMA-register when the RDMA path is wired up.
    // See MemoryRegion above for semantics.
    std::vector<MemoryRegion> rdma_pools;
  };

  // Construct the singleton. Throws std::runtime_error on second call
  // (gflags are process-global; one engine per process is the contract).
  static std::unique_ptr<NativeEngine> Create(const InitOptions& opts);

  ~NativeEngine();

  NativeEngine(const NativeEngine&) = delete;
  NativeEngine& operator=(const NativeEngine&) = delete;

  // Idempotent shutdown. Waits for in-flight bthreads to drain.
  void Shutdown();

  int event_fd() const { return queue_.event_fd(); }

  // CPU memory regions handed to us at construction for future RDMA
  // registration. The RDMA bring-up (separate PR) will iterate this and
  // call ibv_reg_mr per region.
  const std::vector<MemoryRegion>& rdma_pools() const { return rdma_pools_; }

  // Sync existence probe. Maps OK -> true, NotFound -> false.
  // Returns false and fills *err on any other error.
  bool ExistsSync(const TensorKey& key, std::string* err);

  // Sync ping — round-trip to any cache group member.
  // Returns true on success; fills *err on failure.
  bool Ping(std::string* err);

  // Batched async ops. Each returns a future_id; completion arrives via
  // Drain() after the eventfd fires.
  uint64_t SubmitBatchSet(std::vector<SetItem> items);
  uint64_t SubmitBatchGet(std::vector<GetItem> items);
  uint64_t SubmitBatchExists(std::vector<TensorKey> keys);

  // Drain queued completions (one or more batches).
  std::vector<Completion> Drain() { return queue_.Drain(); }

 private:
  NativeEngine();

  void InstallFlags(const InitOptions& opts);
  void StartCache();

  std::atomic<bool> running_{false};
  std::unique_ptr<cache::RemoteBlockCacheImpl> block_cache_;
  CompletionQueue queue_;
  std::vector<MemoryRegion> rdma_pools_;
};

}  // namespace lmcache
}  // namespace integration
}  // namespace dingofs

#endif  // DINGOFS_INTEGRATION_LMCACHE_NATIVE_ENGINE_H_
