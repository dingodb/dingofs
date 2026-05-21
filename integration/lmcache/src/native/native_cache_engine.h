/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#ifndef DINGOFS_LMCACHE_NATIVE_NATIVE_CACHE_ENGINE_H_
#define DINGOFS_LMCACHE_NATIVE_NATIVE_CACHE_ENGINE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cache/remotecache/upstream.h"
#include "connector_base.h"  // from LMCache csrc/storage_backends/

namespace dingofs {
namespace lmcache {

// Workers receive an UpstreamHandle copy. Upstream itself is shared & thread-
// safe; the handle just bundles per-engine routing metadata so worker code
// stays straight-line.
struct UpstreamHandle {
  ::dingofs::cache::Upstream* upstream;
  uint32_t fs_id;
};

// IStorageConnector implementation backed by dingo-cache.
//
// One engine instance owns:
//   - the gflags-bound Upstream client (MDSClient + PeerGroup + brpc channels)
//   - the worker thread pool (provided by ConnectorBase)
//   - the eventfd used to signal Python (provided by ConnectorBase)
//
// All worker threads share the same Upstream — brpc internally multiplexes
// bthread/pthread, so blocking calls from std::thread workers are fine.
class NativeCacheEngine
    : public ::lmcache::connector::ConnectorBase<UpstreamHandle> {
 public:
  // mds_addrs is the comma-joined MDS endpoint list. group_name is the cache
  // group name to query members from. fs_id is the routing id stamped on every
  // BlockContext. num_workers controls fan-out of batched submissions.
  NativeCacheEngine(const std::vector<std::string>& mds_addrs,
                    const std::string& group_name, uint32_t fs_id,
                    int num_workers, uint32_t request_timeout_ms);

  ~NativeCacheEngine() override;

  uint32_t service_version() const { return service_version_; }

 protected:
  UpstreamHandle create_connection() override;
  void do_single_get(UpstreamHandle& conn, const std::string& key, void* buf,
                     size_t len, size_t batch_chunk_num_bytes) override;
  void do_single_set(UpstreamHandle& conn, const std::string& key,
                     const void* buf, size_t len,
                     size_t batch_chunk_num_bytes) override;
  bool do_single_exists(UpstreamHandle& conn, const std::string& key) override;
  void shutdown_connections() override;

 private:
  // Resolve the server's BlockCacheService version via Ping. Throws if it's
  // below the minimum required (TensorKey oneof support).
  void VerifyServiceVersion();

  uint32_t fs_id_;
  std::unique_ptr<::dingofs::cache::Upstream> upstream_;
  uint32_t service_version_{0};
};

}  // namespace lmcache
}  // namespace dingofs

#endif  // DINGOFS_LMCACHE_NATIVE_NATIVE_CACHE_ENGINE_H_
