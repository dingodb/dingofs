/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "native_cache_engine.h"

#include <gflags/gflags.h>

#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>

#include "cache/blockcache/cache_store.h"
#include "cache/cachegroup/service.h"
#include "cache/common/block_key_helper.h"
#include "cache/common/context.h"
#include "common/block/block_context.h"
#include "common/block/tensor_key.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "key_codec.h"

namespace dingofs {
namespace lmcache {

namespace {

constexpr uint32_t kMinServiceVersion = 1;

// Build a tensor-flavored BlockContext from a key string + payload size. The
// BlockKey field is filled with a deterministic synthetic id so the legacy
// in-server call sites that index by BlockKey keep working — the tensor branch
// in node.cc skips the S3 path entirely.
::dingofs::BlockContext MakeContext(const std::string& key_str, uint32_t fs_id,
                                    size_t size) {
  auto tkey = std::make_shared<::dingofs::TensorKey>(ParseTensorKey(key_str));
  ::dingofs::BlockContext ctx;
  ctx.fs_id = fs_id;
  uint64_t synth_id = std::hash<std::string>{}(tkey->Filename());
  ctx.key = ::dingofs::BlockKey(synth_id, 0, static_cast<uint32_t>(size));
  ctx.tensor_key = std::move(tkey);
  return ctx;
}

void SetGflag(const char* name, const std::string& value) {
  // Returns "" on success.
  std::string r = ::google::SetCommandLineOption(name, value.c_str());
  if (r.empty()) {
    throw std::runtime_error(std::string("failed to set gflag ") + name);
  }
}

}  // namespace

NativeCacheEngine::NativeCacheEngine(const std::vector<std::string>& mds_addrs,
                                     const std::string& group_name,
                                     uint32_t fs_id, int num_workers,
                                     uint32_t request_timeout_ms)
    : ::lmcache::connector::ConnectorBase<UpstreamHandle>(num_workers),
      fs_id_(fs_id) {
  if (mds_addrs.empty()) {
    throw std::invalid_argument("mds_addrs must not be empty");
  }
  if (group_name.empty()) {
    throw std::invalid_argument("group_name must not be empty");
  }

  std::ostringstream csv;
  for (size_t i = 0; i < mds_addrs.size(); ++i) {
    if (i) csv << ',';
    csv << mds_addrs[i];
  }
  SetGflag("mds_addrs", csv.str());
  SetGflag("cache_group", group_name);
  SetGflag("cache_rpc_default_timeout_ms", std::to_string(request_timeout_ms));

  upstream_ = std::make_unique<::dingofs::cache::Upstream>();
  upstream_->Start();

  VerifyServiceVersion();

  // ConnectorBase requires start_workers() to be called at the END of derived
  // ctor (after the engine is fully constructed). Workers will call back into
  // create_connection() / do_single_*().
  start_workers();
}

NativeCacheEngine::~NativeCacheEngine() {
  // Close ConnectorBase first (signals workers to stop, joins them, closes
  // eventfd). Then shut Upstream so the brpc machinery winds down cleanly.
  close();
  if (upstream_) {
    upstream_->Shutdown();
  }
}

UpstreamHandle NativeCacheEngine::create_connection() {
  return UpstreamHandle{upstream_.get(), fs_id_};
}

void NativeCacheEngine::shutdown_connections() {
  // Nothing per-worker — Upstream is shared and shut down in the dtor after
  // workers have exited.
}

void NativeCacheEngine::do_single_get(UpstreamHandle& conn,
                                      const std::string& key, void* buf,
                                      size_t len,
                                      size_t /*batch_chunk_num_bytes*/) {
  auto ctx_sptr = ::dingofs::cache::NewContext("lmcache_get");
  auto block_ctx = MakeContext(key, conn.fs_id, len);
  ::dingofs::IOBuffer recv;
  auto status = conn.upstream->SendRangeRequest(ctx_sptr, block_ctx, 0, len,
                                                &recv, len);
  if (!status.ok()) {
    throw std::runtime_error("dingofs get failed: " + status.ToString());
  }
  if (recv.Size() != len) {
    throw std::runtime_error("dingofs get short-read: got " +
                             std::to_string(recv.Size()) + " want " +
                             std::to_string(len));
  }
  recv.CopyTo(static_cast<char*>(buf), len);
}

void NativeCacheEngine::do_single_set(UpstreamHandle& conn,
                                      const std::string& key, const void* buf,
                                      size_t len,
                                      size_t /*batch_chunk_num_bytes*/) {
  auto ctx_sptr = ::dingofs::cache::NewContext("lmcache_set");
  auto block_ctx = MakeContext(key, conn.fs_id, len);

  // Zero-copy: brpc IOBuf references the Python-owned buffer with a no-op
  // deleter. LMCache guarantees the buffer outlives the request.
  ::dingofs::IOBuffer iobuf;
  iobuf.AppendUserData(const_cast<void*>(buf), len, [](void*) {});
  ::dingofs::cache::Block block(std::move(iobuf));

  auto status = conn.upstream->SendPutRequest(ctx_sptr, block_ctx, block);
  if (!status.ok()) {
    throw std::runtime_error("dingofs set failed: " + status.ToString());
  }
}

bool NativeCacheEngine::do_single_exists(UpstreamHandle& conn,
                                         const std::string& key) {
  auto ctx_sptr = ::dingofs::cache::NewContext("lmcache_exists");
  auto block_ctx = MakeContext(key, conn.fs_id, 0);
  bool exists = false;
  auto status = conn.upstream->SendHeadRequest(ctx_sptr, block_ctx, &exists);
  if (!status.ok()) {
    // Treat transport errors as "doesn't exist" — LMCache will re-fetch from
    // the source. Throwing here would mark the whole batch failed.
    return false;
  }
  return exists;
}

void NativeCacheEngine::VerifyServiceVersion() {
  uint32_t version = 0;
  auto status = upstream_->SendPingRequest(&version);
  if (!status.ok()) {
    throw std::runtime_error(
        "failed to ping dingo-cache service for version handshake: " +
        status.ToString());
  }
  if (version < kMinServiceVersion) {
    throw std::runtime_error(
        "dingo-cache server is too old: service_version=" +
        std::to_string(version) + " (need >= " +
        std::to_string(kMinServiceVersion) +
        "); please upgrade dingo-cache to a version with TensorKey support");
  }
  service_version_ = version;
}

}  // namespace lmcache
}  // namespace dingofs
