/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>

#include "cache/common/slab_pool.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/server.h"
#include "dingofs/blockcache.pb.h"

DEFINE_string(device, "mlx5_0", "IB device name (e.g. mlx5_0)");
DEFINE_int32(port_num, 1, "IB HCA port, 1-based");
DEFINE_int32(brpc_port, 8888,
             "TCP port for the connection-management brpc service");
DEFINE_int32(attachment_pool_size, 256,
             "Number of attachment buffers; must be >= max concurrent RPCs");
DEFINE_int32(attachment_buffer_size, 4194304,
             "Attachment buffer size in bytes");
DEFINE_string(server_verify, "full",
              "Server-side verification mode for Put/Cache: full|markers|none");
DEFINE_bool(server_fill_response, true,
            "Fill Range response buffers before RDMA_WRITE");

namespace {

using ::dingofs::cache::infiniband::Controller;
using ::dingofs::cache::infiniband::EndPoint;
using ::dingofs::cache::infiniband::Infiniband;
using ::dingofs::cache::infiniband::MemoryRegion;
using ::dingofs::cache::infiniband::MemoryRegionUPtr;

enum class VerifyMode {
  kFull,
  kMarkers,
  kNone,
};

VerifyMode ParseVerifyMode(const std::string& value) {
  if (value == "full") return VerifyMode::kFull;
  if (value == "markers") return VerifyMode::kMarkers;
  if (value == "none") return VerifyMode::kNone;
  LOG(FATAL) << "Unsupported --server_verify=" << value;
  return VerifyMode::kFull;
}

uint64_t SeedFromHandle(const ::dingofs::pb::cache::BlockHandle& handle) {
  if (handle.has_block_key()) {
    return handle.block_key().id();
  }
  if (handle.has_tensor_key()) {
    return std::hash<std::string>{}(handle.tensor_key().chunk_hash());
  }
  return 0;
}

void FillPattern(char* data, size_t size, uint64_t seed) {
  const uint64_t pattern = seed;
  size_t pos = 0;
  while (pos + sizeof(pattern) <= size) {
    std::memcpy(data + pos, &pattern, sizeof(pattern));
    pos += sizeof(pattern);
  }
  if (pos < size) {
    std::memcpy(data + pos, &pattern, size - pos);
  }
}

bool CheckByte(const char* data, size_t pos, uint64_t seed) {
  char pattern[sizeof(seed)];
  std::memcpy(pattern, &seed, sizeof(seed));
  return data[pos] == pattern[pos % sizeof(seed)];
}

bool VerifyPattern(const char* data, size_t size, uint64_t seed,
                   VerifyMode mode) {
  if (mode == VerifyMode::kNone || size == 0) {
    return true;
  }

  if (mode == VerifyMode::kMarkers) {
    return CheckByte(data, 0, seed) && CheckByte(data, size / 2, seed) &&
           CheckByte(data, size - 1, seed);
  }

  const uint64_t pattern = seed;
  size_t pos = 0;
  while (pos + sizeof(pattern) <= size) {
    uint64_t actual = 0;
    std::memcpy(&actual, data + pos, sizeof(actual));
    if (actual != pattern) {
      return false;
    }
    pos += sizeof(pattern);
  }
  if (pos < size) {
    char expected[sizeof(pattern)];
    std::memcpy(expected, &pattern, sizeof(pattern));
    return std::memcmp(data + pos, expected, size - pos) == 0;
  }
  return true;
}

class PooledBufferGuard {
 public:
  PooledBufferGuard(::dingofs::cache::SlabPool* pool,
                    ::dingofs::cache::SlabBuffer* buffer)
      : pool_(pool), buffer_(buffer) {}
  ~PooledBufferGuard() {
    if (buffer_ != nullptr) {
      pool_->Free(buffer_);
    }
  }

  PooledBufferGuard(const PooledBufferGuard&) = delete;
  PooledBufferGuard& operator=(const PooledBufferGuard&) = delete;

  ::dingofs::cache::SlabBuffer* get() const { return buffer_; }

  ::dingofs::cache::SlabBuffer* Release() {
    auto* buffer = buffer_;
    buffer_ = nullptr;
    return buffer;
  }

 private:
  ::dingofs::cache::SlabPool* pool_;
  ::dingofs::cache::SlabBuffer* buffer_;
};

MemoryRegionUPtr RegisterSlabPool(const EndPoint& ep,
                                  ::dingofs::cache::SlabPool* pool) {
  Infiniband::Context context;
  auto status = Infiniband::Init(ep.device_name, ep.port_num, &context);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to initialize infiniband context: "
               << status.ToString();
    return nullptr;
  }
  return MemoryRegion::Register(context.protect_domain, pool->Base(),
                                pool->ByteSize());
}

class BlockCacheServiceImpl final
    : public ::dingofs::pb::cache::BlockCacheService {
 public:
  BlockCacheServiceImpl(::dingofs::cache::SlabPool* pool,
                        VerifyMode verify_mode, bool fill_response)
      : pool_(pool), verify_mode_(verify_mode), fill_response_(fill_response) {}

  void Ping(::google::protobuf::RpcController* /*controller*/,
            const ::dingofs::pb::cache::PingRequest* /*request*/,
            ::dingofs::pb::cache::PingResponse* /*response*/,
            ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
  }

  void Put(::google::protobuf::RpcController* controller,
           const ::dingofs::pb::cache::PutRequest* request,
           ::dingofs::pb::cache::PutResponse* response,
           ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
    auto* cntl = static_cast<Controller*>(controller);
    const auto& attachment = cntl->request_attachment();
    if (attachment.Length() != request->block_size()) {
      response->set_status(::dingofs::pb::cache::BlockCacheErrInvalidParam);
      return;
    }

    const uint64_t seed = SeedFromHandle(request->handle());
    const char* data =
        request->block_size() == 0 ? nullptr : attachment.Fetch1();
    response->set_status(
        VerifyPattern(data, request->block_size(), seed, verify_mode_)
            ? ::dingofs::pb::cache::BlockCacheOk
            : ::dingofs::pb::cache::BlockCacheErrInvalidParam);
  }

  void Cache(::google::protobuf::RpcController* controller,
             const ::dingofs::pb::cache::CacheRequest* request,
             ::dingofs::pb::cache::CacheResponse* response,
             ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
    auto* cntl = static_cast<Controller*>(controller);
    const auto& attachment = cntl->request_attachment();
    if (attachment.Length() != request->block_size()) {
      response->set_status(::dingofs::pb::cache::BlockCacheErrInvalidParam);
      return;
    }

    const uint64_t seed = SeedFromHandle(request->handle());
    const char* data =
        request->block_size() == 0 ? nullptr : attachment.Fetch1();
    response->set_status(
        VerifyPattern(data, request->block_size(), seed, verify_mode_)
            ? ::dingofs::pb::cache::BlockCacheOk
            : ::dingofs::pb::cache::BlockCacheErrInvalidParam);
  }

  void Range(::google::protobuf::RpcController* controller,
             const ::dingofs::pb::cache::RangeRequest* request,
             ::dingofs::pb::cache::RangeResponse* response,
             ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
    auto* cntl = static_cast<Controller*>(controller);

    auto* buf = pool_->Alloc(request->length());
    if (buf == nullptr) {
      response->set_status(::dingofs::pb::cache::BlockCacheErrFailure);
      return;
    }
    PooledBufferGuard guard(pool_, buf);

    if (buf->meta == 0) {
      response->set_status(::dingofs::pb::cache::BlockCacheErrFailure);
      return;
    }

    if (fill_response_) {
      FillPattern(static_cast<char*>(buf->data), request->length(),
                  request->offset());
    }
    cntl->response_attachment() = dingofs::IOBuffer();
    if (request->length() > 0) {
      auto* pool = pool_;
      auto* response_buffer = guard.Release();
      cntl->response_attachment().AppendUserDataWithMeta(
          response_buffer->data, request->length(),
          [pool, response_buffer](void*) { pool->Free(response_buffer); },
          response_buffer->meta);
    }

    response->set_status(::dingofs::pb::cache::BlockCacheOk);
    response->set_cache_hit(true);
  }

 private:
  ::dingofs::cache::SlabPool* pool_;
  VerifyMode verify_mode_;
  bool fill_response_;
};

}  // namespace

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  using namespace dingofs::cache::infiniband;  // NOLINT

  brpc::Server brpc_server;
  Server rdma_server;

  EndPoint ep;
  ep.device_name = FLAGS_device;
  ep.port_num = static_cast<uint8_t>(FLAGS_port_num);

  if (FLAGS_attachment_buffer_size != 4 * 1024 * 1024) {
    LOG(WARNING) << "--attachment_buffer_size is ignored by SlabPool; "
                 << "using fixed 4MiB slabs";
  }

  auto& slab_pool = ::dingofs::cache::GetGlobalSendSlabPool();
  slab_pool.Init(FLAGS_attachment_pool_size);
  auto slab_region = RegisterSlabPool(ep, &slab_pool);
  CHECK_NOTNULL(slab_region);
  slab_pool.SetMeta(slab_region->GetLkey());

  ServerOptions options;
  options.brpc_server = &brpc_server;

  auto status = rdma_server.Start(ep, &options);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start RDMA server: " << status.ToString();
    return 1;
  }

  const VerifyMode verify_mode = ParseVerifyMode(FLAGS_server_verify);
  BlockCacheServiceImpl service(&slab_pool, verify_mode,
                                FLAGS_server_fill_response);
  status = rdma_server.AddService(&service);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to add service: " << status.ToString();
    return 1;
  }

  brpc::ServerOptions brpc_options;
  if (brpc_server.Start(FLAGS_brpc_port, &brpc_options) != 0) {
    LOG(ERROR) << "Fail to start brpc server on port=" << FLAGS_brpc_port;
    return 1;
  }

  LOG(INFO) << "RDMA server is up: device=" << FLAGS_device
            << " port_num=" << FLAGS_port_num
            << " brpc_port=" << FLAGS_brpc_port
            << " attachment_buffer_size=" << FLAGS_attachment_buffer_size
            << " attachment_pool_size=" << FLAGS_attachment_pool_size
            << " server_verify=" << FLAGS_server_verify
            << " server_fill_response=" << FLAGS_server_fill_response;

  brpc_server.RunUntilAskedToQuit();
  return 0;
}
