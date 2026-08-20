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

#include "client/vfs/blockstore/block_store_v2.h"

#include <bthread/bthread.h>
#include <butil/time.h>
#include <bvar/bvar.h>
#include <fmt/format.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "blockcache/api/cache.h"
#include "blockcache/common/flag_decls.h"
#include "blockcache/common/mds_client.h"
#include "blockcache/object/client.h"
#include "blockcache/object/object.h"
#include "client/vfs/blockstore/block_store_access_log.h"
#include "client/vfs/blockstore/block_store_v2_util.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/options/client.h"

namespace dingofs {
namespace client {
namespace vfs {

namespace {

constexpr int64_t kSubmitRetryDelayUs = 100;
constexpr int64_t kSubmitRetryTimeoutUs = 60LL * 1000 * 1000;

}  // namespace

class BlockStoreV2 final : public BlockStore {
 public:
  BlockStoreV2(VFSHub* hub, std::string uuid, uint64_t block_size)
      : hub_(hub), uuid_(std::move(uuid)), block_size_(block_size) {}

  ~BlockStoreV2() override { Shutdown(); }

  Status Start() override;

  void Shutdown() override;

  void RangeAsync(ContextSPtr ctx, RangeReq req,
                  StatusCallback callback) override;

  void PutAsync(ContextSPtr ctx, PutReq req, StatusCallback callback) override;

  void PrefetchAsync(ContextSPtr ctx, PrefetchReq req,
                     StatusCallback callback) override;

  Status RegisterMemory(void* base, size_t bytes) override;

  // utility
  bool EnableCache() const override;
  cache::BlockCache* GetBlockCache() const override;

 private:
  template <typename TrySubmit>
  void SubmitWithRetry(const blockcache::AsyncCallback& callback,
                       const TrySubmit& try_submit);

  VFSHub* hub_;
  const std::string uuid_;
  const uint64_t block_size_;
  std::atomic<bool> started_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<int64_t> pending_{0};
  bool enable_cache_{false};
  blockcache::MDSClientUPtr mds_client_;
  blockcache::StorageClientUPtr storage_client_;
  std::unique_ptr<blockcache::BlockCacheImpl> cache_;

  bvar::Adder<int64_t> num_async_put_{"dingofs_blockstore_num_async_put"};
};

Status BlockStoreV2::Start() {
  if (started_) {
    return Status::OK();
  }

  uint64_t page_size = FLAGS_vfs_write_buffer_page_size;
  if (page_size == 0 || (block_size_ + page_size - 1) / page_size >
                            blockcache::kMaxBufferViews) {
    return Status::InvalidParam(fmt::format(
        "block size {} needs more than {} buffer segments with page size {}, "
        "raise vfs_write_buffer_page_size",
        block_size_, blockcache::kMaxBufferViews, page_size));
  }

  blockcache::FLAGS_cache_dir_uuid = uuid_;

  auto mds_client = std::make_unique<blockcache::MDSClientImpl>();
  auto status = mds_client->Start();
  if (!status.ok()) {
    return status;
  }

  auto storage_client =
      std::make_unique<blockcache::StorageClient>(mds_client.get());
  storage_client->Start();

  auto block_cache = std::make_unique<blockcache::BlockCacheImpl>(
      mds_client.get(),
      std::make_unique<blockcache::ObjectStorage>(storage_client.get()));
  status = block_cache->Start();
  if (!status.ok()) {
    storage_client->Shutdown();
    mds_client->Shutdown();
    return status;
  }

  mds_client_ = std::move(mds_client);
  storage_client_ = std::move(storage_client);
  cache_ = std::move(block_cache);

  enable_cache_ = blockcache::FLAGS_cache_store == "disk" ||
                  !blockcache::FLAGS_cache_group.empty();

  started_ = true;
  return Status::OK();
}

void BlockStoreV2::Shutdown() {
  if (!started_.exchange(false)) {
    return;
  }

  stopping_ = true;
  while (pending_.load(std::memory_order_acquire) > 0) {
    bthread_usleep(1000);
  }

  cache_->Shutdown();
  storage_client_->Shutdown();
  mds_client_->Shutdown();

  cache_.reset();
  storage_client_.reset();
  mds_client_.reset();
}

template <typename TrySubmit>
void BlockStoreV2::SubmitWithRetry(const blockcache::AsyncCallback& callback,
                                   const TrySubmit& try_submit) {
  pending_.fetch_add(1, std::memory_order_acq_rel);

  int64_t start_us = butil::cpuwide_time_us();
  for (;;) {
    if (try_submit()) {
      break;
    }
    if (stopping_.load(std::memory_order_acquire)) {
      callback(Status::Stop("block store shutting down"));
      break;
    }
    if (butil::cpuwide_time_us() - start_us > kSubmitRetryTimeoutUs) {
      callback(Status::Internal("blockcache submit queue full"));
      break;
    }
    bthread_usleep(kSubmitRetryDelayUs);
  }

  pending_.fetch_sub(1, std::memory_order_acq_rel);
}

void BlockStoreV2::RangeAsync(ContextSPtr ctx, RangeReq req,
                              StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan("BlockStoreV2::RangeAsync",
                                                      ctx->GetTraceSpan());

  int64_t start_us = butil::cpuwide_time_us();

  blockcache::AsyncCallback wrapper = [start_us, req, cb = std::move(callback),
                                      span](Status s) {
    BlockStoreAccessLogGuard log(start_us, [&]() {
      return fmt::format("range_async ({}, {}, [{}-{})) : {}",
                         req.handle.Filename(), req.length, req.offset,
                         (req.offset + req.length), s.ToString());
    });
    SpanScope::End(span);
    cb(s);
  };

  blockcache::BlockHandle handle;
  auto status = ToV2Handle(req.handle, &handle);
  if (!status.ok()) {
    wrapper(status);
    return;
  }

  SubmitWithRetry(wrapper, [this, handle, req, wrapper]() {
    return cache_->AsyncGet(handle, static_cast<uint64_t>(req.offset),
                            static_cast<uint32_t>(req.dst.len),
                            reinterpret_cast<char*>(req.dst.data()), wrapper);
  });
}

void BlockStoreV2::PutAsync(ContextSPtr ctx, PutReq req,
                            StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan("BlockStoreV2::PutAsync",
                                                      ctx->GetTraceSpan());

  int64_t start_us = butil::cpuwide_time_us();

  num_async_put_ << 1;

  blockcache::AsyncCallback wrapper = [this, start_us, req,
                                      cb = std::move(callback), span](Status s) {
    BlockStoreAccessLogGuard log(start_us, [&]() {
      return fmt::format("put_async ({}, {}) : {}", req.handle.Filename(),
                         req.data.Size(), s.ToString());
    });
    SpanScope::End(span);
    cb(s);

    num_async_put_ << -1;
  };

  blockcache::BlockHandle handle;
  auto status = ToV2Handle(req.handle, &handle);
  if (!status.ok()) {
    wrapper(status);
    return;
  }

  std::vector<blockcache::BufferView> views;
  status = BuildBufferViews(req.data, &views);
  if (!status.ok()) {
    wrapper(status);
    return;
  }

  blockcache::PutOption option{.stage = req.write_back};
  SubmitWithRetry(wrapper, [this, handle, views = std::move(views), wrapper,
                            option]() {
    return cache_->AsyncPut(
        handle, blockcache::BufferViews(views.data(), views.size()), wrapper,
        option);
  });
}

void BlockStoreV2::PrefetchAsync(ContextSPtr ctx, PrefetchReq req,
                                 StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "BlockStoreV2::PrefetchAsync", ctx->GetTraceSpan());

  int64_t start_us = butil::cpuwide_time_us();

  blockcache::AsyncCallback wrapper = [start_us, req, cb = std::move(callback),
                                      span](Status s) {
    BlockStoreAccessLogGuard log(start_us, [&]() {
      return fmt::format("prefetch_async ({}, {}) : {}", req.handle.Filename(),
                         req.handle.StoreSize(), s.ToString());
    });
    SpanScope::End(span);
    cb(s);
  };

  blockcache::BlockHandle handle;
  auto status = ToV2Handle(req.handle, &handle);
  if (!status.ok()) {
    wrapper(status);
    return;
  }

  SubmitWithRetry(wrapper, [this, handle, wrapper]() {
    return cache_->AsyncPrefetch(handle, wrapper);
  });
}

Status BlockStoreV2::RegisterMemory(void* base, size_t bytes) {
  return cache_->RegisterBuffers(base, bytes);
}

bool BlockStoreV2::EnableCache() const { return enable_cache_; }

cache::BlockCache* BlockStoreV2::GetBlockCache() const { return nullptr; }

std::unique_ptr<BlockStore> NewBlockStoreV2(VFSHub* hub, std::string uuid,
                                            uint64_t block_size) {
  return std::make_unique<BlockStoreV2>(hub, std::move(uuid), block_size);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
