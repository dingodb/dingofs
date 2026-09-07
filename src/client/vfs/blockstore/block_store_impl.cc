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

#include "client/vfs/blockstore/block_store_impl.h"

#include <bthread/bthread.h>
#include <butil/time.h>
#include <bvar/bvar.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "blockcache/api/cache.h"
#include "blockcache/common/block_handle.h"
#include "blockcache/common/mds_client.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/object/client.h"
#include "blockcache/object/object.h"
#include "client/vfs/blockstore/block_store_access_log.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/blockaccess/block_accesser.h"
#include "common/options/cache.h"
#include "common/options/client.h"

namespace dingofs {
namespace client {
namespace vfs {

class HubStorageClient final : public blockcache::StorageClient {
 public:
  HubStorageClient(uint32_t fs_id, blockaccess::BlockAccesser* accesser)
      : StorageClient(/*mds_client=*/nullptr),
        fs_id_(fs_id),
        accesser_(CHECK_NOTNULL(accesser)) {}

  Status GetOrCreate(uint64_t fs_id,
                     blockaccess::BlockAccesser** accesser) override {
    if (fs_id != fs_id_) {
      return Status::InvalidParam(
          fmt::format("fs_id {} is not the mounted fs {}", fs_id, fs_id_));
    }
    *accesser = accesser_;
    return Status::OK();
  }

 private:
  const uint32_t fs_id_;
  blockaccess::BlockAccesser* const accesser_;
};

class BlockStoreImpl final : public BlockStore {
 public:
  BlockStoreImpl(VFSHub* hub, std::string uuid, uint32_t fs_id,
                 uint64_t block_size)
      : hub_(hub),
        uuid_(std::move(uuid)),
        fs_id_(fs_id),
        block_size_(block_size) {}

  ~BlockStoreImpl() override { Shutdown(); }

  Status Start() override;

  void Shutdown() override;

  void RangeAsync(ContextSPtr ctx, RangeReq req,
                  StatusCallback callback) override;

  void PutAsync(ContextSPtr ctx, PutReq req, StatusCallback callback) override;

  void PrefetchAsync(ContextSPtr ctx, PrefetchReq req,
                     StatusCallback callback) override;

  void DeleteAsync(ContextSPtr ctx, DeleteReq req,
                   StatusCallback callback) override;

  bool EnableCache() const override;

 private:
  static constexpr int64_t kSubmitRetryDelayUs = 100;
  static constexpr int64_t kSubmitRetryTimeoutUs = 60LL * 1000 * 1000;

  static blockcache::BlockHandle ToCacheHandle(const BlockHandle& handle);
  static Status BuildBufferViews(const IOBuffer& data,
                                 std::vector<blockcache::BufferView>* views);

  template <typename TrySubmit>
  void SubmitWithRetry(const blockcache::AsyncCallback& callback,
                       const TrySubmit& try_submit);

  VFSHub* hub_;
  const std::string uuid_;
  const uint32_t fs_id_;
  const uint64_t block_size_;
  std::atomic<bool> started_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<int64_t> pending_{0};
  blockcache::MDSClientUPtr mds_client_;  // null without a cache group
  blockcache::StorageClientUPtr storage_client_;
  std::unique_ptr<blockcache::BlockCacheImpl> cache_;

  bvar::Adder<int64_t> num_async_put_{"dingofs_blockstore_num_async_put"};
};

Status BlockStoreImpl::Start() {
  if (started_) {
    return Status::OK();
  }

  uint64_t page_size = FLAGS_vfs_write_buffer_page_size;
  if (page_size == 0 ||
      (block_size_ + page_size - 1) / page_size > blockcache::kMaxBufferViews) {
    return Status::InvalidParam(fmt::format(
        "block size {} needs more than {} buffer segments with page size {}, "
        "raise vfs_write_buffer_page_size",
        block_size_, blockcache::kMaxBufferViews, page_size));
  }

  blockcache::FLAGS_cache_dir_uuid = uuid_;

  blockcache::MDSClientUPtr mds_client;
  if (!blockcache::FLAGS_cache_group.empty()) {
    mds_client = std::make_unique<blockcache::MDSClientImpl>();
    Status status = mds_client->Start();
    if (!status.ok()) {
      return status;
    }
  }

  auto storage_client =
      std::make_unique<HubStorageClient>(fs_id_, hub_->GetBlockAccesser());
  storage_client->Start();

  auto block_cache = std::make_unique<blockcache::BlockCacheImpl>(
      mds_client.get(),
      std::make_unique<blockcache::ObjectStorage>(storage_client.get()));
  Status status = block_cache->Start();
  if (!status.ok()) {
    storage_client->Shutdown();
    if (mds_client != nullptr) {
      mds_client->Shutdown();
    }
    return status;
  }

  mds_client_ = std::move(mds_client);
  storage_client_ = std::move(storage_client);
  cache_ = std::move(block_cache);

  started_ = true;
  return Status::OK();
}

void BlockStoreImpl::Shutdown() {
  if (!started_.exchange(false)) {
    return;
  }

  stopping_ = true;
  while (pending_.load(std::memory_order_acquire) > 0) {
    bthread_usleep(1000);
  }

  cache_->Shutdown();
  storage_client_->Shutdown();
  if (mds_client_ != nullptr) {
    mds_client_->Shutdown();
  }

  cache_.reset();
  storage_client_.reset();
  mds_client_.reset();
}

void BlockStoreImpl::RangeAsync(ContextSPtr ctx, RangeReq req,
                                StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "BlockStoreImpl::RangeAsync", ctx->GetTraceSpan());

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

  blockcache::BlockHandle handle = ToCacheHandle(req.handle);
  SubmitWithRetry(wrapper, [this, handle, &req, &wrapper]() {
    return cache_->AsyncGet(handle, static_cast<uint64_t>(req.offset),
                            static_cast<uint32_t>(req.dst.len),
                            reinterpret_cast<char*>(req.dst.data()), wrapper);
  });
}

void BlockStoreImpl::PutAsync(ContextSPtr ctx, PutReq req,
                              StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "BlockStoreImpl::PutAsync", ctx->GetTraceSpan());

  int64_t start_us = butil::cpuwide_time_us();

  num_async_put_ << 1;

  blockcache::AsyncCallback wrapper =
      [this, start_us, req, cb = std::move(callback), span](Status s) {
        BlockStoreAccessLogGuard log(start_us, [&]() {
          return fmt::format("put_async ({}, {}) : {}", req.handle.Filename(),
                             req.data.Size(), s.ToString());
        });
        SpanScope::End(span);
        cb(s);

        num_async_put_ << -1;
      };

  std::vector<blockcache::BufferView> views;
  auto status = BuildBufferViews(req.data, &views);
  if (!status.ok()) {
    wrapper(status);
    return;
  }

  blockcache::BlockHandle handle = ToCacheHandle(req.handle);
  blockcache::PutOption option{.stage = req.write_back};
  SubmitWithRetry(wrapper, [this, handle, &views, &wrapper, option]() {
    return cache_->AsyncPut(handle,
                            blockcache::BufferViews(views.data(), views.size()),
                            wrapper, option);
  });
}

void BlockStoreImpl::PrefetchAsync(ContextSPtr ctx, PrefetchReq req,
                                   StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "BlockStoreImpl::PrefetchAsync", ctx->GetTraceSpan());

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

  blockcache::BlockHandle handle = ToCacheHandle(req.handle);
  SubmitWithRetry(wrapper, [this, handle, &wrapper]() {
    return cache_->AsyncPrefetch(handle, wrapper);
  });
}

void BlockStoreImpl::DeleteAsync(ContextSPtr ctx, DeleteReq req,
                                 StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "BlockStoreImpl::DeleteAsync", ctx->GetTraceSpan());

  int64_t start_us = butil::cpuwide_time_us();

  blockcache::AsyncCallback wrapper = [start_us, req, cb = std::move(callback),
                                       span](Status s) {
    BlockStoreAccessLogGuard log(start_us, [&]() {
      return fmt::format("delete_async ({}) : {}", req.handle.Filename(),
                         s.ToString());
    });
    SpanScope::End(span);
    cb(s);
  };

  blockcache::BlockHandle handle = ToCacheHandle(req.handle);
  SubmitWithRetry(wrapper, [this, handle, &wrapper]() {
    return cache_->AsyncDelete(handle, wrapper);
  });
}

bool BlockStoreImpl::EnableCache() const {
  return blockcache::FLAGS_cache_store == "disk" ||
         !blockcache::FLAGS_cache_group.empty();
}

template <typename TrySubmit>
void BlockStoreImpl::SubmitWithRetry(const blockcache::AsyncCallback& callback,
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

blockcache::BlockHandle BlockStoreImpl::ToCacheHandle(const BlockHandle& in) {
  return in.Visit([&](const auto& key) -> blockcache::BlockHandle {
    if constexpr (std::is_same_v<std::decay_t<decltype(key)>, BlockKey>) {
      return blockcache::BlockHandle{.fs_id = in.FsId(),
                                     .id = key.id,
                                     .index = key.index,
                                     .size = key.size};
    } else {
      CHECK(false) << "tensor block is not supported by blockcache: "
                   << key.Filename();
      return blockcache::BlockHandle{};
    }
  });
}

Status BlockStoreImpl::BuildBufferViews(
    const IOBuffer& data, std::vector<blockcache::BufferView>* views) {
  views->clear();
  for (const auto& iov : data.Fetch()) {
    char* base = static_cast<char*>(iov.iov_base);
    size_t len = iov.iov_len;
    if (len == 0) {
      continue;
    }

    if (!views->empty()) {
      auto& last = views->back();
      char* last_end = static_cast<char*>(last.data) + last.size;
      if (last_end == base && last.size + len <= UINT32_MAX) {
        last.size += len;
        continue;
      }
    }

    if (views->size() == blockcache::kMaxBufferViews) {
      return Status::InvalidParam("block has too many buffer segments");
    }
    views->emplace_back(base, static_cast<uint32_t>(len));
  }

  return Status::OK();
}

std::unique_ptr<BlockStore> NewBlockStore(VFSHub* hub, std::string uuid,
                                          uint32_t fs_id, uint64_t block_size) {
  return std::make_unique<BlockStoreImpl>(hub, std::move(uuid), fs_id,
                                          block_size);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
