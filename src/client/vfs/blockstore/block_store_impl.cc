/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include <butil/time.h>
#include <glog/logging.h>
#include <google/protobuf/descriptor.pb.h>

#include "cache/infiniband/memory.h"
#include "cache/tiercache/tier_block_cache.h"
#include "client/vfs/blockstore/block_store_access_log.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/options/cache.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("BlockStoreImpl::" + std::string(__FUNCTION__))

Status BlockStoreImpl::Start() {
  if (started_) {
    return Status::OK();
  }

  cache::FLAGS_cache_dir_uuid = uuid_;
  auto block_cache = std::make_unique<cache::TierBlockCache>(block_accesser_);
  DINGOFS_RETURN_NOT_OK(block_cache->Start());

  block_cache_ = std::move(block_cache);

  started_.store(true);
  return Status::OK();
}

void BlockStoreImpl::Shutdown() {
  if (!started_) {
    return;
  }

  Status s = block_cache_->Shutdown();
  if (!s.ok()) {
    LOG(WARNING) << "BlockStoreImpl Shutdown block_cache_ failed: "
                 << s.ToString();
    return;
  }

  block_cache_.reset();
  started_.store(false);
}

void BlockStoreImpl::RangeAsync(ContextSPtr ctx, RangeReq req,
                                StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "BlockStoreImpl::RangeAsync", ctx->GetTraceSpan());

  int64_t start_us = butil::cpuwide_time_us();

  // buffer
  auto* buffer = new IOBuffer();
  if (cache::FLAGS_use_rdma) {
    buffer->AppendUserDataWithMeta(
        reinterpret_cast<void*>(req.dst.data()), req.dst.len, [](void*) {},
        cache::infiniband::GetRkey(cache::FLAGS_cache_rdma_device, req.dst.data(),
                                   req.dst.len));
  } else {
    buffer->AppendUserData(reinterpret_cast<void*>(req.dst.data()), req.dst.len,
                           [](void*) {});
  }

  // option
  cache::RangeOption option;
  option.retrieve_storage = true;
  option.block_whole_length = req.handle.StoreSize();

  block_cache_->AsyncRange(
      req.handle, req.offset, req.dst.len, buffer,
      [start_us, req, buffer, cb = std::move(callback), span](Status s) {
        BlockStoreAccessLogGuard log(start_us, [&]() {
          return fmt::format("range_async ({}, {}, [{}-{})) : {}",
                             req.handle.Filename(), req.length, req.offset,
                             (req.offset + req.length), s.ToString());
        });
        delete buffer;
        SpanScope::End(span);
        cb(s);
      },
      option);
}

void BlockStoreImpl::PutAsync(ContextSPtr ctx, PutReq req,
                              StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "BlockStoreImpl::PutAsync", ctx->GetTraceSpan());

  int64_t start_us = butil::cpuwide_time_us();

  num_async_put_ << 1;

  auto wrapper = [this, start_us, req, cb = std::move(callback),
                  span](Status s) {
    BlockStoreAccessLogGuard log(start_us, [&]() {
      return fmt::format("put_async ({}, {}) : {}", req.handle.Filename(),
                         req.data.Size(), s.ToString());
    });
    SpanScope::End(span);
    cb(s);

    num_async_put_ << -1;
  };

  cache::PutOption option{.writeback = req.write_back};

  block_cache_->AsyncPut(req.handle, req.data, std::move(wrapper), option);
}

void BlockStoreImpl::PrefetchAsync(ContextSPtr ctx, PrefetchReq req,
                                   StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "BlockStoreImpl::PrefetchAsync", ctx->GetTraceSpan());
  if (block_cache_->IsCached(req.handle)) {
    callback(Status::OK());
    return;
  }

  int64_t start_us = butil::cpuwide_time_us();

  uint64_t prefetch_size = req.handle.StoreSize();
  auto wrapper = [this, start_us, req, prefetch_size, cb = std::move(callback),
                  span](Status s) {
    BlockStoreAccessLogGuard log(start_us, [&]() {
      return fmt::format("prefetch_async ({}, {}) : {}", req.handle.Filename(),
                         prefetch_size, s.ToString());
    });
    SpanScope::End(span);
    cb(s);
  };

  // transfer ownership of block_data to BlockDataFlushed
  block_cache_->AsyncPrefetch(req.handle, prefetch_size, std::move(wrapper));
}

// utility
bool BlockStoreImpl::EnableCache() const { return block_cache_->EnableCache(); }

cache::BlockCache* BlockStoreImpl::GetBlockCache() const {
  return block_cache_.get();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
