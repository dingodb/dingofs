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
#include <google/protobuf/descriptor.pb.h>

#include "cache/tiercache/tier_block_cache.h"
#include "client/common/const.h"
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

  auto wrapper = [this, start_us, req, cb = std::move(callback),
                  span](Status s) {
    BlockStoreAccessLogGuard log(start_us, [&]() {
      return fmt::format("range_async ({}, {}, [{}-{})) : {}",
                         req.block.Filename(), req.length, req.offset,
                         (req.offset + req.length), s.ToString());
    });
    SpanScope::End(span);
    // dedicated use ctx for callback
    cb(s);
  };

  cache::RangeOption option;
  option.retrive = true;
  option.block_size = req.block_size;

  block_cache_->AsyncRange(cache::NewContext(), req.block, req.offset,
                           req.length, req.data, std::move(wrapper), option);
}

void BlockStoreImpl::PutAsync(ContextSPtr ctx, PutReq req,
                              StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan("BlockStoreImpl::PutAsync",
                                                     ctx->GetTraceSpan());

  int64_t start_us = butil::cpuwide_time_us();

  auto wrapper = [this, start_us, req, cb = std::move(callback),
                  span](Status s) {
    BlockStoreAccessLogGuard log(start_us, [&]() {
      return fmt::format("put_async ({}, {}) : {}", req.block.Filename(),
                         req.data.Size(), s.ToString());
    });
    SpanScope::End(span);
    cb(s);
  };

  cache::PutOption option{.writeback = req.write_back};

  block_cache_->AsyncPut(cache::NewContext(), req.block, cache::Block(req.data),
                         std::move(wrapper), option);
}

void BlockStoreImpl::PrefetchAsync(ContextSPtr ctx, PrefetchReq req,
                                   StatusCallback callback) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "BlockStoreImpl::PrefetchAsync", ctx->GetTraceSpan());
  if (block_cache_->IsCached(req.block)) {
    callback(Status::OK());
    return;
  }

  int64_t start_us = butil::cpuwide_time_us();

  auto wrapper = [this, start_us, req, cb = std::move(callback),
                  span](Status s) {
    BlockStoreAccessLogGuard log(start_us, [&]() {
      return fmt::format("prefetch_async ({}, {}) : {}", req.block.Filename(),
                         req.block_size, s.ToString());
    });
    SpanScope::End(span);
    cb(s);
  };

  // transfer ownership of block_data to BlockDataFlushed
  block_cache_->AsyncPrefetch(cache::NewContext(), req.block, req.block_size,
                              std::move(wrapper));
}

// utility
bool BlockStoreImpl::EnableCache() const { return block_cache_->EnableCache(); }

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
