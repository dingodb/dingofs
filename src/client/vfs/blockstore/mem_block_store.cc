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

#include "client/vfs/blockstore/mem_block_store.h"

#include <google/protobuf/descriptor.pb.h>

#include "client/common/const.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/static_mem.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("MemBlockStore::" + std::string(__FUNCTION__))

static void NoopDeleter(void* data) {}

MemBlockStore::MemBlockStore(VFSHub* hub, std::string uuid)
    : hub_(hub), uuid_(std::move(uuid)) {}

Status MemBlockStore::Start() {
  if (started_) {
    return Status::OK();
  }

  started_.store(true);
  return Status::OK();
}

void MemBlockStore::Shutdown() {
  if (!started_) {
    return;
  }

  started_.store(false);
}

void MemBlockStore::DoRangeAsync(BlockKey key, uint64_t offset, uint64_t length,
                                 IOBuffer* buffer, StatusCallback callback) {
  (void)key;
  (void)offset;
  CHECK_GE(kStaticMemSize, length);
  buffer->AppendUserData(const_cast<char*>(kStaticMemory), length, NoopDeleter);
  callback(Status::OK());
}

void MemBlockStore::RangeAsync(ContextSPtr ctx, RangeReq req,
                               StatusCallback callback) {
  auto span = hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                      METHOD_NAME(), ctx);
  auto wrapper = [this, cb = std::move(callback),
                  span_ptr = span.release()](Status s) {
    // capture this ptr to extend its lifetime
    std::unique_ptr<ITraceSpan> scoped_span(span_ptr);
    scoped_span->End();
    // dedicated use ctx for callback
    cb(s);
  };

  DoRangeAsync(req.block, req.offset, req.length, req.data, std::move(wrapper));
}

void MemBlockStore::PutAsync(ContextSPtr ctx, PutReq req,
                             StatusCallback callback) {
  (void)req;
  auto span = hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                      METHOD_NAME(), ctx);
  auto wrapper = [this, cb = std::move(callback),
                  span_ptr = span.release()](Status s) {
    // capture this ptr to extend its lifetime
    std::unique_ptr<ITraceSpan> scoped_span(span_ptr);
    scoped_span->End();
    cb(s);
  };

  wrapper(Status::OK());
}

void MemBlockStore::PrefetchAsync(ContextSPtr ctx, PrefetchReq req,
                                  StatusCallback callback) {
  (void)req;
  auto span = hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                      METHOD_NAME(), ctx);
  auto wrapper = [this, cb = std::move(callback),
                  span_ptr = span.release()](Status s) {
    // capture this ptr to extend its lifetime
    std::unique_ptr<ITraceSpan> scoped_span(span_ptr);
    scoped_span->End();
    cb(s);
  };

  wrapper(Status::OK());
}

// utility
bool MemBlockStore::EnableCache() const { return false; }

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
