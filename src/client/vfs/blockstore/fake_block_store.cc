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

#include "client/vfs/blockstore/fake_block_store.h"

#include <google/protobuf/descriptor.pb.h>

#include "client/common/const.h"
#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {

static constexpr int64_t kStaticMemSize = 4 * 1024 * 1024;  // 4MB
static char kStaticMemory[kStaticMemSize] = {0};

#define METHOD_NAME() ("FakeBlockStore::" + std::string(__FUNCTION__))

static void NoopDeleter(void* data) {}

FakeBlockStore::FakeBlockStore(VFSHub* hub, std::string uuid)
    : hub_(hub), uuid_(std::move(uuid)) {}

Status FakeBlockStore::Start() {
  if (started_) {
    return Status::OK();
  }

  started_.store(true);
  return Status::OK();
}

void FakeBlockStore::Shutdown() {
  if (!started_) {
    return;
  }

  started_.store(false);
}

void FakeBlockStore::DoRangeAsync(BlockKey key, uint64_t offset,
                                  uint64_t length, IOBuffer* buffer,
                                  StatusCallback callback) {
  (void)key;
  (void)offset;
  CHECK_GE(kStaticMemSize, length);
  buffer->AppendUserData(kStaticMemory, length, NoopDeleter);
  callback(Status::OK());
}

void FakeBlockStore::RangeAsync(ContextSPtr ctx, RangeReq req,
                                StatusCallback callback) {
  auto span = hub_->GetTraceManager().StartChildSpan(
      "FakeBlockStore::PrefetchAsync", ctx->GetTraceSpan());
  auto wrapper = [this, cb = std::move(callback), span](Status s) {
    SpanScope::End(span);
    // dedicated use ctx for callback
    cb(s);
  };

  DoRangeAsync(req.block, req.offset, req.length, req.data, std::move(wrapper));
}

void FakeBlockStore::PutAsync(ContextSPtr ctx, PutReq req,
                              StatusCallback callback) {
  (void)req;
  auto span = hub_->GetTraceManager().StartChildSpan("FakeBlockStore::PutAsync",
                                                     ctx->GetTraceSpan());
  auto wrapper = [this, cb = std::move(callback), span](Status s) {
    SpanScope::End(span);
    cb(s);
  };

  wrapper(Status::OK());
}

void FakeBlockStore::PrefetchAsync(ContextSPtr ctx, PrefetchReq req,
                                   StatusCallback callback) {
  (void)req;

  auto span = hub_->GetTraceManager().StartChildSpan(
      "FakeBlockStore::PrefetchAsync", ctx->GetTraceSpan());
  auto wrapper = [this, cb = std::move(callback), span](Status s) {
    SpanScope::End(span);
    cb(s);
  };

  wrapper(Status::OK());
}

// utility
bool FakeBlockStore::EnableCache() const { return false; }

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
