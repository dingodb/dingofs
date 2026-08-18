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

#include "cache/v2/node/service.h"

#include "cache/v2/common/flag_decls.h"
#include "cache/v2/common/status.h"
#include "cache/v2/core/memory/buffer.h"
#include "cache/v2/core/memory/buffer_view.h"
#include "cache/v2/core/net/body.h"
#include "cache/v2/core/runtime/smp.h"
#include "cache/v2/store/local_filesystem.h"
#include "cache/v2/utils/align.h"

namespace dingofs {
namespace cache {
namespace v2 {

CacheService::CacheService(ShardedLocalCache* block_cache)
    : ProtoService(pb::cache::v2::CacheService::descriptor()),
      block_cache_(block_cache) {
  AddMethod("Put", &CacheService::Put);
  AddMethod("Get", &CacheService::Get);
  AddMethod("Prefetch", &CacheService::Prefetch);
  AddMethod("Delete", &CacheService::Delete);
  AddMethod("Ping", &CacheService::Ping);
  AddMethod("GetNodeInfo", &CacheService::GetNodeInfo);
}

Future<> CacheService::Put(Controller* cntl,
                           const pb::cache::v2::PutRequest* request,
                           pb::cache::v2::PutResponse* response) {
  Status status =
      CheckAttachment(request->handle(), cntl->request_attachment_size());
  if (!status.ok()) {
    response->set_status(ToErrno(status));
    co_return;
  }

  const BlockHandle handle = BlockHandle::FromPb(request->handle());
  const BufferView attachment = cntl->request_attachment().view();
  const BufferViews block = {&attachment, 1};
  status =
      co_await block_cache_->Put(handle, block, {.stage = request->stage()});

  response->set_status(ToErrno(status));
  co_return;
}

Future<> CacheService::Get(Controller* cntl,
                           const pb::cache::v2::GetRequest* request,
                           pb::cache::v2::GetResponse* response) {
  const uint64_t offset = request->offset();
  const uint32_t length = request->length();
  Status status = CheckRange(request->handle(), offset, length);
  if (!status.ok()) {
    response->set_status(ToErrno(status));
    co_return;
  }

  const AlignedRange aligned = AlignRequest(offset, length);
  Buffer buffer = Buffer::Alloc(aligned.length);
  if (buffer.empty()) {
    response->set_status(ToErrno(Status::OutOfMemory("buffer pool exhausted")));
    co_return;
  }

  const BlockHandle handle = BlockHandle::FromPb(request->handle());
  status = co_await block_cache_->Get(handle, aligned.offset, aligned.length,
                                      buffer.data());

  response->set_status(ToErrno(status));
  if (!status.ok()) {
    co_return;
  }

  if (aligned.offset != offset) {
    buffer.PopFront(offset - aligned.offset);
  }
  if (aligned.length != length) {
    buffer.PopBack(aligned.offset + aligned.length - (offset + length));
  }
  cntl->response_attachment() = std::move(buffer);
  co_return;
}

Future<> CacheService::Prefetch(Controller* /*cntl*/,
                                const pb::cache::v2::PrefetchRequest* request,
                                pb::cache::v2::PrefetchResponse* response) {
  Status status = CheckHandle(request->handle());
  if (!status.ok()) {
    response->set_status(ToErrno(status));
    co_return;
  }

  const BlockHandle handle = BlockHandle::FromPb(request->handle());
  status = co_await block_cache_->Prefetch(handle);

  response->set_status(ToErrno(status));
  co_return;
}

Future<> CacheService::Delete(Controller* /*cntl*/,
                              const pb::cache::v2::DeleteRequest* request,
                              pb::cache::v2::DeleteResponse* response) {
  Status status = CheckHandle(request->handle());
  if (!status.ok()) {
    response->set_status(ToErrno(status));
    co_return;
  }

  const BlockHandle handle = BlockHandle::FromPb(request->handle());
  status = co_await block_cache_->Delete(handle);

  response->set_status(ToErrno(status));
  co_return;
}

Future<> CacheService::Ping(Controller* /*cntl*/,
                            const pb::cache::v2::PingRequest*,
                            pb::cache::v2::PingResponse*) {
  return MakeReadyFuture<>();
}

Future<> CacheService::GetNodeInfo(
    Controller* /*cntl*/, const pb::cache::v2::GetNodeInfoRequest*,
    pb::cache::v2::GetNodeInfoResponse* response) {
  response->set_status(pb::error::OK);
  response->set_id(FLAGS_id);
  response->set_shards(ShardCount());
  response->set_rdma_enabled(FLAGS_rdma);
  co_return;
}

CacheService::AlignedRange CacheService::AlignRequest(uint64_t offset,
                                                      uint32_t length) {
  const uint64_t start = AlignDown<uint64_t>(offset, kBlockAlign);
  return {.offset = start,
          .length = static_cast<uint32_t>(
              AlignUp<uint64_t>(offset - start + length, kBlockAlign))};
}

Status CacheService::CheckHandle(const pb::cache::v2::BlockHandle& handle) {
  if (handle.size() == 0 || handle.size() > kMaxBodyBytes) {
    return Status::InvalidParam("block size out of range");
  }
  return Status::OK();
}

Status CacheService::CheckAttachment(const pb::cache::v2::BlockHandle& handle,
                                     uint32_t attachment_size) {
  Status status = CheckHandle(handle);
  if (!status.ok()) {
    return status;
  } else if (attachment_size != handle.size()) {
    return Status::InvalidParam("attachment size != block size");
  }
  return Status::OK();
}

Status CacheService::CheckRange(const pb::cache::v2::BlockHandle& handle,
                                uint64_t offset, uint32_t length) {
  Status status = CheckHandle(handle);
  if (!status.ok()) {
    return status;
  }

  if (length == 0) {
    return Status::InvalidParam("length is zero");
  } else if (offset > handle.size() || length > handle.size() - offset) {
    return Status::InvalidParam("range beyond block");
  }
  return Status::OK();
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
