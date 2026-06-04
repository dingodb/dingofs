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

/*
 * Project: DingoFS
 * Created Date: 2025-06-17
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/factory.h"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <vector>

#include <butil/iobuf.h>
#include <glog/logging.h>

#include "cache/benchmark/option.h"
#include "cache/iutil/string_util.h"
#include "cache/remotecache/rdma_buffer_manager.h"
#include "common/block/block_handle.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

namespace {

char PatternAt(uint64_t pos, uint64_t worker_idx) {
  return static_cast<char>((pos + worker_idx * 17) % 251);
}

void FillPattern(char* data, uint64_t length, uint64_t worker_idx) {
  for (uint64_t i = 0; i < length; ++i) {
    data[i] = PatternAt(i, worker_idx);
  }
}

bool VerifyMarkers(const char* data, uint64_t length, uint64_t worker_idx) {
  if (length == 0) {
    return true;
  }
  const uint64_t positions[] = {0, length / 2, length - 1};
  for (uint64_t pos : positions) {
    if (data[pos] != PatternAt(pos, worker_idx)) {
      return false;
    }
  }
  return true;
}

bool VerifyFull(const char* data, uint64_t length, uint64_t worker_idx) {
  for (uint64_t i = 0; i < length; ++i) {
    if (data[i] != PatternAt(i, worker_idx)) {
      return false;
    }
  }
  return true;
}

}  // namespace

BlockKeyIterator::BlockKeyIterator(uint64_t idx, uint64_t blksize,
                                   uint64_t blocks)
    : idx_(idx),
      blksize_(blksize),
      blocks_(blocks),
      id_(FLAGS_start_block_id + (idx_ * blocks_)),
      index_(0),
      allocated_(0) {}

void BlockKeyIterator::SeekToFirst() {
  id_ = FLAGS_start_block_id + (idx_ * blocks_);
  index_ = 0;
  allocated_ = 0;
}

bool BlockKeyIterator::Valid() const { return allocated_ < blocks_; }

void BlockKeyIterator::Next() {
  allocated_++;
  index_++;
  if (index_ == kBlocksPerChunk) {
    id_++;
    index_ = 0;
  }
}

BlockKey BlockKeyIterator::Key() const {
  return BlockKey(id_, static_cast<uint32_t>(index_),
                  static_cast<uint32_t>(blksize_));
}

constexpr uint64_t pagesize = 64 * 1024;

IOBuffer NewBlock(uint64_t blksize) {
  butil::IOBuf pages;
  auto length = blksize;
  while (length > 0) {
    auto size = std::min(pagesize, length);
    char* data = new char[size];
    std::memset(data, 0, size);
    pages.append_user_data(data, size, iutil::DeleteBuffer);

    length -= size;
  }
  return IOBuffer(pages);
}

IOBuffer NewRangeBuffer(uint64_t length) {
  if (FLAGS_use_rdma) {
    auto& mgr = RdmaBufferManager::GetInstance();
    if (mgr.Enabled()) {
      auto out = mgr.NewBuffer(length);
      if (out.Size() != 0) {
        return out;
      }
    }
  }

  IOBuffer out;
  out.AppendUserData(new char[length], length,
                     [](void* p) { delete[] static_cast<char*>(p); });
  return out;
}

Status TaskContext::Init(uint64_t idx) {
  worker_idx = idx;
  if (!FLAGS_bench_rdma_registered_buffers || !FLAGS_use_rdma) {
    request_body = NewBlock(FLAGS_blksize);
    return Status::OK();
  }

  // Registered request buffer from the shared manager (meta=rkey), so the cache
  // layer advertises it for a zero-copy server RDMA-read on Put/Cache.
  auto& mgr = RdmaBufferManager::GetInstance();
  if (!mgr.Enabled()) {
    return Status::Internal("client rdma buffer pool is unavailable");
  }
  request_body = mgr.NewBuffer(FLAGS_blksize);
  if (request_body.Size() == 0) {
    return Status::Internal("alloc client rdma buffer failed");
  }
  FillPattern(request_body.Fetch1(), FLAGS_blksize, worker_idx);
  return Status::OK();
}

bool TaskContext::VerifyRange(const IOBuffer& buffer, uint64_t length) const {
  if (FLAGS_verify == "none") {
    return true;
  }
  if (buffer.Size() < length) {
    return false;
  }

  char* data = nullptr;
  std::vector<char> copied;
  auto iovs = buffer.Fetch();
  if (iovs.size() == 1) {
    data = static_cast<char*>(iovs[0].iov_base);
  } else {
    copied.resize(length);
    buffer.CopyTo(copied.data(), length);
    data = copied.data();
  }

  if (FLAGS_verify == "markers") {
    return VerifyMarkers(data, length, worker_idx);
  } else if (FLAGS_verify == "full") {
    return VerifyFull(data, length, worker_idx);
  }
  LOG(ERROR) << "Unknown verify mode: " << FLAGS_verify;
  return false;
}

PutTaskFactory::PutTaskFactory(BlockCacheSPtr block_cache)
    : block_cache_(block_cache) {}

Task PutTaskFactory::GenTask(const BlockKey& key, TaskContext* context) {
  return [this, key, context]() { return Put(key, context); };
}

TaskResult PutTaskFactory::Put(const BlockKey& key, TaskContext* context) {
  auto option = PutOption();
  option.writeback = FLAGS_writeback;

  BlockHandle handle(static_cast<uint32_t>(FLAGS_fsid), key);
  auto status = block_cache_->Put(handle, *context->RequestBody(), option);
  if (!status.ok()) {
    LOG(ERROR) << "Put block (key= " << key.Filename()
               << ") failed: " << status.ToString();
  }
  return TaskResult{status, status.ok() ? FLAGS_blksize : 0};
}

CacheTaskFactory::CacheTaskFactory(BlockCacheSPtr block_cache)
    : block_cache_(block_cache) {}

Task CacheTaskFactory::GenTask(const BlockKey& key, TaskContext* context) {
  return [this, key, context]() { return Cache(key, context); };
}

TaskResult CacheTaskFactory::Cache(const BlockKey& key, TaskContext* context) {
  BlockHandle handle(static_cast<uint32_t>(FLAGS_fsid), key);
  auto status = block_cache_->Cache(handle, *context->RequestBody());
  if (!status.ok()) {
    LOG(ERROR) << "Cache block (key= " << key.Filename()
               << ") failed: " << status.ToString();
  }
  return TaskResult{status, status.ok() ? FLAGS_blksize : 0};
}

RangeTaskFactory::RangeTaskFactory(BlockCacheSPtr block_cache)
    : block_cache_(block_cache) {}

Task RangeTaskFactory::GenTask(const BlockKey& key, TaskContext* context) {
  return [this, key, context]() { return RangeAll(key, context); };
}

TaskResult RangeTaskFactory::RangeAll(const BlockKey& key,
                                      TaskContext* context) {
  if (FLAGS_length == 0) {
    return TaskResult{Status::InvalidParam("range length is zero"), 0};
  }

  uint64_t done = 0;
  IOBuffer buffer;
  off_t offset = FLAGS_offset;
  uint64_t remaining = FLAGS_blksize - FLAGS_offset;
  while (remaining > 0) {
    size_t length = std::min<uint64_t>(remaining, FLAGS_length);
    // Range requires a caller-allocated destination (single contiguous block).
    // RDMA can write directly into a registered benchmark buffer and avoid the
    // client-side staging copy.
    IOBuffer out = NewRangeBuffer(length);
    auto status = Range(key, offset, length, &out);
    if (!status.ok()) {
      return TaskResult{status, done};
    }

    offset += length;
    remaining -= length;
    done += length;
    buffer.Append(&out);
  }

  if (!context->VerifyRange(buffer, done)) {
    return TaskResult{Status::Internal("range verification failed"), done};
  }
  return TaskResult{Status::OK(), done};
}

Status RangeTaskFactory::Range(const BlockKey& key, off_t offset,
                               size_t length, IOBuffer* buffer) {
  auto option = RangeOption();
  option.retrieve_storage = FLAGS_retrive;
  option.block_whole_length = FLAGS_blksize;

  BlockHandle handle(static_cast<uint32_t>(FLAGS_fsid), key);
  auto status =
      block_cache_->Range(handle, offset, length, buffer, option);

  if (!status.ok()) {
    LOG(ERROR) << "Range block (key=" << key.Filename()
               << ") failed: " << status.ToString();
  }
  return status;
}

TaskFactoryUPtr NewFactory(BlockCacheSPtr block_cache, const std::string& op) {
  if (op == "put") {
    return std::make_unique<PutTaskFactory>(block_cache);
  } else if (op == "cache") {
    return std::make_unique<CacheTaskFactory>(block_cache);
  } else if (op == "range") {
    return std::make_unique<RangeTaskFactory>(block_cache);
  }

  CHECK(false) << "Unknown operation: " << op;
}

}  // namespace cache
}  // namespace dingofs
