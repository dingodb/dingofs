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

#include "blockcache/tools/benchmark/factory.h"

#include <glog/logging.h>

#include <utility>

#include "blockcache/tools/benchmark/option.h"

namespace dingofs {
namespace blockcache {

BlockKeyIterator::BlockKeyIterator(uint64_t idx, uint64_t fsid,
                                   uint64_t blksize, uint64_t blocks)
    : idx_(idx),
      fsid_(fsid),
      blksize_(blksize),
      blocks_(blocks),
      chunkid_((idx * blocks) + 1),
      blockidx_(0),
      allocated_(0) {}

void BlockKeyIterator::SeekToFirst() {
  chunkid_ = (idx_ * blocks_) + 1;
  blockidx_ = 0;
  allocated_ = 0;
}

bool BlockKeyIterator::Valid() const { return allocated_ < blocks_; }

void BlockKeyIterator::Next() {
  allocated_++;
  blockidx_++;
  if (blockidx_ == kBlocksPerChunk) {
    chunkid_++;
    blockidx_ = 0;
  }
}

BlockHandle BlockKeyIterator::Key() const {
  return {.fs_id = fsid_,
          .id = chunkid_,
          .index = static_cast<uint32_t>(blockidx_),
          .size = static_cast<uint32_t>(blksize_)};
}

PutTaskFactory::PutTaskFactory(BlockCacheImpl* block_cache)
    : block_cache_(block_cache) {}

bool PutTaskFactory::SubmitTask(Slot* slot, AsyncCallback cb) {
  slot->view = BufferView(slot->data, slot->key.size);
  return block_cache_->AsyncPut(slot->key, BufferViews(&slot->view, 1),
                                std::move(cb), {.stage = FLAGS_stage});
}

uint64_t PutTaskFactory::BytesPerOp(const Slot* /*slot*/) const {
  return FLAGS_blksize;
}

GetTaskFactory::GetTaskFactory(BlockCacheImpl* block_cache)
    : block_cache_(block_cache) {}

bool GetTaskFactory::SubmitTask(Slot* slot, AsyncCallback cb) {
  return block_cache_->AsyncGet(
      slot->key, slot->offset, static_cast<uint32_t>(slot->length), slot->data,
      std::move(cb), {.retrieve_storage = FLAGS_retrieve_storage});
}

uint64_t GetTaskFactory::BytesPerOp(const Slot* slot) const {
  return slot->length;
}

DeleteTaskFactory::DeleteTaskFactory(BlockCacheImpl* block_cache)
    : block_cache_(block_cache) {}

bool DeleteTaskFactory::SubmitTask(Slot* slot, AsyncCallback cb) {
  return block_cache_->AsyncDelete(slot->key, std::move(cb));
}

uint64_t DeleteTaskFactory::BytesPerOp(const Slot* /*slot*/) const { return 0; }

TaskFactoryUPtr NewFactory(BlockCacheImpl* block_cache, const std::string& op) {
  if (op == "put") {
    return std::make_unique<PutTaskFactory>(block_cache);
  } else if (op == "get") {
    return std::make_unique<GetTaskFactory>(block_cache);
  } else if (op == "delete") {
    return std::make_unique<DeleteTaskFactory>(block_cache);
  }

  CHECK(false) << "Unknown operation: " << op;
}

}  // namespace blockcache
}  // namespace dingofs
