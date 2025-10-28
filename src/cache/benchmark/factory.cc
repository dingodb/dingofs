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

#include "cache/benchmark/option.h"
#include "cache/utils/context.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

BlockKeyIterator::BlockKeyIterator(uint64_t idx, uint64_t fsid, uint64_t ino,
                                   uint64_t blksize, uint64_t blocks)
    : idx_(idx),
      fsid_(fsid),
      ino_(ino),
      blksize_(blksize),
      blocks_(blocks),
      chunkid_((idx_ * blocks) + 1),
      blockidx_(0),
      allocated_(0) {}

void BlockKeyIterator::SeekToFirst() {
  chunkid_ = idx_ * blocks_ + 1;
  blockidx_ = 0;
}

bool BlockKeyIterator::Valid() const { return allocated_ < blocks_; }

void BlockKeyIterator::Next() {
  allocated_++;

  if (allocated_ == blocks_) {
    SeekToFirst();
    return;
  }

  blockidx_++;
  if (blockidx_ == kBlocksPerChunk) {
    chunkid_++;
    blockidx_ = 0;
  }
}

BlockKey BlockKeyIterator::Key() const {
  return BlockKey(fsid_, ino_, chunkid_, blockidx_, 0);
}

constexpr uint64_t pagesize = 64 * 1024;

Block NewBlock(uint64_t blksize) {
  butil::IOBuf pages;
  auto length = blksize;
  while (length > 0) {
    auto size = std::min(pagesize, length);
    char* data = new char[size];
    std::memset(data, 0, size);
    pages.append_user_data(data, size, Helper::DeleteBuffer);

    length -= size;
  }
  return Block(IOBuffer(pages));
}

PutTaskFactory::PutTaskFactory(BlockCacheSPtr block_cache)
    : block_cache_(block_cache), block_(NewBlock(FLAGS_blksize)) {}

Task PutTaskFactory::GenTask(const BlockKey& key) {
  return [this, key]() { Put(key); };
}

void PutTaskFactory::Put(const BlockKey& key) {
  auto option = PutOption();
  option.writeback = FLAGS_writeback;

  auto status = block_cache_->Put(NewContext(), key, block_, option);
  if (!status.ok()) {
    LOG(ERROR) << "Put block (key= " << key.Filename()
               << ") failed: " << status.ToString();
  }
}

RangeTaskFactory::RangeTaskFactory(BlockCacheSPtr block_cache)
    : block_cache_(block_cache) {}

Task RangeTaskFactory::GenTask(const BlockKey& key) {
  return [this, key]() { RangeAll(key); };
}

void RangeTaskFactory::RangeAll(const BlockKey& key) {
  IOBuffer buffer;
  off_t offset = FLAGS_offset;
  size_t blksize = FLAGS_blksize;
  while (blksize) {
    size_t length = std::min(FLAGS_blksize - offset, FLAGS_length);
    Range(key, offset, length, &buffer);

    offset += length;
    blksize -= length;
  }
}

void RangeTaskFactory::Range(const BlockKey& key, off_t offset, size_t length,
                             IOBuffer* buffer) {
  auto option = RangeOption();
  option.retrive = FLAGS_retrive;
  option.block_size = FLAGS_blksize;
  auto status =
      block_cache_->Range(NewContext(), key, offset, length, buffer, option);

  if (!status.ok()) {
    LOG(ERROR) << "Range block (key=" << key.Filename()
               << ") failed: " << status.ToString();
  }
}

TaskFactoryUPtr NewFactory(BlockCacheSPtr block_cache, const std::string& op) {
  if (op == "put") {
    return std::make_unique<PutTaskFactory>(block_cache);
  } else if (op == "range") {
    return std::make_unique<RangeTaskFactory>(block_cache);
  }

  CHECK(false) << "Unknown operation: " << op;
}

}  // namespace cache
}  // namespace dingofs
