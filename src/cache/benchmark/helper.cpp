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
 * Created Date: 2025-06-04
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/helper.h"

#include <optional>

#include "base/time/time.h"
#include "cache/config/benchmark.h"
#include "cache/config/config.h"

namespace dingofs {
namespace cache {

static constexpr uint64_t kBlocksPerChunk = 16;

blockaccess::BlockAccessOptions NewBlockAccessOptions() {
  blockaccess::S3Options s3_option;
  s3_option.s3_info.ak = FLAGS_s3_ak;
  s3_option.s3_info.sk = FLAGS_s3_sk;
  s3_option.s3_info.endpoint = FLAGS_s3_endpoint;
  s3_option.s3_info.bucket_name = FLAGS_s3_bucket;

  blockaccess::BlockAccessOptions block_access_option;
  block_access_option.type = blockaccess::AccesserType::kS3;
  block_access_option.s3_options = s3_option;

  return block_access_option;
}

static void AppendOnePage(butil::IOBuf* buffer, size_t size) {
  char* data = new char[size];
  std::memset(data, 0, size);
  buffer->append_user_data(
      data, size, [](void* data) { delete[] static_cast<char*>(data); });
}

Block NewOneBlock() {
  butil::IOBuf pages;
  auto page_size = FLAGS_page_size;
  auto length = FLAGS_op_blksize;
  while (length > 0) {
    auto size = std::min(page_size, length);
    AppendOnePage(&pages, size);
    length -= size;
  }

  return Block(IOBuffer(pages));
}

BlockKeyFactory::BlockKeyFactory(uint64_t worker_id, uint64_t blocks)
    : fs_id_(FLAGS_fsid),
      ino_(FLAGS_ino),
      chunkid_(worker_id * blocks + 1),
      block_index_(0),
      allocated_blocks_(0),
      total_blocks_(blocks) {}

std::optional<BlockKey> BlockKeyFactory::Next() {
  if (allocated_blocks_ == total_blocks_) {
    return std::nullopt;
  }

  BlockKey key(fs_id_, ino_, chunkid_, block_index_++, 0);
  if (block_index_ == kBlocksPerChunk) {
    block_index_ = 0;
    chunkid_++;
  }

  allocated_blocks_++;
  return key;
}

}  // namespace cache
}  // namespace dingofs
