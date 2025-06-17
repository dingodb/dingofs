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
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/storage/storage_closure.h"

#include <butil/iobuf.h>

#include "cache/storage/storage.h"
#include "cache/utils/helper.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

PutClosure::PutClosure(ContextSPtr ctx, const BlockKey& key, const Block& block,
                       Storage::PutOption option,
                       blockaccess::BlockAccesser* block_accesser)
    : ctx_(ctx),
      key_(key),
      block_(block),
      option_(option),
      block_accesser_(block_accesser) {}

void PutClosure::Run() {
  auto block = CopyBlock();  // Copy data to continuous memory
  if (option_.async_cache_func) {
    option_.async_cache_func(key_, block);
  }

  auto retry_cb = [this, block](int retcode) {
    if (retcode == 0) {  // Success
      this->OnComplete(retcode);
      return blockaccess::RetryStrategy::kNotRetry;
    }
    return blockaccess::RetryStrategy::kRetry;
  };

  block_accesser_->AsyncPut(key_.StoreKey(), block.buffer.Fetch1(), block_.size,
                            retry_cb);
}

void PutClosure::OnComplete(int retcode) {
  StorageClosure::status() =
      retcode == 0 ? Status::OK() : Status::Internal("storage put failed");
  StorageClosure::Run();
}

Block PutClosure::CopyBlock() {
  char* data = new char[block_.size];
  block_.buffer.CopyTo(data);

  butil::IOBuf iobuf;
  iobuf.append_user_data(data, block_.size, Helper::DeleteBuffer);
  return Block(IOBuffer(iobuf));
}

RangeClosure::RangeClosure(ContextSPtr ctx, const BlockKey& key, off_t offset,
                           size_t length, IOBuffer* buffer,
                           Storage::RangeOption option,
                           blockaccess::BlockAccesser* block_accesser)
    : ctx_(ctx),
      key_(key),
      offset_(offset),
      length_(length),
      buffer_(buffer),
      option_(option),
      block_accesser_(block_accesser) {}

void RangeClosure::Run() {
  char* data = new char[length_];
  butil::IOBuf iobuf;
  iobuf.append_user_data(data, length_, Helper::DeleteBuffer);
  *buffer_ = IOBuffer(iobuf);

  auto retry_cb = [this](int retcode) {
    this->OnComplete(retcode);
    return blockaccess::RetryStrategy::kNotRetry;  // Never retry for range
  };

  block_accesser_->AsyncRange(key_.StoreKey(), offset_, length_,
                              buffer_->Fetch1(), retry_cb);
}

void RangeClosure::OnComplete(int retcode) {
  StorageClosure::status() =
      retcode == 0 ? Status::OK() : Status::Internal("storage range failed");
  StorageClosure::Run();
}

}  // namespace cache
}  // namespace dingofs
