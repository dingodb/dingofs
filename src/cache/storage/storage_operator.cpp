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

#include "cache/storage/storage_operator.h"

#include <cstddef>

namespace dingofs {
namespace cache {

using RetryStrategy = StorageOperator::RetryStrategy;

PutOp::PutOp(StorageClosure* closure) : closure_(closure), data_(nullptr) {}

// TODO(Wine93): use thread pool to copy buffer
char* PutOp::OnPrepare() {
  auto& iobuf = closure_->buffer_in.IOBuf();
  if (iobuf.backing_block_num() > 1) {  // with copying
    data_ = new char[closure_->length];
  }
  return (char*)iobuf.fetch(&data_, iobuf.length());
}

RetryStrategy PutOp::OnRetry(bool succ) {
  if (!succ) {  // retry forever until success
    LOG(ERROR) << "Put object to storage failed: key = " << closure_->key;
    return RetryStrategy::kRetry;
  }
  return RetryStrategy::kNotRetry;
}

void PutOp::OnComplete(bool succ) {
  CHECK_EQ(succ, true);
  closure_->Run();

  delete[] data_;
  delete this;
}

RangeOp::RangeOp(StorageClosure* closure) : closure_(closure), data_(nullptr) {}

RetryStrategy RangeOp::OnRetry(bool /*succ*/) {
  return RetryStrategy::kNotRetry;
}

char* RangeOp::OnPrepare() {
  data_ = new char[closure_->length];
  return data_;
}

void RangeOp::OnComplete(bool succ) {
  if (!succ) {
    LOG(ERROR) << "Get object from storage failed: key = " << closure_->key;
    delete[] data_;
  } else {  // release data later
    butil::IOBuf iobuf;
    iobuf.append_user_data(data_, closure_->length, [](void* data) {
      delete[] static_cast<char*>(data);
    });
    *closure_->buffer_out = IOBuffer(iobuf);
  }

  closure_->Run();
  delete this;
}

}  // namespace cache
}  // namespace dingofs
