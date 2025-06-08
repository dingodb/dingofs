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
 * Created Date: 2025-05-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_STORAGE_STORAGE_OPERATOR_H_
#define DINGOFS_SRC_CACHE_STORAGE_STORAGE_OPERATOR_H_

#include "blockaccess/block_accesser.h"
#include "cache/storage/storage.h"

namespace dingofs {
namespace cache {

class StorageOperator {
 public:
  using RetryStrategy = blockaccess::RetryStrategy;

  virtual ~StorageOperator() = default;

  virtual char* OnPrepare() = 0;
  virtual RetryStrategy OnRetry(bool succ) = 0;
  virtual void OnComplete(bool succ) = 0;
};

class PutOp final : public StorageOperator {
 public:
  explicit PutOp(StorageClosure* closure);

  char* OnPrepare() override;
  RetryStrategy OnRetry(bool succ) override;
  void OnComplete(bool succ) override;

 private:
  char* data_;
  StorageClosure* closure_;
};

class RangeOp final : public StorageOperator {
 public:
  explicit RangeOp(StorageClosure* closure);

  char* OnPrepare() override;
  RetryStrategy OnRetry(bool succ) override;
  void OnComplete(bool succ) override;

 private:
  char* data_;
  StorageClosure* closure_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_STORAGE_OPERATOR_H_
