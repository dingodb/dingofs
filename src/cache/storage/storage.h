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

#ifndef DINGOFS_SRC_CACHE_STORAGE_STORAGE_H_
#define DINGOFS_SRC_CACHE_STORAGE_STORAGE_H_

#include "blockaccess/block_accesser.h"
#include "cache/common/common.h"
#include "cache/storage/closure.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

enum class OperatorType : uint8_t {
  kPut = 0,
  kRange = 1,
};

struct StorageClosure : public Closure {
  StorageClosure(OperatorType optype, const std::string& key, off_t offset,
                 size_t length, const IOBuffer& buffer_in, IOBuffer* buffer_out)
      : optype(optype),
        key(key),
        offset(offset),
        length(length),
        buffer_in(buffer_in),
        buffer_out(buffer_out) {}

  void Run() override {
    std::unique_lock<BthreadMutex> lk(mutex);
    cond.notify_all();
  }

  void Wait() {
    std::unique_lock<BthreadMutex> lk(mutex);
    cond.wait(lk);
  }

  OperatorType optype;
  std::string key;
  off_t offset;
  size_t length;
  IOBuffer buffer_in;
  IOBuffer* buffer_out;
  BthreadMutex mutex;
  BthreadConditionVariable cond;
};

inline StorageClosure StoragePutClosure(const std::string& key,
                                        const IOBuffer& buffer) {
  return StorageClosure(OperatorType::kPut, key, 0, buffer.Size(), buffer,
                        nullptr);
}

inline StorageClosure StorageRangeClosure(const std::string& key, off_t offset,
                                          size_t length, IOBuffer* buffer) {
  return StorageClosure(OperatorType::kRange, key, offset, length, IOBuffer(),
                        buffer);
}

inline bool IsPutOp(StorageClosure* closure) {
  return closure->optype == OperatorType::kPut;
}

inline bool IsRangeOp(StorageClosure* closure) {
  return closure->optype == OperatorType::kRange;
}

class Storage {
 public:
  virtual ~Storage() = default;

  virtual Status Init() = 0;
  virtual Status Shutdown() = 0;

  virtual Status Put(const std::string& key, const IOBuffer& buffer) = 0;
  virtual Status Range(const std::string& key, off_t offset, size_t length,
                       IOBuffer* buffer) = 0;
};

using StorageSPtr = std::shared_ptr<Storage>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_STORAGE_H_
