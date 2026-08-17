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

#ifndef DINGOFS_CACHE_V2_BLOCK_RETRIEVER_H_
#define DINGOFS_CACHE_V2_BLOCK_RETRIEVER_H_

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <cstdlib>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "cache/v2/core/memory/buffer_view.h"
#include "cache/v2/common/status.h"
#include "cache/v2/object/object.h"
#include "cache/v2/utils/align.h"
#include "cache/v2/utils/gate.h"

namespace dingofs {
namespace cache {
namespace v2 {

class SharedBlock {
 public:
  static SharedBlock Alloc(size_t size) {
    char* base = AlignedAlloc(size);
    if (base == nullptr) {
      return {};
    }
    return SharedBlock(new Control{.base = base, .refs = {1}}, size);
  }

  SharedBlock() = default;
  ~SharedBlock() { Release(); }

  SharedBlock(const SharedBlock& o) : ctl_(o.ctl_), size_(o.size_) { Retain(); }
  SharedBlock& operator=(const SharedBlock& o) {
    if (this != &o) {
      Release();
      ctl_ = o.ctl_;
      size_ = o.size_;
      Retain();
    }
    return *this;
  }

  SharedBlock(SharedBlock&& o) noexcept
      : ctl_(std::exchange(o.ctl_, nullptr)),
        size_(std::exchange(o.size_, 0)) {}
  SharedBlock& operator=(SharedBlock&& o) noexcept {
    if (this != &o) {
      Release();
      ctl_ = std::exchange(o.ctl_, nullptr);
      size_ = std::exchange(o.size_, 0);
    }
    return *this;
  }

  char* data() const { return ctl_ == nullptr ? nullptr : ctl_->base; }
  size_t size() const { return size_; }
  bool empty() const { return ctl_ == nullptr; }
  BufferView view() const {
    return BufferView(data(), static_cast<uint32_t>(size_));
  }

 private:
  struct Control {
    char* base;
    std::atomic<uint32_t> refs;
  };

  SharedBlock(Control* ctl, size_t size) : ctl_(ctl), size_(size) {}

  void Retain() {
    if (ctl_ != nullptr) {
      ctl_->refs.fetch_add(1, std::memory_order_relaxed);
    }
  }

  void Release() {
    Control* ctl = std::exchange(ctl_, nullptr);
    size_ = 0;
    if (ctl != nullptr &&
        ctl->refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      std::free(ctl->base);
      delete ctl;
    }
  }

  Control* ctl_ = nullptr;
  size_t size_ = 0;
};

class InflightTracker {
 public:
  using Waiters = std::vector<Promise<StatusOr<SharedBlock>>>;

  Future<StatusOr<SharedBlock>> GetOrCreate(BlockHandle handle, bool* created);
  bool Create(BlockHandle handle);
  Waiters TakeWaiters(BlockHandle handle);

 private:
  absl::flat_hash_map<BlockHandle, Waiters, BlockHandleHash> inflight_;
};

using CacheFunc =
    std::function<Future<>(BlockHandle handle, SharedBlock block)>;

class ObjectRetriever final {
 public:
  ObjectRetriever(ObjectStorage* storage, CacheFunc cache_func);

  Future<> Start();
  Future<> Shutdown();

  Future<Status> GetPart(BlockHandle handle, uint64_t offset, uint32_t length,
                         char* buffer);
  Future<StatusOr<SharedBlock>> GetWholeAndCache(BlockHandle handle);
  Future<Status> Prefetch(BlockHandle handle);

 private:
  void StartRetrieval(BlockHandle handle);
  Future<> RunRetrieval(BlockHandle handle, Gate::Holder holder);

  ObjectStorage* storage_;
  CacheFunc cache_func_;
  InflightTracker inflight_;
  Gate gate_;
};

using ObjectRetrieverUPtr = std::unique_ptr<ObjectRetriever>;

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_BLOCK_RETRIEVER_H_
