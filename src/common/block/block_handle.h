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

/*
 * Project: DingoFS
 * Created Date: 2026-05-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_COMMON_BLOCK_BLOCK_HANDLE_H_
#define DINGOFS_SRC_COMMON_BLOCK_BLOCK_HANDLE_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>
#include <variant>

#include "common/block/block_key.h"
#include "common/block/tensor_key.h"

namespace dingofs {

class BlockHandle {
 public:
  BlockHandle() = default;

  BlockHandle(uint32_t fs_id, const BlockKey& key)
      : type_(HandleType::kBlock), fs_id_(fs_id), key_(key) {}

  BlockHandle(const TensorKey& key) : type_(HandleType::kTensor), key_(key) {}

  uint32_t FsId() const { return fs_id_; }

  std::string Id() const {
    if (type_ == HandleType::kBlock) {
      return std::get<BlockKey>(key_).Id();
    }
    return std::get<TensorKey>(key_).Id();
  }

  std::string Filename() const {
    if (type_ == HandleType::kBlock) {
      return std::get<BlockKey>(key_).Filename();
    }
    return std::get<TensorKey>(key_).Filename();
  }

  std::string StoreKey() const {
    if (type_ == HandleType::kBlock) {
      return std::get<BlockKey>(key_).StoreKey();
    }
    return std::get<TensorKey>(key_).StoreKey();
  }

  uint64_t StoreSize() const {
    if (type_ == HandleType::kBlock) {
      return std::get<BlockKey>(key_).StoreSize();
    }
    return std::get<TensorKey>(key_).StoreSize();
  }

  template <typename Visitor>
  auto Visit(Visitor&& v) const {
    return std::visit(std::forward<Visitor>(v), key_);
  }

  // fs_id is excluded: the on-disk cache path has no fs_id, so the loader
  // recovers blocks with fs_id 0 while runtime uses the real fs_id and they
  // must still match. The slice id is globally unique, so (id,index,size) is
  // the identity. Keep Hash() consistent with this.
  bool operator==(const BlockHandle& o) const {
    return type_ == o.type_ && key_ == o.key_;
  }
  bool operator!=(const BlockHandle& o) const { return !(*this == o); }

  // Identity hash from integer fields (BlockKey) or strings (TensorKey); never
  // builds a Filename() string. Consistent with operator== (fs_id excluded).
  size_t Hash() const {
    if (type_ == HandleType::kBlock) {
      const auto& k = std::get<BlockKey>(key_);
      uint64_t h = Mix(k.id);
      h = Mix(h ^ ((static_cast<uint64_t>(k.index) << 32) | k.size));
      return static_cast<size_t>(h);
    }
    const auto& k = std::get<TensorKey>(key_);
    size_t h = std::hash<std::string>{}(k.chunk_hash);
    HashCombine(&h, std::hash<std::string>{}(k.model_name));
    HashCombine(&h, std::hash<std::string>{}(k.dtype));
    HashCombine(&h, (static_cast<uint64_t>(k.world_size) << 32) | k.worker_id);
    return h;
  }

 private:
  enum class HandleType : uint8_t { kBlock = 0, kTensor = 1 };

  // splitmix64 finalizer: strong avalanche in ~5 ops, no allocation.
  static uint64_t Mix(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
  }
  static void HashCombine(size_t* h, size_t v) {
    *h ^= v + 0x9e3779b97f4a7c15ULL + (*h << 6) + (*h >> 2);
  }

  HandleType type_{};
  uint32_t fs_id_{0};
  std::variant<BlockKey, TensorKey> key_;
};

};  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_BLOCK_BLOCK_HANDLE_H_
