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

 private:
  enum class HandleType : uint8_t { kBlock = 0, kTensor = 1 };

  HandleType type_{};
  uint32_t fs_id_{0};
  std::variant<BlockKey, TensorKey> key_;
};

};  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_BLOCK_BLOCK_HANDLE_H_
