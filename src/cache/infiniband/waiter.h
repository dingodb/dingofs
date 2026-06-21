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
 * Created Date: 2026-06-15
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_WAITER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_WAITER_H_

#include <cstddef>
#include <cstdint>
#include <memory>

#include "cache/infiniband/controller.h"

namespace dingofs {
namespace cache {
namespace infiniband {

struct Waiter {
  uint64_t correlation_id{0};
  struct Context {
    Controller* cntl{nullptr};
    google::protobuf::Message* response{nullptr};
  } ctx;
  bthread::CountdownEvent notify{1};
};

class Waiters {
 public:
  Waiters() = default;

  void Add(uint64_t correlation_id, Waiter* waiter) {
    auto& shard = GetShard(correlation_id);
    BAIDU_SCOPED_LOCK(shard.mutex);
    shard.index[correlation_id] = waiter;
  }

  bool Remove(uint64_t correlation_id) {
    auto& shard = GetShard(correlation_id);
    BAIDU_SCOPED_LOCK(shard.mutex);
    return shard.index.erase(correlation_id) != 0;
  }

  Waiter* Take(uint64_t correlation_id) {
    auto& shard = GetShard(correlation_id);
    BAIDU_SCOPED_LOCK(shard.mutex);
    auto it = shard.index.find(correlation_id);
    if (it == shard.index.end()) {
      return nullptr;
    }

    auto* waiter = it->second;
    shard.index.erase(it);
    return waiter;
  }

  void GetAll(std::vector<Waiter*>* waiters) {
    for (auto& shard : shards_) {
      BAIDU_SCOPED_LOCK(shard.mutex);
      for (const auto [_, waiter] : shard.index) {
        waiters->emplace_back(waiter);
      }
    }
  }

 private:
  static constexpr size_t kShardNum = 512;
  static_assert((kShardNum & (kShardNum - 1)) == 0,
                "kShardNum must be a power of 2");

  struct alignas(64) Shard {
    bthread::Mutex mutex;
    std::unordered_map<uint64_t, Waiter*> index;
  };

  static size_t ShardIndex(uint64_t correlation_id) {
    return std::hash<uint64_t>{}(correlation_id) & (kShardNum - 1);
  }

  Shard& GetShard(uint64_t correlation_id) {
    return shards_[ShardIndex(correlation_id)];
  }

  std::array<Shard, kShardNum> shards_;
};

using WaitersUPtr = std::unique_ptr<Waiters>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_WAITER_H_
