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
 * Created Date: 2026-04-04
 * Author: DingoFS Contributors
 */

#ifndef DINGOFS_SRC_SDK_KVCACHE_KVCACHE_CLIENT_H_
#define DINGOFS_SRC_SDK_KVCACHE_KVCACHE_CLIENT_H_

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cache/blockcache/cache_store.h"
#include "cache/remotecache/upstream.h"
#include "common/status.h"

namespace dingofs {
namespace sdk {

class KVCacheClient {
 public:
  KVCacheClient(const std::string& mds_addrs, uint64_t model_id);
  ~KVCacheClient();

  Status Start();
  Status Shutdown();

  Status Put(const uint8_t hash[32], const char* data, size_t size);
  Status Get(const uint8_t hash[32], std::string* data);
  Status BatchExists(const std::vector<std::array<uint8_t, 32>>& hashes,
                     std::vector<bool>* results);

 private:
  static constexpr uint64_t kMagic = 0xDFCA;

  cache::BlockKey HashToBlockKey(const uint8_t hash[32]) const;

  std::string mds_addrs_;
  uint64_t model_id_;
  std::unique_ptr<cache::Upstream> upstream_;
};

}  // namespace sdk
}  // namespace dingofs

#endif  // DINGOFS_SRC_SDK_KVCACHE_KVCACHE_CLIENT_H_
