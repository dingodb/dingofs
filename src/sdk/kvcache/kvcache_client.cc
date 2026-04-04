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

#include "sdk/kvcache/kvcache_client.h"

#include <cstring>

#include "cache/blockcache/cache_store.h"
#include "cache/common/context.h"
#include "cache/remotecache/upstream.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace sdk {

KVCacheClient::KVCacheClient(const std::string& mds_addrs, uint64_t model_id)
    : mds_addrs_(mds_addrs),
      model_id_(model_id),
      upstream_(std::make_unique<cache::Upstream>()) {}

KVCacheClient::~KVCacheClient() = default;

Status KVCacheClient::Start() {
  upstream_->Start();
  return Status::OK();
}

Status KVCacheClient::Shutdown() {
  upstream_->Shutdown();
  return Status::OK();
}

cache::BlockKey KVCacheClient::HashToBlockKey(const uint8_t hash[32]) const {
  uint64_t h0, h1, h2;
  std::memcpy(&h0, &hash[0], 8);
  std::memcpy(&h1, &hash[8], 8);
  std::memcpy(&h2, &hash[16], 8);
  return cache::BlockKey(kMagic, model_id_, h0, h1, h2);
}

Status KVCacheClient::Put(const uint8_t hash[32], const char* data,
                          size_t size) {
  auto key = HashToBlockKey(hash);
  cache::Block block(data, size);
  auto ctx = cache::NewContext();
  return upstream_->SendPutRequest(ctx, key, block);
}

Status KVCacheClient::Get(const uint8_t hash[32], std::string* data) {
  auto key = HashToBlockKey(hash);
  IOBuffer buffer;
  auto ctx = cache::NewContext();
  auto status = upstream_->SendRangeRequest(ctx, key, 0, 0, &buffer, 0);
  if (status.ok()) {
    data->resize(buffer.Size());
    buffer.CopyTo(data->data(), buffer.Size());
  }
  return status;
}

Status KVCacheClient::BatchExists(
    const std::vector<std::array<uint8_t, 32>>& hashes,
    std::vector<bool>* results) {
  std::vector<cache::BlockKey> keys;
  keys.reserve(hashes.size());
  for (const auto& hash : hashes) {
    keys.push_back(HashToBlockKey(hash.data()));
  }
  return upstream_->SendBatchExistsRequest(keys, results);
}

}  // namespace sdk
}  // namespace dingofs
