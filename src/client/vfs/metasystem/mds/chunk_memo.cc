// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs/metasystem/mds/chunk_memo.h"

#include <algorithm>

#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

void ChunkMemo::Remember(Ino ino, uint32_t chunk_index, uint64_t version) {
  utils::WriteLockGuard guard(lock_);

  auto it = chunk_map_.find({ino, chunk_index});
  if (it == chunk_map_.end()) {
    chunk_map_[{ino, chunk_index}] = {.version = version,
                                      .time_ns = utils::TimestampNs()};
  } else {
    it->second.version = std::max(version, it->second.version);
    it->second.time_ns = utils::TimestampNs();
  }
}

void ChunkMemo::Forget(Ino ino, uint32_t chunk_index) {
  utils::WriteLockGuard guard(lock_);

  chunk_map_.erase({ino, chunk_index});
}
void ChunkMemo::ForgetExpired(uint64_t expire_time_ns) {
  utils::WriteLockGuard guard(lock_);

  for (auto it = chunk_map_.begin(); it != chunk_map_.end();) {
    if (it->second.time_ns < expire_time_ns) {
      it = chunk_map_.erase(it);
    } else {
      ++it;
    }
  }
}

uint64_t ChunkMemo::GetVersion(Ino ino, uint32_t chunk_index) {
  utils::ReadLockGuard guard(lock_);

  auto it = chunk_map_.find({ino, chunk_index});
  return (it != chunk_map_.end()) ? it->second.version : 0;
}

bool ChunkMemo::Dump(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  Json::Value items = Json::arrayValue;
  for (const auto& [key, value] : chunk_map_) {
    Json::Value item;
    item["ino"] = key.ino;
    item["chunk_index"] = key.chunk_index;
    item["version"] = value.version;
    item["time_ns"] = value.time_ns;

    items.append(item);
  }
  value["chunk_memo"] = items;

  return true;
}

bool ChunkMemo::Load(const Json::Value& value) {
  utils::WriteLockGuard lk(lock_);

  chunk_map_.clear();
  const Json::Value& items = value["chunk_memo"];
  if (!items.isArray()) {
    LOG(ERROR) << "[meta.chunk_memo] value is not an array.";
    return false;
  }

  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();
    uint32_t chunk_index = item["chunk_index"].asUInt();
    uint64_t version = item["version"].asUInt64();
    uint64_t time_ns = item["time_ns"].asUInt64();

    chunk_map_[{ino, chunk_index}] = {.version = version, .time_ns = time_ns};
  }

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
