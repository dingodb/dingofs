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

#include <utility>
#include <vector>

#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

void ChunkMemo::Remember(
    Ino ino, const std::vector<mds::ChunkDescriptor>& chunk_descriptors) {
  for (const auto& chunk_descriptor : chunk_descriptors) {
    Remember(ino, chunk_descriptor.index(), chunk_descriptor.version());
  }
}

void ChunkMemo::Remember(Ino ino, uint32_t chunk_index, uint64_t version) {
  chunk_map_.withWLock(
      [ino, chunk_index, version](Map& map) mutable {
        auto it = map.find({ino, chunk_index});
        if (it == map.end()) {
          map[{ino, chunk_index}] = {.version = version,
                                     .time_ns = utils::TimestampNs()};
        } else {
          it->second.version = std::max(version, it->second.version);
          it->second.time_ns = utils::TimestampNs();
        }
      },
      ino);
}

void ChunkMemo::Forget(Ino ino) {
  chunk_map_.withWLock(
      [ino](Map& map) mutable {
        auto it = map.lower_bound({ino, 0});
        for (; it != map.end();) {
          if (it->first.ino != ino) break;

          it = map.erase(it);
        }
      },
      ino);
}

void ChunkMemo::Forget(Ino ino, uint32_t chunk_index) {
  chunk_map_.withWLock(
      [ino, chunk_index](Map& map) mutable { map.erase({ino, chunk_index}); },
      ino);
}

void ChunkMemo::ForgetExpired(uint64_t expire_time_ns) {
  chunk_map_.iterateWLock([&](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (it->second.time_ns < expire_time_ns) {
        it = map.erase(it);
      } else {
        ++it;
      }
    }
  });
}

uint64_t ChunkMemo::GetVersion(Ino ino, uint32_t chunk_index) {
  uint64_t version = 0;
  chunk_map_.withRLock(
      [ino, chunk_index, &version](Map& map) {
        auto it = map.find({ino, chunk_index});
        if (it != map.end()) version = it->second.version;
      },
      ino);

  return version;
}

std::vector<std::pair<uint32_t, uint64_t>> ChunkMemo::GetVersion(Ino ino) {
  std::vector<std::pair<uint32_t, uint64_t>> versions;

  chunk_map_.withRLock(
      [ino, &versions, this](Map& map) {
        auto it = map.lower_bound({ino, 0});
        for (; it != map.end(); ++it) {
          if (it->first.ino != ino) break;

          versions.emplace_back(it->first.chunk_index, it->second.version);
        }
      },
      ino);

  return versions;
}

size_t ChunkMemo::Size() {
  size_t size = 0;
  chunk_map_.iterate([&size](Map& map) { size += map.size(); });
  return size;
}

bool ChunkMemo::Dump(Json::Value& value) {
  std::vector<std::pair<Key, Value>> chunk_map_copy;
  chunk_map_copy.reserve(Size());

  chunk_map_.iterate([&](Map& map) {
    for (const auto& [key, val] : map) {
      chunk_map_copy.emplace_back(key, val);
    }
  });

  Json::Value items = Json::arrayValue;
  for (const auto& [key, value] : chunk_map_copy) {
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
  const Json::Value& items = value["chunk_memo"];
  if (!items.isArray()) {
    LOG(ERROR) << "[meta.chunk_memo] value is not an array.";
    return false;
  }

  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();
    uint32_t chunk_index = item["chunk_index"].asUInt();
    uint64_t version = item["version"].asUInt64();

    Remember(ino, chunk_index, version);
  }

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
