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

#include "client/vfs/metasystem/mds/dentry_cache.h"

#include "common/options/client.h"
#include "mds/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

void DentryCache::Put(Ino parent_ino, const std::string& name, Ino ino) {
  Key key{parent_ino, name};

  shard_map_.withWLock(
      [&](Map& map) {
        auto [it, inserted] = map.try_emplace(key, Value(ino));
        if (!inserted) {
          it->second.ino = ino;
          it->second.last_fresh_s = utils::Timestamp();
        }
      },
      parent_ino);

  total_count_ << 1;
}

void DentryCache::Delete(Ino parent_ino, const std::string& name) {
  Key key{parent_ino, name};
  shard_map_.withWLock([&](Map& map) { map.erase(key); }, parent_ino);
}

Ino DentryCache::Get(Ino parent_ino, const std::string& name) {
  Ino ino{0};
  Key key{parent_ino, name};

  uint64_t now = utils::Timestamp();
  shard_map_.withRLock(
      [&](const Map& map) {
        auto it = map.find(key);
        if (it != map.end() && now <= (it->second.last_fresh_s +
                                       FLAGS_vfs_meta_dentry_cache_ttl_s)) {
          ino = it->second.ino;
        }
      },
      parent_ino);

  return ino;
}

std::vector<Ino> DentryCache::ListFile(Ino parent_ino) {
  uint64_t now = utils::Timestamp();

  std::vector<Ino> inos;
  shard_map_.withRLock(
      [&](const Map& map) {
        auto it = map.lower_bound(Key{parent_ino, ""});
        while (it != map.end() && it->first.parent_ino == parent_ino) {
          if (IsFile(it->second.ino) &&
              now <=
                  it->second.last_fresh_s + FLAGS_vfs_meta_dentry_cache_ttl_s) {
            inos.push_back(it->second.ino);
          }
          ++it;
        }
      },
      parent_ino);

  return inos;
}

void DentryCache::CleanExpired(uint64_t expire_s) {
  if (Size() < FLAGS_vfs_meta_clean_threshold_count) return;

  uint64_t now = utils::Timestamp();
  shard_map_.iterateWLock([&](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (now - it->second.last_fresh_s > expire_s) {
        it = map.erase(it);
        clean_count_ << 1;

      } else {
        ++it;
      }
    }
  });
}

size_t DentryCache::Size() {
  size_t size = 0;
  shard_map_.iterate([&size](Map& map) { size += map.size(); });
  return size;
}

size_t DentryCache::Bytes() {
  size_t bytes = 0;
  shard_map_.iterate([&bytes](Map& map) {
    for (const auto& [key, value] : map) {
      bytes += sizeof(key.parent_ino) + key.name.size() + sizeof(value);
    }
  });
  return bytes;
}

void DentryCache::Summary(Json::Value& value) {
  value["name"] = "dentrycache";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
  value["clean_count"] = clean_count_.get_value();
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs