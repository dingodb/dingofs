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

#include "client/vfs/metasystem/mds/parent_memo.h"

#include "common/const.h"
#include "fmt/core.h"
#include "json/value.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

ParentMemo::ParentMemo() {
  // root ino is its own parent
  UpsertEntry(kRootIno, Entry{kRootIno, 0, 0});
}

bool ParentMemo::GetParent(Ino ino, Ino& parent) {
  bool found = false;
  shard_map_.withRLock(
      [ino, &parent, &found](Map& map) {
        auto it = map.find(ino);
        if (it != map.end() && it->second.parent != 0) {
          found = true;
          parent = it->second.parent;
        }
      },
      ino);

  return found;
}

bool ParentMemo::GetVersion(Ino ino, uint64_t& version) {
  bool found = false;
  shard_map_.withRLock(
      [ino, &version, &found](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) {
          found = true;
          version = it->second.version;
        }
      },
      ino);

  return found;
}

std::vector<uint64_t> ParentMemo::GetAncestors(uint64_t ino) {
  std::vector<uint64_t> ancestors;
  ancestors.reserve(32);
  ancestors.push_back(ino);

  Ino parent;
  do {
    if (!GetParent(ino, parent)) break;
    ancestors.push_back(parent);

    if (parent == 1) break;

    ino = parent;

  } while (true);

  return ancestors;
}

bool ParentMemo::GetRenameRefCount(Ino ino, int32_t& rename_ref_count) {
  bool found = false;
  shard_map_.withRLock(
      [ino, &rename_ref_count, &found](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) {
          found = true;
          rename_ref_count = it->second.rename_ref_count;
        }
      },
      ino);

  return found;
}

void ParentMemo::Upsert(Ino ino, Ino parent) {
  shard_map_.withWLock(
      [ino, parent](Map& map) mutable {
        auto it = map.find(ino);
        if (it != map.end()) {
          it->second.parent = parent;
        } else {
          map[ino] = Entry{.parent = parent, .version = 0};
        }
      },
      ino);
}

void ParentMemo::UpsertVersion(Ino ino, uint64_t version) {
  shard_map_.withWLock(
      [ino, version](Map& map) mutable {
        auto it = map.find(ino);
        if (it != map.end()) {
          it->second.version = std::max(it->second.version, version);

        } else {
          map[ino] = Entry{.parent = 0, .version = version};
        }
      },
      ino);
}

void ParentMemo::UpsertVersionAndRenameRefCount(Ino ino, uint64_t version) {
  shard_map_.withWLock(
      [ino, version](Map& map) mutable {
        auto it = map.find(ino);
        if (it != map.end()) {
          it->second.version = std::max(it->second.version, version);
          ++it->second.rename_ref_count;

        } else {
          map[ino] =
              Entry{.parent = 0, .version = version, .rename_ref_count = 1};
        }
      },
      ino);
}

void ParentMemo::Upsert(Ino ino, Ino parent, uint64_t version) {
  shard_map_.withWLock(
      [ino, parent, version](Map& map) mutable {
        auto it = map.find(ino);
        if (it != map.end()) {
          it->second.parent = parent;
          it->second.version = std::max(version, it->second.version);

        } else {
          map[ino] = Entry{.parent = parent, .version = version};
        }
      },
      ino);
}

void ParentMemo::UpsertEntry(Ino ino, const Entry& entry) {
  shard_map_.withWLock([ino, &entry](Map& map) mutable { map[ino] = entry; },
                       ino);
}

void ParentMemo::Delete(Ino ino) {
  shard_map_.withWLock([ino](Map& map) mutable { map.erase(ino); }, ino);
}

void ParentMemo::DecRenameRefCount(Ino ino) {
  shard_map_.withWLock(
      [ino](Map& map) mutable {
        auto it = map.find(ino);
        if (it != map.end() && it->second.rename_ref_count > 0) {
          --it->second.rename_ref_count;
        }
      },
      ino);
}

size_t ParentMemo::Size() {
  size_t size = 0;
  shard_map_.iterate([&size](Map& map) { size += map.size(); });
  return size;
}

bool ParentMemo::Dump(Json::Value& value) {
  std::vector<std::pair<Ino, Entry>> ino_map_copy;
  ino_map_copy.reserve(Size());
  shard_map_.iterateWLock([&](Map& map) {
    for (const auto& [ino, entry] : map) {
      ino_map_copy.emplace_back(ino, entry);
    }
  });

  Json::Value items = Json::arrayValue;
  for (const auto& [ino, entry] : ino_map_copy) {
    Json::Value item;
    item["ino"] = ino;
    item["parent"] = entry.parent;
    item["version"] = entry.version;
    item["rename_ref_count"] = entry.rename_ref_count;

    items.append(item);
  }
  value["parent_memo"] = items;

  return true;
}

bool ParentMemo::Load(const Json::Value& value) {
  const Json::Value& items = value["parent_memo"];
  if (!items.isArray()) {
    LOG(ERROR) << "[meta.parent_memo] value is not an array.";
    return false;
  }

  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();
    Ino parent = item["parent"].asUInt64();
    uint64_t version = item["version"].asUInt64();
    int32_t rename_ref_count = item["rename_ref_count"].asInt();

    UpsertEntry(ino, Entry{parent, version, rename_ref_count});
  }

  return true;
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs