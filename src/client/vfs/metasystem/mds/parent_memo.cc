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

#include <json/value.h>

#include <algorithm>

#include "fmt/core.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

ParentMemo::ParentMemo() {
  // root ino is its own parent
  ino_map_.insert({1, Entry{1, 0}});
}

bool ParentMemo::GetParent(Ino ino, Ino& parent) {
  utils::ReadLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it == ino_map_.end()) {
    return false;
  }

  if (it->second.parent == 0) {
    return false;
  }

  parent = it->second.parent;
  return true;
}

bool ParentMemo::GetVersion(Ino ino, uint64_t& version) {
  utils::ReadLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it == ino_map_.end()) {
    return false;
  }

  version = it->second.version;
  return true;
}

std::vector<uint64_t> ParentMemo::GetAncestors(uint64_t ino) {
  utils::ReadLockGuard lk(lock_);

  std::vector<uint64_t> ancestors;
  ancestors.reserve(32);
  ancestors.push_back(ino);

  do {
    auto it = ino_map_.find(ino);
    if (it == ino_map_.end()) {
      break;
    }
    auto& entry = it->second;

    ancestors.push_back(entry.parent);

    if (entry.parent == 1) {
      break;
    }

    ino = entry.parent;
  } while (true);

  return ancestors;
}

bool ParentMemo::GetRenameRefCount(Ino ino, int32_t& rename_ref_count) {
  utils::ReadLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it == ino_map_.end()) {
    return false;
  }

  rename_ref_count = it->second.rename_ref_count;

  return true;
}

void ParentMemo::Upsert(Ino ino, Ino parent) {
  utils::WriteLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it != ino_map_.end()) {
    it->second.parent = parent;
  } else {
    ino_map_[ino] = Entry{.parent = parent, .version = 0};
  }
}

void ParentMemo::UpsertVersion(Ino ino, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it != ino_map_.end()) {
    it->second.version = std::max(it->second.version, version);

  } else {
    ino_map_[ino] = Entry{.parent = 0, .version = version};
  }
}

void ParentMemo::UpsertVersionAndRenameRefCount(Ino ino, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it != ino_map_.end()) {
    it->second.version = std::max(it->second.version, version);
    ++it->second.rename_ref_count;

  } else {
    ino_map_[ino] =
        Entry{.parent = 0, .version = version, .rename_ref_count = 1};
  }
}

void ParentMemo::Upsert(Ino ino, Ino parent, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it != ino_map_.end()) {
    it->second.parent = parent;
    it->second.version = std::max(version, it->second.version);

  } else {
    ino_map_[ino] = Entry{.parent = parent, .version = version};
  }
}

void ParentMemo::Delete(Ino ino) {
  utils::WriteLockGuard lk(lock_);

  ino_map_.erase(ino);
}

void ParentMemo::DecRenameRefCount(Ino ino) {
  utils::WriteLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it != ino_map_.end()) {
    if (it->second.rename_ref_count > 0) {
      --it->second.rename_ref_count;
    }
  }
}

bool ParentMemo::Dump(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  Json::Value items = Json::arrayValue;
  for (const auto& [ino, entry] : ino_map_) {
    Json::Value item;
    item["ino"] = ino;
    item["parent"] = entry.parent;
    item["version"] = entry.version;

    items.append(item);
  }
  value["parent_memo"] = items;

  return true;
}

bool ParentMemo::Load(const Json::Value& value) {
  utils::WriteLockGuard lk(lock_);

  ino_map_.clear();
  const Json::Value& items = value["parent_memo"];
  if (!items.isArray()) {
    LOG(ERROR) << "[meta.parent_memo] value is not an array.";
    return false;
  }

  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();
    Ino parent = item["parent"].asUInt64();
    uint64_t version = item["version"].asUInt64();

    ino_map_[ino] = Entry{parent, version};
  }

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs