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

#include "client/vfs/metasystem/local/dir_iterator.h"

#include <sys/types.h>

#include <cstdint>
#include <vector>

#include "client/vfs/common/helper.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/codec.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace local {

void DirIterator::Remember(uint64_t off) { offset_stats_.push_back(off); }

Status DirIterator::GetValue(ContextSPtr&, uint64_t off, DirEntry& dir_entry) {
  if (off >= dentries_.size()) {
    return Status::NoData("not more dentry");
  }

  auto& dentry = dentries_.at(off);

  dir_entry.ino = dentry.ino();
  dir_entry.name = dentry.name();

  return Status::OK();
}

bool DirIterator::Dump(Json::Value& value) {
  value["ino"] = ino_;
  value["fh"] = fh_;

  Json::Value entries = Json::arrayValue;
  for (const auto& dentry : dentries_) {
    Json::Value entry_item;
    entry_item["ino"] = dentry.ino();
    entry_item["name"] = dentry.name();
    entries.append(entry_item);
  }
  value["entries"] = entries;

  return true;
}

bool DirIterator::Load(const Json::Value& value) {
  if (!value.isObject()) {
    LOG(ERROR) << "[meta.dir_iterator] value is not an object.";
    return false;
  }

  const Json::Value& entries = value["entries"];
  if (!entries.isArray()) return false;

  dentries_.reserve(entries.size());
  for (const auto& entry : entries) {
    mds::DentryEntry dentry;
    dentry.set_ino(entry["ino"].asUInt64());
    dentry.set_name(entry["name"].asString());

    dentries_.push_back(std::move(dentry));
  }

  return true;
}

void DirIteratorManager::PutWithFunc(Ino ino, uint64_t,
                                     DirIteratorSPtr& dir_iterator,
                                     PutFunc&& f) {
  utils::WriteLockGuard lk(lock_);

  auto it = dir_iterator_map_.find(ino);
  if (it == dir_iterator_map_.end()) {
    DirIteratorSet vec = {dir_iterator};
    dir_iterator_map_[ino] = vec;
    f(vec);

  } else {
    auto& vec = it->second;
    vec.push_back(dir_iterator);
    f(vec);
  }
}

DirIteratorSPtr DirIteratorManager::Get(Ino ino, uint64_t fh) {
  utils::ReadLockGuard lk(lock_);

  auto it = dir_iterator_map_.find(ino);
  if (it == dir_iterator_map_.end()) return nullptr;

  for (const auto& dir_iterator : it->second) {
    if (dir_iterator->Fh() == fh) return dir_iterator;
  }

  return nullptr;
}

void DirIteratorManager::Delete(Ino ino, uint64_t fh) {
  utils::WriteLockGuard lk(lock_);

  auto it = dir_iterator_map_.find(ino);
  if (it == dir_iterator_map_.end()) return;

  auto& vec = it->second;

  uint32_t erase_index = UINT32_MAX;
  for (uint32_t i = 0; i < vec.size(); ++i) {
    if (vec[i]->Fh() == fh) {
      erase_index = i;
      break;
    }
  }
  CHECK(erase_index != UINT32_MAX)
      << fmt::format("dir iterator not found, ino({}) fh({}).", ino, fh);

  if (vec.size() == 1) {
    dir_iterator_map_.erase(it);
  } else {
    vec[erase_index] = vec.back();
    vec.pop_back();
  }
}

bool DirIteratorManager::Dump(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  Json::Value items = Json::arrayValue;
  for (const auto& [ino, dir_iterator_set] : dir_iterator_map_) {
    Json::Value item;
    item["ino"] = ino;
    Json::Value sub_items = Json::arrayValue;
    for (const auto& dir_iterator : dir_iterator_set) {
      Json::Value sub_item = Json::objectValue;
      if (!dir_iterator->Dump(sub_item)) {
        return false;
      }

      sub_items.append(sub_item);
    }

    item["dir_iterators"] = sub_items;

    items.append(item);
  }

  value["dir_iterators_map"] = items;

  return true;
}

bool DirIteratorManager::Load(const Json::Value& value) {
  utils::WriteLockGuard lk(lock_);

  dir_iterator_map_.clear();
  const Json::Value& items = value["dir_iterators_map"];
  if (!items.isArray()) {
    LOG(ERROR) << "[meta.dir_iterator] value is not an array.";
    return false;
  }

  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();

    DirIteratorSet dir_iterator_set;
    for (auto const& sub_item : item["dir_iterators"]) {
      uint64_t fh = sub_item["fh"].asUInt64();

      auto dir_iterator = DirIterator::New(ino, fh, {});
      if (!dir_iterator->Load(item)) {
        LOG(ERROR) << fmt::format(
            "[meta.dir_iterator] load dir({}) iterator fail.", ino);
        return false;
      }

      dir_iterator_set.push_back(dir_iterator);
    }

    dir_iterator_map_[ino] = dir_iterator_set;
  }

  return true;
}

}  // namespace local
}  // namespace vfs
}  // namespace client
}  // namespace dingofs