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

#include "client/vfs/metasystem/mds/dir_iterator.h"

#include <sys/types.h>

#include <cstdint>
#include <vector>

#include "client/vfs/common/helper.h"
#include "common/options/client.h"
#include "fmt/format.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

DirIterator::~DirIterator() {
  std::string str;
  for (auto offset : offset_stats_) {
    str += fmt::format("{},", offset);
  }

  VLOG(1) << fmt::format("[dir_iterator.{}.{}] offset stats: {} {}.", ino_, fh_,
                         offset_stats_.size(), str);
}

void DirIterator::Remember(uint64_t off) { offset_stats_.push_back(off); }

Status DirIterator::GetValue(ContextSPtr& ctx, uint64_t off, bool with_attr,
                             DirEntry& dir_entry) {
  CHECK(off >= offset_) << fmt::format(
      "[dir_iterator.{}.{}] off out of range, {} {}.", ino_, fh_, offset_, off);

  with_attr_ = with_attr;

  do {
    if (off < offset_ + entries_.size()) {
      dir_entry = entries_[off - offset_];
      return Status::OK();
    }

    if (is_fetch_ && entries_.size() < FLAGS_vfs_meta_read_dir_batch_size) {
      return Status::NoData("not more dentry");
    }

    auto status = Fetch(ctx);
    if (!status.ok()) return status;

  } while (true);

  return Status::OK();
}

Status DirIterator::Fetch(ContextSPtr& ctx) {
  std::vector<DirEntry> entries;
  auto status = mds_client_->ReadDir(ctx, ino_, fh_, last_name_,
                                     FLAGS_vfs_meta_read_dir_batch_size,
                                     with_attr_, entries);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[dir_iterator.{}.{}] readdir fail, offset({}) last_name({}).", ino_,
        fh_, offset_, last_name_);
    return status;
  }

  is_fetch_ = true;

  offset_ += entries_.size();
  entries_ = std::move(entries);
  if (!entries_.empty()) {
    last_name_ = entries_.back().name;
  }

  return Status::OK();
}

bool DirIterator::Dump(Json::Value& value) {
  value["ino"] = ino_;
  value["fh"] = fh_;
  value["last_name"] = last_name_;
  value["with_attr"] = with_attr_;
  value["offset"] = offset_;
  value["is_fetch"] = is_fetch_;
  value["last_fetch_time_ns"] = last_fetch_time_ns_.load();

  Json::Value entries = Json::arrayValue;
  for (const auto& entry : entries_) {
    Json::Value entry_item;
    entry_item["ino"] = entry.ino;
    entry_item["name"] = entry.name;
    DumpAttr(entry.attr, entry_item["attr"]);
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

  last_name_ = value["last_name"].asString();
  with_attr_ = value["with_attr"].asBool();
  offset_ = value["offset"].asUInt();
  is_fetch_ = value["is_fetch"].asBool();
  last_fetch_time_ns_.store(value["last_fetch_time_ns"].asUInt64());

  const Json::Value& entries = value["entries"];
  if (!entries.isArray()) {
    LOG(ERROR) << "[meta.dir_iterator] entries is not an array.";
    return false;
  }

  for (const auto& entry : entries) {
    DirEntry dir_entry;
    dir_entry.ino = entry["ino"].asUInt64();
    dir_entry.name = entry["name"].asString();
    LoadAttr(entry["attr"], dir_entry.attr);
    entries_.push_back(dir_entry);
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

bool DirIteratorManager::Load(MDSClientSPtr mds_client,
                              const Json::Value& value) {
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

      auto dir_iterator = DirIterator::New(mds_client, ino, fh);
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

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs