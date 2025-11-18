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

#include <cstdint>

#include "client/vfs/common/helper.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "options/client/option.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

DirIterator::~DirIterator() {
  std::string str;
  for (auto offset : offset_stats_) {
    str += fmt::format("{},", offset);
  }

  VLOG(1) << fmt::format("[dir_iterator.{}.{}] offset stats: {} {}.", ino_, fh_,
                         offset_stats_.size(), str);
}

void DirIterator::Remember(uint64_t off) { offset_stats_.push_back(off); }

Status DirIterator::PreFetch() {
  last_fetch_time_ns_ = utils::TimestampNs();

  // return Next();
  return Status::OK();
}

Status DirIterator::GetValue(uint64_t off, bool with_attr,
                             DirEntry& dir_entry) {
  CHECK(off >= offset_) << fmt::format(
      "[dir_iterator.{}.{}] off out of range, {} {}.", ino_, fh_, offset_, off);

  with_attr_ = with_attr;

  do {
    if (off < offset_ + entries_.size()) {
      dir_entry = entries_[off - offset_];
      return Status::OK();
    }

    if (is_fetch_ && entries_.size() < FLAGS_client_vfs_read_dir_batch_size) {
      return Status::NoData("not more dentry");
    }

    auto status = Next();
    if (!status.ok()) return status;

  } while (true);

  return Status::OK();
}

Status DirIterator::Next() {
  std::vector<DirEntry> entries;
  auto status = mds_client_->ReadDir(ctx_, ino_, fh_, last_name_,
                                     FLAGS_client_vfs_read_dir_batch_size,
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
  value["last_name"] = last_name_;
  value["with_attr"] = with_attr_;
  value["offset"] = offset_;

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

  ino_ = value["ino"].asUInt64();
  last_name_ = value["last_name"].asString();
  with_attr_ = value["with_attr"].asBool();
  offset_ = value["offset"].asUInt();

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

void DirIteratorManager::Put(uint64_t fh, DirIteratorSPtr dir_iterator) {
  utils::WriteLockGuard lk(lock_);

  dir_iterator_map_[fh] = dir_iterator;
}

DirIteratorSPtr DirIteratorManager::Get(uint64_t fh) {
  utils::ReadLockGuard lk(lock_);

  auto it = dir_iterator_map_.find(fh);
  if (it != dir_iterator_map_.end()) {
    return it->second;
  }
  return nullptr;
}

void DirIteratorManager::Delete(uint64_t fh) {
  utils::WriteLockGuard lk(lock_);

  dir_iterator_map_.erase(fh);
}

bool DirIteratorManager::Dump(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  Json::Value items = Json::arrayValue;
  for (const auto& [fh, dir_iterator] : dir_iterator_map_) {
    Json::Value item;
    item["fh"] = fh;
    if (!dir_iterator->Dump(item)) {
      return false;
    }

    items.append(item);
  }

  value["dir_iterators"] = items;

  return true;
}

bool DirIteratorManager::Load(MDSClientSPtr mds_client,
                              const Json::Value& value) {
  utils::WriteLockGuard lk(lock_);

  dir_iterator_map_.clear();
  const Json::Value& items = value["dir_iterators"];
  if (!items.isArray()) {
    LOG(ERROR) << "[meta.dir_iterator] value is not an array.";
    return false;
  }

  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();
    uint64_t fh = item["fh"].asUInt64();
    auto dir_iterator = DirIterator::New(nullptr, mds_client, ino, fh);
    if (!dir_iterator->Load(item)) {
      LOG(ERROR) << fmt::format(
          "[meta.dir_iterator] load dir({}) iterator fail.", ino);
      return false;
    }

    dir_iterator_map_[item["fh"].asUInt64()] = dir_iterator;
  }

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs