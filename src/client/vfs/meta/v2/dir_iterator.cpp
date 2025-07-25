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

#include "client/vfs/meta/v2/dir_iterator.h"

#include <fmt/format.h>

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

DEFINE_uint32(read_dir_batch_size, 1024, "read dir batch size.");

Status DirIterator::Seek() {
  std::vector<DirEntry> entries;
  auto status = mds_client_->ReadDir(ino_, last_name_,
                                     FLAGS_read_dir_batch_size, true, entries);
  if (!status.ok()) {
    return status;
  }

  offset_ = 0;
  entries_ = std::move(entries);
  if (!entries_.empty()) {
    last_name_ = entries_.back().name;
  }

  return Status::OK();
}

bool DirIterator::Valid() { return offset_ < entries_.size(); }

DirEntry DirIterator::GetValue(bool with_attr) {
  CHECK(offset_ < entries_.size()) << "offset out of range";

  with_attr_ = with_attr;
  return entries_[offset_];
}

void DirIterator::Next() {
  if (++offset_ < entries_.size()) {
    return;
  }

  std::vector<DirEntry> entries;
  auto status = mds_client_->ReadDir(
      ino_, last_name_, FLAGS_read_dir_batch_size, with_attr_, entries);
  if (!status.ok()) {
    return;
  }

  offset_ = 0;
  entries_ = std::move(entries);
  if (!entries_.empty()) {
    last_name_ = entries_.back().name;
  }
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
    LOG(ERROR) << "value is not an object.";
    return false;
  }

  ino_ = value["ino"].asUInt64();
  last_name_ = value["last_name"].asString();
  with_attr_ = value["with_attr"].asBool();
  offset_ = value["offset"].asUInt();

  const Json::Value& entries = value["entries"];
  if (!entries.isArray()) {
    LOG(ERROR) << "entries is not an array.";
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

bool DirIteratorManager::Load(MDSClientPtr mds_client,
                              const Json::Value& value) {
  utils::WriteLockGuard lk(lock_);

  dir_iterator_map_.clear();
  const Json::Value& items = value["dir_iterators"];
  if (!items.isArray()) {
    LOG(ERROR) << "dir_iterators is not an array.";
    return false;
  }

  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();
    auto dir_iterator = DirIterator::New(mds_client, ino);
    if (!dir_iterator->Load(item)) {
      LOG(ERROR) << fmt::format("load dir({}) iterator fail.", ino);
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