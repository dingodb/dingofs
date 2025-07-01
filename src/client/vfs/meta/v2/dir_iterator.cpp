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

  Json::Value entries;
  for (const auto& entry : entries_) {
    Json::Value entry_item;
    entry_item["ino"] = entry.ino;
    entry_item["name"] = entry.name;
    // entry_item["attr"] = entry.attr.;
    entries.append(entry_item);
  }
  value["entries"] = entries;

  return true;
}

bool DirIterator::Load(const Json::Value& value) {
  ino_ = value["ino"].asUInt64();
  last_name_ = value["last_name"].asString();
  with_attr_ = value["with_attr"].asBool();
  offset_ = value["offset"].asUInt();

  const Json::Value& entries = value["entries"];
  for (const auto& entry : entries) {
    DirEntry dir_entry;
    dir_entry.ino = entry["ino"].asUInt64();
    dir_entry.name = entry["name"].asString();
    // dir_entry.attr = entry["attr"].asString(); // TODO: parse attr
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

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs