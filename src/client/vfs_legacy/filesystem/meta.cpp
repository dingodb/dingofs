/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Dingofs
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#include "client/vfs_legacy/filesystem/meta.h"

#include <glog/logging.h>
#include <json/json.h>
#include <sys/stat.h>

#include <cstdint>
#include <memory>

#include "client/meta/vfs_meta.h"
#include "utils/string.h"

namespace dingofs {
namespace client {
namespace filesystem {

using utils::BufToHexString;
using utils::HexStringToBuf;
using utils::Mutex;
using utils::ReadLockGuard;
using utils::RWLock;
using utils::UniqueLock;
using utils::WriteLockGuard;

HandlerManager::HandlerManager() = default;

HandlerManager::~HandlerManager() = default;

std::shared_ptr<FileHandler> HandlerManager::NewHandler() {
  UniqueLock lk(mutex_);
  auto handler = std::make_shared<FileHandler>();
  handler->fh = vfs::GenFh();
  handler->padding = false;
  handlers_.emplace(handler->fh, handler);
  return handler;
}

std::shared_ptr<FileHandler> HandlerManager::FindHandler(uint64_t fh) {
  UniqueLock lk(mutex_);
  auto iter = handlers_.find(fh);
  if (iter == handlers_.end()) {
    return nullptr;
  }
  return iter->second;
}

void HandlerManager::ReleaseHandler(uint64_t fh) {
  UniqueLock lk(mutex_);
  handlers_.erase(fh);
}

bool HandlerManager::Dump(Json::Value& value) {
  UniqueLock lk(mutex_);
  Json::Value handle_array;

  for (const auto& handle : handlers_) {
    auto fileHandle = handle.second;

    Json::Value item;
    item["fh"] = fileHandle->fh;
    item["flags"] = fileHandle->flags;
    item["padding"] = fileHandle->padding;

    Json::Value timespec_item;
    timespec_item["seconds"] = fileHandle->mtime.seconds;
    timespec_item["nanoSeconds"] = fileHandle->mtime.nanoSeconds;
    item["timespec_item"] = timespec_item;

    Json::Value entry_array;
    for (const auto& entry : fileHandle->entries) {
      Json::Value entry_item;
      entry_item["ino"] = entry.ino;
      entry_item["name"] = entry.name;
      Json::Value attr_jvalue;
      vfs::DumpAttr(entry.attr, attr_jvalue);
      entry_item["attr"] = attr_jvalue;
      entry_array.append(entry_item);
    }
    item["entries"] = entry_array;

    handle_array.append(item);
  }
  value["handlers"] = handle_array;

  LOG(INFO) << "successfuly dump " << handlers_.size() << " handlers";

  return true;
}

bool HandlerManager::Load(const Json::Value& value) {
  const Json::Value& handlers = value["handlers"];
  if (!handlers.isArray() && !handlers.isNull()) {
    LOG(ERROR) << "handlers is not an array.";
    return false;
  }

  uint64_t max_fh = 0;
  for (const auto& handler : handlers) {
    // peek fh,padding,flags
    uint64_t fh = handler["fh"].asUInt64();
    bool padding = handler["padding"].asBool();
    uint flags = handler["flags"].asUInt();

    // peek timespec
    const Json::Value& timespec_item = handler["timespec_item"];
    uint64_t seconds = timespec_item["seconds"].asUInt64();
    uint32_t nanoSeconds = timespec_item["nanoSeconds"].asUInt();

    // peek entries
    std::vector<vfs::DirEntry> dir_entries;
    const Json::Value& entries = handler["entries"];
    // entries shouble be null for file handler
    if (!entries.isArray() && !entries.isNull()) {
      LOG(ERROR) << "entries is not an array.";
      return false;
    }
    // iterate entries
    for (const auto& entry : entries) {
      vfs::DirEntry dir_entry;
      dir_entry.ino = entry["ino"].asUInt64();
      dir_entry.name = entry["name"].asString();
      vfs::LoadAttr(entry["attr"], dir_entry.attr);

      dir_entries.emplace_back(dir_entry);
    }

    {
      UniqueLock lk(mutex_);
      auto handler = std::make_shared<FileHandler>();
      handler->fh = fh;
      handler->flags = flags;
      handler->mtime = utils::TimeSpec(seconds, nanoSeconds);
      handler->padding = padding;
      handler->entries = std::move(dir_entries);
      handlers_.emplace(handler->fh, handler);
    }
    max_fh = std::max(max_fh, fh);
  }
  vfs::next_fh.store(max_fh + 1);  // update next_fh

  LOG(INFO) << "successfuly load " << handlers_.size()
            << " handlers, next fh is:" << vfs::next_fh.load();

  return true;
}

std::string StrMode(uint16_t mode) {
  static std::map<uint16_t, char> type2char = {
      {S_IFSOCK, 's'}, {S_IFLNK, 'l'}, {S_IFREG, '-'}, {S_IFBLK, 'b'},
      {S_IFDIR, 'd'},  {S_IFCHR, 'c'}, {S_IFIFO, 'f'}, {0, '?'},
  };

  std::string s("?rwxrwxrwx");
  s[0] = type2char[mode & (S_IFMT & 0xffff)];
  if (mode & S_ISUID) {
    s[3] = 's';
  }
  if (mode & S_ISGID) {
    s[6] = 's';
  }
  if (mode & S_ISVTX) {
    s[9] = 't';
  }

  for (auto i = 0; i < 9; i++) {
    if ((mode & (1 << i)) == 0) {
      if ((s[9 - i] == 's') || (s[9 - i] == 't')) {
        s[9 - i] &= 0xDF;
      } else {
        s[9 - i] = '-';
      }
    }
  }
  return s;
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
