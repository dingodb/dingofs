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
#include <fstream>

#include "base/string/string.h"

namespace dingofs {
namespace client {
namespace filesystem {

using base::string::BufToHexString;
using base::string::HexStringToBuf;
using utils::Mutex;
using utils::ReadLockGuard;
using utils::RWLock;
using utils::UniqueLock;
using utils::WriteLockGuard;

HandlerManager::HandlerManager()
    : mutex_(), dirBuffer_(std::make_shared<DirBuffer>()), handlers_() {}

HandlerManager::~HandlerManager() { dirBuffer_->DirBufferFreeAll(); }

std::shared_ptr<FileHandler> HandlerManager::NewHandler() {
  UniqueLock lk(mutex_);
  auto handler = std::make_shared<FileHandler>();
  handler->fh = dirBuffer_->DirBufferNew();
  handler->buffer = dirBuffer_->DirBufferGet(handler->fh);
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
  dirBuffer_->DirBufferRelease(fh);
  handlers_.erase(fh);
}

void HandlerManager::SaveAllHandlers(const std::string& path) {
  UniqueLock lk(mutex_);
  Json::Value root;
  Json::Value handle_array;

  for (const auto& handle : handlers_) {
    auto fileHandle = handle.second;

    Json::Value item;
    item["fh"] = fileHandle->fh;
    item["flags"] = fileHandle->flags;
    item["padding"] = fileHandle->padding;
    // do not store dir_handler_init, it's can reinitialize
    // item["dir_handler_init"] = fileHandle->dir_handler_init;

    Json::Value timespec_item;
    timespec_item["seconds"] = fileHandle->mtime.seconds;
    timespec_item["nanoSeconds"] = fileHandle->mtime.nanoSeconds;
    item["timespec_item"] = timespec_item;

    Json::Value buffer_item;
    buffer_item["wasRead"] = fileHandle->buffer->wasRead;
    size_t size = fileHandle->buffer->size;
    buffer_item["size"] = size;
    buffer_item["p"] =
        size > 0
            ? BufToHexString(
                  reinterpret_cast<unsigned char*>(fileHandle->buffer->p), size)
            : "";
    item["buffer"] = buffer_item;

    handle_array.append(item);
  }
  root["handlers"] = handle_array;

  std::ofstream file(path);
  if (file.is_open()) {
    Json::StreamWriterBuilder writer;
    std::string json_string = Json::writeString(writer, root);
    file << json_string;
    file.close();
    LOG(INFO) << "successfuly write " << handlers_.size()
              << " handlers to file: " << path;
  } else {
    LOG(ERROR) << "write dingo-fuse state file failed, file: " << path;
  }
}

void HandlerManager::LoadAllHandlers(const std::string& path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    LOG(ERROR) << "open dingo-fuse state file failed, file: " << path;
    return;
  }

  Json::Value root;
  Json::CharReaderBuilder reader;
  std::string errs;
  if (!Json::parseFromStream(reader, file, &root, &errs)) {
    LOG(ERROR) << "failed to parse state json file: " << path
               << ", errors: " << errs;
    return;
  }

  const Json::Value handlers = root["handlers"];
  if (!handlers.isArray()) {
    return;
  }
  for (const auto& handler : handlers) {
    // peek fh,padding,flags
    uint64_t fh = handler["fh"].asUInt64();
    bool padding = handler["padding"].asBool();
    uint flags = handler["flags"].asUInt();
    // peek timespec
    const Json::Value timespec_item = handler["timespec_item"];
    uint64_t seconds = timespec_item["seconds"].asUInt64();
    uint32_t nanoSeconds = timespec_item["nanoSeconds"].asUInt();
    // peek buffer
    const Json::Value buffer = handler["buffer"];
    std::string p = buffer["p"].asString();
    size_t size = buffer["size"].asUInt64();
    bool wasRead = buffer["wasRead"].asBool();

    {
      UniqueLock lk(mutex_);
      auto handler = std::make_shared<FileHandler>();
      handler->fh = dirBuffer_->DirBufferNewWithIndex(fh);
      handler->buffer = dirBuffer_->DirBufferGet(handler->fh);
      handler->padding = padding;
      handler->flags = flags;
      handler->mtime = base::time::TimeSpec(seconds, nanoSeconds);
      handler->buffer->size = size;
      handler->buffer->wasRead = wasRead;
      if (size > 0 && size == (p.size() / 2)) {
        handler->buffer->p = (char*)malloc(size);
        int ret = HexStringToBuf(
            p.c_str(), reinterpret_cast<unsigned char*>(handler->buffer->p),
            size);
        if (ret == -1) {
          free(handler->buffer->p);
          handler->buffer->p = nullptr;
        }
      }

      handlers_.emplace(handler->fh, handler);
    }
  }
  LOG(INFO) << "successfuly load " << handlers_.size()
            << " handlers from: " << path;
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

Status FsDirIterator::Seek() {
  offset_ = 0;
  return Status::OK();
}

bool FsDirIterator::Valid() { return offset_ < entries_.size(); }

vfs::DirEntry FsDirIterator::GetValue(bool with_attr) {
  CHECK(offset_ < entries_.size()) << "offset out of range";

  auto entry = entries_[offset_];
  if (with_attr) {
    entry.attr = entry.attr;
  }

  return entry;
}

void FsDirIterator::Next() { offset_++; }

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
