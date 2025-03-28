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

#include "client/vfs_old/filesystem/meta.h"

#include <glog/logging.h>
#include <sys/stat.h>

#include <cstdint>

namespace dingofs {
namespace client {
namespace filesystem {

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
