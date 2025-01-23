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
 * Created Date: 2023-03-09
 * Author: Jingli Chen (Wine93)
 */

#include "client/filesystem/openfile.h"

#include <memory>

#include "client/filesystem/utils.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace filesystem {

using common::OpenFilesOption;
using utils::ReadLockGuard;
using utils::WriteLockGuard;

OpenFiles::OpenFiles(OpenFilesOption option,
                     std::shared_ptr<DeferSync> defer_sync)
    : option_(option), deferSync_(defer_sync) {
  metric_ = std::make_shared<OpenfilesMetric>();
  LOG(INFO) << "Using openfile lru cache but ignored, capacity is "
            << option.lruSize;
}

void OpenFiles::Open(Ino ino, std::shared_ptr<InodeWrapper> inode) {
  WriteLockGuard lk(rwlock_);

  const auto iter = files_.find(ino);
  if (iter != files_.end()) {
    VLOG(3) << "Open file already exists: ino = " << ino
            << ", refs = " << iter->second->refs
            << ", mtime = " << InodeMtime(iter->second->inode);
    iter->second->refs++;
    return;
  }

  auto file = std::make_unique<OpenFile>(inode);
  file->refs++;
  VLOG(1) << "Insert open file cache: ino = " << ino
          << ", refs = " << file->refs
          << ", mtime = " << InodeMtime(file->inode);

  CHECK(files_.insert({ino, std::move(file)}).second);
  metric_->AddOpenfiles(1);
}

bool OpenFiles::IsOpened(Ino ino, std::shared_ptr<InodeWrapper>* inode) {
  ReadLockGuard lk(rwlock_);

  auto iter = files_.find(ino);
  if (iter == files_.end()) {
    VLOG(3) << "Open file not found: ino = " << ino;
    return false;
  }

  *inode = iter->second->inode;
  return true;
}

// file should already flushed before close
void OpenFiles::Close(Ino ino) {
  WriteLockGuard lk(rwlock_);

  auto iter = files_.find(ino);
  if (iter == files_.end()) {
    LOG(INFO) << "No need to close, Open file not found: ino = " << ino;
    return;
  }

  CHECK_GT(iter->second->refs, 0);
  iter->second->refs--;

  if (iter->second->refs == 0) {
    VLOG(3) << "Delete open file cache: ino = " << ino
            << ", refs = " << iter->second->refs
            << ", mtime = " << InodeMtime(iter->second->inode);

    files_.erase(iter);
    metric_->AddOpenfiles(-1);
  }
}

void OpenFiles::CloseAll() {
  WriteLockGuard lk(rwlock_);
  auto iter = files_.begin();

  while (iter != files_.end()) {
    deferSync_->Push(iter->second->inode);

    VLOG(6) << "Delete open file cache: ino = " << iter->first
            << ", refs = " << iter->second->refs
            << ", mtime = " << InodeMtime(iter->second->inode);
    iter = files_.erase(iter);
    metric_->AddOpenfiles(-1);
  }
}

bool OpenFiles::GetFileAttr(Ino ino, pb::metaserver::InodeAttr* attr) {
  ReadLockGuard lk(rwlock_);
  auto iter = files_.find(ino);
  if (iter == files_.end()) {
    VLOG(3) << "Open file not found: ino = " << ino;
    return false;
  }

  iter->second->inode->GetInodeAttr(attr);
  return true;
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
