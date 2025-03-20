/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: dingo
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "client/vfs_old/dir_buffer.h"

namespace dingofs {
namespace client {

uint64_t DirBuffer::DirBufferNew() {
  uint64_t dindex = index_++;
  dingofs::utils::WriteLockGuard wlg(bufferMtx_);
  DirBufferHead* head = new DirBufferHead();
  buffer_.emplace(dindex, head);
  return dindex;
}

uint64_t DirBuffer::DirBufferNewWithIndex(uint64_t index) {
  dingofs::utils::WriteLockGuard wlg(bufferMtx_);
  DirBufferHead* head = new DirBufferHead();
  buffer_.emplace(index, head);
  if (index_.load() <= index) {
    index_.store(index + 1);
  }
  return index;
}

DirBufferHead* DirBuffer::DirBufferGet(uint64_t dindex) {
  dingofs::utils::ReadLockGuard rlg(bufferMtx_);
  auto it = buffer_.find(dindex);
  if (it != buffer_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

// TODO(xuchaojie) : these two function need to be called in right place.
void DirBuffer::DirBufferRelease(uint64_t dindex) {
  dingofs::utils::WriteLockGuard wlg(bufferMtx_);
  auto it = buffer_.find(dindex);
  if (it != buffer_.end()) {
    free(it->second->p);
    delete it->second;
    buffer_.erase(it);
  }
}

void DirBuffer::DirBufferFreeAll() {
  dingofs::utils::WriteLockGuard wlg(bufferMtx_);
  for (auto it : buffer_) {
    free(it.second->p);
    delete it.second;
  }
  buffer_.clear();
  index_ = 0;
}

}  // namespace client
}  // namespace dingofs
