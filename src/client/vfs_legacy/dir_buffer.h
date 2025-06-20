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

#ifndef DINGOFS_SRC_CLIENT_DIR_BUFFER_H_
#define DINGOFS_SRC_CLIENT_DIR_BUFFER_H_

#include <atomic>
#include <cstdint>
#include <deque>
#include <unordered_map>

#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {

struct DirBufferHead {
  bool wasRead;
  size_t size;
  char* p;
  DirBufferHead() : wasRead(false), size(0), p(nullptr) {}
};

// directory buffer
class DirBuffer {
 public:
  DirBuffer() : index_(0) {}
  // New a buffer head, and return a dindex
  uint64_t DirBufferNew();
  // New a buffer head by index, and return same index
  uint64_t DirBufferNewWithIndex(uint64_t index);
  // Get the buffer head by the dindex
  DirBufferHead* DirBufferGet(uint64_t dindex);
  // Release the buffer and buffer head by the dindex
  void DirBufferRelease(uint64_t dindex);
  // Release all buffer and buffer head
  void DirBufferFreeAll();

 private:
  std::unordered_map<uint64_t, DirBufferHead*> buffer_;
  dingofs::utils::RWLock bufferMtx_;

  dingofs::utils::Atomic<uint64_t> index_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_DIR_BUFFER_H_
