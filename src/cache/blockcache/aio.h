/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-04-09
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_AIO_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_AIO_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <ostream>

#include "cache/common/closure.h"
#include "cache/common/context.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

struct Aio : public Closure {
  Aio(ContextSPtr ctx, int fd, off_t offset, size_t length, char* buffer,
      int buf_index, bool for_read)
      : ctx(ctx),
        fd(fd),
        offset(offset),
        length(length),
        buffer(buffer),
        buf_index(buf_index),
        for_read(for_read) {
    CHECK_GE(fd, 0);
    CHECK_GE(offset, 0);
    CHECK_GT(length, 0);
    CHECK_NOTNULL(buffer);
    CHECK_GE(buf_index, 0);
  }

  void Wait() {
    std::unique_lock<bthread::Mutex> lk(mutex);
    while (!finish) {
      cond.wait(lk);
    }
  }

  void Run() override {
    std::lock_guard<bthread::Mutex> lk(mutex);
    finish = true;
    cond.notify_one();
  }

  ContextSPtr ctx;
  int fd;
  off_t offset;
  size_t length;
  char* buffer;
  int buf_index;
  bool for_read;

  bool finish{false};
  bthread::Mutex mutex;
  bthread::ConditionVariable cond;
};

inline std::ostream& operator<<(std::ostream& os, const Aio& aio) {
  os << "Aio{" << (aio.for_read ? "read" : "write") << " fd=" << aio.fd
     << " offset=" << aio.offset << " length=" << aio.length
     << " buf_index=" << aio.buf_index << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_AIO_H_
