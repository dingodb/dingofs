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
 * Created Date: 2025-02-22
 * Author: Jingli Chen (Wine93)
 */

#include <butil/time.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <malloc.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

#include "base/math/math.h"
#include "cache/blockcache/aio.h"
#include "cache/common/local_filesystem.h"

using ::dingofs::base::math::kMiB;
using ::dingofs::client::blockcache::AioQueue;
using ::dingofs::client::blockcache::AioQueueImpl;
using ::dingofs::client::blockcache::AioType;
using ::dingofs::client::blockcache::PosixFileSystem;
using AioContext = ::dingofs::client::blockcache::AioContext;

// 主要用 aio、mmap 配合 brpc 测一下，能否实现零拷贝，以及耗时怎么样
//    case 1: read + iobuf
//    case 2: io_uring + iobuf
//    case 3: io_uring(memory pin) + iobuf
//    case 4: mmap + iobuf
//    case 5: sendfile
// 测一下 aio 的各种模式

butil::Timer k_timer;

std::shared_ptr<AioQueueImpl> NewAioQueue() {
  auto aio_queue = std::make_shared<AioQueueImpl>();
  bool rc = aio_queue->Init(128);
  LOG_IF(FATAL, rc != true) << "Init aio queue failed, rc=" << rc;
  return aio_queue;
}

int OpenFile(const std::string& filepath) {
  int fd;
  auto fs = std::make_shared<PosixFileSystem>(nullptr);
  // auto rc = fs->Create(filepath, &fd, true);
  // auto rc = fs->Open(filepath, O_DIRECT, &fd);
  auto rc = fs->Open(filepath, O_DIRECT, &fd);
  LOG_IF(FATAL, fd <= 0) << "Open file failed.";
  return fd;
}

static void AioCallback(AioContext* /*aio*/, int rc) {
  k_timer.stop();
  LOG(INFO) << "Cost " << k_timer.u_elapsed() * 1.0 / 1e6
            << " seconds, rc=" << rc;
}

void Submit(int fd) {}

char* AllocBuffer(size_t size) {
  // char* buffer = new char[size];
  char* buffer = reinterpret_cast<char*>(memalign(4096, size));

  //::butil::Timer timer;
  // timer.start();

  for (int i = 0; i < size; i++) {
    buffer[i] = 'a' + i % 26;
  }

  /*
    int rc = madvise(buffer, 4 * kMiB, MADV_WILLNEED | MADV_HUGEPAGE);
    LOG_IF(FATAL, rc != 0) << "madvise() failed, rc=" << rc;
  */

  // timer.stop();
  // LOG(INFO) << "Cost " << timer.u_elapsed() * 1.0 / 1e6 << " seconds.";

  return buffer;
}

void FreeBuffer(const char* buffer) { delete[] buffer; }

int main(int argc, char** argv) {
  CHECK(argc == 2) << "argc=" << argc;

  if (!AioQueue::Supported()) {
    CHECK(false) << "Aio not supported";
  }

  auto aio_queue = NewAioQueue();
  int fd = OpenFile(argv[1]);
  char* buffer = AllocBuffer(4 * kMiB);

  /*
  {
    k_timer.start();
    int rc = ::read(fd, buffer, 4 * kMiB);
    LOG_IF(FATAL, rc != 4 * kMiB);
    k_timer.stop();
    LOG(INFO) << "Cost " << k_timer.u_elapsed() * 1.0 / 1e6
              << " seconds, rc=" << rc;
    k_timer.stop();
  }

  */

  /*
    {
      k_timer.start();

      int rc = ::fallocate(fd, 0, 0, 4 * kMiB);
      LOG_IF(FATAL, rc != 0) << "fallocate failed, rc=" << rc;
      rc = ::posix_fadvise(fd, 0, 4 * kMiB, POSIX_FADV_DONTNEED);
      LOG_IF(FATAL, rc != 0) << "posix_fadvise() failed, rc=" << rc;

      int rc = 0;

      int n = ::write(fd, buffer, 4 * kMiB);
      LOG_IF(FATAL, n != 4 * kMiB) << "write() failed, rc=" << rc;

      k_timer.stop();

      LOG(INFO) << "Cost " << k_timer.u_elapsed() * 1.0 / 1e6
                << " seconds, n=" << n;
    }
  */

  /*
  {
    AioContext aio(AioType::kRead, fd, 0, 4 * kMiB, buffer, AioCallback);
    k_timer.start();

    // int rc = ::fallocate(fd, 0, 0, 4 * kMiB);
    // LOG_IF(FATAL, rc != 0) << "fallocate failed, rc=" << rc;
    // rc = ::posix_fadvise(fd, 0, 4 * kMiB, POSIX_FADV_DONTNEED);
    // LOG_IF(FATAL, rc != 0) << "posix_fadvise() failed, rc=" << rc;
    // rc = ::posix_fadvise(fd, 0, 4 * kMiB, POSIX_FADV_SEQUENTIAL);
    // LOG_IF(FATAL, rc != 0) << "posix_fadvise() failed, rc=" << rc;

    aio_queue->Submit(std::vector<AioContext*>{&aio});
  }
  */

  {
    char* out = (char*)::mmap(nullptr, 4 * kMiB, PROT_READ, MAP_PRIVATE, fd, 0);
    CHECK(out != MAP_FAILED) << "mmap() failed.";
    AioContext aio(AioType::kRead, fd, 0, 4 * kMiB, buffer, AioCallback);

    int rc = madvise(buffer, 4 * kMiB, MADV_WILLNEED | MADV_HUGEPAGE);
    LOG_IF(FATAL, rc != 0) << "madvise() failed, rc=" << rc;

    k_timer.start();
    /*
    for (int i = 0; i < 4 * kMiB; i++) {
      out[i] = 'a';
    }
    */

    aio_queue->Submit(std::vector<AioContext*>{&aio});
  }

  std::this_thread::sleep_for(std::chrono::seconds(3600));

  LOG(ERROR) << "Hello wold";
  return 0;
}
