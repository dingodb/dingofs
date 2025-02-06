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
 * Created Date: 2025-02-19
 * Author: Jingli Chen (Wine93)
 */

#include <butil/time.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <malloc.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <thread>

#include "base/filepath/filepath.h"
#include "base/math/math.h"
#include "base/string/string.h"
#include "cache/blockcache/aio.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/common/local_filesystem.h"
#include "gtest/gtest.h"
#include "utils/concurrent/count_down_event.h"
#include "utils/concurrent/rw_lock.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::dingofs::base::filepath::PathJoin;
using ::dingofs::base::math::kGiB;
using ::dingofs::base::math::kMiB;
using ::dingofs::base::string::StrFormat;
using ::dingofs::utils::CountDownEvent;
using ::dingofs::utils::ReadLockGuard;
// using ::curvefs::utils::RWLock;

class AioTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

class TestLock {
 public:
  TestLock() : n(0) {}

  void Do() {
    // ReadLockGuard lk(rwlock_);
    n++;
  }

 private:
  int n;
  // RWLock rwlock_;
};

class FioJob {
  struct Buffer {
    Buffer(uint64_t size) {
      buffer = reinterpret_cast<char*>(memalign(512, size));
      std::memset(buffer, 0, size);
    }
    //~Buffer() { delete buffer; }
    char* buffer;
  };

 public:
  FioJob(const std::string directory, const std::string& name, uint32_t index,
         uint32_t file_size, uint64_t nfiles,
         std::shared_ptr<AioQueue> aio_queue)
      : index_(index),
        file_prefix_(PathJoin({directory, StrFormat("%s.%d", name, index)})),
        file_size_(file_size),
        nfiles_(nfiles),
        aio_queue_(aio_queue) {
    buffers_.reserve(kBatchSize);
    for (auto i = 0; i < kBatchSize; i++) {
      // buffers_[i] = new Buffer(file_size);
      buffers_.push_back(new Buffer(file_size));
    }
  }

  void Start() {
    if (!running_) {
      thread_id_ = std::thread(&FioJob::Worker, this);
      LOG(INFO) << "job-" << index_ << " (" << file_size_ << "," << nfiles_
                << ") started.";
      running_ = true;
    }
  }

  void Wait() { thread_id_.join(); }

 private:
  void Worker() {
    ::butil::Timer timer;
    timer.start();

    LOG(ERROR) << "<<< nfiles = " << nfiles_;

    uint32_t num_submit = 0;
    while (running_) {
      int n = std::min(kBatchSize, nfiles_ - num_submit);
      if (n <= 0) {  // FIXME(@Wine93)
        break;
      }

      // LOG(ERROR) << "<<< num_submit: " << num_submit << ", n: " << n;

      std::vector<AioContext> contexts;
      CountDownEvent count_down(n);

      for (auto i = 0; i < n; i++) {
        auto filepath = StrFormat("%s.%d", file_prefix_, num_submit++);
        int fd = CreateFile(filepath);

        CHECK(buffers_.size() > 0);
        CHECK(buffers_[i]->buffer != nullptr);

        auto context = AioContext(AioType::kWrite, fd, 0, file_size_,
                                  buffers_[i]->buffer, [&](AioContext* ctx) {
                                    CHECK(ctx->retval == file_size_);
                                    count_down.Signal();
                                  });
        contexts.push_back(context);
      }
      aio_queue_->Submit(contexts);
      count_down.Wait();
    }

    timer.stop();

    double elapsed = timer.u_elapsed() / 1e6;  // seconds
    CHECK(elapsed > 0);
    uint64_t bandwidth = file_size_ * nfiles_ * 1.0 / kMiB / elapsed;
    double latency = elapsed * 1.0 / nfiles_;

    LOG(INFO) << StrFormat(
        "job-%d summary: write %d MiB, cost %.6lf seconds, bw=%d MiB/s, "
        "lat=%.6lf "
        "seconds/file",
        index_, file_size_ * nfiles_ / kMiB, elapsed, bandwidth, latency);
  }

  static int CreateFile(const std::string& filepath) {
    static auto fs = NewTempLocalFileSystem();
    int fd;
    auto rc = fs->Do([&](const std::shared_ptr<PosixFileSystem> posix) {
      return posix->Create(filepath, &fd, true);
    });
    CHECK(rc == Errno::OK);
    return fd;
  }

 private:
  mutable bool running_;
  std::thread thread_id_;
  uint32_t index_;
  std::string file_prefix_;
  uint32_t file_size_;
  uint64_t nfiles_;
  std::shared_ptr<AioQueue> aio_queue_;
  std::vector<Buffer*> buffers_;
  static constexpr uint64_t kBatchSize = 16;
};

/*
TEST_F(AioTest, Basic) {
  ASSERT_TRUE(AioQueue::Supported());

  auto queue = AioQueue();
  ASSERT_TRUE(queue.Init(512));

  int fd;
  auto fs = NewTempLocalFileSystem();
  auto rc = fs->Do([&](const std::shared_ptr<PosixFileSystem> posix) {
    auto rc = posix->Create("/tmp/wine93/my-file", &fd, true);
    return rc;
  });
  ASSERT_EQ(rc, Errno::OK);

  ::butil::Timer timer;

  auto aio_cb = [&](AioContext* ctx) {
    timer.stop();
    std::cout << timer.u_elapsed() / 1e6 << std::endl;

    std::cout << ::strerror(-ctx->status_code) << std::endl;
    ASSERT_EQ(ctx->status_code, 0);
    std::cout << "YES" << std::endl;
  };

  // char* buffer = new char[4 * kMiB];
  // std::memset(buffer, 0, 4 * kMiB);
  // for (int i = 0; i < 4096; i++) {
  //   buffer[i] = 'a';
  // }

  char* buffer = reinterpret_cast<char*>(memalign(512, 4 * kMiB));

  AioContext context(AioType::kWrite, fd, 0, 4 * kMiB, buffer, aio_cb);
  timer.start();
  queue.Submit(std::vector<AioContext>{context});
  std::this_thread::sleep_for(3600 * std::chrono::seconds(1));
}
*/

/*
TEST_F(AioTest, Benchmark) {
  auto aio_queue = std::make_shared<AioQueue>();
  ASSERT_TRUE(aio_queue->Init(512));

  uint64_t total_size = 400 * kGiB;
  uint64_t jobs = 1;
  uint nfiles = total_size / jobs / (4 * kMiB);
  std::string directory = "/mnt/dingofs-cache/aio";

  std::vector<std::shared_ptr<FioJob>> fio_jobs;
  for (auto i = 0; i < jobs; i++) {
    // auto* fio_job =
    //     new FioJob(directory, "io_uring", i, 4 * kMiB, nfiles, aio_queue);
    auto job = std::make_shared<FioJob>(directory, "io_uring", i, 4 * kMiB,
                                        nfiles, aio_queue);
    fio_jobs.push_back(job);
    job->Start();
  }

  std::this_thread::sleep_for(3600 * std::chrono::seconds(1));
}
*/

// TEST_F(AioTest, Lock) {
//   TestLock lock;
//   lock.Do();
// }

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
