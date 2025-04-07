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
 * Created Date: 2025-03-30
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_QUEUE_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_QUEUE_H_

#include <bthread/execution_queue.h>
#include <google/protobuf/stubs/callback.h>
#include <hf3fs_usrbio.h>

#include <cstddef>
#include <thread>

#include "client/blockcache/error.h"

namespace dingofs {
namespace client {
namespace blockcache {

class Closure : public ::google::protobuf::Closure {
 public:
  void SetCode(BCACHE_ERROR code) { code_ = code; }

  BCACHE_ERROR Code() const { return code_; }

 private:
  BCACHE_ERROR code_;
};

struct BlockTask {
  BlockTask(int fd, off_t offset, size_t length, size_t block_size,
            Closure* done)
      : fd(fd),
        offset(offset),
        length(length),
        iov(),
        block_size(block_size),
        done(done) {}

  int fd;
  off_t offset;
  size_t length;
  struct hf3fs_iov iov;
  size_t block_size;
  Closure* done;
};

class AioQueue {
 public:
  virtual ~AioQueue() = default;

  virtual bool Init(uint32_t io_depth) = 0;

  virtual bool Shutdown() = 0;

  virtual void Submit(BlockTask* task) = 0;
};

class AioQueueImpl : public AioQueue {
 public:
  AioQueueImpl(const std::string& mountpoint, bool for_read);

  bool Init(uint32_t io_depth) override;

  bool Shutdown() override;

  void Submit(BlockTask* task) override;

 private:
  static int BatchSubmit(void* meta, bthread::TaskIterator<BlockTask*>& iter);

  bool PrepareIO(BlockTask* task);

  void BackgroundWait();

  static void OnError(const std::vector<BlockTask*>& tasks);

  static void OnSuccess(const std::vector<BlockTask*>& tasks);

  static void RunClosure(const hf3fs_cqe* completed_entries, int n);

 private:
  std::atomic<bool> running_;
  std::string mountpoint_;
  bool for_read_;
  hf3fs_ior io_uring_;
  std::thread thread_;  // wait aio completed
  bthread::ExecutionQueueId<BlockTask*> submit_queue_id_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_QUEUE_H_
