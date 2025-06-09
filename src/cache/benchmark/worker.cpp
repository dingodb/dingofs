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
 * Created Date: 2025-06-04
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/worker.h"

#include <memory>

#include "cache/benchmark/reporter.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/type.h"
#include "cache/config/benchmark.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

Worker::Worker(int worker_id, BlockCacheSPtr block_cache, ReporterSPtr reporter,
               BthreadCountdownEventSPtr countdown_event)
    : worker_id_(worker_id),
      block_(NewOneBlock()),
      block_cache_(block_cache),
      reporter_(reporter),
      countdown_event_(countdown_event) {}

Status Worker::Init() { return NeedPrepBlock() ? PrepBlock() : Status::OK(); }

void Worker::Run() {
  auto factory = NewBlockKeyFactory();
  auto task_factory = NewTaskFactory();

  for (auto i = 0; i < FLAGS_op_blocks; i++) {
    auto key = factory.Next();
    CHECK(key != std::nullopt);
    ExecuteTask(task_factory(key.value()));
  }
  countdown_event_->signal(1);
}

void Worker::Shutdown() {
  // do nothing
}

std::function<Worker::TaskFunc(const BlockKey& key)> Worker::NewTaskFactory() {
  if (FLAGS_op == "put") {
    return [this](const BlockKey& key) {
      return [this, key]() { return PutBlock(key); };
    };
  } else if (FLAGS_op == "async_put") {
    return [this](const BlockKey& key) {
      return [this, key]() { return AsyncPutBlock(key); };
    };
  } else if (FLAGS_op == "range") {
    return [this](const BlockKey& key) {
      return [this, key]() { return RangeBlock(key); };
    };
  } else if (FLAGS_op == "async_range") {
    return [this](const BlockKey& key) {
      return [this, key]() { return AsyncRangeBlock(key); };
    };
  }

  CHECK(false) << "Unsupported operation: " << FLAGS_op;
}

BlockKeyFactory Worker::NewBlockKeyFactory() const {
  return BlockKeyFactory(worker_id_, FLAGS_op_blocks);
}

Status Worker::PrepBlock() {
  std::cout << absl::StrFormat("[worker-%d] %d blocks preparing...\n",
                               worker_id_, FLAGS_op_blocks);

  butil::Timer timer;
  timer.start();

  auto factory = NewBlockKeyFactory();
  for (auto i = 0; i < FLAGS_op_blocks; i++) {
    auto key = factory.Next();
    CHECK(key != std::nullopt);
    auto status = block_cache_->Cache(key.value(), block_);
    if (!status.ok()) {
      return status;
    }
  }

  timer.stop();

  std::cout << absl::StrFormat(
      "[worker-%d] %d blocks prepared, cost %.6lf seconds.\n", worker_id_,
      FLAGS_op_blocks, timer.u_elapsed() * 1.0 / 1e6);

  return Status::OK();
}

Status Worker::PutBlock(const BlockKey& key) {
  auto option = PutOption();
  option.writeback = FLAGS_put_writeback;
  return block_cache_->Put(key, block_, option);
}

Status Worker::RangeBlock(const BlockKey& key) {
  IOBuffer buffer;
  auto option = RangeOption();
  option.retrive = FLAGS_range_retrive;
  auto status = block_cache_->Range(key, 0, FLAGS_op_blksize, &buffer, option);
  if (!status.ok()) {
    return status;
  }

  CHECK_EQ(buffer.Size(), FLAGS_op_blksize)
      << absl::StrFormat("Range block size mismatch: expected %lu, got %lu",
                         FLAGS_op_blksize, buffer.Size());

  auto data = std::make_unique<char[]>(buffer.Size());
  buffer.CopyTo(data.get());
  return Status::OK();
}

Status Worker::AsyncPutBlock(const BlockKey& key) {
  Status status;
  auto option = PutOption();
  option.writeback = FLAGS_put_writeback;
  BthreadCountdownEvent countdown_event(1);

  block_cache_->AsyncPut(
      key, block_,
      [&](Status st) {
        status = st;
        countdown_event.signal(1);
      },
      option);

  countdown_event.wait();
  return status;
}

Status Worker::AsyncRangeBlock(const BlockKey& key) {
  Status status;
  auto option = RangeOption();
  option.retrive = FLAGS_range_retrive;

  BthreadCountdownEvent countdown_event(1);

  IOBuffer buffer;
  block_cache_->AsyncRange(
      key, 0, FLAGS_op_blksize, &buffer,
      [&](Status st) {
        status = st;
        countdown_event.signal(1);
      },
      option);

  countdown_event.wait();

  auto data = std::make_unique<char[]>(buffer.Size());
  buffer.CopyTo(data.get());

  return status;
}

void Worker::ExecuteTask(std::function<Status()> task) {
  butil::Timer timer;
  timer.start();

  auto status = task();
  if (!status.ok()) {
    std::cout << absl::StrFormat("[worker-%d] execute task failed: %s\n",
                                 worker_id_, status.ToString());
  }

  timer.stop();

  reporter_->Submit(Event(EventType::kAddStat, FLAGS_op_blksize,
                          timer.u_elapsed() * 1.0 / 1e6));
}

}  // namespace cache
}  // namespace dingofs
