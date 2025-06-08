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

#include <butil/iobuf.h>
#include <butil/time.h>

#include <thread>

#include "cache/benchmark/helper.h"
#include "cache/benchmark/reporter.h"
#include "cache/blockcache/cache_store.h"
#include "cache/config/config.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

extern BenchmarkOption* g_option;

static constexpr uint64_t kBlocksPerChunk = 16;

Worker::Worker(int worker_id, BlockCacheSPtr block_cache, ReporterSPtr reporter)
    : worker_id_(worker_id),
      block_(NewBlock()),
      block_cache_(block_cache),
      reporter_(reporter) {}

Status Worker::Init() {
  auto op = g_option->op;
  if (op == "range" || op == "async_range") {
    return PrepRange();
  }
  return Status::OK();
}

Status Worker::PrepRange() {
  std::cout << "Preapre worker " << worker_id_ << "...." << std::endl;
  BlockKey key;
  auto key_generator = BlockKeyGenerator(worker_id_, g_option->blocks);
  for (;;) {
    auto key = key_generator.Next();
    if (key == std::nullopt) {
      break;
    }

    auto status = block_cache_->Cache(key.value(), block_);
    if (!status.ok()) {
      return status;
    }
  }

  std::cout << "Preapre worker " << worker_id_ << "success." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::cout << "start to run" << std::endl;

  return Status::OK();
}

void Worker::DoPut() {
  auto key_generator = BlockKeyGenerator(worker_id_, g_option->blocks);
  for (;;) {
    auto key = key_generator.Next();
    if (key == std::nullopt) {
      break;
    }

    Execute([this, key]() {
      return block_cache_->Put(key.value(), block_, PutOption(true));
    });
  }
}

void Worker::DoRange() {
  auto key_generator = BlockKeyGenerator(worker_id_, g_option->blocks);
  for (;;) {
    auto key = key_generator.Next();
    if (key == std::nullopt) {
      break;
    }

    Execute([this, key]() {
      IOBuffer buffer;
      auto status = block_cache_->Range(key.value(), 0, g_option->blksize,
                                        &buffer, RangeOption(false));
      if (status.ok()) {
        // char* null = new char[g_option->blksize];
        //  buffer.CopyTo(null);
        // auto bufvec = buffer.Fetch();
        //  CHECK_EQ(bufvec.size(), 1);
        //   CHECK_EQ(buffer.Size(), 0);
        //  std::memcpy(null, bufvec[0].iov_base, buffer.Size());
        // delete[] null;
      }
      return status;
    });
  }
}

void Worker::Run() {
  auto op = g_option->op;
  if (op == "put") {
    DoPut();
  } else if (op == "range") {
    DoRange();
  }
}

void Worker::Execute(std::function<Status()> task) {
  butil::Timer timer;

  timer.start();
  auto status = task();
  timer.stop();

  reporter_->Submit(Record(g_option->blksize, timer.u_elapsed() / 1e6));
}

Block Worker::NewBlock() {
  butil::IOBuf iobuf;
  auto page_size = g_option->page_size;
  auto length = g_option->blksize;
  while (length > 0) {
    auto size = std::min(page_size, length);
    AppendPage(&iobuf, size);
    LOG(INFO) << "size = " << size;
    length -= size;
  }

  auto* buffer = new IOBuffer(iobuf);

  LOG(INFO) << "buffer size: " << buffer->Size();
  LOG(INFO) << "buffer size: " << buffer->IOBuf().size();
  LOG(INFO) << "buffer size: " << buffer->IOBuf().length();
  LOG(INFO) << "block num: " << buffer->IOBuf().backing_block_num();
  LOG(INFO) << "block count: " << buffer->IOBuf().block_count();

  return Block(buffer);
}

void Worker::AppendPage(butil::IOBuf* iobuf, size_t size) {
  char* data = new char[size];
  std::memset(data, 0, size);
  iobuf->append_user_data(
      data, size, [](void* data) { delete[] static_cast<char*>(data); });
}

}  // namespace cache
}  // namespace dingofs
