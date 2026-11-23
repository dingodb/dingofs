/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "blockcache/common/status.h"
#include "blockcache/core/fs/filesystem.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/utils/admission_queue.h"
#include "test/unit/blockcache/base/runtime.h"
#include "test/unit/blockcache/base/temp_dir.h"

namespace dingofs {
namespace blockcache {

// An io the queue admits; it notes the order the ios were submitted in.
struct Op {
  int id;
  std::vector<int>* submitted;
  Op* park_next = nullptr;

  void Submit() const { submitted->push_back(id); }
};

static std::vector<Op> MakeOps(int n, std::vector<int>* submitted) {
  std::vector<Op> ops;
  ops.reserve(n);
  for (int i = 0; i < n; i++) {
    ops.push_back({.id = i, .submitted = submitted});
  }
  return ops;
}

TEST(AdmissionQueueTest, Admit) {
  {  // up to the limit an op goes straight through, beyond it it waits
    std::vector<int> submitted;
    std::vector<Op> ops = MakeOps(4, &submitted);
    AdmissionQueue<Op> queue(2);
    for (Op& op : ops) {
      queue.Admit(&op);
    }
    EXPECT_EQ(submitted, (std::vector<int>{0, 1}));
  }

  {  // 128 in flight unless told otherwise
    std::vector<int> submitted;
    std::vector<Op> ops = MakeOps(130, &submitted);
    AdmissionQueue<Op> queue;
    for (Op& op : ops) {
      queue.Admit(&op);
    }
    EXPECT_EQ(submitted.size(), 128);
    EXPECT_EQ(submitted.back(), 127);
  }
}

TEST(AdmissionQueueTest, OnComplete) {
  std::vector<int> submitted;
  std::vector<Op> ops = MakeOps(8, &submitted);
  AdmissionQueue<Op> queue(2);
  for (int i = 0; i < 4; i++) {
    queue.Admit(&ops[i]);
  }
  ASSERT_EQ(submitted, (std::vector<int>{0, 1}));

  {  // a completion hands its slot to the op that has waited longest
    queue.OnComplete();
    EXPECT_EQ(submitted, (std::vector<int>{0, 1, 2}));
    queue.OnComplete();
    EXPECT_EQ(submitted, (std::vector<int>{0, 1, 2, 3}));
  }

  {  // with nobody waiting the slot is freed for the next op
    queue.OnComplete();
    queue.Admit(&ops[4]);
    EXPECT_EQ(submitted, (std::vector<int>{0, 1, 2, 3, 4}));
    queue.Admit(&ops[5]);
    EXPECT_EQ(submitted, (std::vector<int>{0, 1, 2, 3, 4}));
  }

  {  // and once everything completes the queue is idle again
    for (int i = 0; i < 3; i++) {
      queue.OnComplete();
    }
    EXPECT_EQ(submitted, (std::vector<int>{0, 1, 2, 3, 4, 5}));
    queue.Admit(&ops[6]);
    queue.Admit(&ops[7]);
    EXPECT_EQ(submitted, (std::vector<int>{0, 1, 2, 3, 4, 5, 6, 7}));
  }
}

class AdmissionQueueRuntimeTest : public ::testing::Test {
 protected:
  // Opens `path` for `io_inflight` ios at a time and writes `n` 4KiB blocks
  // to it back to back; returns the order the writes finished in.
  static Future<std::vector<int>> WriteBlocks(std::string path,
                                              uint32_t io_inflight, int n) {
    StatusOr<File> file = co_await FileSystem::Open(
        std::move(path), OpenFlags::kWrite | OpenFlags::kCreate,
        {.io_inflight = io_inflight, .direct = false});
    EXPECT_TRUE(file.ok()) << file.status().ToString();
    std::vector<int> done;
    if (file.ok()) {
      std::vector<Future<>> writes;
      for (int i = 0; i < n; i++) {
        writes.push_back(WriteBlock(&file.value(), i, &done));
      }
      for (Future<>& write : writes) {
        co_await std::move(write);
      }
      EXPECT_TRUE((co_await file->Close()).ok());
    }
    co_return done;
  }

  static Future<> WriteBlock(File* file, int index, std::vector<int>* done) {
    static const std::string kBlock(4096, 'x');
    StatusOr<size_t> written = co_await file->Write(
        index * kBlock.size(), kBlock.data(), kBlock.size());
    EXPECT_TRUE(written.ok()) << written.status().ToString();
    EXPECT_EQ(written.ok() ? *written : 0, kBlock.size());
    done->push_back(index);
  }

  ScopedRuntime runtime_;
};

TEST_F(AdmissionQueueRuntimeTest, FileQueue) {
  // A file opened for one io at a time parks the writes that come while one
  // runs, and lets them through one by one, oldest first.
  TempDir dir;
  const std::string path = (dir.path() / "1001_0_4194304").string();
  const std::vector<int> done =
      RunOnShard(0, [&path] { return WriteBlocks(path, 1, 3); });
  EXPECT_EQ(done, (std::vector<int>{0, 1, 2}));
}

}  // namespace blockcache
}  // namespace dingofs
