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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include "cache/common/task_tracker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <sstream>
#include <thread>

#include "common/block/block_handle.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {

namespace {
BlockHandle MakeHandle(uint64_t id) {
  return BlockHandle(1, BlockKey(id, 0, 4194304));
}
}  // namespace

TEST(DownloadTaskTest, AttrAndResult) {
  auto task = std::make_shared<DownloadTask>(MakeHandle(100), 4194304);
  EXPECT_EQ(task->Attr().handle.Filename(), "100_0_4194304");
  EXPECT_EQ(task->Attr().length, 4194304u);

  task->Result().status = Status::OK();
  EXPECT_TRUE(task->Result().status.ok());
  task->Result().status = Status::IoError("read failed");
  EXPECT_TRUE(task->Result().status.IsIoError());
}

TEST(DownloadTaskTest, WaitReturnsAfterRun) {
  {  // Run() from another thread wakes Wait()
    auto task = std::make_shared<DownloadTask>(MakeHandle(1), 1);
    std::thread worker([&task]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      task->Run();
    });
    EXPECT_TRUE(task->Wait(2000));
    worker.join();
  }

  {  // Wait() after Run() returns immediately
    auto task = std::make_shared<DownloadTask>(MakeHandle(2), 1);
    task->Run();
    EXPECT_TRUE(task->Wait(0));
  }
}

TEST(DownloadTaskTest, WaitTimesOut) {
  auto task = std::make_shared<DownloadTask>(MakeHandle(3), 1);

  auto start = std::chrono::steady_clock::now();
  bool finished = task->Wait(100);
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();

  EXPECT_FALSE(finished);
  EXPECT_GE(elapsed, 90);
}

TEST(TaskTrackerTest, GetOrCreateAndRemove) {
  TaskTracker tracker;
  auto handle = MakeHandle(100);

  DownloadTaskSPtr first;
  EXPECT_TRUE(tracker.GetOrCreateTask(handle, 4194304, first));
  ASSERT_NE(first, nullptr);

  DownloadTaskSPtr second;
  EXPECT_FALSE(tracker.GetOrCreateTask(handle, 4194304, second));
  EXPECT_EQ(first.get(), second.get());

  tracker.RemoveTask(handle);

  DownloadTaskSPtr third;
  EXPECT_TRUE(tracker.GetOrCreateTask(handle, 4194304, third));
  EXPECT_NE(first.get(), third.get());
}

TEST(TaskTrackerTest, DistinctHandlesGetDistinctTasks) {
  TaskTracker tracker;

  DownloadTaskSPtr a;
  DownloadTaskSPtr b;
  EXPECT_TRUE(tracker.GetOrCreateTask(MakeHandle(1), 1, a));
  EXPECT_TRUE(tracker.GetOrCreateTask(MakeHandle(2), 1, b));
  EXPECT_NE(a.get(), b.get());
}

TEST(TaskTrackerTest, StreamOperator) {
  auto task = std::make_shared<DownloadTask>(MakeHandle(100), 4194304);
  std::ostringstream oss;
  oss << task;
  EXPECT_EQ(oss.str(), "DownloadTask{key=100_0_4194304 length=4194304}");
}

}  // namespace cache
}  // namespace dingofs
