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

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <string>

#include "client/vfs/data/write_pressure_controller.h"
#include "client/vfs/data/writer_table.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace {

class ManualExecutor final : public Executor {
 public:
  bool Start() override {
    running_ = true;
    return true;
  }
  bool Stop() override {
    running_ = false;
    return true;
  }
  bool Execute(std::function<void()> func) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) return false;
    if (reject_execute_once_) {
      reject_execute_once_ = false;
      return false;
    }
    tasks_.push_back(std::move(func));
    return true;
  }
  bool Schedule(std::function<void()> func, int delay_ms) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) return false;
    tasks_.push_back(std::move(func));
    return true;
  }
  int ThreadNum() const override { return 1; }
  int TaskNum() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<int>(tasks_.size());
  }
  std::string Name() const override { return "manual"; }

  void RejectNextExecute() {
    std::lock_guard<std::mutex> lock(mutex_);
    reject_execute_once_ = true;
  }

  void RunOne() {
    std::function<void()> task;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ASSERT_FALSE(tasks_.empty());
      task = std::move(tasks_.front());
      tasks_.pop_front();
    }
    task();
  }

 private:
  mutable std::mutex mutex_;
  bool running_{false};
  bool reject_execute_once_{false};
  std::deque<std::function<void()>> tasks_;
};

TEST(WritePressureControllerTest, CoalescesEventsBeforeRoundRuns) {
  WriterTable table(nullptr);
  ASSERT_TRUE(table.Start().ok());
  ManualExecutor executor;
  ASSERT_TRUE(executor.Start());
  WritePressureController controller(&table, &executor);

  controller.OnWritePressure();
  controller.OnWritePressure();
  controller.OnWritePressure();
  EXPECT_EQ(executor.TaskNum(), 1);

  executor.RunOne();
  EXPECT_EQ(executor.TaskNum(), 0);

  controller.StopAndDrain();
  controller.OnWritePressure();
  EXPECT_EQ(executor.TaskNum(), 0);
  table.Stop();
}

TEST(WritePressureControllerTest, RetriesFirstRejectedSubmit) {
  WriterTable table(nullptr);
  ASSERT_TRUE(table.Start().ok());
  ManualExecutor executor;
  ASSERT_TRUE(executor.Start());
  executor.RejectNextExecute();
  WritePressureController controller(&table, &executor);

  controller.OnWritePressure();
  ASSERT_EQ(executor.TaskNum(), 1) << "bounded retry must be scheduled";

  executor.RunOne();
  ASSERT_EQ(executor.TaskNum(), 1) << "retry must submit the flush round";
  executor.RunOne();
  EXPECT_EQ(executor.TaskNum(), 0);

  controller.StopAndDrain();
  table.Stop();
}

TEST(WritePressureControllerTest, StopWaitsForQueuedRoundToRetire) {
  WriterTable table(nullptr);
  ASSERT_TRUE(table.Start().ok());
  ManualExecutor executor;
  ASSERT_TRUE(executor.Start());
  WritePressureController controller(&table, &executor);

  controller.OnWritePressure();
  ASSERT_EQ(executor.TaskNum(), 1);

  auto stopped = std::async(std::launch::async, [&] {
    controller.StopAndDrain();
    return true;
  });
  EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);

  executor.RunOne();
  EXPECT_EQ(stopped.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_TRUE(stopped.get());
  table.Stop();
}

}  // namespace
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
