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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "client/vfs/metasystem/mds/batch_processor.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"
#include "mds/filesystem/fs_info.h"
#include "test/unit/client/vfs/metasystem/mock/mock_mds_client.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {
namespace test {

using namespace std::chrono_literals;
using ::testing::_;
using ::testing::Invoke;

class BatchProcessorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mds::FsInfoEntry fs_info_entry;
    fs_info_ = std::make_unique<mds::FsInfo>(fs_info_entry);
    trace_manager_ = std::make_unique<TraceManager>();
    mock_mds_client_ = std::make_unique<MockMDSClient>(
        ClientId(), *fs_info_, RPC(butil::EndPoint()), *trace_manager_);
    ctx_ = std::make_shared<Context>("batch_processor_test");
  }

  static CommitTaskSPtr NewTask(uint64_t task_id, uint32_t chunk_index) {
    CommitTask::DeltaSlice delta;
    delta.chunk_index = chunk_index;
    delta.slices.push_back(
        Slice{.id = task_id, .size = 4096, .off = 0, .len = 4096, .pos = 0});

    std::vector<CommitTask::DeltaSlice> deltas;
    deltas.push_back(std::move(delta));
    return std::make_shared<CommitTask>(task_id, std::move(deltas), 1 << 20);
  }

  WriteSliceOperationSPtr NewOperation(CommitTaskSPtr task,
                                       uint32_t expected_chunk_index,
                                       int* done_count) {
    CommitTaskSPtr expected_task = task;
    return std::make_shared<WriteSliceOperation>(
        ctx_, 100, task,
        [expected_chunk_index, expected_task, done_count](
            const Status& status, CommitTaskSPtr completed_task,
            const MDSClient::WriteSliceResult& result) {
          ++(*done_count);
          EXPECT_TRUE(status.ok());
          EXPECT_EQ(completed_task, expected_task);
          ASSERT_EQ(result.chunks.size(), 1);
          EXPECT_EQ(result.chunks.front().index(), expected_chunk_index);
        });
  }

  std::unique_ptr<mds::FsInfo> fs_info_;
  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<MockMDSClient> mock_mds_client_;
  ContextSPtr ctx_;
};

}  // namespace test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
