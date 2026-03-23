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

// Smoke tests for VFS brpc HTTP services.
//
// These services respond to HTTP RPC calls; their logic is tightly coupled to
// a running brpc server, so we only test that:
//   1. Each service can be constructed and destroyed without crashing.
//   2. Init() accepts a valid VFSHub* without crashing.
//
// Full end-to-end HTTP tests are integration-test scope.

#include <gtest/gtest.h>

#include "client/vfs/service/client_stat_service.h"
#include "client/vfs/service/compact_service.h"
#include "client/vfs/service/inode_blocks_service.h"
#include "test/unit/client/vfs/mock/mock_vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::AnyNumber;

// A minimal fixture that provides a MockVFSHub.
class ServicesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_hub_ = std::make_unique<test::MockVFSHub>();
    // No expectations needed; Init() only stores the pointer.
  }

  std::unique_ptr<test::MockVFSHub> mock_hub_;
};

TEST_F(ServicesTest, CompactService_ConstructAndInit_NoCrash) {
  CompactServiceImpl svc;
  svc.Init(mock_hub_.get());
  // Destruction must not crash.
}

TEST_F(ServicesTest, InodeBlocksService_ConstructAndInit_NoCrash) {
  InodeBlocksServiceImpl svc;
  svc.Init(mock_hub_.get());
}

TEST_F(ServicesTest, ClientStatService_ConstructAndInit_NoCrash) {
  ClientStatServiceImpl svc;
  svc.Init(mock_hub_.get());
}

TEST_F(ServicesTest, MultipleServices_ConstructedTogether_NoCrash) {
  CompactServiceImpl compact;
  InodeBlocksServiceImpl inode_blocks;
  ClientStatServiceImpl client_stat;

  compact.Init(mock_hub_.get());
  inode_blocks.Init(mock_hub_.get());
  client_stat.Init(mock_hub_.get());

  // All three share the same hub pointer - no ownership conflict.
}

TEST_F(ServicesTest, Services_DefaultConstructedWithoutInit_NoCrash) {
  // Default construction must be safe (Init not called).
  {
    CompactServiceImpl compact;
    (void)compact;
  }
  {
    InodeBlocksServiceImpl inode_blocks;
    (void)inode_blocks;
  }
  {
    ClientStatServiceImpl client_stat;
    (void)client_stat;
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
