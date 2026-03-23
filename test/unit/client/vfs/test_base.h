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

#ifndef DINGOFS_TEST_UNIT_CLIENT_VFS_TEST_BASE_H_
#define DINGOFS_TEST_UNIT_CLIENT_VFS_TEST_BASE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "client/vfs/components/file_suffix_watcher.h"
#include "common/options/client.h"
#include "client/vfs/handle/handle_manager.h"
#include "client/vfs/memory/read_buffer_manager.h"
#include "client/vfs/memory/write_buffer_manager.h"
#include "client/vfs/metasystem/meta_wrapper.h"
#include "common/trace/context.h"
#include "common/status.h"
#include "test/unit/client/vfs/mock/mock_block_store.h"
#include "test/unit/client/vfs/mock/mock_compactor.h"
#include "test/unit/client/vfs/mock/mock_meta_system.h"
#include "test/unit/client/vfs/mock/mock_vfs_hub.h"
#include "test/unit/client/vfs/test_common.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

using ::testing::AnyNumber;
using ::testing::Return;

// VFSTestBase: shared test fixture for all VFS component tests.
//
// Inspired by dingo-sdk TestBase:
//   - Constructor creates all mocks and real objects
//   - ON_CALL + WillByDefault sets permissive defaults
//   - EXPECT_CALL + AnyNumber() prevents over-specification
//   - Real Executors (not mocked); Start() in ctor, Stop() in dtor
//   - MockBlockStore callbacks invoked synchronously (deterministic)
//
// Usage:
//   class MyTest : public VFSTestBase { ... };
class VFSTestBase : public ::testing::Test {
 public:
  VFSTestBase() {
    // --- 1. MockVFSHub ---
    auto hub_uptr = std::make_unique<MockVFSHub>();
    mock_hub_ = hub_uptr.get();
    hub_uptr_ = std::move(hub_uptr);

    // --- 2. Real stateless objects ---
    read_buf_mgr_ =
        std::make_unique<ReadBufferManager>(64 * 1024 * 1024);  // 64MB
    write_buf_mgr_ =
        std::make_unique<WriteBufferManager>(64 * 1024 * 1024, 4096);
    file_suffix_watcher_ = std::make_unique<FileSuffixWatcher>("");

    // --- 3. MockMetaSystem + MetaWrapper ---
    auto ms = std::make_unique<MockMetaSystem>();
    mock_meta_system_ = ms.get();
    meta_wrapper_ = std::make_unique<MetaWrapper>(std::move(ms));

    // --- 4. MockBlockStore + MockCompactor ---
    auto bs = std::make_unique<MockBlockStore>();
    mock_block_store_ = bs.get();
    block_store_uptr_ = std::move(bs);

    auto cp = std::make_unique<MockCompactor>();
    mock_compactor_ = cp.get();
    compactor_uptr_ = std::move(cp);

    // --- 5. HandleManager ---
    handle_manager_ = std::make_unique<HandleManager>(mock_hub_);
    CHECK(handle_manager_->Start().ok());

    // --- 6. Real thread pools (sdk pattern: don't mock executors) ---
    read_executor_ = std::make_unique<ExecutorImpl>("test_read", 2);
    flush_executor_ = std::make_unique<ExecutorImpl>("test_flush", 2);
    bg_executor_ = std::make_unique<ExecutorImpl>("test_bg", 1);
    cb_executor_ = std::make_unique<ExecutorImpl>("test_cb", 1);
    CHECK(read_executor_->Start());
    CHECK(flush_executor_->Start());
    CHECK(bg_executor_->Start());
    CHECK(cb_executor_->Start());

    // --- 7. MockVFSHub defaults (ON_CALL + AnyNumber pattern) ---
    ON_CALL(*mock_hub_, GetMetaSystem())
        .WillByDefault(Return(meta_wrapper_.get()));
    ON_CALL(*mock_hub_, GetHandleManager())
        .WillByDefault(Return(handle_manager_.get()));
    ON_CALL(*mock_hub_, GetBlockStore())
        .WillByDefault(Return(mock_block_store_));
    ON_CALL(*mock_hub_, GetReadBufferManager())
        .WillByDefault(Return(read_buf_mgr_.get()));
    ON_CALL(*mock_hub_, GetWriteBufferManager())
        .WillByDefault(Return(write_buf_mgr_.get()));
    ON_CALL(*mock_hub_, GetFileSuffixWatcher())
        .WillByDefault(Return(file_suffix_watcher_.get()));
    ON_CALL(*mock_hub_, GetCompactor())
        .WillByDefault(Return(mock_compactor_));
    ON_CALL(*mock_hub_, GetReadExecutor())
        .WillByDefault(Return(read_executor_.get()));
    ON_CALL(*mock_hub_, GetFlushExecutor())
        .WillByDefault(Return(flush_executor_.get()));
    ON_CALL(*mock_hub_, GetBGExecutor())
        .WillByDefault(Return(bg_executor_.get()));
    ON_CALL(*mock_hub_, GetCBExecutor())
        .WillByDefault(Return(cb_executor_.get()));
    ON_CALL(*mock_hub_, GetFsInfo()).WillByDefault(Return(MakeTestFsInfo()));

    EXPECT_CALL(*mock_hub_, GetMetaSystem()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetHandleManager()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetBlockStore()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetReadBufferManager()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetWriteBufferManager()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetFileSuffixWatcher()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetCompactor()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetReadExecutor()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetFlushExecutor()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetBGExecutor()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetCBExecutor()).Times(AnyNumber());
    EXPECT_CALL(*mock_hub_, GetFsInfo()).Times(AnyNumber());

    // --- 8. MockBlockStore: synchronous success by default ---
    // Callbacks invoked inline (no async) to eliminate timing non-determinism.
    ON_CALL(*mock_block_store_, PutAsync)
        .WillByDefault([](ContextSPtr, PutReq, StatusCallback cb) {
          cb(Status::OK());
        });
    ON_CALL(*mock_block_store_, RangeAsync)
        .WillByDefault([](ContextSPtr, RangeReq req, StatusCallback cb) {
          // Fill output buffer with zeros so io_buffer.Size() == block_req.len.
          if (req.data && req.length > 0) {
            char* buf = new char[req.length]();
            req.data->AppendUserData(buf, req.length,
                                     [](void* p) { delete[] static_cast<char*>(p); });
          }
          cb(Status::OK());
        });
    ON_CALL(*mock_block_store_, PrefetchAsync)
        .WillByDefault([](ContextSPtr, PrefetchReq, StatusCallback cb) {
          cb(Status::OK());
        });
    ON_CALL(*mock_block_store_, EnableCache()).WillByDefault(Return(false));
    EXPECT_CALL(*mock_block_store_, PutAsync).Times(AnyNumber());
    EXPECT_CALL(*mock_block_store_, RangeAsync).Times(AnyNumber());
    EXPECT_CALL(*mock_block_store_, PrefetchAsync).Times(AnyNumber());
    EXPECT_CALL(*mock_block_store_, EnableCache()).Times(AnyNumber());

    // --- 9. MockMetaSystem defaults ---
    static std::atomic<uint64_t> slice_id_counter{1};
    ON_CALL(*mock_meta_system_, NewSliceId)
        .WillByDefault([](ContextSPtr, Ino, uint64_t* id) {
          *id = slice_id_counter.fetch_add(1, std::memory_order_relaxed);
          return Status::OK();
        });
    ON_CALL(*mock_meta_system_, WriteSlice)
        .WillByDefault(Return(Status::OK()));
    ON_CALL(*mock_meta_system_, ReadSlice)
        .WillByDefault(
            [](ContextSPtr, Ino, uint64_t, uint64_t, std::vector<Slice>* s,
               uint64_t& v) {
              s->clear();
              v = 0;
              return Status::OK();
            });
    ON_CALL(*mock_meta_system_, Flush).WillByDefault(Return(Status::OK()));
    ON_CALL(*mock_meta_system_, Close).WillByDefault(Return(Status::OK()));
    ON_CALL(*mock_meta_system_, Open).WillByDefault(Return(Status::OK()));
    EXPECT_CALL(*mock_meta_system_, NewSliceId).Times(AnyNumber());
    EXPECT_CALL(*mock_meta_system_, WriteSlice).Times(AnyNumber());
    EXPECT_CALL(*mock_meta_system_, ReadSlice).Times(AnyNumber());
    EXPECT_CALL(*mock_meta_system_, Flush).Times(AnyNumber());
    EXPECT_CALL(*mock_meta_system_, Close).Times(AnyNumber());
    EXPECT_CALL(*mock_meta_system_, Open).Times(AnyNumber());

    // Disable meta access logging (meta_logger is null in tests; InitMetaLog
    // is never called, but FLAGS_vfs_meta_access_logging defaults to true).
    FLAGS_vfs_meta_access_logging = false;

    // Default context for tests
    ctx_ = std::make_shared<Context>("test");
  }

  ~VFSTestBase() override {
    handle_manager_->Stop();
    cb_executor_->Stop();
    flush_executor_->Stop();
    read_executor_->Stop();
    bg_executor_->Stop();
  }

 protected:
  MockVFSHub* mock_hub_{nullptr};
  MockMetaSystem* mock_meta_system_{nullptr};
  MockBlockStore* mock_block_store_{nullptr};
  MockCompactor* mock_compactor_{nullptr};

  // Owned objects (uptr for ownership, raw ptr for convenience)
  std::unique_ptr<MockVFSHub> hub_uptr_;
  std::unique_ptr<MockBlockStore> block_store_uptr_;
  std::unique_ptr<MockCompactor> compactor_uptr_;
  std::unique_ptr<MetaWrapper> meta_wrapper_;
  std::unique_ptr<HandleManager> handle_manager_;
  std::unique_ptr<ReadBufferManager> read_buf_mgr_;
  std::unique_ptr<WriteBufferManager> write_buf_mgr_;
  std::unique_ptr<FileSuffixWatcher> file_suffix_watcher_;

  // Real executors
  std::unique_ptr<ExecutorImpl> read_executor_;
  std::unique_ptr<ExecutorImpl> flush_executor_;
  std::unique_ptr<ExecutorImpl> bg_executor_;
  std::unique_ptr<ExecutorImpl> cb_executor_;

  ContextSPtr ctx_;
};

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CLIENT_VFS_TEST_BASE_H_
