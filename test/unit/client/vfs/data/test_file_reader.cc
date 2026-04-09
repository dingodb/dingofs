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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

#include "client/vfs/data/reader/file_reader.h"
#include "client/vfs/data_buffer.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/test_base.h"
#include "test/unit/client/vfs/test_common.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using dingofs::client::vfs::test::VFSTestBase;

// Helper: build an Attr with a given file length for ino 300.
static Attr MakeAttr(Ino ino, uint64_t length) {
  Attr a;
  a.ino = ino;
  a.type = dingofs::kFile;
  a.length = length;
  a.mode = 0644;
  a.nlink = 1;
  return a;
}

class FileReaderTest : public VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    // Default: GetAttr returns a 4 MiB file.
    file_length_ = 4 * 1024 * 1024;
    Attr attr = MakeAttr(kIno, file_length_);
    ON_CALL(*mock_meta_system_, GetAttr(_, kIno, _))
        .WillByDefault(DoAll(SetArgPointee<2>(attr), Return(Status::OK())));
    EXPECT_CALL(*mock_meta_system_, GetAttr(_, kIno, _)).Times(AnyNumber());
  }

  // Creates, acquires ref, and opens a FileReader.
  FileReader* MakeOpenReader(uint64_t ino = kIno, uint64_t fh = kFh) {
    auto* r = new FileReader(mock_hub_, fh, ino);
    r->AcquireRef();
    CHECK(r->Open().ok());
    return r;
  }

  void CloseAndRelease(FileReader* r) {
    r->Close();
    r->ReleaseRef();
  }

  static constexpr Ino kIno = 300;
  static constexpr uint64_t kFh = 3;
  uint64_t file_length_ = 0;
  std::unique_ptr<TraceManager> trace_manager_;
};

// 1. Read() of a zero-length range returns 0 bytes.
TEST_F(FileReaderTest, Read_ZeroSize_ReturnsZero) {
  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0xDEAD;
  Status s = r->Read(ctx_, &buf, /*size=*/0, /*offset=*/0, &rsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(rsize, 0u);

  CloseAndRelease(r);
}

// 2. Read() at an offset beyond EOF returns 0 bytes.
TEST_F(FileReaderTest, Read_OffsetBeyondEOF_ReturnsZero) {
  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0xDEAD;
  // file_length_ = 4 MiB; read from offset 8 MiB.
  Status s = r->Read(ctx_, &buf, 1024, file_length_ + 1024, &rsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(rsize, 0u);

  CloseAndRelease(r);
}

// 3. Read() within file range: RangeAsync is called and returns 0-filled data.
//    The returned size matches requested size (capped at file length).
TEST_F(FileReaderTest, Read_WithinRange_RangeAsyncCalled) {
  // Return a real non-zero slice so the block store is consulted.
  ON_CALL(*mock_meta_system_, ReadSlice)
      .WillByDefault([](ContextSPtr, Ino, uint64_t, uint64_t,
                        std::vector<Slice>* s, uint64_t& v) {
        s->clear();
        Slice sl;
        sl.id = 1;
        sl.pos = 0;
        sl.size = 4 * 1024 * 1024;
        sl.off = 0;
        sl.len = sl.size;
        s->push_back(sl);
        v = 1;
        return Status::OK();
      });

  // shared_ptr capture: see Read_VerifyRangeReqFields for the rationale —
  // readahead is fire-and-forget and may run after the test scope dies.
  auto range_async_calls = std::make_shared<std::atomic<int>>(0);
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          [range_async_calls](ContextSPtr, RangeReq req, StatusCallback cb) {
            range_async_calls->fetch_add(1, std::memory_order_relaxed);
            if (req.data && req.length > 0) {
              char* buf = new char[req.length]();
              req.data->AppendUserData(
                  buf, req.length,
                  [](void* p) { delete[] static_cast<char*>(p); });
            }
            cb(Status::OK());
          });

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  constexpr uint64_t kReadSize = 4096;
  Status s = r->Read(ctx_, &buf, kReadSize, /*offset=*/0, &rsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(rsize, kReadSize);
  EXPECT_GE(range_async_calls->load(std::memory_order_relaxed), 1);

  CloseAndRelease(r);
}

// 3b. Read() propagates BlockKey fields correctly to RangeReq.
//     After removing RangeReq.block_size, block size must be read from
//     req.block_ctx.key.size.
TEST_F(FileReaderTest, Read_VerifyRangeReqFields) {
  constexpr int32_t kBlockSize = 4 * 1024 * 1024;  // 4 MiB
  constexpr uint64_t kSliceId = 12345;

  // One slice covering one full block
  ON_CALL(*mock_meta_system_, ReadSlice)
      .WillByDefault([](ContextSPtr, Ino, uint64_t, int64_t,
                        std::vector<Slice>* s, uint64_t& v) {
        s->clear();
        Slice sl;
        sl.id = kSliceId;
        sl.pos = 0;
        sl.size = kBlockSize;
        sl.off = 0;
        sl.len = sl.size;
        s->push_back(sl);
        v = 1;
        return Status::OK();
      });

  // Capture RangeReqs with a shared, mutex-protected state.
  //
  // Two important non-obvious facts about the production code:
  //   1. A Read at offset=0 promotes ReadaheadPoclicy from level 0 to level 1,
  //      which dispatches extra readahead RangeAsync calls in parallel from
  //      read_executor_. So multiple threads call this lambda concurrently
  //      and captured_reqs needs a mutex.
  //   2. FileReader::Read only waits for the user request; readahead requests
  //      are fire-and-forget and may still run after Read() returns.
  //      Therefore the lambda's captured state must outlive the test function:
  //      we hold it in a shared_ptr captured by value, so it lives as long as
  //      the mock object (released in fixture dtor after executors are
  //      stopped). A naive [&] capture of stack vars triggers stack smashing
  //      when the readahead callback fires after the test scope dies.
  struct Captured {
    std::mutex mu;
    std::vector<RangeReq> reqs;
  };
  auto captured = std::make_shared<Captured>();

  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault([captured](ContextSPtr, RangeReq req, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lk(captured->mu);
          captured->reqs.push_back(req);
        }
        if (req.data && req.length > 0) {
          char* buf = new char[req.length]();
          req.data->AppendUserData(
              buf, req.length,
              [](void* p) { delete[] static_cast<char*>(p); });
        }
        cb(Status::OK());
      });

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  constexpr uint64_t kReadSize = 4096;
  Status s = r->Read(ctx_, &buf, kReadSize, /*offset=*/0, &rsize);
  EXPECT_TRUE(s.ok());

  // Locate the user-read request by its (offset,length) signature; other
  // captured requests belong to readahead.
  RangeReq req;
  {
    std::lock_guard<std::mutex> lk(captured->mu);
    ASSERT_GE(captured->reqs.size(), 1u);
    auto it = std::find_if(
        captured->reqs.begin(), captured->reqs.end(), [&](const RangeReq& r) {
          return r.offset == 0 && r.length == static_cast<int64_t>(kReadSize);
        });
    ASSERT_NE(it, captured->reqs.end())
        << "user-read RangeReq (offset=0,length=" << kReadSize << ") not found";
    req = *it;
  }
  // BlockKey must carry the correct slice id and actual block size.
  // block_size field was removed from RangeReq; downstream uses key.size.
  EXPECT_EQ(req.block_ctx.key.id, kSliceId);
  EXPECT_EQ(req.block_ctx.key.index, 0);
  EXPECT_EQ(req.block_ctx.key.size, kBlockSize);
  EXPECT_EQ(req.offset, 0);
  EXPECT_EQ(req.length, static_cast<int64_t>(kReadSize));

  CloseAndRelease(r);
}

// 4. Read() with file containing no slices (hole) returns zeros.
//    ReadSlice returns empty vector (hole); block store is not called for holes.
TEST_F(FileReaderTest, Read_Hole_ReturnsZero) {
  // ReadSlice returns empty slices → this is a zero/hole range.
  ON_CALL(*mock_meta_system_, ReadSlice)
      .WillByDefault(
          [](auto, auto, auto, auto, std::vector<Slice>* s, uint64_t& v) {
            s->clear();
            v = 0;
            return Status::OK();
          });

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status s = r->Read(ctx_, &buf, /*size=*/4096, /*offset=*/0, &rsize);
  EXPECT_TRUE(s.ok());
  // Hole reads should still return the requested number of bytes (zeros).
  EXPECT_EQ(rsize, 4096u);

  CloseAndRelease(r);
}

// 5. GetAttr failure causes Read() to return an error.
TEST_F(FileReaderTest, Read_GetAttrFails_ReturnsError) {
  ON_CALL(*mock_meta_system_, GetAttr(_, kIno, _))
      .WillByDefault(Return(Status::Internal("attr error")));

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status s = r->Read(ctx_, &buf, 4096, 0, &rsize);
  EXPECT_FALSE(s.ok());

  CloseAndRelease(r);
}

// 6. Block store read error causes Read() to return an error.
TEST_F(FileReaderTest, Read_BlockStoreError_ReturnsError) {
  // Return a real non-zero slice so the block store is actually consulted.
  ON_CALL(*mock_meta_system_, ReadSlice)
      .WillByDefault([](ContextSPtr, Ino, uint64_t, uint64_t,
                        std::vector<Slice>* s, uint64_t& v) {
        s->clear();
        Slice sl;
        sl.id = 1;
        sl.pos = 0;
        sl.size = 4 * 1024 * 1024;
        sl.off = 0;
        sl.len = sl.size;
        s->push_back(sl);
        v = 1;
        return Status::OK();
      });

  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          [](ContextSPtr, RangeReq, StatusCallback cb) { cb(Status::IoError("io err")); });

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status s = r->Read(ctx_, &buf, 4096, 0, &rsize);
  EXPECT_FALSE(s.ok());

  CloseAndRelease(r);
}

// 7. Invalidate() on a range where there are no active requests does not crash.
TEST_F(FileReaderTest, Invalidate_NoRequests_NoCrash) {
  auto* r = MakeOpenReader();

  // No reads have been issued, so invalidate is a no-op.
  r->Invalidate(/*offset=*/0, /*size=*/4096);

  CloseAndRelease(r);
}

// 8. Close() sets the closing flag; a subsequent Read() is aborted.
TEST_F(FileReaderTest, Close_SetsClosingFlag_ReadAborted) {
  auto* r = MakeOpenReader();
  r->Close();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status s = r->Read(ctx_, &buf, 4096, 0, &rsize);
  // After Close() the reader is closing; read should fail or return 0.
  // Depending on timing it may succeed with 0 bytes or return Abort.
  if (s.ok()) {
    EXPECT_EQ(rsize, 0u);
  } else {
    EXPECT_FALSE(s.ok());
  }

  r->ReleaseRef();
}

// 9. Read() that spans two block-aligned segments within a single chunk
//    succeeds and returns the full requested size.
TEST_F(FileReaderTest, Read_MultiBlock_ReturnsFullSize) {
  // File is large enough to span multiple blocks (block_size = 4 MiB by
  // default in MakeTestFsInfo).
  uint64_t big_length = 16 * 1024 * 1024;  // 16 MiB
  Attr attr = MakeAttr(kIno, big_length);
  ON_CALL(*mock_meta_system_, GetAttr(_, kIno, _))
      .WillByDefault(DoAll(SetArgPointee<2>(attr), Return(Status::OK())));

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  // Read 8 MiB starting at 0 (crosses two 4 MiB blocks).
  constexpr uint64_t kReadSize = 8 * 1024 * 1024;
  Status s = r->Read(ctx_, &buf, kReadSize, /*offset=*/0, &rsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(rsize, kReadSize);

  CloseAndRelease(r);
}

// 10. AcquireRef/ReleaseRef lifecycle: extra ref keeps reader alive; final
//     release destroys it.
TEST_F(FileReaderTest, AcquireRef_ReleaseRef_Lifecycle) {
  auto* r = MakeOpenReader();
  // MakeOpenReader already called AcquireRef once (refs = 1).

  r->AcquireRef();  // refs = 2
  r->Close();       // mark closing

  r->ReleaseRef();  // refs = 1, not destroyed yet
  r->ReleaseRef();  // refs = 0 → destroys; must not crash
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
