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
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include "client/vfs/data/reader/file_reader.h"
#include "client/vfs/data_buffer.h"
#include "common/options/client.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/test_base.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

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

class FileReaderTest : public test::VFSTestBase {
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

// Read-only white-box synchronization for concurrency tests. Production
// behavior is still driven exclusively through FileReader's public API; the
// peer only lets a test wait for a precise state instead of guessing executor
// progress with sleep_for().
class FileReaderTestPeer {
 public:
  static bool WaitForRequest(
      FileReader* reader, int64_t offset, ReadRequestState state,
      int32_t min_readers,
      std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
      {
        std::lock_guard<std::mutex> reader_lock(reader->mutex_);
        for (const auto& [id, req] : reader->requests_) {
          std::lock_guard<std::mutex> req_lock(req->mutex);
          if (req->req.frange.offset == offset && req->state == state &&
              req->readers >= min_readers) {
            return true;
          }
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
  }
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
            if (req.dst.base != nullptr && req.length > 0) {
              std::memset(req.dst.data(), 0, req.length);  // fill slot in place
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
        if (req.dst.base != nullptr && req.length > 0) {
          std::memset(req.dst.data(), 0, req.length);  // fill slot in place
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
  // Handle must carry the correct slice id and actual block size.
  EXPECT_EQ(req.handle.Filename(),
            fmt::format("{}_{}_{}", kSliceId, 0, kBlockSize));
  EXPECT_EQ(req.offset, 0);
  EXPECT_EQ(req.length, static_cast<int64_t>(kReadSize));

  CloseAndRelease(r);
}

// 4. Read() with file containing no slices (hole) returns zeros.
//    ReadSlice returns empty vector (hole); block store is not called for
//    holes.
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
      .WillByDefault([](ContextSPtr, RangeReq, StatusCallback cb) {
        cb(Status::IoError("io err"));
      });

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status s = r->Read(ctx_, &buf, 4096, 0, &rsize);
  EXPECT_FALSE(s.ok());

  CloseAndRelease(r);
}

// A failure after resolving a non-hole block from inode slice metadata is a
// data I/O failure. It must not leak a storage/cache status such as NotFound
// (ENOENT) to the file read caller.
TEST_F(FileReaderTest, Read_BlockStoreNotFound_ReturnsIoError) {
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
      .WillByDefault([](ContextSPtr, RangeReq, StatusCallback cb) {
        cb(Status::NotFound("no such object"));
      });

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status s = r->Read(ctx_, &buf, 4096, 0, &rsize);
  EXPECT_TRUE(s.IsIoError());
  EXPECT_EQ(s.ToSysErrNo(), EIO);

  CloseAndRelease(r);
}

TEST_F(FileReaderTest, Read_BlockStoreNonIoError_ReturnsIoError) {
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
      .WillByDefault([](ContextSPtr, RangeReq, StatusCallback cb) {
        cb(Status::Timeout("storage timeout"));
      });

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status s = r->Read(ctx_, &buf, 4096, 0, &rsize);
  EXPECT_TRUE(s.IsIoError());
  EXPECT_EQ(s.ToSysErrNo(), EIO);

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
  EXPECT_TRUE(s.IsAbort()) << s.ToString();
  EXPECT_EQ(rsize, 0u);

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

// 11. Read mempool exhausted: a foreground read whose request can't get a pool
//     slot fails with OutOfMemory (ENOMEM), not a crash. Hogging the whole pool
//     also makes the readahead requests fail+free at once, exercising the
//     idempotent delete path.
TEST_F(FileReaderTest, Read_PoolExhausted_ReturnsOutOfMemory) {
  // Raise the backpressure watermark so the foreground read doesn't burn the
  // bounded wait (~2s); we're testing the hard-fail path, not the wait.
  double saved_wm = FLAGS_vfs_read_mempool_backpressure_watermark;
  FLAGS_vfs_read_mempool_backpressure_watermark = 2.0;
  SCOPED_CLEANUP({ FLAGS_vfs_read_mempool_backpressure_watermark = saved_wm; });

  // Hog the entire read mempool: any further Allocate returns an empty handle.
  auto hog = mock_hub_->GetReadMemPool()->Allocate(64ull * 1024 * 1024);
  ASSERT_TRUE(static_cast<bool>(hog));

  // Real (non-hole) slice so the read actually needs a pool slot.
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

  auto* r = MakeOpenReader();
  DataBuffer buf;
  uint64_t rsize = 0;
  Status s = r->Read(ctx_, &buf, 4096, /*offset=*/0, &rsize);
  EXPECT_TRUE(s.IsOutOfMemory()) << s.ToString();

  CloseAndRelease(r);
}

// ---------------------------------------------------------------------------
// Concurrency coverage. Methodology: a "gated" RangeAsync mock blocks the
// first matching callback on a test-controlled latch, turning racy windows
// (request kBusy, foreground waiting) into deterministic interleavings
// without touching production code.
// ---------------------------------------------------------------------------

namespace {

// One non-hole slice covering the whole 4 MiB file so reads consult the
// block store instead of zero-filling holes.
void InstallFullSlice(test::MockMetaSystem* meta) {
  ON_CALL(*meta, ReadSlice)
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
}

// Latch that blocks the first RangeAsync matching (offset, length); later
// calls (and all non-matching calls) complete inline with zero-filled data.
struct RangeGate {
  std::mutex mu;
  std::condition_variable cv;
  bool released{false};
  std::once_flag enter_once;
  std::promise<void> entered_promise;
  std::shared_future<void> entered{entered_promise.get_future().share()};

  void EnterAndWait() {
    std::call_once(enter_once, [&] { entered_promise.set_value(); });
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return released; });
  }

  bool WaitEntered(
      std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    return entered.wait_for(timeout) == std::future_status::ready;
  }

  void Release() {
    {
      std::lock_guard<std::mutex> lk(mu);
      released = true;
    }
    cv.notify_all();
  }
};

}  // namespace

// 15. Regression for the request birth-window race: a request must not become
// runnable before its creator registered a reference. With an inline-error
// BlockStore the completion path fires as early as possible; pre-fix this
// crashed CHECK(CanDeleteRequest) within a few hundred iterations on 2 cores.
TEST_F(FileReaderTest, Read_RepeatedInlineError_NoCleanupRace) {
  InstallFullSlice(mock_meta_system_);
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault([](ContextSPtr, RangeReq, StatusCallback cb) {
        cb(Status::IoError("inline failure"));
      });

  auto* r = MakeOpenReader();
  for (int i = 0; i < 200; ++i) {
    DataBuffer buf;
    uint64_t rsize = 0;
    Status s = r->Read(ctx_, &buf, 4096, /*offset=*/0, &rsize);
    ASSERT_FALSE(s.ok()) << "iter=" << i;
  }
  CloseAndRelease(r);
}

// 16. Invalidate() on a kBusy request must trigger a re-read (kRefresh ->
// kNew -> second RangeAsync). Even if the invalidated first attempt fails, its
// result is stale and must not be returned to the foreground reader.
TEST_F(FileReaderTest, Invalidate_BusyFailedRequest_RefreshRereads) {
  InstallFullSlice(mock_meta_system_);

  // Gate only the first fetch of the foreground request (offset 0, 4 KiB) so
  // an ungated readahead fetch cannot steal the latch and let the read finish
  // before Invalidate() lands on the kBusy request.
  auto gate = std::make_shared<RangeGate>();
  auto target_calls = std::make_shared<std::atomic<int>>(0);
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          [gate, target_calls](ContextSPtr, RangeReq req, StatusCallback cb) {
            bool is_target = (req.offset == 0 && req.length == 4096);
            int call = is_target ? target_calls->fetch_add(1) : -1;
            if (call == 0) {
              gate->EnterAndWait();
            }
            if (call == 0) {
              cb(Status::IoError("stale first attempt"));
              return;
            }
            if (req.dst.base != nullptr && req.length > 0) {
              std::memset(req.dst.data(), 0, req.length);
            }
            cb(Status::OK());
          });

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status read_status;
  std::thread reader(
      [&] { read_status = r->Read(ctx_, &buf, 4096, 0, &rsize); });

  if (!gate->WaitEntered()) {
    gate->Release();
    reader.join();
    CloseAndRelease(r);
    FAIL() << "foreground request did not enter RangeAsync";
  }
  r->Invalidate(0, 4096);  // kBusy -> kRefresh
  r->Invalidate(0, 4096);  // repeated invalidation must remain refresh-safe
  gate->Release();         // first read completes, must be discarded

  reader.join();
  EXPECT_TRUE(read_status.ok()) << read_status.ToString();
  EXPECT_EQ(rsize, 4096u);
  // The refreshed request re-fetched its block from the block store.
  EXPECT_EQ(target_calls->load(), 2);

  CloseAndRelease(r);
}

// 17. Two concurrent reads of the same range share one in-flight request
// (readers > 1) instead of each fetching the block.
TEST_F(FileReaderTest, ConcurrentReads_ShareOneRequest) {
  InstallFullSlice(mock_meta_system_);

  auto gate = std::make_shared<RangeGate>();
  auto target_calls = std::make_shared<std::atomic<int>>(0);
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          [gate, target_calls](ContextSPtr, RangeReq req, StatusCallback cb) {
            bool is_target = (req.offset == 0 && req.length == 4096);
            if (is_target && target_calls->fetch_add(1) == 0) {
              gate->EnterAndWait();
            }
            if (req.dst.base != nullptr && req.length > 0) {
              std::memset(req.dst.data(), 0, req.length);
            }
            cb(Status::OK());
          });

  auto* r = MakeOpenReader();

  DataBuffer buf1, buf2;
  uint64_t rsize1 = 0, rsize2 = 0;
  Status s1, s2;
  std::thread t1([&] { s1 = r->Read(ctx_, &buf1, 4096, 0, &rsize1); });
  if (!gate->WaitEntered()) {
    gate->Release();
    t1.join();
    CloseAndRelease(r);
    FAIL() << "first request did not enter RangeAsync";
  }
  std::thread t2([&] { s2 = r->Read(ctx_, &buf2, 4096, 0, &rsize2); });

  // Do not release the fetch until the second foreground read has actually
  // attached to this kBusy request. This makes readers>1 deterministic.
  if (!FileReaderTestPeer::WaitForRequest(
          r, /*offset=*/0, ReadRequestState::kBusy, /*min_readers=*/2)) {
    gate->Release();
    t1.join();
    t2.join();
    CloseAndRelease(r);
    FAIL() << "second reader did not attach to the shared request";
  }
  gate->Release();

  t1.join();
  t2.join();
  EXPECT_TRUE(s1.ok()) << s1.ToString();
  EXPECT_TRUE(s2.ok()) << s2.ToString();
  EXPECT_EQ(rsize1, 4096u);
  EXPECT_EQ(rsize2, 4096u);
  // Exactly one fetch of the shared block: the second read attached to the
  // in-flight request rather than issuing its own.
  EXPECT_EQ(target_calls->load(), 1);

  CloseAndRelease(r);
}

// 18. Close() while a read is blocked waiting for its request must abort the
// read once the in-flight completion delivers (completion sees closing_ and
// invalidates even a successful fetch).
TEST_F(FileReaderTest, Close_WhileWaiting_ReturnsAbort) {
  InstallFullSlice(mock_meta_system_);

  // Gate only the foreground request (offset 0, 4 KiB): the first read jumps
  // readahead to level 1, and an ungated readahead fetch must not steal the
  // latch or the foreground read would complete before Close().
  auto gate = std::make_shared<RangeGate>();
  auto target_calls = std::make_shared<std::atomic<int>>(0);
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          [gate, target_calls](ContextSPtr, RangeReq req, StatusCallback cb) {
            bool is_target = (req.offset == 0 && req.length == 4096);
            if (is_target && target_calls->fetch_add(1) == 0) {
              gate->EnterAndWait();
            }
            if (req.dst.base != nullptr && req.length > 0) {
              std::memset(req.dst.data(), 0, req.length);
            }
            cb(Status::OK());
          });

  auto* r = MakeOpenReader();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status read_status;
  std::thread reader(
      [&] { read_status = r->Read(ctx_, &buf, 4096, 0, &rsize); });

  if (!gate->WaitEntered()) {
    gate->Release();
    reader.join();
    CloseAndRelease(r);
    FAIL() << "foreground request did not enter RangeAsync";
  }
  r->Close();
  gate->Release();  // completion fires, sees closing_, invalidates

  reader.join();
  EXPECT_TRUE(read_status.IsAbort()) << read_status.ToString();

  r->ReleaseRef();
}

// 19. Invalidate() on a kReady request that is still owned by a foreground
// read must retain the request, move it back to kNew, and refill it before the
// foreground read is allowed to consume that range.
TEST_F(FileReaderTest, Invalidate_ReadyRequestWithReader_Rereads) {
  InstallFullSlice(mock_meta_system_);
  ON_CALL(*mock_hub_, GetFsInfo())
      .WillByDefault(Return(test::MakeTestFsInfo(4 * 1024 * 1024, 4096)));

  auto first_gate = std::make_shared<RangeGate>();
  auto second_block_calls = std::make_shared<std::atomic<int>>(0);
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault([first_gate, second_block_calls](ContextSPtr, RangeReq req,
                                                      StatusCallback cb) {
        const std::string key = req.handle.Filename();
        if (key.rfind("1_0_", 0) == 0) {
          first_gate->EnterAndWait();
        } else if (key.rfind("1_1_", 0) == 0) {
          second_block_calls->fetch_add(1);
        }
        if (req.dst.base != nullptr && req.length > 0) {
          std::memset(req.dst.data(), 0, req.length);
        }
        cb(Status::OK());
      });

  auto* r = MakeOpenReader();
  DataBuffer buf;
  uint64_t rsize = 0;
  Status read_status;
  std::thread reader(
      [&] { read_status = r->Read(ctx_, &buf, 8192, 0, &rsize); });

  if (!first_gate->WaitEntered()) {
    first_gate->Release();
    reader.join();
    CloseAndRelease(r);
    FAIL() << "first block did not enter RangeAsync";
  }
  // The foreground Read waits on block 0 while block 1 has already completed,
  // leaving block 1 in kReady with its reader reference still registered.
  if (!FileReaderTestPeer::WaitForRequest(
          r, /*offset=*/4096, ReadRequestState::kReady, /*min_readers=*/1)) {
    first_gate->Release();
    reader.join();
    CloseAndRelease(r);
    FAIL() << "second block did not reach kReady with a reader";
  }

  r->Invalidate(/*offset=*/4096, /*size=*/4096);
  if (!FileReaderTestPeer::WaitForRequest(
          r, /*offset=*/4096, ReadRequestState::kReady, /*min_readers=*/1)) {
    first_gate->Release();
    reader.join();
    CloseAndRelease(r);
    FAIL() << "invalidated second block did not become ready again";
  }
  first_gate->Release();

  reader.join();
  EXPECT_TRUE(read_status.ok()) << read_status.ToString();
  EXPECT_EQ(rsize, 8192u);
  EXPECT_EQ(second_block_calls->load(), 2);

  CloseAndRelease(r);
}

// 20. Bounded chaos test for the public concurrency surface. Multiple readers
// and invalidators operate on one FileReader until Close races all of them.
// Fixed per-thread seeds keep failures reproducible; the test accepts only a
// complete read before Close or Abort while/after Close.
TEST_F(FileReaderTest, ConcurrentReadInvalidateClose_Chaos) {
  InstallFullSlice(mock_meta_system_);
  ON_CALL(*mock_hub_, GetFsInfo())
      .WillByDefault(Return(test::MakeTestFsInfo(4 * 1024 * 1024, 64 * 1024)));
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault([](ContextSPtr, RangeReq req, StatusCallback cb) {
        if (req.dst.base != nullptr && req.length > 0) {
          std::memset(req.dst.data(), 0, req.length);
        }
        cb(Status::OK());
      });

  auto* r = MakeOpenReader();
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> successful_reads{0};
  std::atomic<uint64_t> aborted_reads{0};
  std::atomic<uint64_t> invalidations{0};
  std::mutex error_mu;
  std::vector<std::string> errors;

  auto record_error = [&](std::string error) {
    std::lock_guard<std::mutex> lk(error_mu);
    errors.push_back(std::move(error));
  };

  constexpr int kReaderThreads = 8;
  constexpr int kInvalidatorThreads = 2;
  constexpr int64_t kReadSize = 4096;
  constexpr int64_t kMaxOffset = 4 * 1024 * 1024 - kReadSize;
  std::vector<std::thread> workers;
  workers.reserve(kReaderThreads + kInvalidatorThreads);

  for (int thread_id = 0; thread_id < kReaderThreads; ++thread_id) {
    workers.emplace_back([&, thread_id] {
      std::mt19937 rng(0x52454144u + static_cast<uint32_t>(thread_id));
      std::uniform_int_distribution<int64_t> offset_dist(0, kMaxOffset / 4096);
      while (!stop.load(std::memory_order_acquire)) {
        int64_t offset = offset_dist(rng) * 4096;
        DataBuffer buf;
        uint64_t rsize = 0;
        Status s = r->Read(ctx_, &buf, kReadSize, offset, &rsize);
        if (s.ok()) {
          successful_reads.fetch_add(1, std::memory_order_relaxed);
          if (rsize != kReadSize) {
            record_error(
                fmt::format("successful read returned {} bytes at offset {}",
                            rsize, offset));
          }
        } else if (s.IsAbort()) {
          aborted_reads.fetch_add(1, std::memory_order_relaxed);
        } else {
          record_error(fmt::format("unexpected read status at offset {}: {}",
                                   offset, s.ToString()));
        }
      }
    });
  }

  for (int thread_id = 0; thread_id < kInvalidatorThreads; ++thread_id) {
    workers.emplace_back([&, thread_id] {
      std::mt19937 rng(0x494e5641u + static_cast<uint32_t>(thread_id));
      std::uniform_int_distribution<int64_t> offset_dist(0, kMaxOffset / 4096);
      while (!stop.load(std::memory_order_acquire)) {
        r->Invalidate(offset_dist(rng) * 4096, kReadSize);
        invalidations.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::yield();
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  r->Close();
  // Let already-running and newly-entering reads observe closing_ before
  // asking workers to stop.
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  stop.store(true, std::memory_order_release);

  for (auto& worker : workers) {
    worker.join();
  }

  EXPECT_TRUE(errors.empty()) << (errors.empty() ? "" : errors.front());
  EXPECT_GT(successful_reads.load(), 0u);
  EXPECT_GT(aborted_reads.load(), 0u);
  EXPECT_GT(invalidations.load(), 0u);

  r->ReleaseRef();
}

// 21. The per-read CleanUpRequest keeps the request table bounded: once more
// than kMaxReadRequests(64) unprotected requests accumulate, old ones are
// evicted and a later read of the same range fetches again.
TEST_F(FileReaderTest, ManySmallReads_EvictionBounded) {
  InstallFullSlice(mock_meta_system_);
  // 64 KiB blocks: with the default 4 MiB block the whole file is one block
  // and IsProtectedReq's window (>= one block around the last offset) would
  // always cover offset 0, so nothing could ever be evicted.
  ON_CALL(*mock_hub_, GetFsInfo())
      .WillByDefault(Return(test::MakeTestFsInfo(4 * 1024 * 1024, 64 * 1024)));

  // Count fetches of the file-offset-0 request only: slice 1 block 0 at
  // in-block offset 0 (strided reads below alias in-block offset 0 in OTHER
  // blocks, so match the block identity too).
  auto offset0_calls = std::make_shared<std::atomic<int>>(0);
  ON_CALL(*mock_block_store_, RangeAsync)
      .WillByDefault(
          [offset0_calls](ContextSPtr, RangeReq req, StatusCallback cb) {
            if (req.handle.Filename().rfind("1_0_", 0) == 0 &&
                req.offset == 0 && req.length == 4096) {
              offset0_calls->fetch_add(1);
            }
            if (req.dst.base != nullptr && req.length > 0) {
              std::memset(req.dst.data(), 0, req.length);
            }
            cb(Status::OK());
          });

  auto* r = MakeOpenReader();
  DataBuffer first;
  uint64_t rsize = 0;
  ASSERT_TRUE(r->Read(ctx_, &first, 4096, /*offset=*/0, &rsize).ok());
  EXPECT_EQ(offset0_calls->load(), 1);

  // Disjoint 4 KiB reads alternating between a low ([64K, ~1M)) and a high
  // ([3M, ~4M)) region: every jump exceeds kSeqAccessWindowSize (2 MiB), so
  // the readahead policy degrades to level 0 after the first hops — no big
  // readahead request swallows the strides (each read creates its own
  // request) and IsProtectedReq() protects nothing. That pushes the table
  // past kMaxReadRequests(64) and lets eviction reclaim the offset-0 entry.
  for (int i = 0; i < 112; ++i) {
    DataBuffer buf;
    int64_t base = (i % 2 == 0) ? 64 * 1024 : 3 * 1024 * 1024;
    int64_t offset = base + static_cast<int64_t>(i / 2) * 16 * 1024;
    ASSERT_TRUE(r->Read(ctx_, &buf, 4096, offset, &rsize).ok()) << i;
  }

  // The offset-0 request was evicted, so this read must fetch again.
  DataBuffer again;
  ASSERT_TRUE(r->Read(ctx_, &again, 4096, /*offset=*/0, &rsize).ok());
  EXPECT_EQ(offset0_calls->load(), 2);

  CloseAndRelease(r);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
