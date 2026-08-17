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

#include <fcntl.h>
#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "client/vfs/components/uid_gid_mapper.h"
#include "client/vfs/data/write_pressure_controller.h"
#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/data/writer_table.h"
#include "client/vfs/data_buffer.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/metasystem/mds/rpc.h"
#include "client/vfs/vfs_impl.h"
#include "common/blockaccess/accesser_common.h"
#include "common/const.h"
#include "common/file_size.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "common/writemempool/write_mem_pool.h"
#include "test/unit/client/vfs/mock/mock_meta_system.h"
#include "test/unit/client/vfs/mock/mock_vfs_hub.h"
#include "test/unit/client/vfs/test_base.h"
#include "test/unit/client/vfs/test_common.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;

namespace {

// Minimal in-memory PasswdSource for the uid/gid wiring tests. Mirrors the
// TableSource used by uid_gid_mapper_test.cc, kept local since that helper is
// not exported in a shared header. These tests don't exercise the fast-path
// cookie, so primary_gid is always 0.
class FakePasswdSource : public PasswdSource {
 public:
  explicit FakePasswdSource(
      std::vector<std::pair<std::string, uint32_t>> users,
      std::vector<std::pair<std::string, uint32_t>> groups)
      : users_(std::move(users)), groups_(std::move(groups)) {}

  std::vector<UserRecord> ListUsersWithPrimaryGid() override {
    std::vector<UserRecord> out;
    out.reserve(users_.size());
    for (const auto& [name, uid] : users_) {
      out.push_back({name, uid, 0});
    }
    return out;
  }
  std::vector<std::pair<std::string, uint32_t>> ListGroups() override {
    return groups_;
  }

 private:
  std::vector<std::pair<std::string, uint32_t>> users_;
  std::vector<std::pair<std::string, uint32_t>> groups_;
};

}  // namespace

TEST(VFSHubImplTest, StartRejectsMisalignedWritePageSize) {
  constexpr uint32_t kMisalignedPageSize = 8 * 1024 * 1024;
  const uint32_t previous_page_size = FLAGS_vfs_write_buffer_page_size;
  FLAGS_vfs_write_buffer_page_size = kMisalignedPageSize;
  auto restore_page_size = MakeScopedCleanup(
      [&]() { FLAGS_vfs_write_buffer_page_size = previous_page_size; });

  VFSConfig config;
  config.fs_name = "write-page-geometry-test";
  config.metasystem_type = MetaSystemType::MEMORY;
  TraceManager trace_manager;
  VFSHubImpl hub(config, ClientId(), trace_manager);

  Status status = hub.Start(/*skip_mount=*/true);
  EXPECT_TRUE(status.IsInvalidParam()) << status.ToString();
  EXPECT_NE(
      status.ToString().find("must be a multiple of write buffer page size"),
      std::string::npos);
}

TEST(VFSHubImplTest, StartRejectsNonIntegralWritePoolCapacity) {
  constexpr uint64_t kNonIntegralTotalMb = 65;
  constexpr uint32_t kPageSize = 4 * 1024 * 1024;
  const uint64_t previous_total_mb = FLAGS_vfs_write_buffer_total_mb;
  const uint32_t previous_page_size = FLAGS_vfs_write_buffer_page_size;
  FLAGS_vfs_write_buffer_total_mb = kNonIntegralTotalMb;
  FLAGS_vfs_write_buffer_page_size = kPageSize;
  auto restore_flags = MakeScopedCleanup([&]() {
    FLAGS_vfs_write_buffer_total_mb = previous_total_mb;
    FLAGS_vfs_write_buffer_page_size = previous_page_size;
  });

  VFSConfig config;
  config.fs_name = "write-pool-geometry-test";
  config.metasystem_type = MetaSystemType::MEMORY;
  TraceManager trace_manager;
  VFSHubImpl hub(config, ClientId(), trace_manager);

  Status status = hub.Start(/*skip_mount=*/true);
  EXPECT_TRUE(status.IsInvalidParam()) << status.ToString();
  EXPECT_NE(status.ToString().find(
                "write buffer total size (68157440) must be a multiple of "
                "page size (4194304)"),
            std::string::npos);
}

TEST(VFSHubImplTest, StartRejectsChunkLargerThanWritePoolCapacity) {
  constexpr uint64_t kWriteBufferTotalMb = 32;
  constexpr uint32_t kPageSize = 4 * 1024 * 1024;
  const uint64_t previous_total_mb = FLAGS_vfs_write_buffer_total_mb;
  const uint32_t previous_page_size = FLAGS_vfs_write_buffer_page_size;
  FLAGS_vfs_write_buffer_total_mb = kWriteBufferTotalMb;
  FLAGS_vfs_write_buffer_page_size = kPageSize;
  auto restore_flags = MakeScopedCleanup([&]() {
    FLAGS_vfs_write_buffer_total_mb = previous_total_mb;
    FLAGS_vfs_write_buffer_page_size = previous_page_size;
  });

  VFSConfig config;
  config.fs_name = "write-pool-chunk-capacity-test";
  config.metasystem_type = MetaSystemType::MEMORY;
  TraceManager trace_manager;
  VFSHubImpl hub(config, ClientId(), trace_manager);

  Status status = hub.Start(/*skip_mount=*/true);
  EXPECT_TRUE(status.IsInvalidParam()) << status.ToString();
  EXPECT_NE(status.ToString().find(
                "filesystem chunk size (67108864) requires up to 16 write "
                "buffer pages, exceeding write buffer capacity (8 pages, "
                "33554432 bytes)"),
            std::string::npos);
}

class VFSImplTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(&trace_manager_));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    ON_CALL(*mock_hub_, GetBlockAccesserOptions())
        .WillByDefault(Return(blockaccess::BlockAccessOptions{}));
    EXPECT_CALL(*mock_hub_, GetBlockAccesserOptions()).Times(AnyNumber());

    // hub_uptr_ is consumed here; after this, use vfs_ to access VFSImpl.
    // VFSImplTest is a friend of VFSImpl, so the private constructor is
    // accessible.
    vfs_.reset(new VFSImpl(std::move(hub_uptr_), trace_manager_));
  }

  void TearDown() override {
    // HandleManager keeps a non-owning pointer to the hub now owned by vfs_.
    // Stop it while that hub is still alive; the VFSTestBase destructor's
    // later Stop is idempotent and will not dereference the destroyed hub.
    // Some tests temporarily redirect GetWriterTable() to a function-local
    // table; restore the fixture-owned table after those locals are gone.
    ON_CALL(*mock_hub_, GetWriterTable())
        .WillByDefault(Return(writer_table_.get()));
    handle_manager_->Stop();
    vfs_.reset();
  }

  TraceManager trace_manager_;
  std::unique_ptr<VFSImpl> vfs_;

  // Helper accessible from TEST_F-derived subclasses (which are not friends
  // of VFSImpl themselves but inherit from this friend class).
  void SetMountRoot(const std::string& path, Ino ino) {
    vfs_->mount_root_path_ = path;
    vfs_->mount_root_ino_ = ino;
  }

  Status StartBrpcServer() { return vfs_->StartBrpcServer(); }

  brpc::Server::Status BrpcServerStatus() const {
    return vfs_->brpc_server_.status();
  }
};

TEST_F(VFSImplTest, StopDrainsBrpcServerBeforeHub) {
  const uint32_t previous_port = FLAGS_vfs_dummy_server_port;
  FLAGS_vfs_dummy_server_port = 0;
  auto restore_port =
      MakeScopedCleanup([&]() { FLAGS_vfs_dummy_server_port = previous_port; });

  ASSERT_TRUE(StartBrpcServer().ok());
  ASSERT_EQ(BrpcServerStatus(), brpc::Server::RUNNING);

  EXPECT_CALL(*mock_hub_, Stop(false)).WillOnce(Invoke([&](bool) {
    EXPECT_EQ(BrpcServerStatus(), brpc::Server::READY);
    return Status::OK();
  }));

  EXPECT_TRUE(vfs_->Stop(false).ok());
  EXPECT_EQ(BrpcServerStatus(), brpc::Server::READY);
}

// --- 3. Lookup delegates to meta ---
TEST_F(VFSImplTest, Lookup_DelegatesToMetaSystem) {
  Attr attr;
  attr.ino = 42;
  attr.type = dingofs::kFile;

  EXPECT_CALL(*mock_meta_system_, Lookup(_, kRootIno, "myfile", _))
      .WillOnce(DoAll(SetArgPointee<3>(attr), Return(Status::OK())));
  // FileSuffixWatcher::Remeber is called on success.
  EXPECT_CALL(*mock_hub_, GetFileSuffixWatcher()).Times(AnyNumber());

  Attr out;
  Status s = vfs_->Lookup(ctx_, kRootIno, "myfile", &out);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(out.ino, 42u);
}

// --- 4. GetAttr delegates ---
TEST_F(VFSImplTest, GetAttr_DelegatesToMetaSystem) {
  Attr attr;
  attr.ino = 55;
  attr.type = dingofs::kFile;

  EXPECT_CALL(*mock_meta_system_, GetAttr(_, 55u, _))
      .WillOnce(DoAll(SetArgPointee<2>(attr), Return(Status::OK())));

  Attr out;
  Status s = vfs_->GetAttr(ctx_, 55u, &out);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(out.ino, 55u);
}

TEST_F(VFSImplTest, SetAttr_RejectsFileSizeAboveLimitBeforeMetaCall) {
  Attr in;
  Attr out;
  ASSERT_TRUE(TryGetMaxFileSize(mock_hub_->GetFsInfo().chunk_size, &in.length));
  ++in.length;

  EXPECT_CALL(*mock_meta_system_, SetAttr(_, _, _, _, _)).Times(0);
  Status s = vfs_->SetAttr(ctx_, 55, kSetAttrSize, in, &out);

  EXPECT_TRUE(s.IsFileTooLarge()) << s.ToString();
  EXPECT_EQ(s.ToSysErrNo(), EFBIG);
}

TEST_F(VFSImplTest, Fallocate_RejectsRangeAboveLimit) {
  uint64_t max_file_size = 0;
  ASSERT_TRUE(
      TryGetMaxFileSize(mock_hub_->GetFsInfo().chunk_size, &max_file_size));

  Status s = vfs_->Fallocate(ctx_, 55, 0, max_file_size - 1, 2);

  EXPECT_TRUE(s.IsFileTooLarge()) << s.ToString();
  EXPECT_EQ(s.ToSysErrNo(), EFBIG);
}

TEST_F(VFSImplTest, CopyFileRange_RejectsDestinationAboveLimit) {
  uint64_t max_file_size = 0;
  ASSERT_TRUE(
      TryGetMaxFileSize(mock_hub_->GetFsInfo().chunk_size, &max_file_size));
  uint64_t copied = 123;

  Status s = vfs_->CopyFileRange(ctx_, 55, 0, 0, 56, max_file_size - 1, 0, 2, 0,
                                 &copied);

  EXPECT_TRUE(s.IsFileTooLarge()) << s.ToString();
  EXPECT_EQ(s.ToSysErrNo(), EFBIG);
  EXPECT_EQ(copied, 0);
}

TEST_F(VFSImplTest, Write_RejectsOffsetAtLimitBeforeBuffering) {
  auto writer_table = std::make_unique<WriterTable>(mock_hub_);
  ON_CALL(*mock_hub_, GetWriterTable())
      .WillByDefault(Return(writer_table.get()));
  EXPECT_CALL(*mock_hub_, GetWriterTable()).Times(AnyNumber());

  constexpr Ino kIno = 57;
  ON_CALL(*mock_meta_system_, Open(_, kIno, _, _, _))
      .WillByDefault(Return(Status::OK()));
  ON_CALL(*mock_meta_system_, Close(_, kIno, _))
      .WillByDefault(Return(Status::OK()));

  uint64_t fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, kIno, O_WRONLY, &fh, nullptr).ok());

  uint64_t max_file_size = 0;
  ASSERT_TRUE(
      TryGetMaxFileSize(mock_hub_->GetFsInfo().chunk_size, &max_file_size));
  EXPECT_CALL(*mock_meta_system_, Write(_, _, _, _, _, _)).Times(0);

  char byte = 'x';
  uint64_t written = 123;
  Status s = vfs_->Write(ctx_, kIno, &byte, 1, max_file_size, fh, &written);

  EXPECT_TRUE(s.IsFileTooLarge()) << s.ToString();
  EXPECT_EQ(s.ToSysErrNo(), EFBIG);
  EXPECT_EQ(written, 0);
  vfs_->Release(ctx_, kIno, fh);
}

// --- 4c. Metadata write fails after the data was already buffered. The writer
// accepts the full write (out_wsize == size, data now dirty in its buffer), but
// the inode metadata update fails, so VFSImpl::Write returns the error. The
// buffered data may still be flushed later -- this is pre-existing behavior,
// not introduced by the short-write change; the test pins the semantic. ---
TEST_F(VFSImplTest, Write_MetaWriteFails_ReturnsErrorAfterDataBuffered) {
  auto writer_table = std::make_unique<WriterTable>(mock_hub_);
  ON_CALL(*mock_hub_, GetWriterTable())
      .WillByDefault(Return(writer_table.get()));
  EXPECT_CALL(*mock_hub_, GetWriterTable()).Times(AnyNumber());
  // Uses the default (64 MiB) write pool from the fixture, so the writer
  // accepts the whole write.

  const uint64_t ino = 301;
  ON_CALL(*mock_meta_system_, Open(_, ino, _, _, _))
      .WillByDefault(Return(Status::OK()));
  ON_CALL(*mock_meta_system_, Close(_, ino, _))
      .WillByDefault(Return(Status::OK()));

  uint64_t fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, ino, O_WRONLY, &fh, nullptr).ok());

  // Writer accepts the data; the metadata update then fails.
  EXPECT_CALL(*mock_meta_system_, Write(_, ino, _, _, _, _))
      .WillOnce(Return(Status::Internal("meta down")));

  std::vector<char> buf(4096, 'X');
  uint64_t wsize = 0;
  Status s =
      vfs_->Write(ctx_, ino, buf.data(), buf.size(), /*offset=*/0, fh, &wsize);

  EXPECT_FALSE(s.ok()) << "metadata failure must surface as an error";
  EXPECT_EQ(wsize, buf.size()) << "writer accepted the full write before "
                                  "metadata failed (data buffered)";

  vfs_->Release(ctx_, ino, fh);
}

// --- 4d. Cross-chunk write whose second chunk blocks on write-pool pressure
// and whose pressure flush then fails asynchronously. Covers the full chain:
// VFSImpl::Write -> FileWriter chunk loop (prefix chunk completes and
// publishes its dirty generation) -> FIFO admission blocks the next chunk ->
// real pressure chain (pool -> observer -> controller -> WriterTable) flushes
// the dirty chunk -> block upload fails -> POSIX short write: Status OK with
// out_wsize == completed prefix only. The unfinished chunk must never upload
// or commit slice/metadata, the failure must be sticky for later Write/Flush,
// and after Release every write-pool page, FIFO waiter, and pressure callback
// must be drained. ---
TEST_F(VFSImplTest, Write_PressureFlushFails_CrossChunkWriteReturnsShortWrite) {
  // Geometry: 4KB pages, 8KB blocks, 16KB chunks, and a 4-page pool. A donor
  // writer pins 2 pages with one sub-block write (6144 < 8192, so nothing
  // streams and no page returns early). The cross-chunk write needs 1 page
  // for the chunk-0 tail (granted, published) and 3 pages for the chunk-1
  // head (blocked with 1 page free -> pressure).
  constexpr int64_t kPage = 4096;
  constexpr uint64_t kChunk = 4 * kPage;
  constexpr uint64_t kBlock = 2 * kPage;
  constexpr int64_t kPoolPages = 4;
  constexpr uint64_t kPrefixSize = 2048;       // chunk-0 tail, 1 page
  constexpr uint64_t kSuffixSize = 3 * kPage;  // chunk-1 head, 3 pages
  constexpr uint64_t kCrossOffset = kChunk - kPrefixSize;
  constexpr Ino kIno = 601;
  constexpr Ino kInoDonor = 600;

  gflags::FlagSaver flag_saver;
  FLAGS_vfs_periodic_flush_interval_ms =
      3600 * 1000;  // keep periodic flush out

  // Local table + tiny pool + the real pressure-controller chain.
  auto writer_table = std::make_unique<WriterTable>(mock_hub_);
  ASSERT_TRUE(writer_table->Start().ok());
  ON_CALL(*mock_hub_, GetWriterTable())
      .WillByDefault(Return(writer_table.get()));
  ON_CALL(*mock_hub_, GetFsInfo())
      .WillByDefault(Return(test::MakeTestFsInfo(kChunk, kBlock)));

  WriteMemPool tiny_pool(kPoolPages * kPage, kPage);
  ON_CALL(*mock_hub_, GetWriteMemPool()).WillByDefault(Return(&tiny_pool));

  ExecutorImpl pressure_executor("test_pressure_flush_fail", 1);
  ASSERT_TRUE(pressure_executor.Start());
  WritePressureController controller(writer_table.get(), &pressure_executor);
  tiny_pool.SetPressureObserver(&controller);

  // Every block upload completes inline with the same error (deterministic,
  // no background timing). The cv turns "the pressure round reached the data
  // plane" into an event the test can wait for.
  std::mutex put_mutex;
  std::condition_variable put_cv;
  int put_async_calls = 0;
  const Status kUploadError = Status::IoError("block store down");
  ON_CALL(*mock_block_store_, PutAsync)
      .WillByDefault([&](ContextSPtr, PutReq, StatusCallback cb) {
        {
          std::lock_guard<std::mutex> lock(put_mutex);
          ++put_async_calls;
        }
        put_cv.notify_all();
        cb(kUploadError);
      });

  ON_CALL(*mock_meta_system_, Open(_, kIno, _, _, _))
      .WillByDefault(Return(Status::OK()));
  ON_CALL(*mock_meta_system_, Close(_, kIno, _))
      .WillByDefault(Return(Status::OK()));
  // A failed flush must not commit slices -- in particular never for the
  // unfinished chunk 1.
  EXPECT_CALL(*mock_meta_system_, WriteSlice(_, _, _, _, _)).Times(0);

  uint64_t fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, kIno, O_WRONLY, &fh, nullptr).ok());
  // Metadata must record exactly the accepted prefix (WillOnce: exactly one
  // call, with the short count as its size).
  EXPECT_CALL(*mock_meta_system_,
              Write(_, kIno, _, kCrossOffset, kPrefixSize, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_meta_system_, RollbackWriteLength(_, kIno, fh))
      .Times(2)
      .WillRepeatedly(Return(Status::OK()));

  // Donor writer: standalone (NOT in WriterTable). The pressure round waits
  // for every table writer, so a table resident would stop StopAndDrain from
  // proving the sticky status landed. It holds 2 dirty pages until the test
  // explicitly fails its flush below.
  auto* donor = new FileWriter(mock_hub_, kInoDonor);
  donor->AcquireRef();
  ASSERT_TRUE(donor->Open().ok());
  std::vector<char> donor_buf(6144, 'd');
  uint64_t donor_wsize = 0;
  ASSERT_TRUE(
      donor->Write(ctx_, donor_buf.data(), donor_buf.size(), 0, &donor_wsize)
          .ok());
  ASSERT_EQ(donor_wsize, donor_buf.size());
  ASSERT_EQ(tiny_pool.GetUsedBytes(), 2 * kPage);

  // The cross-chunk write under test.
  std::vector<char> cross_buf(kPrefixSize + kSuffixSize, 'x');
  auto write_result = std::async(std::launch::async, [&] {
    uint64_t wsize = 0;
    Status s = vfs_->Write(ctx_, kIno, cross_buf.data(), cross_buf.size(),
                           kCrossOffset, fh, &wsize);
    return std::make_pair(s, wsize);
  });

  // Phase 1: the pressure round flushed the blocked writer's dirty chunk 0
  // and its upload failed. Waiting on the PutAsync event proves the pool ->
  // observer -> controller -> WriterTable chain reached the data plane (the
  // wait is bounded only as a deadlock guard; Close keeps the failing path
  // tear-down-safe, mirroring the writer-table pressure tests).
  bool round_reached_data_plane = false;
  {
    std::unique_lock<std::mutex> lock(put_mutex);
    round_reached_data_plane = put_cv.wait_for(
        lock, std::chrono::seconds(5), [&] { return put_async_calls >= 1; });
  }
  if (!round_reached_data_plane) tiny_pool.Close();
  EXPECT_TRUE(round_reached_data_plane)
      << "pressure round never flushed the dirty chunk-0 prefix";

  // Phase 2: retire the round (and any coalesced follow-up round). The round
  // callback runs strictly after FileWriter::FileFlushTaskDone called
  // SetStatusIfBroken on the same thread, so returning here proves the sticky
  // error is published while the write is still blocked: chunk-0's released
  // page alone (2 free of 3 needed) cannot grant the FIFO waiter yet.
  controller.StopAndDrain();

  // Phase 3: fail the donor's flush. Its 2 pages return inside the upload
  // completion on the single-threaded callback executor, which grants the
  // blocked write before this synchronous Flush returns. The freshly granted
  // lease must observe the sticky error and bail out without touching
  // chunk 1 (the unused pages go back through the lease).
  Status donor_flush_status = donor->Flush();
  ASSERT_TRUE(donor_flush_status.IsIoError()) << donor_flush_status.ToString();
  EXPECT_EQ(donor_flush_status.ToString(), kUploadError.ToString());

  ASSERT_EQ(write_result.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  auto [write_status, wsize] = write_result.get();
  EXPECT_TRUE(write_status.ok()) << write_status.ToString();
  EXPECT_EQ(wsize, kPrefixSize) << "short write: only the completed prefix";

  // Sticky error: a later write fast-fails with zero bytes ...
  char again = 'y';
  uint64_t again_wsize = 12345;  // poison: fast-fail must reset it to 0
  Status again_s =
      vfs_->Write(ctx_, kIno, &again, 1, /*offset=*/0, fh, &again_wsize);
  EXPECT_TRUE(again_s.IsIoError()) << again_s.ToString();
  EXPECT_EQ(again_s.ToString(), kUploadError.ToString());
  EXPECT_EQ(again_wsize, 0u);

  // ... and Flush surfaces the original writeback failure, triggers length
  // rollback, and does not retry uploads.
  Status flush_s = vfs_->Flush(ctx_, kIno, fh);
  EXPECT_TRUE(flush_s.IsIoError()) << flush_s.ToString();
  EXPECT_EQ(flush_s.ToString(), kUploadError.ToString());

  // Release triggers the second rollback and returns the same sticky failure,
  // but the handle must still go away and the writer leave the table.
  Status release_s = vfs_->Release(ctx_, kIno, fh);
  EXPECT_TRUE(release_s.IsIoError()) << release_s.ToString();
  EXPECT_EQ(release_s.ToString(), kUploadError.ToString());
  EXPECT_EQ(writer_table->Size(), 0u);

  donor->Close();
  donor->ReleaseRef();

  // Pool drained: pages returned, no queued FIFO waiter, no in-flight
  // pressure callback (SetPressureObserver(nullptr) waits for them). Two
  // uploads total means chunk 1 never uploaded a block.
  EXPECT_EQ(tiny_pool.GetUsedBytes(), 0);
  EXPECT_FALSE(tiny_pool.IsPressured());
  {
    std::lock_guard<std::mutex> lock(put_mutex);
    EXPECT_EQ(put_async_calls, 2) << "chunk 1 must never upload a block";
  }
  tiny_pool.Close();
  tiny_pool.SetPressureObserver(nullptr);
  controller.StopAndDrain();
  ASSERT_TRUE(pressure_executor.Stop());
}

// --- 5. GetAttr on .stats inode returns virtual attr ---
TEST_F(VFSImplTest, GetAttr_StatsIno_ReturnsVirtualAttr) {
  Attr out;
  Status s = vfs_->GetAttr(ctx_, kStatsIno, &out);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(out.ino, kStatsIno);
}

// --- 6. Lookup on .stats from root returns virtual attr ---
TEST_F(VFSImplTest, Lookup_StatsFile_ReturnsVirtualAttr) {
  Attr out;
  Status s = vfs_->Lookup(ctx_, kRootIno, kStatsName, &out);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(out.ino, kStatsIno);
}

// --- 7. Open meta fail, no handle returned ---
TEST_F(VFSImplTest, Open_MetaFail_NoHandle) {
  EXPECT_CALL(*mock_meta_system_, Open(_, 100u, _, _, _))
      .WillOnce(Return(Status::Internal("meta error")));

  uint64_t fh = 0;
  Status s = vfs_->Open(ctx_, 100u, 0, &fh, nullptr);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(fh, 0u);
}

// --- 8. Flush valid stats fh returns OK (no meta call) ---
TEST_F(VFSImplTest, Flush_StatsIno_ReturnsOk) {
  // Open kStatsIno to get a valid fh.
  uint64_t fh = 0;
  // Open will try to dump bvar metrics - may or may not have data.
  // Just test that if open succeeds flush returns OK.
  Status open_s = vfs_->Open(ctx_, kStatsIno, 0, &fh, nullptr);
  if (!open_s.ok()) {
    GTEST_SKIP() << "Skipping: no metrics data in .stats";
  }
  EXPECT_NE(fh, 0u);

  // Flush on .stats ino returns OK immediately (no meta call needed).
  EXPECT_CALL(*mock_meta_system_, Flush(_, _, _)).Times(0);
  Status s = vfs_->Flush(ctx_, kStatsIno, fh);
  EXPECT_TRUE(s.ok());

  vfs_->Release(ctx_, kStatsIno, fh);
}

// --- 9. Release stats handle removes it ---
TEST_F(VFSImplTest, Release_StatsHandle_RemovesHandle) {
  uint64_t fh = 0;
  Status open_s = vfs_->Open(ctx_, kStatsIno, 0, &fh, nullptr);
  if (!open_s.ok()) {
    GTEST_SKIP() << "Skipping: no metrics data in .stats";
  }
  EXPECT_NE(fh, 0u);

  Status s = vfs_->Release(ctx_, kStatsIno, fh);
  EXPECT_TRUE(s.ok());
}

TEST_F(VFSImplTest, Release_WritableHandle_FlushesBeforeMetaClose) {
  auto writer_table = std::make_unique<WriterTable>(mock_hub_);
  ON_CALL(*mock_hub_, GetWriterTable())
      .WillByDefault(Return(writer_table.get()));
  EXPECT_CALL(*mock_hub_, GetWriterTable()).Times(AnyNumber());

  constexpr Ino kReleaseIno = 401;
  uint64_t fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, kReleaseIno, O_WRONLY, &fh, nullptr).ok());

  const char data[] = "dirty data";
  uint64_t written = 0;
  EXPECT_CALL(*mock_meta_system_, Write(_, kReleaseIno, _, 0, sizeof(data), fh))
      .WillOnce(Return(Status::OK()));
  ASSERT_TRUE(
      vfs_->Write(ctx_, kReleaseIno, data, sizeof(data), 0, fh, &written).ok());
  ASSERT_EQ(written, sizeof(data));

  {
    InSequence sequence;
    EXPECT_CALL(*mock_meta_system_, WriteSlice(_, kReleaseIno, _, 0, _))
        .WillOnce(Return(Status::OK()));
    EXPECT_CALL(*mock_meta_system_, Close(_, kReleaseIno, fh))
        .WillOnce(Return(Status::OK()));
  }

  Status s = vfs_->Release(ctx_, kReleaseIno, fh);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_FALSE(handle_manager_->FindHandlerForRelease(fh));
}

TEST_F(VFSImplTest, Release_SharedWriter_FlushesBeforeClosingEachSession) {
  auto writer_table = std::make_unique<WriterTable>(mock_hub_);
  ON_CALL(*mock_hub_, GetWriterTable())
      .WillByDefault(Return(writer_table.get()));
  EXPECT_CALL(*mock_hub_, GetWriterTable()).Times(AnyNumber());

  constexpr Ino kReleaseIno = 402;
  uint64_t first_fh = 0;
  uint64_t second_fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, kReleaseIno, O_WRONLY, &first_fh, nullptr).ok());
  ASSERT_TRUE(
      vfs_->Open(ctx_, kReleaseIno, O_WRONLY, &second_fh, nullptr).ok());

  const char data[] = "shared writer dirty data";
  uint64_t written = 0;
  EXPECT_CALL(*mock_meta_system_,
              Write(_, kReleaseIno, _, 0, sizeof(data), first_fh))
      .WillOnce(Return(Status::OK()));
  ASSERT_TRUE(
      vfs_->Write(ctx_, kReleaseIno, data, sizeof(data), 0, first_fh, &written)
          .ok());

  {
    InSequence sequence;
    EXPECT_CALL(*mock_meta_system_, WriteSlice(_, kReleaseIno, _, 0, _))
        .WillOnce(Return(Status::OK()));
    EXPECT_CALL(*mock_meta_system_, Close(_, kReleaseIno, first_fh))
        .WillOnce(Return(Status::OK()));
    EXPECT_CALL(*mock_meta_system_, Close(_, kReleaseIno, second_fh))
        .WillOnce(Return(Status::OK()));
  }

  EXPECT_TRUE(vfs_->Release(ctx_, kReleaseIno, first_fh).ok());
  EXPECT_EQ(writer_table->Size(), 1u);
  EXPECT_TRUE(vfs_->Release(ctx_, kReleaseIno, second_fh).ok());
  EXPECT_EQ(writer_table->Size(), 0u);
}

TEST_F(VFSImplTest, Release_FlushFailureStillClosesAndReleasesHandle) {
  auto writer_table = std::make_unique<WriterTable>(mock_hub_);
  ON_CALL(*mock_hub_, GetWriterTable())
      .WillByDefault(Return(writer_table.get()));
  EXPECT_CALL(*mock_hub_, GetWriterTable()).Times(AnyNumber());

  constexpr Ino kReleaseIno = 403;
  uint64_t fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, kReleaseIno, O_WRONLY, &fh, nullptr).ok());

  const char data[] = "writeback failure";
  uint64_t written = 0;
  EXPECT_CALL(*mock_meta_system_, Write(_, kReleaseIno, _, 0, sizeof(data), fh))
      .WillOnce(Return(Status::OK()));
  ASSERT_TRUE(
      vfs_->Write(ctx_, kReleaseIno, data, sizeof(data), 0, fh, &written).ok());

  {
    InSequence sequence;
    EXPECT_CALL(*mock_meta_system_, WriteSlice(_, kReleaseIno, _, 0, _))
        .WillOnce(Return(Status::Internal("write slice failed")));
    EXPECT_CALL(*mock_meta_system_, Close(_, kReleaseIno, fh))
        .WillOnce(Return(Status::OK()));
  }

  Status s = vfs_->Release(ctx_, kReleaseIno, fh);
  EXPECT_FALSE(s.ok());
  EXPECT_NE(s.ToString().find("write slice failed"), std::string::npos);
  EXPECT_FALSE(handle_manager_->FindHandlerForRelease(fh));
  EXPECT_EQ(writer_table->Size(), 0u);
}

TEST_F(VFSImplTest, Fsync_DataFlushFailureIsNotOverwrittenByMetaFlush) {
  auto writer_table = std::make_unique<WriterTable>(mock_hub_);
  ON_CALL(*mock_hub_, GetWriterTable())
      .WillByDefault(Return(writer_table.get()));
  EXPECT_CALL(*mock_hub_, GetWriterTable()).Times(AnyNumber());

  constexpr Ino kFsyncIno = 404;
  uint64_t fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, kFsyncIno, O_WRONLY, &fh, nullptr).ok());

  const char data[] = "fsync failure";
  uint64_t written = 0;
  EXPECT_CALL(*mock_meta_system_, Write(_, kFsyncIno, _, 0, sizeof(data), fh))
      .WillOnce(Return(Status::OK()));
  ASSERT_TRUE(
      vfs_->Write(ctx_, kFsyncIno, data, sizeof(data), 0, fh, &written).ok());

  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(Return(Status::Internal("data flush failed")));
  EXPECT_CALL(*mock_meta_system_, Flush(_, kFsyncIno, fh)).Times(0);

  Status s = vfs_->Fsync(ctx_, kFsyncIno, /*datasync=*/0, fh);
  EXPECT_FALSE(s.ok());
  EXPECT_NE(s.ToString().find("data flush failed"), std::string::npos);

  EXPECT_CALL(*mock_meta_system_, Close(_, kFsyncIno, fh))
      .WillOnce(Return(Status::OK()));
  EXPECT_FALSE(vfs_->Release(ctx_, kFsyncIno, fh).ok());
}

TEST_F(VFSImplTest, Fsync_MetaFlushFailureReturnedAfterDataFlushSuccess) {
  auto writer_table = std::make_unique<WriterTable>(mock_hub_);
  ON_CALL(*mock_hub_, GetWriterTable())
      .WillByDefault(Return(writer_table.get()));
  EXPECT_CALL(*mock_hub_, GetWriterTable()).Times(AnyNumber());

  constexpr Ino kFsyncIno = 405;
  uint64_t fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, kFsyncIno, O_WRONLY, &fh, nullptr).ok());

  const char data[] = "fsync metadata";
  uint64_t written = 0;
  EXPECT_CALL(*mock_meta_system_, Write(_, kFsyncIno, _, 0, sizeof(data), fh))
      .WillOnce(Return(Status::OK()));
  ASSERT_TRUE(
      vfs_->Write(ctx_, kFsyncIno, data, sizeof(data), 0, fh, &written).ok());

  EXPECT_CALL(*mock_meta_system_, WriteSlice(_, kFsyncIno, _, 0, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_meta_system_, Flush(_, kFsyncIno, fh))
      .WillOnce(Return(Status::Internal("metadata flush failed")));

  Status s = vfs_->Fsync(ctx_, kFsyncIno, /*datasync=*/0, fh);
  EXPECT_FALSE(s.ok());
  EXPECT_NE(s.ToString().find("metadata flush failed"), std::string::npos);

  EXPECT_CALL(*mock_meta_system_, Close(_, kFsyncIno, fh))
      .WillOnce(Return(Status::OK()));
  EXPECT_TRUE(vfs_->Release(ctx_, kFsyncIno, fh).ok());
}

// --- 10. Unlink internal file returns EPERM ---
TEST_F(VFSImplTest, Unlink_InternalFile_EPERM) {
  // Unlink ".stats" from root is blocked.
  Status s = vfs_->Unlink(ctx_, kRootIno, kStatsName);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- 11. StatFs delegates ---
TEST_F(VFSImplTest, StatFs_ReturnsDynamicFilesystemId) {
  FsStat fs_stat;
  fs_stat.max_bytes = 1024 * 1024 * 1024LL;
  fs_stat.used_bytes = 512 * 1024 * 1024LL;

  auto fs_info = test::MakeTestFsInfo();
  fs_info.id = 314159;
  ON_CALL(*mock_hub_, GetFsInfo()).WillByDefault(Return(fs_info));
  EXPECT_CALL(*mock_meta_system_, StatFs(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(fs_stat), Return(Status::OK())));

  FsStat out;
  Status s = vfs_->StatFs(ctx_, kRootIno, &out);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(out.max_bytes, fs_stat.max_bytes);
  EXPECT_EQ(out.fs_id, fs_info.id);
}

// --- 13. GetInfo returns non-empty JSON ---
TEST_F(VFSImplTest, GetInfo_ReturnsNonEmpty) {
  std::string info;
  Status s = vfs_->GetInfo(&info);
  EXPECT_TRUE(s.ok());
  EXPECT_FALSE(info.empty());
  EXPECT_NE(info.find("fs_name"), std::string::npos);
}

// --- 14. RmDir on internal name from root is blocked ---
TEST_F(VFSImplTest, RmDir_InternalName_Blocked) {
  Status s = vfs_->RmDir(ctx_, kRootIno, kStatsName);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- 15. Rename internal name from root is blocked ---
TEST_F(VFSImplTest, Rename_InternalName_Blocked) {
  Status s = vfs_->Rename(ctx_, kRootIno, kStatsName, kRootIno, "newname");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- 16. Subdir mount: Lookup translates virtual root to real subdir ino ---
TEST_F(VFSImplTest, SubdirMount_Lookup_TranslatesParent) {
  constexpr Ino kSubdirIno = 100;
  SetMountRoot("/team", kSubdirIno);

  Attr attr;
  attr.ino = 42;
  attr.type = dingofs::kFile;

  // Caller passes virtual root (kRootIno); MetaSystem must see kSubdirIno.
  EXPECT_CALL(*mock_meta_system_, Lookup(_, kSubdirIno, "child", _))
      .WillOnce(DoAll(SetArgPointee<3>(attr), Return(Status::OK())));
  EXPECT_CALL(*mock_hub_, GetFileSuffixWatcher()).Times(AnyNumber());

  Attr out;
  EXPECT_TRUE(vfs_->Lookup(ctx_, kRootIno, "child", &out).ok());
  EXPECT_EQ(out.ino, 42u);
}

// --- 17. Subdir mount: GetAttr on virtual root returns ino == kRootIno ---
TEST_F(VFSImplTest, SubdirMount_GetAttr_RewritesRootAttr) {
  constexpr Ino kSubdirIno = 100;
  SetMountRoot("/team", kSubdirIno);

  Attr attr;
  attr.ino = kSubdirIno;
  attr.type = dingofs::kDirectory;

  EXPECT_CALL(*mock_meta_system_, GetAttr(_, kSubdirIno, _))
      .WillOnce(DoAll(SetArgPointee<2>(attr), Return(Status::OK())));

  Attr out;
  EXPECT_TRUE(vfs_->GetAttr(ctx_, kRootIno, &out).ok());
  // FUSE-visible root must remain inode 1.
  EXPECT_EQ(out.ino, kRootIno);
}

// --- 18. Subdir mount: descendant inode passes through unchanged ---
TEST_F(VFSImplTest, SubdirMount_Descendant_NoTranslation) {
  constexpr Ino kSubdirIno = 100;
  constexpr Ino kChildIno = 200;
  SetMountRoot("/team", kSubdirIno);

  Attr attr;
  attr.ino = kChildIno;
  attr.type = dingofs::kFile;

  EXPECT_CALL(*mock_meta_system_, GetAttr(_, kChildIno, _))
      .WillOnce(DoAll(SetArgPointee<2>(attr), Return(Status::OK())));

  Attr out;
  EXPECT_TRUE(vfs_->GetAttr(ctx_, kChildIno, &out).ok());
  EXPECT_EQ(out.ino, kChildIno);
}

// --- Trash: Unlink under sub-trash hour bucket is blocked at client ---
TEST_F(VFSImplTest, Trash_Unlink_HourBucket_BlockedAtClient) {
  EXPECT_CALL(*mock_meta_system_, Unlink(_, _, _)).Times(0);

  ctx_->uid = 1000;
  Status s = vfs_->Unlink(ctx_, /*parent=*/0x7FFFFFFF00000005ULL, "anything");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- Trash: RmDir under sub-trash hour bucket is blocked at client ---
TEST_F(VFSImplTest, Trash_RmDir_HourBucket_BlockedAtClient) {
  EXPECT_CALL(*mock_meta_system_, RmDir(_, _, _)).Times(0);

  ctx_->uid = 1000;
  Status s = vfs_->RmDir(ctx_, /*parent=*/0x7FFFFFFF00000005ULL, "subdir");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- Trash: Root manual unlink under hour bucket passes through ---
TEST_F(VFSImplTest, Trash_Unlink_HourBucket_Root_PassesThrough) {
  ctx_->uid = 0;
  EXPECT_CALL(*mock_meta_system_, Unlink(_, 0x7FFFFFFF00000005ULL, _))
      .WillOnce(Return(Status::OK()));

  Status s = vfs_->Unlink(ctx_, /*parent=*/0x7FFFFFFF00000005ULL, "file");
  EXPECT_TRUE(s.ok());
}

// --- Trash: Root manual rmdir under hour bucket passes through ---
TEST_F(VFSImplTest, Trash_RmDir_HourBucket_Root_PassesThrough) {
  ctx_->uid = 0;
  EXPECT_CALL(*mock_meta_system_, RmDir(_, 0x7FFFFFFF00000005ULL, _))
      .WillOnce(Return(Status::OK()));

  Status s = vfs_->RmDir(ctx_, /*parent=*/0x7FFFFFFF00000005ULL, "subdir");
  EXPECT_TRUE(s.ok());
}

// --- Trash: Root cannot rmdir an hour bucket directly under .trash ---
TEST_F(VFSImplTest, Trash_RmDir_KTrashIno_Root_Blocked) {
  ctx_->uid = 0;
  EXPECT_CALL(*mock_meta_system_, RmDir(_, _, _)).Times(0);

  Status s = vfs_->RmDir(ctx_, /*parent=*/kTrashIno, "2026-04-30-13");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- Trash: MkNod under trash hour bucket blocked at client ---
TEST_F(VFSImplTest, Trash_MkNod_InTrashBucket_BlockedAtClient) {
  EXPECT_CALL(*mock_meta_system_, MkNod(_, _, _, _, _, _, _, _)).Times(0);
  Attr out;
  Status s = vfs_->MkNod(ctx_, /*parent=*/0x7FFFFFFF00000005ULL, "file",
                         /*uid=*/0, /*gid=*/0, /*mode=*/0644, /*dev=*/0, &out);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- Trash: MkDir under trash hour bucket blocked at client ---
TEST_F(VFSImplTest, Trash_MkDir_InTrashBucket_BlockedAtClient) {
  EXPECT_CALL(*mock_meta_system_, MkDir(_, _, _, _, _, _, _)).Times(0);
  Attr out;
  Status s = vfs_->MkDir(ctx_, /*parent=*/0x7FFFFFFF00000005ULL, "subdir",
                         /*uid=*/0, /*gid=*/0, /*mode=*/0755, &out);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- Trash: Symlink under trash hour bucket blocked at client ---
TEST_F(VFSImplTest, Trash_Symlink_InTrashBucket_BlockedAtClient) {
  EXPECT_CALL(*mock_meta_system_, Symlink(_, _, _, _, _, _, _)).Times(0);
  Attr out;
  Status s = vfs_->Symlink(ctx_, /*parent=*/0x7FFFFFFF00000005ULL, "link",
                           /*uid=*/0, /*gid=*/0, "/tmp/target", &out);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- Trash: MkNod ".trash" under root blocked at client ---
TEST_F(VFSImplTest, Trash_MkNod_DotTrashUnderRoot_BlockedAtClient) {
  EXPECT_CALL(*mock_meta_system_, MkNod(_, _, _, _, _, _, _, _)).Times(0);
  Attr out;
  Status s = vfs_->MkNod(ctx_, kRootIno, kTrashDirName, 0, 0, 0644, 0, &out);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- Trash: Rename out of .trash root by non-root is blocked at client ---
TEST_F(VFSImplTest, Trash_Rename_OutOfTrashRoot_NonRoot_BlockedAtClient) {
  EXPECT_CALL(*mock_meta_system_, Rename(_, _, _, _, _)).Times(0);

  // Force non-root uid on ctx_ via the public field.
  ctx_->uid = 1000;

  Status s = vfs_->Rename(ctx_, /*old_parent=*/kTrashIno, "anything",
                          /*new_parent=*/kRootIno, "newname");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- Trash: Rename out of .trash root by root passes through to MDS ---
TEST_F(VFSImplTest, Trash_Rename_OutOfTrashRoot_Root_PassesThrough) {
  EXPECT_CALL(*mock_meta_system_, Rename(_, kTrashIno, _, _, _))
      .WillOnce(Return(Status::OK()));

  ctx_->uid = 0;  // root
  Status s = vfs_->Rename(ctx_, /*old_parent=*/kTrashIno, "anything",
                          /*new_parent=*/kRootIno, "newname");
  EXPECT_TRUE(s.ok());
}

// --- Trash: Link with new_parent in trash bucket blocked at client ---
TEST_F(VFSImplTest, Trash_Link_InTrashBucket_BlockedAtClient) {
  EXPECT_CALL(*mock_meta_system_, Link(_, _, _, _, _)).Times(0);

  Attr out;
  Status s = vfs_->Link(ctx_, /*ino=*/100u,
                        /*new_parent=*/0x7FFFFFFF00000005ULL, "linkname", &out);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- Trash: Rename into trash bucket blocked at client ---
TEST_F(VFSImplTest, Trash_Rename_IntoTrash_BlockedAtClient) {
  EXPECT_CALL(*mock_meta_system_, Rename(_, _, _, _, _)).Times(0);

  Status s = vfs_->Rename(ctx_, /*old_parent=*/kRootIno, "anything",
                          /*new_parent=*/0x7FFFFFFF00000005ULL, "newname");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// ===========================================================================
// uid/gid translation wiring (moved from the removed MDS-layer contract tests)
//
// The mapper now lives at the VFS layer: VFSImpl translates OUTBOUND (local ->
// stored hash) before backend calls and INBOUND (stored -> local) on returned
// attrs. These tests verify that wiring through the public VFSImpl surface.
// ===========================================================================
class VFSImplUidGidTest : public VFSImplTest {
 protected:
  // Build an enabled mapper preset with alice@kLocalUid and route the hub's
  // GetUidGidMapper() to it. Stored as a member so it outlives the calls.
  void EnableMapper() {
    auto src = std::make_unique<FakePasswdSource>(
        std::vector<std::pair<std::string, uint32_t>>{{"alice", kLocalUid}},
        std::vector<std::pair<std::string, uint32_t>>{{"alice", kLocalUid}});
    mapper_ = std::make_unique<UidGidMapper>(/*enabled=*/true, "salt-X",
                                             std::move(src));
    mapper_->Refresh();
    EXPECT_CALL(*mock_hub_, GetUidGidMapper())
        .WillRepeatedly(Return(mapper_.get()));
  }

  static constexpr uint32_t kLocalUid = 1001;
  std::unique_ptr<UidGidMapper> mapper_;
};

// Outbound: MkNod with a local uid must reach the meta system as the hashed id.
TEST_F(VFSImplUidGidTest, UidGid_OutboundTranslates_MkNod) {
  EnableMapper();
  const uint32_t hashed =
      mapper_->LocalIdToHashedId(UidGidMapper::Kind::kUid, kLocalUid);
  ASSERT_GE(hashed, UidGidMapper::kLocalUidMax);  // actually translated
  ASSERT_NE(hashed, kLocalUid);

  uint32_t seen_uid = 0;
  EXPECT_CALL(*mock_meta_system_, MkNod(_, kRootIno, "f", _, _, _, _, _))
      .WillOnce(DoAll(SaveArg<3>(&seen_uid), Return(Status::OK())));

  Attr out;
  Status s = vfs_->MkNod(ctx_, kRootIno, "f", /*uid=*/kLocalUid, /*gid=*/0,
                         /*mode=*/0644, /*dev=*/0, &out);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(seen_uid, hashed);
}

// Inbound: a stored hashed uid returned by the backend must surface to the
// caller as the local uid.
TEST_F(VFSImplUidGidTest, UidGid_InboundTranslates_GetAttr) {
  EnableMapper();
  const uint32_t hashed =
      mapper_->LocalIdToHashedId(UidGidMapper::Kind::kUid, kLocalUid);
  ASSERT_GE(hashed, UidGidMapper::kLocalUidMax);

  Attr attr;
  attr.ino = 77;
  attr.type = dingofs::kFile;
  attr.uid = hashed;  // backend stores the hashed id

  EXPECT_CALL(*mock_meta_system_, GetAttr(_, 77u, _))
      .WillOnce(DoAll(SetArgPointee<2>(attr), Return(Status::OK())));

  Attr out;
  Status s = vfs_->GetAttr(ctx_, 77u, &out);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(out.uid, kLocalUid);
}

// Passthrough: with a null mapper (the fixture default), uid is unchanged in
// both directions.
TEST_F(VFSImplUidGidTest, UidGid_Passthrough_NullMapper) {
  // GetUidGidMapper() returns nullptr by VFSTestBase default.

  // Outbound: MkNod uid reaches the backend unchanged.
  uint32_t seen_uid = 0;
  EXPECT_CALL(*mock_meta_system_, MkNod(_, kRootIno, "f", _, _, _, _, _))
      .WillOnce(DoAll(SaveArg<3>(&seen_uid), Return(Status::OK())));
  Attr mk_out;
  EXPECT_TRUE(vfs_->MkNod(ctx_, kRootIno, "f", /*uid=*/kLocalUid, /*gid=*/0,
                          /*mode=*/0644, /*dev=*/0, &mk_out)
                  .ok());
  EXPECT_EQ(seen_uid, kLocalUid);

  // Inbound: GetAttr uid surfaces unchanged.
  Attr attr;
  attr.ino = 88;
  attr.type = dingofs::kFile;
  attr.uid = kLocalUid;
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, 88u, _))
      .WillOnce(DoAll(SetArgPointee<2>(attr), Return(Status::OK())));
  Attr out;
  EXPECT_TRUE(vfs_->GetAttr(ctx_, 88u, &out).ok());
  EXPECT_EQ(out.uid, kLocalUid);
}

// ===========================================================================
// ReadDir synthesized entries: "."/".." for every directory, plus
// ".stats"/".trash" at the FUSE-visible root. Synthesized entries share one
// positional cookie space with real dentries (stream position p has cookie
// p+1). See docs/adr/0001-readdir-synthesized-entries.md.
// ===========================================================================
namespace {

struct ReadDirRecord {
  std::string name;
  Ino ino;
  Attr attr;
  uint64_t cookie;
};

// Collect every (entry, cookie) the handler sees until the stream ends.
std::vector<ReadDirRecord> CollectReadDir(VFSImpl* vfs, ContextSPtr ctx,
                                          Ino ino, uint64_t offset,
                                          bool with_attr, uint32_t& count,
                                          Status& status) {
  std::vector<ReadDirRecord> out;
  status = vfs->ReadDir(
      ctx, ino, /*fh=*/1, offset, with_attr,
      [&](const DirEntry& e, uint64_t off) {
        out.push_back(ReadDirRecord{e.name, e.ino, e.attr, off});
        return true;
      },
      count);
  return out;
}

// Serves `entries` as the meta layer's real-dentry stream (0-based
// positions), honoring the delegate offset. Matches the tail of
// MockMetaSystem::ReadDir's signature for use with Invoke.
Status ServeRealEntries(const std::vector<DirEntry>& entries, uint64_t offset,
                        ReadDirHandler& handler, uint32_t& count) {
  for (uint64_t i = offset; i < entries.size(); ++i) {
    if (!handler(entries[i], i + 1)) return Status::OK();
    ++count;
  }
  return Status::OK();
}

}  // namespace

// Case 1: a normal directory at offset 0 yields [".", "..", ...reals] with
// contiguous cookies, and the meta delegate sees a 0-based offset.
TEST_F(VFSImplTest, ReadDir_SynthesizesDotAndDotDot) {
  const Ino kDir = 100, kParent = 50;
  Attr dir_attr = test::MakeDirAttr(kDir);
  dir_attr.parents = {kParent};
  ON_CALL(*mock_meta_system_, GetAttr(_, kDir, _))
      .WillByDefault(DoAll(SetArgPointee<2>(dir_attr), Return(Status::OK())));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  std::vector<DirEntry> reals = {DirEntry{101, "a", Attr{}},
                                 DirEntry{102, "b", Attr{}}};
  uint64_t delegate_offset = UINT64_MAX;
  EXPECT_CALL(*mock_meta_system_, ReadDir(_, kDir, _, _, false, _, _))
      .WillOnce(Invoke([&](ContextSPtr, Ino, uint64_t, uint64_t off, bool,
                           ReadDirHandler h, uint32_t& count) {
        delegate_offset = off;
        return ServeRealEntries(reals, off, h, count);
      }));

  uint32_t count = 0;
  Status s;
  auto got = CollectReadDir(vfs_.get(), ctx_, kDir, /*offset=*/0,
                            /*with_attr=*/false, count, s);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(got.size(), 4u);
  EXPECT_EQ(got[0].name, ".");
  EXPECT_EQ(got[0].ino, kDir);
  EXPECT_EQ(got[0].cookie, 1u);
  EXPECT_EQ(got[1].name, "..");
  EXPECT_EQ(got[1].ino, kParent);
  EXPECT_EQ(got[1].cookie, 2u);
  EXPECT_EQ(got[2].name, "a");
  EXPECT_EQ(got[2].cookie, 3u);
  EXPECT_EQ(got[3].name, "b");
  EXPECT_EQ(got[3].cookie, 4u);
  EXPECT_EQ(count, 4u);  // synthesized entries included
  EXPECT_EQ(delegate_offset, 0u);
}

// Case 2: resuming mid-stream from arbitrary kernel cookies lands on the
// right entry, and the meta delegate offset is shifted by the synthesized
// prefix length.
TEST_F(VFSImplTest, ReadDir_MidStreamOffsets) {
  const Ino kDir = 100, kParent = 50;
  Attr dir_attr = test::MakeDirAttr(kDir);
  dir_attr.parents = {kParent};
  ON_CALL(*mock_meta_system_, GetAttr(_, kDir, _))
      .WillByDefault(DoAll(SetArgPointee<2>(dir_attr), Return(Status::OK())));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  std::vector<DirEntry> reals = {DirEntry{101, "a", Attr{}},
                                 DirEntry{102, "b", Attr{}}};
  std::vector<uint64_t> delegate_offsets;
  ON_CALL(*mock_meta_system_, ReadDir(_, kDir, _, _, false, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino, uint64_t, uint64_t off, bool,
                                ReadDirHandler h, uint32_t& count) {
        delegate_offsets.push_back(off);
        return ServeRealEntries(reals, off, h, count);
      }));
  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _))
      .Times(AnyNumber());

  // offset=1: resume at ".."
  {
    uint32_t count = 0;
    Status s;
    auto got = CollectReadDir(vfs_.get(), ctx_, kDir, 1, false, count, s);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got[0].name, "..");
    EXPECT_EQ(got[0].cookie, 2u);
    EXPECT_EQ(got[1].name, "a");
    EXPECT_EQ(got[2].name, "b");
  }
  // offset=2: resume at the first real dentry
  {
    uint32_t count = 0;
    Status s;
    auto got = CollectReadDir(vfs_.get(), ctx_, kDir, 2, false, count, s);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0].name, "a");
    EXPECT_EQ(got[0].cookie, 3u);
    EXPECT_EQ(got[1].name, "b");
    EXPECT_EQ(got[1].cookie, 4u);
  }
  // offset=3: resume inside the real stream
  {
    uint32_t count = 0;
    Status s;
    auto got = CollectReadDir(vfs_.get(), ctx_, kDir, 3, false, count, s);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].name, "b");
    EXPECT_EQ(got[0].cookie, 4u);
  }

  EXPECT_EQ(delegate_offsets, (std::vector<uint64_t>{0, 0, 1}));
}

// Case 3: readdirplus populates "." with the directory's attr and ".." with
// the parent's attr.
TEST_F(VFSImplTest, ReadDir_WithAttr_PopulatesDotAttrs) {
  const Ino kDir = 100, kParent = 50;
  Attr dir_attr = test::MakeDirAttr(kDir);
  dir_attr.parents = {kParent};
  dir_attr.uid = 7;
  Attr parent_attr = test::MakeDirAttr(kParent);
  parent_attr.uid = 8;

  ON_CALL(*mock_meta_system_, GetAttr(_, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino ino, Attr* attr) {
        if (ino == kDir) {
          *attr = dir_attr;
          return Status::OK();
        }
        if (ino == kParent) {
          *attr = parent_attr;
          return Status::OK();
        }
        return Status::NotExist("no such ino");
      }));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  std::vector<DirEntry> reals = {DirEntry{201, "x", test::MakeFileAttr(201)}};
  ON_CALL(*mock_meta_system_, ReadDir(_, kDir, _, _, true, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino, uint64_t, uint64_t off, bool,
                                ReadDirHandler h, uint32_t& count) {
        return ServeRealEntries(reals, off, h, count);
      }));
  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _))
      .Times(AnyNumber());

  uint32_t count = 0;
  Status s;
  auto got =
      CollectReadDir(vfs_.get(), ctx_, kDir, 0, /*with_attr=*/true, count, s);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(got.size(), 3u);
  EXPECT_EQ(got[0].name, ".");
  EXPECT_EQ(got[0].attr.ino, kDir);
  EXPECT_EQ(got[0].attr.uid, 7u);
  EXPECT_EQ(got[1].name, "..");
  EXPECT_EQ(got[1].attr.ino, kParent);
  EXPECT_EQ(got[1].attr.uid, 8u);
}

// Case 4a: the FUSE root with trash disabled yields [".", "..", ".stats"];
// ".." of the root self-loops to kRootIno.
TEST_F(VFSImplTest, ReadDir_Root_TrashHidden) {
  Attr root_attr = test::MakeDirAttr(kRootIno);  // parents empty
  ON_CALL(*mock_meta_system_, GetAttr(_, kRootIno, _))
      .WillByDefault(DoAll(SetArgPointee<2>(root_attr), Return(Status::OK())));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  std::vector<DirEntry> reals = {DirEntry{101, "a", Attr{}}};
  ON_CALL(*mock_meta_system_, ReadDir(_, kRootIno, _, _, false, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino, uint64_t, uint64_t off, bool,
                                ReadDirHandler h, uint32_t& count) {
        return ServeRealEntries(reals, off, h, count);
      }));
  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _))
      .Times(AnyNumber());

  uint32_t count = 0;
  Status s;
  auto got = CollectReadDir(vfs_.get(), ctx_, kRootIno, 0, false, count, s);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(got.size(), 4u);
  EXPECT_EQ(got[0].name, ".");
  EXPECT_EQ(got[0].ino, kRootIno);
  EXPECT_EQ(got[1].name, "..");
  EXPECT_EQ(got[1].ino, kRootIno);  // root self-loop
  EXPECT_EQ(got[2].name, kStatsName);
  EXPECT_EQ(got[2].ino, kStatsIno);
  EXPECT_EQ(got[2].cookie, 3u);
  EXPECT_EQ(got[3].name, "a");
  EXPECT_EQ(got[3].cookie, 4u);
}

// Case 4b: with trash enabled the root stream is [".", "..", ".stats",
// ".trash", ...reals].
TEST_F(VFSImplTest, ReadDir_Root_TrashVisible) {
  auto fs_info = test::MakeTestFsInfo();
  fs_info.trash_days = 7;
  ON_CALL(*mock_hub_, GetFsInfo()).WillByDefault(Return(fs_info));

  Attr root_attr = test::MakeDirAttr(kRootIno);
  ON_CALL(*mock_meta_system_, GetAttr(_, kRootIno, _))
      .WillByDefault(DoAll(SetArgPointee<2>(root_attr), Return(Status::OK())));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  std::vector<DirEntry> reals = {DirEntry{101, "a", Attr{}}};
  ON_CALL(*mock_meta_system_, ReadDir(_, kRootIno, _, _, false, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino, uint64_t, uint64_t off, bool,
                                ReadDirHandler h, uint32_t& count) {
        return ServeRealEntries(reals, off, h, count);
      }));
  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _))
      .Times(AnyNumber());

  uint32_t count = 0;
  Status s;
  auto got = CollectReadDir(vfs_.get(), ctx_, kRootIno, 0, false, count, s);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(got.size(), 5u);
  EXPECT_EQ(got[0].name, ".");
  EXPECT_EQ(got[1].name, "..");
  EXPECT_EQ(got[2].name, kStatsName);
  EXPECT_EQ(got[3].name, kTrashDirName);
  EXPECT_EQ(got[3].ino, kTrashIno);
  EXPECT_EQ(got[3].cookie, 4u);
  EXPECT_EQ(got[4].name, "a");
  EXPECT_EQ(got[4].cookie, 5u);
}

// Case 4c: resuming a root stream past ".." (positions 2/3 are
// ".stats"/".trash") needs no directory attr — GetAttr must not be called.
TEST_F(VFSImplTest, ReadDir_RootContinuation_SkipsGetAttr) {
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(0);

  std::vector<DirEntry> reals = {DirEntry{101, "a", Attr{}}};
  ON_CALL(*mock_meta_system_, ReadDir(_, kRootIno, _, _, false, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino, uint64_t, uint64_t off, bool,
                                ReadDirHandler h, uint32_t& count) {
        return ServeRealEntries(reals, off, h, count);
      }));
  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _))
      .Times(AnyNumber());

  uint32_t count = 0;
  Status s;
  auto got =
      CollectReadDir(vfs_.get(), ctx_, kRootIno, /*offset=*/2, false, count, s);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(got.size(), 2u);
  EXPECT_EQ(got[0].name, kStatsName);
  EXPECT_EQ(got[0].cookie, 3u);
  EXPECT_EQ(got[1].name, "a");
  EXPECT_EQ(got[1].cookie, 4u);
}

// Case 5: a handler returning false (full kernel page) stops the stream
// immediately; the meta delegate is not entered when the stop happens inside
// the synthesized prefix.
TEST_F(VFSImplTest, ReadDir_HandlerStopsEarly) {
  const Ino kDir = 100;
  Attr dir_attr = test::MakeDirAttr(kDir);
  dir_attr.parents = {50};
  ON_CALL(*mock_meta_system_, GetAttr(_, kDir, _))
      .WillByDefault(DoAll(SetArgPointee<2>(dir_attr), Return(Status::OK())));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _)).Times(0);

  std::vector<std::string> seen;
  uint32_t count = 0;
  Status s = vfs_->ReadDir(
      ctx_, kDir, /*fh=*/1, /*offset=*/0, /*with_attr=*/false,
      [&](const DirEntry& e, uint64_t) {
        seen.push_back(e.name);
        return e.name != "..";  // stop after ".." is offered... refuse it
      },
      count);
  ASSERT_TRUE(s.ok()) << s.ToString();
  // "." consumed, ".." refused -> stream stops before any real dentry.
  EXPECT_EQ(seen, (std::vector<std::string>{".", ".."}));
  EXPECT_EQ(count, 1u);
}

// Case 6: a directory deleted while held open reads as empty and never
// reaches the meta delegate.
TEST_F(VFSImplTest, ReadDir_DeletedDir_ReadsEmpty) {
  const Ino kDir = 100;
  ON_CALL(*mock_meta_system_, GetAttr(_, kDir, _))
      .WillByDefault(Return(Status::NotExist("dir deleted")));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _)).Times(0);

  uint32_t count = 0;
  Status s;
  auto got = CollectReadDir(vfs_.get(), ctx_, kDir, 0, false, count, s);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_TRUE(got.empty());
  EXPECT_EQ(count, 0u);
}

// Case 7: subdir mount — readdir of the FUSE-visible root reports "."/".."
// as kRootIno, the meta delegate sees the translated mount-root ino, and a
// direct child of the mount root reports ".." as kRootIno (reverse of
// TranslateIno).
TEST_F(VFSImplTest, ReadDir_SubdirMount_DotDotMapping) {
  const Ino kMountRoot = 999, kChild = 100;
  SetMountRoot("/sub", kMountRoot);

  Attr mount_root_attr = test::MakeDirAttr(kMountRoot);
  mount_root_attr.parents = {88};  // real parent outside the mount namespace
  Attr child_attr = test::MakeDirAttr(kChild);
  child_attr.parents = {kMountRoot};

  ON_CALL(*mock_meta_system_, GetAttr(_, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino ino, Attr* attr) {
        if (ino == kMountRoot) {
          *attr = mount_root_attr;
          return Status::OK();
        }
        if (ino == kChild) {
          *attr = child_attr;
          return Status::OK();
        }
        return Status::NotExist("no such ino");
      }));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  Ino delegate_ino = 0;
  ON_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino ino, uint64_t, uint64_t, bool,
                                ReadDirHandler, uint32_t&) {
        delegate_ino = ino;
        return Status::OK();
      }));
  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _))
      .Times(AnyNumber());

  // readdir(kRootIno): "."/".." both report kRootIno; meta sees kMountRoot.
  {
    uint32_t count = 0;
    Status s;
    auto got = CollectReadDir(vfs_.get(), ctx_, kRootIno, 0, false, count, s);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_GE(got.size(), 2u);
    EXPECT_EQ(got[0].ino, kRootIno);
    EXPECT_EQ(got[1].ino, kRootIno);  // mount root self-loop
    EXPECT_EQ(delegate_ino, kMountRoot);
  }
  // readdir(child of mount root): ".." is rewritten to kRootIno.
  {
    uint32_t count = 0;
    Status s;
    auto got = CollectReadDir(vfs_.get(), ctx_, kChild, 0, false, count, s);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[1].name, "..");
    EXPECT_EQ(got[1].ino, kRootIno);
  }
}

// Case 8: an empty directory yields exactly [".", ".."].
TEST_F(VFSImplTest, ReadDir_EmptyDir) {
  const Ino kDir = 100;
  Attr dir_attr = test::MakeDirAttr(kDir);
  dir_attr.parents = {50};
  ON_CALL(*mock_meta_system_, GetAttr(_, kDir, _))
      .WillByDefault(DoAll(SetArgPointee<2>(dir_attr), Return(Status::OK())));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  std::vector<DirEntry> no_reals;
  ON_CALL(*mock_meta_system_, ReadDir(_, kDir, _, _, false, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino, uint64_t, uint64_t off, bool,
                                ReadDirHandler h, uint32_t& count) {
        return ServeRealEntries(no_reals, off, h, count);
      }));
  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _))
      .Times(AnyNumber());

  uint32_t count = 0;
  Status s;
  auto got = CollectReadDir(vfs_.get(), ctx_, kDir, 0, false, count, s);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(got.size(), 2u);
  EXPECT_EQ(got[0].name, ".");
  EXPECT_EQ(got[1].name, "..");
  EXPECT_EQ(count, 2u);
}

// Case 9: with uid/gid mapping enabled, synthesized entries carry GetAttr's
// already-localized ids (single translation, NOT re-translated by the real
// -entry wrapper), while real entries are translated exactly once.
TEST_F(VFSImplUidGidTest, ReadDir_UidGid_SynthNotDoubleTranslated) {
  EnableMapper();
  const uint32_t hashed =
      mapper_->LocalIdToHashedId(UidGidMapper::Kind::kUid, kLocalUid);
  ASSERT_NE(hashed, kLocalUid);

  const Ino kDir = 100, kParent = 50;
  Attr dir_attr = test::MakeDirAttr(kDir);
  dir_attr.parents = {kParent};
  dir_attr.uid = hashed;  // backend stores hashed ids
  Attr parent_attr = test::MakeDirAttr(kParent);
  parent_attr.uid = hashed;

  ON_CALL(*mock_meta_system_, GetAttr(_, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino ino, Attr* attr) {
        if (ino == kDir) {
          *attr = dir_attr;
          return Status::OK();
        }
        if (ino == kParent) {
          *attr = parent_attr;
          return Status::OK();
        }
        return Status::NotExist("no such ino");
      }));
  EXPECT_CALL(*mock_meta_system_, GetAttr(_, _, _)).Times(AnyNumber());

  Attr real_attr = test::MakeFileAttr(201);
  real_attr.uid = hashed;
  std::vector<DirEntry> reals = {DirEntry{201, "x", real_attr}};
  ON_CALL(*mock_meta_system_, ReadDir(_, kDir, _, _, true, _, _))
      .WillByDefault(Invoke([&](ContextSPtr, Ino, uint64_t, uint64_t off, bool,
                                ReadDirHandler h, uint32_t& count) {
        return ServeRealEntries(reals, off, h, count);
      }));
  EXPECT_CALL(*mock_meta_system_, ReadDir(_, _, _, _, _, _, _))
      .Times(AnyNumber());

  uint32_t count = 0;
  Status s;
  auto got =
      CollectReadDir(vfs_.get(), ctx_, kDir, 0, /*with_attr=*/true, count, s);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(got.size(), 3u);
  // All three entries surface the same local uid. If synthesized entries were
  // pushed through the real-entry wrapper, "."/".." would be translated
  // twice and diverge from kLocalUid.
  EXPECT_EQ(got[0].name, ".");
  EXPECT_EQ(got[0].attr.uid, kLocalUid);
  EXPECT_EQ(got[1].name, "..");
  EXPECT_EQ(got[1].attr.uid, kLocalUid);
  EXPECT_EQ(got[2].name, "x");
  EXPECT_EQ(got[2].attr.uid, kLocalUid);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
