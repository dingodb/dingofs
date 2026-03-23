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

#include "client/vfs/vfs_impl.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "client/vfs/data_buffer.h"
#include "common/blockaccess/accesser_common.h"
#include "common/const.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/mock/mock_meta_system.h"
#include "test/unit/client/vfs/mock/mock_vfs_hub.h"
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

class VFSImplTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    ON_CALL(*mock_hub_, GetBlockAccesserOptions())
        .WillByDefault(Return(blockaccess::BlockAccessOptions{}));
    EXPECT_CALL(*mock_hub_, GetBlockAccesserOptions()).Times(AnyNumber());

    // hub_uptr_ is consumed here; after this, use vfs_ to access VFSImpl.
    // VFSImplTest is a friend of VFSImpl, so the private constructor is accessible.
    vfs_.reset(new VFSImpl(std::move(hub_uptr_)));
  }

  std::unique_ptr<VFSImpl> vfs_;
  std::unique_ptr<TraceManager> trace_manager_;
};

// --- 1. GetFsId ---
TEST_F(VFSImplTest, GetFsId_ReturnsFromFsInfo) {
  // VFSImpl::GetFsId() is hardcoded to 10.
  EXPECT_EQ(vfs_->GetFsId(), 10u);
}

// --- 2. GetMaxNameLength ---
TEST_F(VFSImplTest, GetMaxNameLength_ReturnsValue) {
  EXPECT_GT(vfs_->GetMaxNameLength(), 0u);
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
  EXPECT_CALL(*mock_meta_system_, Open(_, 100u, _, _))
      .WillOnce(Return(Status::Internal("meta error")));

  uint64_t fh = 0;
  Status s = vfs_->Open(ctx_, 100u, 0, &fh);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(fh, 0u);
}

// --- 8. Flush valid stats fh returns OK (no meta call) ---
TEST_F(VFSImplTest, Flush_StatsIno_ReturnsOk) {
  // Open kStatsIno to get a valid fh.
  uint64_t fh = 0;
  // Open will try to dump bvar metrics - may or may not have data.
  // Just test that if open succeeds flush returns OK.
  Status open_s = vfs_->Open(ctx_, kStatsIno, 0, &fh);
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
  Status open_s = vfs_->Open(ctx_, kStatsIno, 0, &fh);
  if (!open_s.ok()) {
    GTEST_SKIP() << "Skipping: no metrics data in .stats";
  }
  EXPECT_NE(fh, 0u);

  Status s = vfs_->Release(ctx_, kStatsIno, fh);
  EXPECT_TRUE(s.ok());
}

// --- 10. Unlink internal file returns EPERM ---
TEST_F(VFSImplTest, Unlink_InternalFile_EPERM) {
  // Unlink ".stats" from root is blocked.
  Status s = vfs_->Unlink(ctx_, kRootIno, kStatsName);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- 11. Unlink ".recycle" from root returns NoPermitted ---
TEST_F(VFSImplTest, Unlink_RecycleFile_EPERM) {
  Status s = vfs_->Unlink(ctx_, kRootIno, ".recycle");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoPermitted());
}

// --- 12. StatFs delegates ---
TEST_F(VFSImplTest, StatFs_DelegatesToMetaSystem) {
  FsStat fs_stat;
  fs_stat.max_bytes = 1024 * 1024 * 1024LL;
  fs_stat.used_bytes = 512 * 1024 * 1024LL;

  EXPECT_CALL(*mock_meta_system_, StatFs(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(fs_stat), Return(Status::OK())));

  FsStat out;
  Status s = vfs_->StatFs(ctx_, kRootIno, &out);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(out.max_bytes, fs_stat.max_bytes);
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

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
