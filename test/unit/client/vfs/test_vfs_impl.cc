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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "client/vfs/components/uid_gid_mapper.h"
#include "client/vfs/data/writer_table.h"
#include "client/vfs/data_buffer.h"
#include "client/vfs/vfs_impl.h"
#include "common/blockaccess/accesser_common.h"
#include "common/const.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "common/writemempool/write_mem_pool.h"
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
    // VFSImplTest is a friend of VFSImpl, so the private constructor is
    // accessible.
    vfs_.reset(new VFSImpl(std::move(hub_uptr_)));
  }

  std::unique_ptr<VFSImpl> vfs_;
  std::unique_ptr<TraceManager> trace_manager_;

  // Helper accessible from TEST_F-derived subclasses (which are not friends
  // of VFSImpl themselves but inherit from this friend class).
  void SetMountRoot(const std::string& path, Ino ino) {
    vfs_->mount_root_path_ = path;
    vfs_->mount_root_ino_ = ino;
  }
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

// --- 4b. Write short-write: metadata gets out_wsize, not the requested size.
// On a cross-chunk write where the page pool is exhausted after chunk0, the
// writer short-writes; VFSImpl must extend the inode length by only the durable
// prefix (MetaSystem::Write sets length to offset+len), else reads past the
// prefix would see a phantom hole. ---
TEST_F(VFSImplTest, Write_ShortWrite_MetaGetsOutWsizeNotSize) {
  // Locals ordered so writer_table (and the writer it owns) is destroyed before
  // `tiny`: vfs_->Release below already tears the writer down synchronously,
  // but this keeps the ordering safe regardless.
  auto tiny = std::make_unique<WriteMemPool>(4096, 4096);  // 1 page
  auto writer_table = std::make_unique<WriterTable>(mock_hub_);

  ON_CALL(*mock_hub_, GetWriteMemPool()).WillByDefault(Return(tiny.get()));
  EXPECT_CALL(*mock_hub_, GetWriteMemPool()).Times(AnyNumber());
  ON_CALL(*mock_hub_, GetWriterTable())
      .WillByDefault(Return(writer_table.get()));
  EXPECT_CALL(*mock_hub_, GetWriterTable()).Times(AnyNumber());

  const uint64_t ino = 300;
  ON_CALL(*mock_meta_system_, Open(_, ino, _, _))
      .WillByDefault(Return(Status::OK()));
  ON_CALL(*mock_meta_system_, Close(_, ino, _))
      .WillByDefault(Return(Status::OK()));

  uint64_t fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, ino, O_WRONLY, &fh).ok());

  // Capture the size MetaSystem::Write receives (arg index 4).
  uint64_t meta_size = 0;
  EXPECT_CALL(*mock_meta_system_, Write(_, ino, _, _, _, _))
      .WillOnce(DoAll(SaveArg<4>(&meta_size), Return(Status::OK())));

  const uint64_t chunk_size = mock_hub_->GetFsInfo().chunk_size;
  const uint64_t offset = chunk_size - 4;  // 4 bytes chunk0, 4 bytes chunk1
  std::vector<char> buf(8, 'X');
  uint64_t wsize = 0;
  Status s = vfs_->Write(ctx_, ino, buf.data(), buf.size(), offset, fh, &wsize);

  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(wsize, 4u);
  EXPECT_EQ(meta_size, 4u)
      << "metadata must extend by out_wsize (4), not the requested size (8)";

  // Synchronously releases the handle -> writer -> frees pages into `tiny`.
  vfs_->Release(ctx_, ino, fh);
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
  ON_CALL(*mock_meta_system_, Open(_, ino, _, _))
      .WillByDefault(Return(Status::OK()));
  ON_CALL(*mock_meta_system_, Close(_, ino, _))
      .WillByDefault(Return(Status::OK()));

  uint64_t fh = 0;
  ASSERT_TRUE(vfs_->Open(ctx_, ino, O_WRONLY, &fh).ok());

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

// --- 11. StatFs delegates ---
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

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
