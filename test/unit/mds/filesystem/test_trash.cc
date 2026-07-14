// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <fcntl.h>
#include <fmt/format.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/const.h"
#include "common/meta.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "gtest/gtest.h"
#include "mds/common/codec.h"
#include "mds/common/context.h"
#include "mds/common/runnable.h"
#include "mds/common/trash.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/id_generator.h"
#include "mds/filesystem/store_operation.h"
#include "mds/quota/quota.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {
namespace unit_test {

// Drain the in-memory dir-stat delta log (summed per ino) and clear it -- the
// new manager has no SwapOut; production flush uses GetFlushSnapshot + Compact.
static std::map<uint64_t, DirStatDelta> DrainDirStats(
    dir_stat::DirStatManager& m) {
  auto snap = m.GetFlushSnapshot();
  std::map<uint64_t, DirStatDelta> out;
  for (const auto& [ino, fe] : snap) {
    out[ino] = fe.delta;
    m.Compact(ino, fe.timepoint);
  }
  return out;
}

// ============================================================================
// Unit Tests — Trash FileSystem behavior
//
// Note: pure-utility tests for Trash constants / entry-name / bucket-name
// helpers live in test/unit/mds/common/test_trash.cc.
// ============================================================================

const int64_t kTrashMdsId = 10000;

static pb::mds::S3Info CreateS3Info() {
  pb::mds::S3Info s3_info;
  s3_info.set_ak("ak");
  s3_info.set_sk("sk");
  s3_info.set_endpoint("http://s3.com");
  s3_info.set_bucketname("bucket");
  return s3_info;
}

static pb::mds::FsInfo CreateFsInfoWithTrash(uint32_t fs_id,
                                             const std::string& fs_name,
                                             uint32_t trash_days,
                                             bool enable_dir_stats = false) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_id(fs_id);
  fs_info.set_fs_name(fs_name);
  fs_info.set_fs_type(pb::mds::FsType::S3);
  fs_info.set_status(pb::mds::FsStatus::NORMAL);
  fs_info.set_block_size(1024 * 1024);
  fs_info.set_chunk_size(1024 * 1024 * 64);
  fs_info.set_enable_dir_stats(enable_dir_stats);
  fs_info.set_owner("test_user");
  fs_info.set_capacity(1024 * 1024 * 1024);
  fs_info.set_recycle_time_hour(24);
  fs_info.set_trash_days(trash_days);
  fs_info.set_immediate_trash_quota(true);
  *fs_info.mutable_extra()->mutable_s3_info() = CreateS3Info();

  auto* partition_policy = fs_info.mutable_partition_policy();
  partition_policy->set_type(pb::mds::PartitionType::MONOLITHIC_PARTITION);
  partition_policy->mutable_mono()->set_mds_id(kTrashMdsId);

  return fs_info;
}

class TrashFileSystemTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    kv_storage = DummyStorage::New();
    ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

    const uint32_t kFsId = 2000;

    // Use KV-backed ID generators that work with DummyStorage.
    auto ino_id_generator = NewInodeIdGenerator(kFsId, kv_storage);
    ASSERT_TRUE(ino_id_generator->Init()) << "init inode id generator fail.";

    auto slice_id_generator = NewSliceIdGenerator(kv_storage);
    ASSERT_TRUE(slice_id_generator->Init()) << "init slice id generator fail.";

    operation_processor = OperationProcessor::New(kv_storage);
    ASSERT_TRUE(operation_processor->Init())
        << "init operation processor fail.";

    // Create a quota worker set (needed to avoid nullptr crash in
    // AsyncUpdateDirUsage).
    quota_worker_set = SimpleWorkerSet::New("trash_test_quota", 1, 1024, false,
                                            /*is_inplace_run=*/true);
    ASSERT_TRUE(quota_worker_set->Init()) << "init quota worker set fail.";

    pb::mds::FsInfo fs_info = CreateFsInfoWithTrash(kFsId, "trash_test_fs", 7);

    fs = FileSystem::New(kTrashMdsId, FsInfo::New(fs_info),
                         std::move(ino_id_generator), slice_id_generator,
                         kv_storage, operation_processor, nullptr,
                         quota_worker_set, quota_worker_set, nullptr);
    auto status = fs->CreateRoot();
    ASSERT_TRUE(status.ok())
        << "create root fail, error: " << status.error_str();
  }

  static void TearDownTestSuite() {
    fs = nullptr;

    if (quota_worker_set != nullptr) {
      quota_worker_set->Destroy();
      quota_worker_set = nullptr;
    }

    if (operation_processor != nullptr) {
      operation_processor->Destroy();
      operation_processor = nullptr;
    }

    kv_storage = nullptr;
  }

 public:
  static FileSystemSPtr fs;
  static KVStorageSPtr kv_storage;
  static OperationProcessorSPtr operation_processor;
  static WorkerSetSPtr quota_worker_set;
  static FileSystemSPtr Fs() { return fs; }
};

FileSystemSPtr TrashFileSystemTest::fs = nullptr;
KVStorageSPtr TrashFileSystemTest::kv_storage = nullptr;
OperationProcessorSPtr TrashFileSystemTest::operation_processor = nullptr;
WorkerSetSPtr TrashFileSystemTest::quota_worker_set = nullptr;

static Ino FindTrashBucketParent(const AttrEntry& attr) {
  for (Ino parent : attr.parents()) {
    if (IsTrashBucketChild(parent)) {
      return parent;
    }
  }

  return 0;
}

// --- Permission guard tests ---

TEST_F(TrashFileSystemTest, CannotUnlinkTrashDirectory) {
  auto fs = Fs();
  Context ctx;

  EntryWithPaOut entry_out;
  auto status = fs->UnLink(ctx, kRootIno, kTrashName, entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

TEST_F(TrashFileSystemTest, CannotRmdirTrashDirectory) {
  auto fs = Fs();
  Context ctx;

  EntryWithPaOut entry_out;
  auto status = fs->RmDir(ctx, kRootIno, kTrashName, entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

TEST_F(TrashFileSystemTest, CannotCreateFileInTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kTrashInodeId;
  param.name = "illegal_file";
  param.mode = 0644;
  param.uid = 0;
  param.gid = 0;
  param.rdev = 0;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

TEST_F(TrashFileSystemTest, SetAttrOnTrashRootRejected) {
  auto fs = Fs();
  Context ctx;

  FileSystem::SetAttrParam param;
  param.to_set = kSetAttrAtime | kSetAttrMtime | kSetAttrCtime;
  // Mimic a utimensat from the FUSE client (this is the touch reproducer).
  param.attr.set_atime(1);
  param.attr.set_mtime(1);
  param.attr.set_ctime(1);

  EntryWithChunkOut entry_out;
  auto status = fs->SetAttr(ctx, kTrashInodeId, param, entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT)
      << "got: " << status.error_str();
}

TEST_F(TrashFileSystemTest, SetXAttrOnTrashRootRejected) {
  auto fs = Fs();
  Context ctx;

  Inode::XAttrMap xattrs;
  xattrs["user.foo"] = "bar";

  EntryOut entry_out;
  auto status = fs->SetXAttr(ctx, kTrashInodeId, xattrs, entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT)
      << "got: " << status.error_str();
}

TEST_F(TrashFileSystemTest, RemoveXAttrOnTrashRootRejected) {
  auto fs = Fs();
  Context ctx;

  EntryOut entry_out;
  auto status = fs->RemoveXAttr(ctx, kTrashInodeId, "user.foo", entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT)
      << "got: " << status.error_str();
}

TEST_F(TrashFileSystemTest, OpenForWriteOnTrashRootRejected) {
  auto fs = Fs();
  Context ctx;

  FileSystem::OpenParam param;
  param.flags = O_WRONLY;

  EntryOutForOpen entry_out;
  auto status = fs->Open(ctx, kTrashInodeId, param, entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT)
      << "got: " << status.error_str();
}

TEST_F(TrashFileSystemTest, FallocateOnTrashRootRejected) {
  auto fs = Fs();
  Context ctx;

  EntryWithChunkOut entry_out;
  auto status = fs->Fallocate(ctx, kTrashInodeId, /*mode=*/0, /*offset=*/0,
                              /*len=*/4096, entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT)
      << "got: " << status.error_str();
}

TEST_F(TrashFileSystemTest, CannotCreateDirInTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam param;
  param.parent = kTrashInodeId;
  param.name = "illegal_dir";
  param.mode = 0755;
  param.uid = 0;
  param.gid = 0;

  EntryWithPaOut entry_out;
  auto status = fs->MkDir(ctx, param, entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

TEST_F(TrashFileSystemTest, CannotLinkIntoTrash) {
  auto fs = Fs();
  Context ctx;

  // Create a file first.
  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "link_source";
  param.mode = 0644;
  param.uid = 1;
  param.gid = 1;
  param.rdev = 0;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << status.error_str();

  // Link into trash — should fail.
  EntryWithPaOut link_out;
  status = fs->Link(ctx, entry_out.attr.ino(), kTrashInodeId, "link_in_trash",
                    link_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

TEST_F(TrashFileSystemTest, CannotRenameIntoTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "rename_source";
  param.mode = 0644;
  param.uid = 1;
  param.gid = 1;
  param.rdev = 0;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << status.error_str();

  FileSystem::RenameParam rename_param;
  rename_param.old_parent = kRootIno;
  rename_param.old_name = "rename_source";
  rename_param.new_parent = kTrashInodeId;
  rename_param.new_name = "moved_to_trash";
  FileSystem::RenameResult result;
  status = fs->Rename(ctx, rename_param, result);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

TEST_F(TrashFileSystemTest, CannotRenameDotTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::RenameParam rename_param;
  rename_param.old_parent = kRootIno;
  rename_param.old_name = kTrashName;
  rename_param.new_parent = kRootIno;
  rename_param.new_name = "renamed_trash";
  FileSystem::RenameResult result;
  auto status = fs->Rename(ctx, rename_param, result);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

TEST_F(TrashFileSystemTest, CannotRenameToDotTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam param;
  param.parent = kRootIno;
  param.name = "rename_to_trash_source";
  param.mode = 0755;
  param.uid = 1;
  param.gid = 1;

  EntryWithPaOut entry_out;
  auto status = fs->MkDir(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << status.error_str();

  FileSystem::RenameParam rename_param;
  rename_param.old_parent = kRootIno;
  rename_param.old_name = "rename_to_trash_source";
  rename_param.new_parent = kRootIno;
  rename_param.new_name = kTrashName;
  FileSystem::RenameResult result;
  status = fs->Rename(ctx, rename_param, result);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

// Hour buckets cannot be moved out of .trash because their entries still
// reference sub_trash_ino as parent, which would orphan them. The gate is
// unconditional (no root exception): granular rescue is via renaming
// individual bucket children out, or via RestoreFromTrash.
TEST_F(TrashFileSystemTest, CannotRenameTrashHourBucketAsNonRoot) {
  auto fs = Fs();

  // Plant a file into trash so a sub-trash hour bucket exists.
  Context root_ctx;
  FileSystem::MkNodParam mk;
  mk.parent = kRootIno;
  mk.name = "evict_setup_nonroot.txt";
  mk.mode = 0644;
  mk.uid = 1;
  mk.gid = 1;
  mk.rdev = 0;
  EntryWithPaOut create_out;
  ASSERT_TRUE(fs->MkNod(root_ctx, mk, create_out).ok());
  EntryWithPaOut unlink_out;
  ASSERT_TRUE(
      fs->UnLink(root_ctx, kRootIno, "evict_setup_nonroot.txt", unlink_out)
          .ok());

  // Pick the (only / first) bucket name from .trash.
  Trace trace;
  std::string bucket_name;
  ScanDentryOperation scan(trace, 2000, kTrashInodeId,
                           [&](const DentryEntry& d) -> bool {
                             bucket_name = d.name();
                             return false;
                           });
  ASSERT_TRUE(operation_processor->RunAlone(&scan).ok());
  ASSERT_FALSE(bucket_name.empty())
      << "trash hour bucket should exist after unlink";

  ContextEntry ce;
  ce.set_uid(1000);
  Context user_ctx(ce, "req-nonroot-rename-bucket", "Rename");

  FileSystem::RenameParam rp;
  rp.old_parent = kTrashInodeId;
  rp.old_name = bucket_name;
  rp.new_parent = kRootIno;
  rp.new_name = "evicted_bucket_nonroot";
  FileSystem::RenameResult result;
  auto status = fs->Rename(user_ctx, rp, result);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

TEST_F(TrashFileSystemTest, CannotRenameTrashHourBucketAsRoot) {
  auto fs = Fs();
  Context root_ctx;

  FileSystem::MkNodParam mk;
  mk.parent = kRootIno;
  mk.name = "evict_setup_root.txt";
  mk.mode = 0644;
  mk.uid = 1;
  mk.gid = 1;
  mk.rdev = 0;
  EntryWithPaOut create_out;
  ASSERT_TRUE(fs->MkNod(root_ctx, mk, create_out).ok());
  EntryWithPaOut unlink_out;
  ASSERT_TRUE(
      fs->UnLink(root_ctx, kRootIno, "evict_setup_root.txt", unlink_out).ok());

  Trace trace;
  std::string bucket_name;
  Ino bucket_ino = 0;
  ScanDentryOperation scan(trace, 2000, kTrashInodeId,
                           [&](const DentryEntry& d) -> bool {
                             if (bucket_name.empty()) {
                               bucket_name = d.name();
                               bucket_ino = d.ino();
                             }
                             return true;
                           });
  ASSERT_TRUE(operation_processor->RunAlone(&scan).ok());
  ASSERT_FALSE(bucket_name.empty());
  ASSERT_GT(bucket_ino, 0u);

  FileSystem::RenameParam rp;
  rp.old_parent = kTrashInodeId;
  rp.old_name = bucket_name;
  rp.new_parent = kRootIno;
  rp.new_name = "evicted_bucket_root";
  FileSystem::RenameResult result;
  auto status = fs->Rename(root_ctx, rp, result);
  ASSERT_FALSE(status.ok())
      << "even root must not be allowed to rename hour bucket out";
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);

  // Bucket dentry still under .trash.
  bool still_in_trash = false;
  ScanDentryOperation rescan(trace, 2000, kTrashInodeId,
                             [&](const DentryEntry& d) -> bool {
                               if (d.ino() == bucket_ino) still_in_trash = true;
                               return true;
                             });
  ASSERT_TRUE(operation_processor->RunAlone(&rescan).ok());
  EXPECT_TRUE(still_in_trash)
      << "hour bucket should remain in .trash after rejected rename";

  // And not visible under the attempted new home.
  EntryOut look_out;
  status = fs->Lookup(root_ctx, kRootIno, "evicted_bucket_root", look_out);
  EXPECT_FALSE(status.ok());
}

TEST_F(TrashFileSystemTest, CannotRmdirTrashSubDirectory) {
  auto fs = Fs();
  Context ctx;

  EntryWithPaOut entry_out;
  auto status = fs->RmDir(ctx, kTrashInodeId, "2026-04-05-14", entry_out);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

// --- Trash move tests ---

TEST_F(TrashFileSystemTest, UnlinkMovesToTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "trash_unlink_file";
  param.mode = 0644;
  param.uid = 1;
  param.gid = 1;
  param.rdev = 0;

  EntryWithPaOut create_out;
  auto status = fs->MkNod(ctx, param, create_out);
  ASSERT_TRUE(status.ok()) << "create file fail: " << status.error_str();
  Ino file_ino = create_out.attr.ino();
  ASSERT_GT(file_ino, 0);

  // Unlink — with trash enabled, file moves to trash.
  EntryWithPaOut unlink_out;
  status = fs->UnLink(ctx, kRootIno, "trash_unlink_file", unlink_out);
  ASSERT_TRUE(status.ok()) << "unlink fail: " << status.error_str();

  // Original dentry should be gone from root.
  EntryOut lookup_out;
  status = fs->Lookup(ctx, kRootIno, "trash_unlink_file", lookup_out);
  EXPECT_FALSE(status.ok()) << "file should not be found in root after unlink";

  // Inode should still exist (not permanently deleted).
  auto& inode_cache = fs->GetInodeCache();
  auto inode = inode_cache.Get(file_ino);
  EXPECT_TRUE(inode != nullptr)
      << "inode should still exist after trash unlink";
}

TEST_F(TrashFileSystemTest, RmDirMovesToTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam param;
  param.parent = kRootIno;
  param.name = "trash_rmdir_dir";
  param.mode = 0755;
  param.uid = 1;
  param.gid = 1;

  EntryWithPaOut create_out;
  auto status = fs->MkDir(ctx, param, create_out);
  ASSERT_TRUE(status.ok()) << "create dir fail: " << status.error_str();
  Ino dir_ino = create_out.attr.ino();
  ASSERT_GT(dir_ino, 0);

  EntryWithPaOut rmdir_out;
  status = fs->RmDir(ctx, kRootIno, "trash_rmdir_dir", rmdir_out);
  ASSERT_TRUE(status.ok()) << "rmdir fail: " << status.error_str();

  // Original dentry should be gone.
  EntryOut lookup_out;
  status = fs->Lookup(ctx, kRootIno, "trash_rmdir_dir", lookup_out);
  EXPECT_FALSE(status.ok()) << "dir should not be found in root after rmdir";

  // Inode should still exist (moved to trash).
  auto& inode_cache = fs->GetInodeCache();
  auto inode = inode_cache.Get(dir_ino);
  EXPECT_TRUE(inode != nullptr)
      << "dir inode should still exist after trash rmdir";
}

// --- Restore test ---

TEST_F(TrashFileSystemTest, RestoreFromTrashToOriginalPath) {
  auto fs = Fs();
  Context ctx;

  // Create a file.
  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "restore_me.txt";
  param.mode = 0644;
  param.uid = 1;
  param.gid = 1;
  param.rdev = 0;

  EntryWithPaOut create_out;
  auto status = fs->MkNod(ctx, param, create_out);
  ASSERT_TRUE(status.ok()) << status.error_str();
  Ino file_ino = create_out.attr.ino();

  // Unlink (moves to trash).
  EntryWithPaOut unlink_out;
  status = fs->UnLink(ctx, kRootIno, "restore_me.txt", unlink_out);
  ASSERT_TRUE(status.ok()) << status.error_str();

  // Get sub_trash_ino from the unlink output — the entry_out.attr contains
  // the updated child attr with its parent redirected to the sub-trash.
  ASSERT_GT(unlink_out.attr.parents_size(), 0)
      << "unlink output should have parent";
  Ino sub_trash_ino = unlink_out.attr.parents(0);
  ASSERT_NE(sub_trash_ino, kRootIno)
      << "parent should have been redirected from root";

  // Build trash entry name.
  std::string trash_name =
      BuildTrashEntryName(kRootIno, file_ino, "restore_me.txt");

  // Restore to original path — dst is always parsed from the trash entry name.
  status = fs->RestoreFromTrash(ctx, sub_trash_ino, trash_name);
  ASSERT_TRUE(status.ok()) << "restore fail: " << status.error_str();

  // File should be visible again at original path.
  EntryOut lookup_out;
  status = fs->Lookup(ctx, kRootIno, "restore_me.txt", lookup_out);
  ASSERT_TRUE(status.ok()) << "lookup after restore fail: "
                           << status.error_str();
  EXPECT_EQ(lookup_out.attr.ino(), file_ino)
      << "restored file should have same inode";
}

TEST_F(TrashFileSystemTest, RestoreCannotTargetTrashRoot) {
  auto fs = Fs();
  Context ctx;

  // trash_name encodes orig_parent = kTrashInodeId (i.e. the trash root). Even
  // with allow_trash_parent=true, restoring directly under .trash is refused.
  std::string trash_name = BuildTrashEntryName(kTrashInodeId, 100, "file.txt");
  auto status = fs->RestoreFromTrash(ctx, kTrashSubInodeStart, trash_name,
                                     /*allow_trash_parent=*/true);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);

  // Without the flag, restore into *any* trash inode is refused.
  std::string into_bucket =
      BuildTrashEntryName(kTrashSubInodeStart, 101, "file2.txt");
  status = fs->RestoreFromTrash(ctx, kTrashSubInodeStart, into_bucket,
                                /*allow_trash_parent=*/false);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

// Tree-rebuild mode: a child trash entry whose original parent was also trashed
// (and is still a normal-range inode under the hour bucket) can be grafted onto
// that trashed parent when allow_trash_parent=true.
TEST_F(TrashFileSystemTest, RestoreTreeRebuildIntoTrashedDirectory) {
  auto fs = Fs();
  Context ctx;

  // Create /tree_dir/child.txt
  FileSystem::MkDirParam dir_param;
  dir_param.parent = kRootIno;
  dir_param.name = "tree_dir";
  dir_param.mode = 0755;
  dir_param.uid = 1;
  dir_param.gid = 1;
  EntryWithPaOut dir_out;
  auto status = fs->MkDir(ctx, dir_param, dir_out);
  ASSERT_TRUE(status.ok()) << status.error_str();
  Ino dir_ino = dir_out.attr.ino();

  FileSystem::MkNodParam file_param;
  file_param.parent = dir_ino;
  file_param.name = "child.txt";
  file_param.mode = 0644;
  file_param.uid = 1;
  file_param.gid = 1;
  file_param.rdev = 0;
  EntryWithPaOut file_out;
  status = fs->MkNod(ctx, file_param, file_out);
  ASSERT_TRUE(status.ok()) << status.error_str();
  Ino file_ino = file_out.attr.ino();

  // Unlink the child first (so dir becomes empty), then rmdir. Both land in
  // the trash under the same hour bucket.
  EntryWithPaOut unlink_out;
  status = fs->UnLink(ctx, dir_ino, "child.txt", unlink_out);
  ASSERT_TRUE(status.ok()) << status.error_str();
  Ino sub_trash_ino = unlink_out.attr.parents(0);

  EntryWithPaOut rmdir_out;
  status = fs->RmDir(ctx, kRootIno, "tree_dir", rmdir_out);
  ASSERT_TRUE(status.ok()) << status.error_str();

  // Tree-rebuild the child: dst = dir_ino (a trashed user dir). Must succeed.
  std::string child_trash_name =
      BuildTrashEntryName(dir_ino, file_ino, "child.txt");
  status = fs->RestoreFromTrash(ctx, sub_trash_ino, child_trash_name,
                                /*allow_trash_parent=*/true);
  ASSERT_TRUE(status.ok()) << "tree-rebuild into trashed dir fail: "
                           << status.error_str();

  // Verify: the file now lives under dir_ino (still in trash) rather than the
  // hour bucket. Use ListDentry to confirm a child.txt dentry exists under
  // dir_ino.
  pb::mds::ReadDirRequest rd_req;
  (void)rd_req;  // not needed for the assertion below
  auto& inode_cache = fs->GetInodeCache();
  auto file_inode = inode_cache.Get(file_ino);
  ASSERT_TRUE(file_inode != nullptr);
  auto attr = file_inode->ToAttr();
  bool has_dir_as_parent = false;
  for (auto p : attr.parents()) {
    if (p == dir_ino) has_dir_as_parent = true;
  }
  EXPECT_TRUE(has_dir_as_parent)
      << "restored file should have dir_ino as a parent";
}

// Collision: restoring a trash entry whose target name is already taken at the
// original parent must fail with EEXISTED without clobbering the incumbent.
TEST_F(TrashFileSystemTest, RestoreCollisionEExisted) {
  auto fs = Fs();
  Context ctx;

  // Create /collide.txt, unlink (→ trash), then create a fresh /collide.txt
  // pointing at a different inode.
  FileSystem::MkNodParam p1;
  p1.parent = kRootIno;
  p1.name = "collide.txt";
  p1.mode = 0644;
  p1.uid = 1;
  p1.gid = 1;
  p1.rdev = 0;
  EntryWithPaOut first;
  auto status = fs->MkNod(ctx, p1, first);
  ASSERT_TRUE(status.ok()) << status.error_str();
  Ino first_ino = first.attr.ino();

  EntryWithPaOut unlink_out;
  status = fs->UnLink(ctx, kRootIno, "collide.txt", unlink_out);
  ASSERT_TRUE(status.ok()) << status.error_str();
  Ino sub_trash_ino = unlink_out.attr.parents(0);

  EntryWithPaOut second;
  status = fs->MkNod(ctx, p1, second);
  ASSERT_TRUE(status.ok()) << status.error_str();
  ASSERT_NE(second.attr.ino(), first_ino)
      << "recreated file must have a new inode";

  std::string trash_name =
      BuildTrashEntryName(kRootIno, first_ino, "collide.txt");
  status = fs->RestoreFromTrash(ctx, sub_trash_ino, trash_name);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::EEXISTED);

  // The incumbent dentry still resolves to the recreated inode.
  EntryOut look;
  status = fs->Lookup(ctx, kRootIno, "collide.txt", look);
  ASSERT_TRUE(status.ok()) << status.error_str();
  EXPECT_EQ(look.attr.ino(), second.attr.ino());
}

// Hardlink: when both links of an inode are trashed in the same hour, restoring
// one of them reconnects the inode to the live parent; the second trash dentry
// and the other hardlink remain operable.
TEST_F(TrashFileSystemTest, RestoreHardlinkOneOfTwo) {
  auto fs = Fs();
  Context ctx;

  // Create /hl_src and hardlink it to /hl_dst. Both under root.
  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "hl_src";
  param.mode = 0644;
  param.uid = 1;
  param.gid = 1;
  param.rdev = 0;
  EntryWithPaOut src_out;
  auto status = fs->MkNod(ctx, param, src_out);
  ASSERT_TRUE(status.ok()) << status.error_str();
  Ino file_ino = src_out.attr.ino();

  EntryWithPaOut link_out;
  status = fs->Link(ctx, file_ino, kRootIno, "hl_dst", link_out);
  ASSERT_TRUE(status.ok()) << "link fail: " << status.error_str();

  // Unlink both links — both go to trash.
  EntryWithPaOut unlink_src;
  status = fs->UnLink(ctx, kRootIno, "hl_src", unlink_src);
  ASSERT_TRUE(status.ok()) << status.error_str();
  // hl_src still has a live hardlink (hl_dst at root), so its parents list is
  // [root(live), sub_trash_bucket]; pick the actual trash bucket from parents.
  Ino sub_trash_ino = FindTrashBucketParent(unlink_src.attr);
  ASSERT_NE(sub_trash_ino, 0u) << "no trash bucket in parents";

  EntryWithPaOut unlink_dst;
  status = fs->UnLink(ctx, kRootIno, "hl_dst", unlink_dst);
  ASSERT_TRUE(status.ok()) << status.error_str();

  // Restore only hl_src via put-back.
  std::string trash_src = BuildTrashEntryName(kRootIno, file_ino, "hl_src");
  status = fs->RestoreFromTrash(ctx, sub_trash_ino, trash_src);
  ASSERT_TRUE(status.ok()) << "restore hl_src fail: " << status.error_str();

  EntryOut look_src;
  status = fs->Lookup(ctx, kRootIno, "hl_src", look_src);
  ASSERT_TRUE(status.ok()) << status.error_str();
  EXPECT_EQ(look_src.attr.ino(), file_ino);

  // hl_dst must remain restorable too (independent trash entry).
  std::string trash_dst = BuildTrashEntryName(kRootIno, file_ino, "hl_dst");
  status = fs->RestoreFromTrash(ctx, sub_trash_ino, trash_dst);
  ASSERT_TRUE(status.ok()) << "restore hl_dst fail: " << status.error_str();

  EntryOut look_dst;
  status = fs->Lookup(ctx, kRootIno, "hl_dst", look_dst);
  ASSERT_TRUE(status.ok()) << status.error_str();
  EXPECT_EQ(look_dst.attr.ino(), file_ino);
}

// Partial-trash inode (cross-directory hardlink): when the original path is
// unlinked into trash but a hardlink in a different directory survives, a new
// Link via the live parent must succeed — the inode is not "in trash" from
// the user's perspective until every real dentry is gone. JuiceFS-aligned
// deferred-delete-on-last-hardlink.
TEST_F(TrashFileSystemTest, LinkOnPartialTrashSurvivor) {
  auto fs = Fs();
  Context ctx;

  // /partial_trash_live_dir/ holds the surviving hardlink;
  // /partial_trash_hl_src lives at root. Keep names unique because this fixture
  // intentionally shares one filesystem across its test cases.
  FileSystem::MkDirParam mkdir_param;
  mkdir_param.parent = kRootIno;
  mkdir_param.name = "partial_trash_live_dir";
  mkdir_param.mode = 0755;
  mkdir_param.uid = 0;
  mkdir_param.gid = 0;
  EntryWithPaOut live_dir_out;
  ASSERT_TRUE(fs->MkDir(ctx, mkdir_param, live_dir_out).ok());
  Ino live_dir_ino = live_dir_out.attr.ino();

  FileSystem::MkNodParam mk;
  mk.parent = kRootIno;
  mk.name = "partial_trash_hl_src";
  mk.mode = 0644;
  mk.uid = 0;
  mk.gid = 0;
  mk.rdev = 0;
  EntryWithPaOut src_out;
  ASSERT_TRUE(fs->MkNod(ctx, mk, src_out).ok());
  Ino file_ino = src_out.attr.ino();

  // Hardlink under live_dir so parents=[root, live_dir_ino] (no dedup).
  EntryWithPaOut hl_out;
  ASSERT_TRUE(
      fs->Link(ctx, file_ino, live_dir_ino, "partial_trash_hl_live", hl_out)
          .ok());

  // Unlink the root copy → trash; live_dir/hl_live remains real.
  EntryWithPaOut unlink_out;
  ASSERT_TRUE(
      fs->UnLink(ctx, kRootIno, "partial_trash_hl_src", unlink_out).ok());
  // Now parents=[live_dir_ino, trash_X], nlink=2.

  // Linking by ino into the live parent must succeed — the partial-trash
  // state is not user-visible because a real dentry still exists.
  EntryWithPaOut new_link_out;
  auto status = fs->Link(ctx, file_ino, live_dir_ino, "partial_trash_hl_new",
                         new_link_out);
  ASSERT_TRUE(status.ok()) << "Link via partial-trash inode: "
                           << status.error_str();
  EXPECT_EQ(new_link_out.attr.ino(), file_ino);

  // Same-directory linking back to root also works (the destination check
  // CheckCreateInTrash does not flag root).
  EntryWithPaOut new_root_out;
  status =
      fs->Link(ctx, file_ino, kRootIno, "partial_trash_hl_back", new_root_out);
  ASSERT_TRUE(status.ok()) << "Link to root for partial-trash inode: "
                           << status.error_str();

  // Lookup confirms both new links resolve to the same inode.
  EntryOut look_new;
  ASSERT_TRUE(
      fs->Lookup(ctx, live_dir_ino, "partial_trash_hl_new", look_new).ok());
  EXPECT_EQ(look_new.attr.ino(), file_ino);
  EntryOut look_back;
  ASSERT_TRUE(
      fs->Lookup(ctx, kRootIno, "partial_trash_hl_back", look_back).ok());
  EXPECT_EQ(look_back.attr.ino(), file_ino);
}

// Link is allowed even when the source inode is fully in trash — the result is
// a "rescue via Link" path equivalent to RestoreFromTrash. CheckCreateInTrash
// still blocks Link INTO trash on the destination side. The trash-sentinel
// source guard (kTrashInodeId) remains in effect.
TEST_F(TrashFileSystemTest, LinkRescuesFullyTrashedInode) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkNodParam mk;
  mk.parent = kRootIno;
  mk.name = "to_be_trashed";
  mk.mode = 0644;
  mk.uid = 0;
  mk.gid = 0;
  mk.rdev = 0;
  EntryWithPaOut create_out;
  ASSERT_TRUE(fs->MkNod(ctx, mk, create_out).ok());
  Ino file_ino = create_out.attr.ino();

  // Unlink to trash — inode now has no real parents.
  EntryWithPaOut unlink_out;
  ASSERT_TRUE(fs->UnLink(ctx, kRootIno, "to_be_trashed", unlink_out).ok());

  // Link by ino into a live parent must succeed.
  EntryWithPaOut link_out;
  auto status = fs->Link(ctx, file_ino, kRootIno, "rescued", link_out);
  ASSERT_TRUE(status.ok()) << "rescue link: " << status.error_str();

  EntryOut look_out;
  ASSERT_TRUE(fs->Lookup(ctx, kRootIno, "rescued", look_out).ok());
  EXPECT_EQ(look_out.attr.ino(), file_ino);

  // Linking the .trash sentinel itself must remain rejected.
  EntryWithPaOut trash_link;
  status = fs->Link(ctx, kTrashInodeId, kRootIno, "trash_alias", trash_link);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

// --- Quota chain capture tests ---

// Helper: scan kTrashInodeId then recurse into the (single) hour bucket and
// return all dentry entries observed inside it.
static std::vector<DentryEntry> ScanAllTrashEntries(uint32_t fs_id,
                                                    OperationProcessorSPtr op) {
  std::vector<DentryEntry> out;
  Trace trace;

  std::vector<DentryEntry> buckets;
  ScanDentryOperation scan_buckets(trace, fs_id, kTrashInodeId,
                                   [&](const DentryEntry& d) -> bool {
                                     buckets.push_back(d);
                                     return true;
                                   });
  if (!op->RunAlone(&scan_buckets).ok()) return out;

  for (const auto& b : buckets) {
    ScanTrashDentryOperation scan_entries(trace, fs_id, b.ino(),
                                          [&](const DentryEntry& d) -> bool {
                                            out.push_back(d);
                                            return true;
                                          });
    op->RunAlone(&scan_entries);
  }
  return out;
}

// Regression for bucket nlink underflow. Hour-bucket attr is intentionally
// write-once (see trash.cc). Pre-fix, plain rmdir on a grafted dentry under
// the bucket (the manual-cleanup path) decremented bucket.nlink each time
// while no path ever incremented it, so after a few cleanups bucket.nlink
// underflowed to uint32(-2) = 4294967294. After the fix every code path
// leaves bucket nlink alone, so it stays at 2 indefinitely.
TEST_F(TrashFileSystemTest, BucketNlinkStaysFixedAcrossOps) {
  auto fs = Fs();
  Context root_ctx;

  // Trash several DIRs in the same hour — naive bookkeeping would make the
  // bucket nlink go to 2+kN.
  constexpr int kN = 4;
  for (int i = 0; i < kN; ++i) {
    FileSystem::MkDirParam mk;
    mk.parent = kRootIno;
    mk.name = fmt::format("nlink_regression_dir_{}", i);
    mk.mode = 0755;
    mk.uid = 1;
    mk.gid = 1;
    EntryWithPaOut create_out;
    ASSERT_TRUE(fs->MkDir(root_ctx, mk, create_out).ok());

    EntryWithPaOut rmdir_out;
    ASSERT_TRUE(
        fs->RmDir(root_ctx, kRootIno, mk.name, rmdir_out).ok());
  }

  // Pick any current sub-trash bucket dentry under .trash.
  Trace trace;
  Ino bucket_ino = 0;
  ScanDentryOperation scan(trace, /*fs_id=*/2000, kTrashInodeId,
                           [&](const DentryEntry& d) -> bool {
                             if (bucket_ino == 0) bucket_ino = d.ino();
                             return true;
                           });
  ASSERT_TRUE(operation_processor->RunAlone(&scan).ok());
  ASSERT_GT(bucket_ino, 0u) << "trash hour bucket should exist after kN rmdirs";
  ASSERT_TRUE(IsTrashInode(bucket_ino));

  // Read bucket attr directly from KV — pre-fix the bucket inode would have
  // accumulated nlink updates per move-in; the design says it stays at 2.
  std::string value;
  auto get_status = kv_storage->Get(
      MetaCodec::EncodeInodeKey(/*fs_id=*/2000, bucket_ino), value);
  ASSERT_TRUE(get_status.ok()) << get_status.error_str();
  AttrEntry bucket_attr = MetaCodec::DecodeInodeValue(value);
  EXPECT_EQ(bucket_attr.nlink(), 2u)
      << "bucket nlink must stay 2 across DIR trash-move-ins";
}

// BatchUnLink mirrors UnLink for trash semantics: a multi-file `rm` on a
// regular directory must move all targets into the hour bucket. Pre-fix,
// BatchUnLink had no trash logic and silently bypassed trash for batched RPCs.
TEST_F(TrashFileSystemTest, BatchUnlinkMovesToTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam mkdir;
  mkdir.parent = kRootIno;
  mkdir.name = "batch_dir";
  mkdir.mode = 0755;
  mkdir.uid = 1;
  mkdir.gid = 1;
  EntryWithPaOut dir_out;
  ASSERT_TRUE(fs->MkDir(ctx, mkdir, dir_out).ok());
  const Ino dir_ino = dir_out.attr.ino();

  std::vector<std::string> names = {"batch_a.txt", "batch_b.txt"};
  std::vector<Ino> file_inos;
  for (const auto& n : names) {
    FileSystem::MkNodParam mn;
    mn.parent = dir_ino;
    mn.name = n;
    mn.mode = 0644;
    mn.uid = 1;
    mn.gid = 1;
    mn.rdev = 0;
    EntryWithPaOut out_f;
    ASSERT_TRUE(fs->MkNod(ctx, mn, out_f).ok());
    file_inos.push_back(out_f.attr.ino());
  }

  EntriesWithPaOut batch_out;
  ASSERT_TRUE(fs->BatchUnLink(ctx, dir_ino, names, batch_out).ok());

  // Both files must now appear under the hour bucket.
  auto entries = ScanAllTrashEntries(/*fs_id=*/2000, operation_processor);
  std::vector<DentryEntry> hits;
  for (const auto& d : entries) {
    for (Ino f : file_inos) {
      if (d.ino() == f) hits.push_back(d);
    }
  }
  ASSERT_EQ(hits.size(), 2u) << "both batched files must have a trash dentry";

  // The original parent must no longer have the deleted dentries.
  Trace trace;
  std::vector<DentryEntry> remaining;
  ScanDentryOperation scan(trace, /*fs_id=*/2000, dir_ino,
                           [&](const DentryEntry& d) -> bool {
                             remaining.push_back(d);
                             return true;
                           });
  ASSERT_TRUE(operation_processor->RunAlone(&scan).ok());
  for (const auto& d : remaining) {
    EXPECT_NE(d.name(), "batch_a.txt");
    EXPECT_NE(d.name(), "batch_b.txt");
  }
}

// Regression for the mv-out-from-trash hang: when the source parent IS the
// current hour bucket (typical mv-out), RenameOperation's hot-path branch
// computed sub_trash_key == old_parent_key and pushed both to BatchGet keys.
// dingo-sdk's TxnBatchGetTask::Init rejects duplicate keys with "duplicate
// key", the rename txn aborts on every retry, the FUSE client times out in
// uninterruptible D state. After the dedup fix, mv-out succeeds.
TEST_F(TrashFileSystemTest, RenameFileOutOfTrashSucceeds) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkNodParam mk;
  mk.parent = kRootIno;
  mk.name = "rename_out_test.txt";
  mk.mode = 0644;
  mk.uid = 1;
  mk.gid = 1;
  mk.rdev = 0;
  EntryWithPaOut create_out;
  ASSERT_TRUE(fs->MkNod(ctx, mk, create_out).ok());
  const Ino file_ino = create_out.attr.ino();

  EntryWithPaOut unlink_out;
  ASSERT_TRUE(
      fs->UnLink(ctx, kRootIno, "rename_out_test.txt", unlink_out).ok());
  ASSERT_GT(unlink_out.attr.parents_size(), 0);
  const Ino bucket_ino = unlink_out.attr.parents(0);
  ASSERT_TRUE(IsTrashInode(bucket_ino));

  const std::string trash_name =
      BuildTrashEntryName(kRootIno, file_ino, "rename_out_test.txt");

  FileSystem::RenameParam rp;
  rp.old_parent = bucket_ino;
  rp.old_name = trash_name;
  rp.new_parent = kRootIno;
  rp.new_name = "recovered_file.txt";
  FileSystem::RenameResult result;
  auto status = fs->Rename(ctx, rp, result);
  ASSERT_TRUE(status.ok()) << "rename file out of trash bucket must succeed: "
                           << status.error_str();

  EntryOut lookup;
  ASSERT_TRUE(fs->Lookup(ctx, kRootIno, "recovered_file.txt", lookup).ok());
  EXPECT_EQ(lookup.attr.ino(), file_ino);

  Trace trace;
  bool still_in_bucket = false;
  ScanTrashDentryOperation scan(trace, /*fs_id=*/2000, bucket_ino,
                                [&](const DentryEntry& d) -> bool {
                                  if (d.ino() == file_ino)
                                    still_in_bucket = true;
                                  return true;
                                });
  ASSERT_TRUE(operation_processor->RunAlone(&scan).ok());
  EXPECT_FALSE(still_in_bucket)
      << "file dentry must be gone from bucket after mv-out";
}

TEST_F(TrashFileSystemTest, RenameDirOutOfTrashSucceeds) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam mk;
  mk.parent = kRootIno;
  mk.name = "rename_dir_out";
  mk.mode = 0755;
  mk.uid = 1;
  mk.gid = 1;
  EntryWithPaOut create_out;
  ASSERT_TRUE(fs->MkDir(ctx, mk, create_out).ok());
  const Ino dir_ino = create_out.attr.ino();

  EntryWithPaOut rmdir_out;
  ASSERT_TRUE(
      fs->RmDir(ctx, kRootIno, "rename_dir_out", rmdir_out).ok());

  Trace trace;
  Ino bucket_ino = 0;
  ScanDentryOperation scan_top(trace, /*fs_id=*/2000, kTrashInodeId,
                               [&](const DentryEntry& d) -> bool {
                                 if (bucket_ino == 0) bucket_ino = d.ino();
                                 return true;
                               });
  ASSERT_TRUE(operation_processor->RunAlone(&scan_top).ok());
  ASSERT_GT(bucket_ino, 0u);

  const std::string trash_name =
      BuildTrashEntryName(kRootIno, dir_ino, "rename_dir_out");

  FileSystem::RenameParam rp;
  rp.old_parent = bucket_ino;
  rp.old_name = trash_name;
  rp.new_parent = kRootIno;
  rp.new_name = "recovered_dir";
  FileSystem::RenameResult result;
  auto status = fs->Rename(ctx, rp, result);
  ASSERT_TRUE(status.ok()) << "rename dir out of trash must succeed: "
                           << status.error_str();

  EntryOut lookup;
  ASSERT_TRUE(fs->Lookup(ctx, kRootIno, "recovered_dir", lookup).ok());
  EXPECT_EQ(lookup.attr.ino(), dir_ino);
}

// rm -rf produces "flat-grafted" entries: leaves named `<origP>-<ino>-<name>`
// where origP itself is also a bucket-top entry. Mv-out of such an entry used
// to hang because RenameOperation prefetched sub_trash_key alongside
// old_parent_key (== sub_trash_key when src parent IS the current bucket),
// and dingo-sdk rejects BatchGet with duplicate keys.
TEST_F(TrashFileSystemTest, RenameFlatGraftedFileOutOfTrashSucceeds) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam mk_root;
  mk_root.parent = kRootIno;
  mk_root.name = "graft_root";
  mk_root.mode = 0755;
  mk_root.uid = 1;
  mk_root.gid = 1;
  EntryWithPaOut out_root;
  ASSERT_TRUE(fs->MkDir(ctx, mk_root, out_root).ok());
  const Ino root_dir_ino = out_root.attr.ino();

  FileSystem::MkNodParam mn_leaf;
  mn_leaf.parent = root_dir_ino;
  mn_leaf.name = "inner.txt";
  mn_leaf.mode = 0644;
  mn_leaf.uid = 1;
  mn_leaf.gid = 1;
  mn_leaf.rdev = 0;
  EntryWithPaOut out_leaf;
  ASSERT_TRUE(fs->MkNod(ctx, mn_leaf, out_leaf).ok());
  const Ino leaf_ino = out_leaf.attr.ino();

  EntryWithPaOut leaf_unlink;
  ASSERT_TRUE(fs->UnLink(ctx, root_dir_ino, "inner.txt", leaf_unlink).ok());
  ASSERT_GT(leaf_unlink.attr.parents_size(), 0);
  const Ino bucket_ino = leaf_unlink.attr.parents(0);
  ASSERT_TRUE(IsTrashInode(bucket_ino));

  EntryWithPaOut root_rmdir;
  ASSERT_TRUE(fs->RmDir(ctx, kRootIno, "graft_root", root_rmdir).ok())
      << "rmdir of now-empty parent must succeed";

  // inner.txt's bucket dentry is now flat-grafted: its name encodes
  // origP=root_dir_ino, but root_dir_ino itself is also a bucket-top entry.
  const std::string trash_name =
      BuildTrashEntryName(root_dir_ino, leaf_ino, "inner.txt");

  FileSystem::RenameParam rp;
  rp.old_parent = bucket_ino;
  rp.old_name = trash_name;
  rp.new_parent = kRootIno;
  rp.new_name = "recovered_inner.txt";
  FileSystem::RenameResult result;
  auto status = fs->Rename(ctx, rp, result);
  ASSERT_TRUE(status.ok())
      << "rename flat-grafted file out of trash must succeed: "
      << status.error_str();

  EntryOut lookup;
  ASSERT_TRUE(fs->Lookup(ctx, kRootIno, "recovered_inner.txt", lookup).ok());
  EXPECT_EQ(lookup.attr.ino(), leaf_ino);

  Trace trace;
  bool still_in_bucket = false;
  ScanTrashDentryOperation scan(trace, /*fs_id=*/2000, bucket_ino,
                                [&](const DentryEntry& d) -> bool {
                                  if (d.ino() == leaf_ino)
                                    still_in_bucket = true;
                                  return true;
                                });
  ASSERT_TRUE(operation_processor->RunAlone(&scan).ok());
  EXPECT_FALSE(still_in_bucket)
      << "flat-grafted file dentry must be gone from bucket after mv-out";
}

// `mv <bucket>/<trash-name> <dst>/` (no rename) keeps the trash-style basename
// in the destination. Real-world reproducer from user testing 2026-04-29:
// rename succeeds MDS-side and source dentry is gone from bucket, but Lookup
// in the destination dir returns ENOENT — the new dentry is unreachable.
TEST_F(TrashFileSystemTest,
       RenameOutOfTrashKeepingTrashStyleNameRemainsLookupable) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam mk_dst;
  mk_dst.parent = kRootIno;
  mk_dst.name = "dst_dir";
  mk_dst.mode = 0755;
  mk_dst.uid = 1;
  mk_dst.gid = 1;
  EntryWithPaOut dst_out;
  ASSERT_TRUE(fs->MkDir(ctx, mk_dst, dst_out).ok());
  const Ino dst_ino = dst_out.attr.ino();

  FileSystem::MkNodParam mk_file;
  mk_file.parent = kRootIno;
  mk_file.name = "user_file.txt";
  mk_file.mode = 0644;
  mk_file.uid = 1;
  mk_file.gid = 1;
  mk_file.rdev = 0;
  EntryWithPaOut file_out;
  ASSERT_TRUE(fs->MkNod(ctx, mk_file, file_out).ok());
  const Ino file_ino = file_out.attr.ino();

  EntryWithPaOut unlink_out;
  ASSERT_TRUE(fs->UnLink(ctx, kRootIno, "user_file.txt", unlink_out).ok());
  ASSERT_GT(unlink_out.attr.parents_size(), 0);
  const Ino bucket_ino = unlink_out.attr.parents(0);
  ASSERT_TRUE(IsTrashInode(bucket_ino));

  const std::string trash_name =
      BuildTrashEntryName(kRootIno, file_ino, "user_file.txt");

  FileSystem::RenameParam rp;
  rp.old_parent = bucket_ino;
  rp.old_name = trash_name;
  rp.new_parent = dst_ino;
  rp.new_name = trash_name;  // KEY: same name as source, simulating `mv
                             // .trash/.../X dst/`
  FileSystem::RenameResult result;
  auto status = fs->Rename(ctx, rp, result);
  ASSERT_TRUE(status.ok()) << "rename must succeed: " << status.error_str();

  EntryOut lookup;
  auto lookup_status = fs->Lookup(ctx, dst_ino, trash_name, lookup);
  ASSERT_TRUE(lookup_status.ok())
      << "Lookup of mv-out file by its trash-style name must succeed in "
         "destination dir; got: "
      << lookup_status.error_str();
  EXPECT_EQ(lookup.attr.ino(), file_ino);

  // Even more important: ListDentry must return the new entry. The bug
  // (filesystem.cc:2660 deleting old_name from new_parent's partition cache)
  // surfaces only here, not in Lookup, because Lookup falls through to KV when
  // the shard cache misses. ListDentry instead trusts the in-memory shard.
  std::vector<Dentry> dentries;
  auto list_status = fs->ListDentry(ctx, dst_ino, "", /*limit=*/0,
                                    /*is_only_dir=*/false, dentries);
  ASSERT_TRUE(list_status.ok()) << list_status.error_str();
  bool found = false;
  for (const auto& d : dentries) {
    if (d.INo() == file_ino) {
      found = true;
      EXPECT_EQ(d.Name(), trash_name);
    }
  }
  EXPECT_TRUE(found)
      << "ListDentry on destination dir must include the renamed entry; got "
      << dentries.size() << " dentries";
}

// ============================================================================
// Symlink + Trash interaction tests
//
// JuiceFS-aligned behavior: when trash is enabled, deleted symlinks ALSO go
// through trash (pre-fix they bypassed trash and were physically deleted).
// Trash plumbing is already type-agnostic downstream — these tests pin the
// contract end-to-end.
// ============================================================================

// Single symlink unlink → goes to trash bucket; the inode stays alive with
// parents_ redirected; ReadLink on the inode still resolves the original
// target.
TEST_F(TrashFileSystemTest, SymlinkUnlinkMovesToTrash) {
  auto fs = Fs();
  Context ctx;

  const std::string target = "/some/where/foo.txt";
  EntryWithPaOut create_out;
  auto status =
      fs->Symlink(ctx, target, kRootIno, "lnk_unlink", 1, 1, create_out);
  ASSERT_TRUE(status.ok()) << "create symlink fail: " << status.error_str();
  const Ino sym_ino = create_out.attr.ino();
  ASSERT_GT(sym_ino, 0u);
  ASSERT_EQ(create_out.attr.type(), pb::mds::FileType::SYM_LINK);

  EntryWithPaOut unlink_out;
  status = fs->UnLink(ctx, kRootIno, "lnk_unlink", unlink_out);
  ASSERT_TRUE(status.ok()) << "unlink symlink fail: " << status.error_str();

  // Original parent no longer has the dentry.
  EntryOut lookup_out;
  status = fs->Lookup(ctx, kRootIno, "lnk_unlink", lookup_out);
  EXPECT_FALSE(status.ok()) << "symlink dentry should be gone from root";

  // Inode survives, parents_ now points at hour bucket.
  ASSERT_GT(unlink_out.attr.parents_size(), 0);
  const Ino bucket_ino = unlink_out.attr.parents(0);
  EXPECT_TRUE(IsTrashInode(bucket_ino))
      << "symlink parent must be redirected to trash bucket";

  // Trash bucket carries a dentry pointing at the symlink ino, with type
  // SYM_LINK preserved and the encoded trash entry name.
  auto entries = ScanAllTrashEntries(/*fs_id=*/2000, operation_processor);
  bool found = false;
  for (const auto& d : entries) {
    if (d.ino() != sym_ino) continue;
    found = true;
    EXPECT_EQ(d.type(), pb::mds::FileType::SYM_LINK);
    EXPECT_EQ(d.name(), BuildTrashEntryName(kRootIno, sym_ino, "lnk_unlink"));
    break;
  }
  EXPECT_TRUE(found) << "symlink trash dentry not found";

  // ReadLink reads target from the inode KV, so it should still work even
  // though the dentry now lives in the bucket.
  std::string got_target;
  status = fs->ReadLink(ctx, sym_ino, got_target);
  ASSERT_TRUE(status.ok()) << "readlink on trashed symlink: "
                           << status.error_str();
  EXPECT_EQ(got_target, target);
}

// Batch unlink containing only symlinks must route the whole batch through
// trash (pre-fix the any_non_symlink gate kept symlink-only batches out of
// trash entirely).
TEST_F(TrashFileSystemTest, BatchUnlinkSymlinksOnlyMovesToTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam mkdir;
  mkdir.parent = kRootIno;
  mkdir.name = "lnk_batch_dir";
  mkdir.mode = 0755;
  mkdir.uid = 1;
  mkdir.gid = 1;
  EntryWithPaOut dir_out;
  ASSERT_TRUE(fs->MkDir(ctx, mkdir, dir_out).ok());
  const Ino dir_ino = dir_out.attr.ino();

  std::vector<std::string> names = {"lnk_batch_a", "lnk_batch_b"};
  std::vector<Ino> sym_inos;
  for (const auto& n : names) {
    EntryWithPaOut out;
    ASSERT_TRUE(
        fs->Symlink(ctx, fmt::format("/target/{}", n), dir_ino, n, 1, 1, out)
            .ok());
    sym_inos.push_back(out.attr.ino());
  }

  EntriesWithPaOut batch_out;
  ASSERT_TRUE(fs->BatchUnLink(ctx, dir_ino, names, batch_out).ok());

  auto entries = ScanAllTrashEntries(/*fs_id=*/2000, operation_processor);
  size_t hits = 0;
  for (const auto& d : entries) {
    for (Ino s : sym_inos) {
      if (d.ino() != s) continue;
      ++hits;
      EXPECT_EQ(d.type(), pb::mds::FileType::SYM_LINK);
    }
  }
  EXPECT_EQ(hits, 2u) << "both symlinks must have a trash dentry";
}

// Mixed batch (file + symlink) must put both into trash.
TEST_F(TrashFileSystemTest, BatchUnlinkMixedFileSymlinkAllInTrash) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam mkdir;
  mkdir.parent = kRootIno;
  mkdir.name = "lnk_mix_dir";
  mkdir.mode = 0755;
  mkdir.uid = 1;
  mkdir.gid = 1;
  EntryWithPaOut dir_out;
  ASSERT_TRUE(fs->MkDir(ctx, mkdir, dir_out).ok());
  const Ino dir_ino = dir_out.attr.ino();

  FileSystem::MkNodParam mn;
  mn.parent = dir_ino;
  mn.name = "mix_file.txt";
  mn.mode = 0644;
  mn.uid = 1;
  mn.gid = 1;
  mn.rdev = 0;
  EntryWithPaOut file_out;
  ASSERT_TRUE(fs->MkNod(ctx, mn, file_out).ok());
  const Ino file_ino = file_out.attr.ino();

  EntryWithPaOut sym_out;
  ASSERT_TRUE(
      fs->Symlink(ctx, "/target/mix", dir_ino, "mix_lnk", 1, 1, sym_out).ok());
  const Ino sym_ino = sym_out.attr.ino();

  std::vector<std::string> names = {"mix_file.txt", "mix_lnk"};
  EntriesWithPaOut batch_out;
  ASSERT_TRUE(fs->BatchUnLink(ctx, dir_ino, names, batch_out).ok());

  auto entries = ScanAllTrashEntries(/*fs_id=*/2000, operation_processor);
  bool saw_file = false, saw_sym = false;
  for (const auto& d : entries) {
    if (d.ino() == file_ino) {
      saw_file = true;
      EXPECT_EQ(d.type(), pb::mds::FileType::FILE);
    }
    if (d.ino() == sym_ino) {
      saw_sym = true;
      EXPECT_EQ(d.type(), pb::mds::FileType::SYM_LINK);
    }
  }
  EXPECT_TRUE(saw_file);
  EXPECT_TRUE(saw_sym);
}

// Restore (mv-out) of a trashed symlink must put it back at the original path
// with type SYM_LINK and the original target intact.
TEST_F(TrashFileSystemTest, RestoreSymlinkFromTrashKeepsTarget) {
  auto fs = Fs();
  Context ctx;

  const std::string target = "/restore/target/x";
  EntryWithPaOut create_out;
  ASSERT_TRUE(
      fs->Symlink(ctx, target, kRootIno, "lnk_restore", 1, 1, create_out).ok());
  const Ino sym_ino = create_out.attr.ino();

  EntryWithPaOut unlink_out;
  ASSERT_TRUE(fs->UnLink(ctx, kRootIno, "lnk_restore", unlink_out).ok());
  ASSERT_GT(unlink_out.attr.parents_size(), 0);
  const Ino bucket_ino = unlink_out.attr.parents(0);

  const std::string trash_name =
      BuildTrashEntryName(kRootIno, sym_ino, "lnk_restore");
  ASSERT_TRUE(fs->RestoreFromTrash(ctx, bucket_ino, trash_name).ok());

  EntryOut lookup;
  ASSERT_TRUE(fs->Lookup(ctx, kRootIno, "lnk_restore", lookup).ok());
  EXPECT_EQ(lookup.attr.ino(), sym_ino);
  EXPECT_EQ(lookup.attr.type(), pb::mds::FileType::SYM_LINK);

  std::string got;
  ASSERT_TRUE(fs->ReadLink(ctx, sym_ino, got).ok());
  EXPECT_EQ(got, target) << "restore must preserve symlink target";
}

// Restore collision: original path is taken by a fresh entry → EEXISTED, the
// incumbent dentry is untouched. Same semantics as the file version.
TEST_F(TrashFileSystemTest, RestoreSymlinkCollisionEExisted) {
  auto fs = Fs();
  Context ctx;

  EntryWithPaOut first;
  ASSERT_TRUE(
      fs->Symlink(ctx, "/orig", kRootIno, "lnk_collide", 1, 1, first).ok());
  const Ino first_ino = first.attr.ino();

  EntryWithPaOut unlink_out;
  ASSERT_TRUE(fs->UnLink(ctx, kRootIno, "lnk_collide", unlink_out).ok());
  ASSERT_GT(unlink_out.attr.parents_size(), 0);
  const Ino bucket_ino = unlink_out.attr.parents(0);

  // Re-create at the same name — different inode, different target.
  EntryWithPaOut second;
  ASSERT_TRUE(
      fs->Symlink(ctx, "/replaced", kRootIno, "lnk_collide", 1, 1, second)
          .ok());
  ASSERT_NE(second.attr.ino(), first_ino);

  const std::string trash_name =
      BuildTrashEntryName(kRootIno, first_ino, "lnk_collide");
  auto status = fs->RestoreFromTrash(ctx, bucket_ino, trash_name);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::EEXISTED);

  EntryOut look;
  ASSERT_TRUE(fs->Lookup(ctx, kRootIno, "lnk_collide", look).ok());
  EXPECT_EQ(look.attr.ino(), second.attr.ino())
      << "incumbent symlink must remain";
}

// Auto-expire path: BatchTrashUnlinkOperation (run by GC against an hour
// bucket) must drop the symlink dentry, decrement nlink to 0, and write a
// DelFile entry — exactly like a regular file. CleanDelFileTask is not
// invoked here (it needs a data accessor we don't have in this fixture); for
// a symlink that path is a no-op anyway since GetChunks returns empty.
TEST_F(TrashFileSystemTest, SymlinkInTrashAutoExpireCleansInode) {
  auto fs = Fs();
  Context ctx;

  EntryWithPaOut sym_out;
  ASSERT_TRUE(
      fs->Symlink(ctx, "/expire/target", kRootIno, "lnk_expire", 1, 1, sym_out)
          .ok());
  const Ino sym_ino = sym_out.attr.ino();

  EntryWithPaOut unlink_out;
  ASSERT_TRUE(fs->UnLink(ctx, kRootIno, "lnk_expire", unlink_out).ok());
  ASSERT_GT(unlink_out.attr.parents_size(), 0);
  const Ino bucket_ino = unlink_out.attr.parents(0);

  // Locate the trash dentry under the bucket.
  Trace trace_scan;
  std::vector<DentryEntry> bucket_entries;
  ScanTrashDentryOperation scan(trace_scan, /*fs_id=*/2000, bucket_ino,
                                [&](const DentryEntry& d) -> bool {
                                  if (d.ino() == sym_ino)
                                    bucket_entries.push_back(d);
                                  return true;
                                });
  ASSERT_TRUE(operation_processor->RunAlone(&scan).ok());
  ASSERT_EQ(bucket_entries.size(), 1u);

  // Run the GC unlink directly. nlink hits 0 → DelFile written; bucket dentry
  // gone.
  std::vector<Dentry> dentries{Dentry(bucket_entries[0])};
  Trace trace_gc;
  BatchTrashUnlinkOperation gc_op(trace_gc, dentries);
  ASSERT_TRUE(operation_processor->RunAlone(&gc_op).ok());
  ASSERT_EQ(gc_op.GetResult().child_attrs.size(), 1u);
  EXPECT_EQ(gc_op.GetResult().child_attrs[0].nlink(), 0u);

  std::string del_value;
  auto del_status = kv_storage->Get(
      MetaCodec::EncodeDelFileKey(/*fs_id=*/2000, sym_ino), del_value);
  EXPECT_TRUE(del_status.ok())
      << "symlink inode must be queued in DelFile after GC";

  std::vector<DentryEntry> remaining;
  Trace trace_recheck;
  ScanTrashDentryOperation rescan(trace_recheck, /*fs_id=*/2000, bucket_ino,
                                  [&](const DentryEntry& d) -> bool {
                                    if (d.ino() == sym_ino)
                                      remaining.push_back(d);
                                    return true;
                                  });
  ASSERT_TRUE(operation_processor->RunAlone(&rescan).ok());
  EXPECT_TRUE(remaining.empty())
      << "symlink trash dentry must be gone after GC";
}

// --- Bucket-dentry codec regression tests ---
//
// Sub-trash bucket dentries are encoded as plain Dentry, the same as tree
// dentries — readers no longer need a parent-based codec switch. Tests below
// pin the round-trip via the public read paths so any regression that mixes
// up codecs gets caught early. Older builds wrapped these values in a
// TrashDentry message that has since been removed.
// GetInodeAttrOperation.
//
// Helper: pull a trash-bucket parent ino from a fresh unlink so each test
// stays self-contained (other tests in this fixture also populate buckets).
static Ino UnlinkAndCaptureBucket(const FileSystemSPtr& fs, Context& ctx,
                                  const std::string& name, Ino& out_file_ino) {
  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = name;
  param.mode = 0644;
  param.uid = 1;
  param.gid = 1;
  param.rdev = 0;
  EntryWithPaOut create_out;
  EXPECT_TRUE(fs->MkNod(ctx, param, create_out).ok());
  out_file_ino = create_out.attr.ino();

  EntryWithPaOut unlink_out;
  EXPECT_TRUE(fs->UnLink(ctx, kRootIno, name, unlink_out).ok());
  EXPECT_GT(unlink_out.attr.parents_size(), 0);
  return unlink_out.attr.parents(0);
}

TEST_F(TrashFileSystemTest, ScanDirShardOperationDecodesBucketEntry) {
  auto fs = Fs();
  Context ctx;

  Ino file_ino = 0;
  const std::string original_name = "scan_shard_canary.txt";
  Ino bucket_ino = UnlinkAndCaptureBucket(fs, ctx, original_name, file_ino);
  ASSERT_TRUE(IsTrashBucketChild(bucket_ino));

  const std::string trash_name =
      BuildTrashEntryName(kRootIno, file_ino, original_name);

  std::vector<DentryEntry> seen;
  Trace trace;
  ScanDirShardOperation op(trace, /*fs_id=*/2000, bucket_ino, Range{},
                           [&](const DentryEntry& d) -> bool {
                             seen.push_back(d);
                             return true;
                           });
  ASSERT_TRUE(operation_processor->RunAlone(&op).ok());

  bool found = false;
  for (const auto& d : seen) {
    if (d.name() == trash_name) {
      EXPECT_EQ(d.ino(), file_ino)
          << "bucket scan must surface the wrapped TrashDentry's inner ino";
      found = true;
    }
  }
  EXPECT_TRUE(found) << "trash entry " << trash_name
                     << " missing from bucket scan";
}

TEST_F(TrashFileSystemTest, GetDentryOperationDecodesBucketEntry) {
  auto fs = Fs();
  Context ctx;

  Ino file_ino = 0;
  const std::string original_name = "get_dentry_canary.txt";
  Ino bucket_ino = UnlinkAndCaptureBucket(fs, ctx, original_name, file_ino);
  ASSERT_TRUE(IsTrashBucketChild(bucket_ino));

  const std::string trash_name =
      BuildTrashEntryName(kRootIno, file_ino, original_name);

  Trace trace;
  GetDentryOperation op(trace, /*fs_id=*/2000, bucket_ino, trash_name);
  ASSERT_TRUE(operation_processor->RunAlone(&op).ok());
  const auto& result = op.GetResult();
  EXPECT_EQ(result.dentry.ino(), file_ino);
  EXPECT_EQ(result.dentry.name(), trash_name);
}

TEST_F(TrashFileSystemTest, ScanDentryOperationDecodesBucketEntry) {
  auto fs = Fs();
  Context ctx;

  Ino file_ino = 0;
  const std::string original_name = "scan_dentry_canary.txt";
  Ino bucket_ino = UnlinkAndCaptureBucket(fs, ctx, original_name, file_ino);
  ASSERT_TRUE(IsTrashBucketChild(bucket_ino));

  const std::string trash_name =
      BuildTrashEntryName(kRootIno, file_ino, original_name);

  Trace trace;
  Ino seen_ino = 0;
  std::string seen_name;
  ScanDentryOperation op(trace, /*fs_id=*/2000, bucket_ino,
                         [&](const DentryEntry& d) -> bool {
                           if (d.name() == trash_name) {
                             seen_ino = d.ino();
                             seen_name = d.name();
                             return false;
                           }
                           return true;
                         });
  ASSERT_TRUE(operation_processor->RunAlone(&op).ok());
  EXPECT_EQ(seen_name, trash_name)
      << "trash entry " << trash_name << " missing from bucket scan";
  EXPECT_EQ(seen_ino, file_ino);
}

// ============================================================================
// Trash x dir-stats: a fixture with BOTH trash and per-directory usage stats
// enabled, to verify dir-stat accounting across trash-move and restore.
// ============================================================================
class TrashDirStatTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    kv_storage = DummyStorage::New();
    ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

    const uint32_t kFsId = 3000;
    auto ino_id_generator = NewInodeIdGenerator(kFsId, kv_storage);
    ASSERT_TRUE(ino_id_generator->Init()) << "init inode id generator fail.";
    auto slice_id_generator = NewSliceIdGenerator(kv_storage);
    ASSERT_TRUE(slice_id_generator->Init()) << "init slice id generator fail.";

    operation_processor = OperationProcessor::New(kv_storage);
    ASSERT_TRUE(operation_processor->Init())
        << "init operation processor fail.";

    quota_worker_set = SimpleWorkerSet::New(
        "trash_ds_quota", 1, 1024, false, /*is_inplace_run=*/true);
    ASSERT_TRUE(quota_worker_set->Init()) << "init quota worker set fail.";

    pb::mds::FsInfo fs_info = CreateFsInfoWithTrash(
        kFsId, "trash_ds_fs", /*trash_days=*/7, /*enable_dir_stats=*/true);

    fs = FileSystem::New(kTrashMdsId, FsInfo::New(fs_info),
                         std::move(ino_id_generator), slice_id_generator,
                         kv_storage, operation_processor, nullptr,
                         quota_worker_set, quota_worker_set, nullptr);
    ASSERT_TRUE(fs->CreateRoot().ok());
  }

  // NOTE: quota_worker_set and operation_processor own real background
  // worker threads. Without an explicit Destroy() here, those threads
  // outlive the test suite and block process exit (and gcov's coverage
  // flush, which only runs on normal exit) at shutdown -- mirror
  // TrashFileSystemTest's teardown above.
  static void TearDownTestSuite() {
    fs = nullptr;

    if (quota_worker_set != nullptr) {
      quota_worker_set->Destroy();
      quota_worker_set = nullptr;
    }

    if (operation_processor != nullptr) {
      operation_processor->Destroy();
      operation_processor = nullptr;
    }

    kv_storage = nullptr;
  }

 public:
  static FileSystemSPtr fs;
  static KVStorageSPtr kv_storage;
  static OperationProcessorSPtr operation_processor;
  static WorkerSetSPtr quota_worker_set;
  static FileSystemSPtr Fs() { return fs; }
};

FileSystemSPtr TrashDirStatTest::fs = nullptr;
KVStorageSPtr TrashDirStatTest::kv_storage = nullptr;
OperationProcessorSPtr TrashDirStatTest::operation_processor = nullptr;
WorkerSetSPtr TrashDirStatTest::quota_worker_set = nullptr;

// Regression: tree-rebuild grafts a child back onto a (trashed) user directory
// with allow_trash_parent=true. The grafted-into directory is a real user dir
// (normal-range inode), so its dir-stat MUST be credited -- otherwise a later
// put_back of the directory carries the child along without ever accounting for
// it, leaving the restored directory's stat under-counted. The credit must be
// gated on "dst is not a trash bucket", not on allow_trash_parent.
TEST_F(TrashDirStatTest, TreeRebuildGraftCreditsGraftedIntoDirStat) {
  auto fs = Fs();
  ASSERT_TRUE(fs->EnableDirStats());
  Context ctx;

  // mkdir /sub
  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "sub";
  dp.mode = 0755;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  ASSERT_TRUE(fs->MkDir(ctx, dp, dout).ok());
  Ino sub_ino = dout.attr.ino();

  // create /sub/child and give it length 5000.
  FileSystem::MkNodParam fp;
  fp.parent = sub_ino;
  fp.name = "child";
  fp.mode = 0644;
  fp.uid = 1;
  fp.gid = 1;
  fp.rdev = 0;
  EntryWithPaOut fo;
  ASSERT_TRUE(fs->MkNod(ctx, fp, fo).ok());
  Ino child_ino = fo.attr.ino();
  {
    FileSystem::FlushFileParam ffp;
    ffp.length = 5000;
    EntryWithFileChangeOut ffo;
    ASSERT_TRUE(fs->FlushFile(ctx, child_ino, ffp, ffo).ok());
  }

  // unlink child (-> trash, debits sub), then rmdir sub (-> trash). sub is now
  // a trashed user directory; child is a flat entry in the same hour bucket.
  EntryWithPaOut uo;
  ASSERT_TRUE(fs->UnLink(ctx, sub_ino, "child", uo).ok());
  Ino sub_trash_ino = uo.attr.parents(0);
  EntryWithPaOut ro;
  ASSERT_TRUE(fs->RmDir(ctx, kRootIno, "sub", ro).ok());

  // Drain accumulated deltas so the graft's delta is isolated.
  DrainDirStats(fs->GetDirStatManager());

  // Tree-rebuild: graft child back onto the trashed sub.
  std::string child_trash_name =
      BuildTrashEntryName(sub_ino, child_ino, "child");
  auto status = fs->RestoreFromTrash(ctx, sub_trash_ino, child_trash_name,
                                     /*allow_trash_parent=*/true);
  ASSERT_TRUE(status.ok()) << "tree-rebuild graft fail: " << status.error_str();

  // The graft must credit sub: +1 inode, +5000 length.
  auto deltas = DrainDirStats(fs->GetDirStatManager());
  ASSERT_TRUE(deltas.count(sub_ino))
      << "graft produced no dir-stat delta for the grafted-into directory";
  EXPECT_EQ(deltas[sub_ino].inodes, 1);
  EXPECT_EQ(deltas[sub_ino].length, 5000);
}

// Regression: with immediate_trash_quota, per-dir QUOTA follows the same
// symmetric attach/detach model as dir-stat. A tree-rebuild graft attaches
// the entry onto a trashed user directory, so that directory's quota must be
// credited right there -- the ancestor walk truncates at the trash-range
// boundary (IsTrashInode is a reserved-range check, the trashed dir itself is
// a normal ino), so the credit settles exactly the rebuilt subtree's interior.
// Without the graft credit, a later put_back carries the child along with
// only the directory's own (0,+1) leg and the child's trash-move debit is
// never reversed (the two-phase-restore quota-loss bug).
TEST_F(TrashDirStatTest, TreeRebuildGraftCreditsInteriorQuota) {
  auto fs = Fs();
  Context ctx;
  auto& quota_mgr = fs->GetQuotaManager();

  auto set_quota = [&](Ino ino) {
    Trace trace;
    QuotaEntry entry;
    entry.set_max_bytes(int64_t{1} << 40);
    entry.set_max_inodes(int64_t{1} << 20);
    // is_lead=false: local UpsertQuota only, no store write / buddy notify.
    ASSERT_TRUE(quota_mgr.SetDirQuota(trace, ino, entry, false).ok());
  };
  auto usage = [&](Ino ino, int64_t& bytes, int64_t& inodes) {
    Trace trace;
    QuotaEntry entry;
    ASSERT_TRUE(
        quota_mgr.GetDirQuota(trace, ino, /*not_use_fs_quota=*/true, entry)
            .ok());
    bytes = entry.used_bytes();
    inodes = entry.used_inodes();
  };

  // Quotas on root and (once created) qsub, BEFORE any charged activity.
  set_quota(kRootIno);

  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "qsub";
  dp.mode = 0755;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  ASSERT_TRUE(fs->MkDir(ctx, dp, dout).ok());
  Ino sub_ino = dout.attr.ino();
  set_quota(sub_ino);

  FileSystem::MkNodParam fp;
  fp.parent = sub_ino;
  fp.name = "qchild";
  fp.mode = 0644;
  fp.uid = 1;
  fp.gid = 1;
  fp.rdev = 0;
  EntryWithPaOut fo;
  ASSERT_TRUE(fs->MkNod(ctx, fp, fo).ok());
  Ino child_ino = fo.attr.ino();
  {
    FileSystem::FlushFileParam ffp;
    ffp.length = 5000;
    EntryWithFileChangeOut ffo;
    ASSERT_TRUE(fs->FlushFile(ctx, child_ino, ffp, ffo).ok());
  }

  int64_t bytes = 0, inodes = 0;
  usage(sub_ino, bytes, inodes);
  ASSERT_EQ(bytes, 5000);
  ASSERT_EQ(inodes, 1);
  usage(kRootIno, bytes, inodes);
  ASSERT_EQ(bytes, 5000);
  ASSERT_EQ(inodes, 2);  // qsub + qchild

  // Trash-move both: unlink qchild (debits sub chain), rmdir qsub (debits
  // root chain).
  EntryWithPaOut uo;
  ASSERT_TRUE(fs->UnLink(ctx, sub_ino, "qchild", uo).ok());
  Ino bucket_ino = uo.attr.parents(0);
  EntryWithPaOut ro;
  ASSERT_TRUE(fs->RmDir(ctx, kRootIno, "qsub", ro).ok());

  usage(sub_ino, bytes, inodes);
  ASSERT_EQ(bytes, 0);
  ASSERT_EQ(inodes, 0);
  usage(kRootIno, bytes, inodes);
  ASSERT_EQ(bytes, 0);
  ASSERT_EQ(inodes, 0);

  // Simulate another MDS's stale parent memo: under parent-hash partitioning
  // the rmdir-to-trash is processed by the parent's owner (which Forgets),
  // while qsub's own owner still remembers qsub -> root from before the
  // trash-move. The graft credit must not trust the memo -- it resolves
  // ancestry from the store (bypass_parent_memo), where qsub's parents are
  // already [bucket], keeping the boundary truncation deterministic.
  fs->GetParentMemo().Remeber(sub_ino, kRootIno);

  // Tree-rebuild: graft qchild back onto the trashed qsub. The graft must
  // credit qsub's own quota (+5000,+1) and NOT leak past the trash boundary
  // to root.
  std::string child_trash_name =
      BuildTrashEntryName(sub_ino, child_ino, "qchild");
  auto status = fs->RestoreFromTrash(ctx, bucket_ino, child_trash_name,
                                     /*allow_trash_parent=*/true);
  ASSERT_TRUE(status.ok()) << "tree-rebuild graft fail: " << status.error_str();

  usage(sub_ino, bytes, inodes);
  EXPECT_EQ(bytes, 5000) << "graft must credit the grafted-into dir's quota";
  EXPECT_EQ(inodes, 1) << "graft must credit the grafted-into dir's quota";
  usage(kRootIno, bytes, inodes);
  EXPECT_EQ(bytes, 0) << "graft credit must truncate at the trash boundary";
  EXPECT_EQ(inodes, 0) << "graft credit must truncate at the trash boundary";

  // put_back qsub: settles exactly one hop into the live chain -- root gets
  // the dir's own (0,+1); the carried child stays settled inside qsub (no
  // double credit). Root's missing (5000,+1) for the carried child is the
  // documented ancestor residual, owned by the restore tool's reconcile step.
  // qsub's trash entry lives in the same hour bucket as qchild's (both moved
  // within this test run); look it up from the inode to stay robust.
  Trace trace;
  GetInodeAttrOperation attr_op(trace, /*fs_id=*/3000, sub_ino);
  ASSERT_TRUE(operation_processor->RunAlone(&attr_op).ok());
  Ino sub_bucket_ino =
      attr_op.GetResult().attr_with_mutation.attr.parents(0);
  std::string sub_trash_name = BuildTrashEntryName(kRootIno, sub_ino, "qsub");
  status = fs->RestoreFromTrash(ctx, sub_bucket_ino, sub_trash_name);
  ASSERT_TRUE(status.ok()) << "put_back fail: " << status.error_str();

  usage(sub_ino, bytes, inodes);
  EXPECT_EQ(bytes, 5000) << "put_back must not re-credit the carried child";
  EXPECT_EQ(inodes, 1) << "put_back must not re-credit the carried child";
  usage(kRootIno, bytes, inodes);
  EXPECT_EQ(bytes, 0);
  EXPECT_EQ(inodes, 1) << "put_back settles the dir's own (0,+1) on root";
}

// Carried settlement: a directory put back after tree-rebuild rides its
// grafted subtree out of trash with no restore leg for the children. The
// restore tool measures the assembled subtree and passes carried totals; the
// put_back credit must be enlarged by exactly that amount -- and ONLY on the
// put_back-of-directory leg: graft legs and file legs must ignore carried.
TEST_F(TrashDirStatTest, PutBackConsumesCarried) {
  auto fs = Fs();
  Context ctx;
  auto& quota_mgr = fs->GetQuotaManager();

  auto set_quota = [&](Ino ino) {
    Trace trace;
    QuotaEntry entry;
    entry.set_max_bytes(int64_t{1} << 40);
    entry.set_max_inodes(int64_t{1} << 20);
    ASSERT_TRUE(quota_mgr.SetDirQuota(trace, ino, entry, false).ok());
  };
  auto usage = [&](Ino ino, int64_t& bytes, int64_t& inodes) {
    Trace trace;
    QuotaEntry entry;
    ASSERT_TRUE(
        quota_mgr.GetDirQuota(trace, ino, /*not_use_fs_quota=*/true, entry)
            .ok());
    bytes = entry.used_bytes();
    inodes = entry.used_inodes();
  };
  auto mkdir = [&](Ino parent, const std::string& name) -> Ino {
    FileSystem::MkDirParam dp;
    dp.parent = parent;
    dp.name = name;
    dp.mode = 0755;
    dp.uid = 1;
    dp.gid = 1;
    dp.rdev = 0;
    EntryWithPaOut out;
    EXPECT_TRUE(fs->MkDir(ctx, dp, out).ok());
    return out.attr.ino();
  };
  auto mknod = [&](Ino parent, const std::string& name,
                   uint64_t length) -> Ino {
    FileSystem::MkNodParam fp;
    fp.parent = parent;
    fp.name = name;
    fp.mode = 0644;
    fp.uid = 1;
    fp.gid = 1;
    fp.rdev = 0;
    EntryWithPaOut out;
    EXPECT_TRUE(fs->MkNod(ctx, fp, out).ok());
    if (length > 0) {
      FileSystem::FlushFileParam ffp;
      ffp.length = length;
      EntryWithFileChangeOut ffo;
      EXPECT_TRUE(fs->FlushFile(ctx, out.attr.ino(), ffp, ffo).ok());
    }
    return out.attr.ino();
  };

  // Quota on qc_top only (the reported production shape); qc_sub has none.
  Ino top_ino = mkdir(kRootIno, "qc_top");
  set_quota(top_ino);
  Ino sub_ino = mkdir(top_ino, "qc_sub");
  Ino child_ino = mknod(sub_ino, "qc_child", 5000);

  int64_t bytes = 0, inodes = 0;
  usage(top_ino, bytes, inodes);
  ASSERT_EQ(bytes, 5000);
  ASSERT_EQ(inodes, 2);  // qc_sub + qc_child

  // Trash-move the whole content.
  EntryWithPaOut uo;
  ASSERT_TRUE(fs->UnLink(ctx, sub_ino, "qc_child", uo).ok());
  Ino bucket_ino = uo.attr.parents(0);
  EntryWithPaOut ro;
  ASSERT_TRUE(fs->RmDir(ctx, top_ino, "qc_sub", ro).ok());
  usage(top_ino, bytes, inodes);
  ASSERT_EQ(bytes, 0);
  ASSERT_EQ(inodes, 0);

  // Tree-rebuild graft with a bogus carried: MUST be ignored on graft legs.
  std::string child_trash_name =
      BuildTrashEntryName(sub_ino, child_ino, "qc_child");
  ASSERT_TRUE(fs->RestoreFromTrash(ctx, bucket_ino, child_trash_name,
                                   /*allow_trash_parent=*/true,
                                   /*carried_bytes=*/999,
                                   /*carried_inodes=*/9)
                  .ok());
  usage(top_ino, bytes, inodes);
  EXPECT_EQ(bytes, 0) << "graft leg must ignore carried";
  EXPECT_EQ(inodes, 0) << "graft leg must ignore carried";

  // put_back qc_sub with the carried totals of its assembled subtree
  // (qc_child: 5000 bytes, 1 entry): qc_top gets carried + the dir's own
  // (0,+1) in one credit, restoring its full baseline.
  Trace trace;
  GetInodeAttrOperation attr_op(trace, /*fs_id=*/3000, sub_ino);
  ASSERT_TRUE(operation_processor->RunAlone(&attr_op).ok());
  Ino sub_bucket_ino = attr_op.GetResult().attr_with_mutation.attr.parents(0);
  std::string sub_trash_name =
      BuildTrashEntryName(top_ino, sub_ino, "qc_sub");
  ASSERT_TRUE(fs->RestoreFromTrash(ctx, sub_bucket_ino, sub_trash_name,
                                   /*allow_trash_parent=*/false,
                                   /*carried_bytes=*/5000,
                                   /*carried_inodes=*/1)
                  .ok());
  usage(top_ino, bytes, inodes);
  EXPECT_EQ(bytes, 5000) << "put_back must settle carried on the live chain";
  EXPECT_EQ(inodes, 2) << "put_back must settle carried + the dir's own +1";

  // FILE put_back with a bogus carried: MUST be ignored. qc_f is a flat
  // bucket entry; its credit is exactly (length, +1).
  Ino f_ino = mknod(top_ino, "qc_f", 700);
  EntryWithPaOut uo2;
  ASSERT_TRUE(fs->UnLink(ctx, top_ino, "qc_f", uo2).ok());
  Ino f_bucket_ino = uo2.attr.parents(0);
  usage(top_ino, bytes, inodes);
  ASSERT_EQ(bytes, 5000);
  ASSERT_EQ(inodes, 2);
  std::string f_trash_name = BuildTrashEntryName(top_ino, f_ino, "qc_f");
  ASSERT_TRUE(fs->RestoreFromTrash(ctx, f_bucket_ino, f_trash_name,
                                   /*allow_trash_parent=*/false,
                                   /*carried_bytes=*/777,
                                   /*carried_inodes=*/7)
                  .ok());
  usage(top_ino, bytes, inodes);
  EXPECT_EQ(bytes, 5700) << "file leg must ignore carried (credit = length)";
  EXPECT_EQ(inodes, 3) << "file leg must ignore carried (credit = +1)";
}

// Regression: ScanDirShardOperation's missing-inode guard (added to surface
// dirs deleted underfoot) must exempt the virtual trash root. Server-side
// Lookup(kTrashInodeId, <hour bucket>) is served by a ShardPartition whose
// shard scan finds the bucket dentries but no inode key -- kTrashInodeId has
// no inode KV record by design. This is exactly how the mds-cli RestoreTrash
// entry path resolves the hour bucket; without the exemption it fails with
// ENOT_FOUND "dir inode not found(kTrashInodeId)".
TEST_F(TrashFileSystemTest, LookupHourBucketUnderTrashRoot) {
  auto fs = Fs();
  Context ctx;

  // Trash a probe file so the current hour bucket exists.
  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "bucket_lookup_probe.txt";
  param.mode = 0644;
  param.uid = 1;
  param.gid = 1;
  param.rdev = 0;
  EntryWithPaOut create_out;
  ASSERT_TRUE(fs->MkNod(ctx, param, create_out).ok());
  EntryWithPaOut unlink_out;
  ASSERT_TRUE(
      fs->UnLink(ctx, kRootIno, "bucket_lookup_probe.txt", unlink_out).ok());
  ASSERT_GT(unlink_out.attr.parents_size(), 0);
  Ino bucket_ino = unlink_out.attr.parents(0);

  // Find the bucket's name among the trash root's dentries.
  Trace trace;
  std::string bucket_name;
  ScanDentryOperation scan_op(trace, /*fs_id=*/2000, kTrashInodeId,
                              [&](const DentryEntry& d) -> bool {
                                if (d.ino() == bucket_ino) {
                                  bucket_name = d.name();
                                  return false;
                                }
                                return true;
                              });
  ASSERT_TRUE(operation_processor->RunAlone(&scan_op).ok());
  ASSERT_FALSE(bucket_name.empty()) << "hour bucket dentry not found";

  // Server-side lookup of the bucket under the virtual trash root.
  EntryOut lookup_out;
  auto status = fs->Lookup(ctx, kTrashInodeId, bucket_name, lookup_out);
  ASSERT_TRUE(status.ok()) << "lookup .trash/" << bucket_name
                           << " fail: " << status.error_str();
  EXPECT_EQ(lookup_out.attr.ino(), bucket_ino);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
