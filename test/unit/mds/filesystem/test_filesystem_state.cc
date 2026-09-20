// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

// Internal-state tests for FileSystem: after each namespace operation, verify
// that the Inode, ShardPartition and DirShard attribute version vectors
// (AttrVersionVec) advance exactly as expected and stay mutually consistent.
//
// The fixture builds a fresh DummyStorage + FileSystem for every test so that
// versions start from 1 and no state leaks between cases (unlike the shared
// static fs in test_filesystem.cc).
//
// Invariants asserted (see review notes): monotonic version, inode==partition
// when the partition is cached, shard never ahead of its partition, fresh
// inode version, read paths do not touch versions, exact serial deltas, and
// the concurrent mutation path.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "json/value.h"
#include "mds/common/context.h"
#include "mds/common/runnable.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/id_generator.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {
namespace unit_test {

namespace {

constexpr uint32_t kTestFsId = 1000;
constexpr uint64_t kTestMdsId = 1000;
constexpr Ino kTestRootIno = 1;

pb::mds::FsInfo MakeFsInfo() {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_id(kTestFsId);
  fs_info.set_fs_name("test_state_fs");
  fs_info.set_fs_type(pb::mds::FsType::S3);
  fs_info.set_status(pb::mds::FsStatus::NORMAL);
  fs_info.set_block_size(1024 * 1024);
  fs_info.set_chunk_size(1024 * 1024 * 64);
  fs_info.set_enable_dir_stats(false);
  fs_info.set_owner("test");
  fs_info.set_capacity(1024ULL * 1024 * 1024);
  fs_info.set_recycle_time_hour(24);
  auto* s3_info = fs_info.mutable_extra()->mutable_s3_info();
  s3_info->set_ak("ak");
  s3_info->set_sk("sk");
  s3_info->set_endpoint("http://s3.com");
  s3_info->set_bucketname("bucket");

  auto* partition_policy = fs_info.mutable_partition_policy();
  partition_policy->set_type(pb::mds::PartitionType::MONOLITHIC_PARTITION);
  partition_policy->mutable_mono()->set_mds_id(kTestMdsId);

  return fs_info;
}

}  // namespace

class FileSystemStateTest : public testing::Test {
 protected:
  void SetUp() override {
    auto storage = DummyStorage::New();
    ASSERT_TRUE(storage->Init("")) << "init kv storage fail.";
    storage_ = storage;

    auto processor = OperationProcessor::New(storage_);
    ASSERT_TRUE(processor->Init()) << "init operation processor fail.";
    processor_ = processor;

    auto inode_id_generator = NewInodeIdGenerator(kTestFsId, storage_);
    ASSERT_TRUE(inode_id_generator->Init()) << "init inode id generator fail.";

    auto slice_id_generator = NewSliceIdGenerator(storage_);
    ASSERT_TRUE(slice_id_generator->Init()) << "init slice id generator fail.";

    auto worker_set = SimpleWorkerSet::New("fs_state_test", 1, 1024, false,
                                           /*is_inplace_run=*/true);
    ASSERT_TRUE(worker_set->Init()) << "init worker set fail.";
    worker_set_ = worker_set;

    fs_ = FileSystem::New(kTestMdsId, FsInfo::New(MakeFsInfo()),
                          std::move(inode_id_generator), slice_id_generator,
                          processor_, nullptr, nullptr, worker_set_,
                          worker_set_, nullptr);
    ASSERT_TRUE(fs_->CreateRoot().ok()) << "create root fail.";
  }

  void TearDown() override {
    fs_ = nullptr;
    if (processor_ != nullptr) {
      processor_->Stop();
      processor_ = nullptr;
    }
  }

  // --- state accessors ---
  InodeSPtr Inode(Ino ino) { return fs_->GetInodeCache().Get(ino); }
  PartitionPtr Partition(Ino ino) { return fs_->GetPartitionCache().Get(ino); }

  // Assert that the cached inode and its partition carry identical version
  // vectors (base + every delta bucket).
  void ExpectInodePartitionVersionEqual(Ino ino) {
    auto inode = Inode(ino);
    auto partition = Partition(ino);
    ASSERT_TRUE(inode != nullptr) << "inode " << ino << " not cached";
    ASSERT_TRUE(partition != nullptr) << "partition " << ino << " not cached";

    const auto iv = inode->VersionVec();
    const auto& pv = partition->VersionVec();
    EXPECT_EQ(iv.BaseVersion(), pv.BaseVersion()) << "ino=" << ino;
    EXPECT_EQ(iv.CompleteVersion(), pv.CompleteVersion()) << "ino=" << ino;
    ASSERT_EQ(iv.delta_versions.size(), pv.delta_versions.size())
        << "ino=" << ino;
    for (size_t i = 0; i < iv.delta_versions.size(); ++i) {
      EXPECT_EQ(iv.DeltaVersion(static_cast<uint32_t>(i)),
                pv.DeltaVersion(static_cast<uint32_t>(i)))
          << "ino=" << ino << " delta_index=" << i;
    }
  }

  // Load the partition for `dir` (and its shards) through a public read path.
  void LoadPartition(Ino dir) {
    Context ctx;
    std::vector<EntryWithNameOut> entries;
    ASSERT_TRUE(fs_->ReadDir(ctx, dir, "", 100, false, entries).ok())
        << "readdir fail for ino " << dir;
    ASSERT_TRUE(Partition(dir) != nullptr)
        << "partition " << dir << " still not cached";
  }

  // --- operation helpers (each uses its own Context) ---
  Ino MkNod(Ino parent, const std::string& name) {
    Context ctx;
    FileSystem::MkNodParam param;
    param.parent = parent;
    param.name = name;
    param.mode = 0777;
    param.uid = 1;
    param.gid = 1;

    EntryWithPaOut out;
    auto status = fs_->MkNod(ctx, param, out);
    EXPECT_TRUE(status.ok())
        << "mknod " << name << " fail: " << status.error_str();
    return out.attr.ino();
  }

  Ino MkDir(Ino parent, const std::string& name) {
    Context ctx;
    FileSystem::MkDirParam param;
    param.parent = parent;
    param.name = name;
    param.mode = 0777;
    param.uid = 1;
    param.gid = 1;

    EntryWithPaOut out;
    auto status = fs_->MkDir(ctx, param, out);
    EXPECT_TRUE(status.ok())
        << "mkdir " << name << " fail: " << status.error_str();
    return out.attr.ino();
  }

  Ino Symlink(Ino parent, const std::string& name, const std::string& target) {
    Context ctx;
    EntryWithPaOut out;
    auto status = fs_->Symlink(ctx, target, parent, name, 1, 1, out);
    EXPECT_TRUE(status.ok())
        << "symlink " << name << " fail: " << status.error_str();
    return out.attr.ino();
  }

  // --- best-effort, thread-safe operation helpers (no gtest assertions,
  //     callers may invoke them from worker threads) ---
  //
  // The MDS retries store conflicts internally (bounded by
  // FLAGS_mds_txn_max_retry_times); a real client retries the op once that
  // budget is exhausted. Under this test's heavy same-inode contention the
  // budget can run out, so mirror the client and retry ESTORE_MAYBE_RETRY here.
  // Any other error (EEXISTED/ENOT_FOUND/...) is returned as-is.
  template <typename Fn>
  bool RetryOnConflict(Fn&& fn) {
    for (int attempt = 0; attempt < 200; ++attempt) {
      auto status = fn();
      if (status.ok()) return true;
      if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) return false;
      std::this_thread::yield();
    }
    return false;
  }

  bool TryMkNod(Ino parent, const std::string& name) {
    return RetryOnConflict([&] {
      Context ctx;
      FileSystem::MkNodParam param;
      param.parent = parent;
      param.name = name;
      param.mode = 0777;
      param.uid = 1;
      param.gid = 1;
      EntryWithPaOut out;
      return fs_->MkNod(ctx, param, out);
    });
  }

  bool TryMkDir(Ino parent, const std::string& name) {
    return RetryOnConflict([&] {
      Context ctx;
      FileSystem::MkDirParam param;
      param.parent = parent;
      param.name = name;
      param.mode = 0777;
      param.uid = 1;
      param.gid = 1;
      EntryWithPaOut out;
      return fs_->MkDir(ctx, param, out);
    });
  }

  bool TryUnLink(Ino parent, const std::string& name) {
    return RetryOnConflict([&] {
      Context ctx;
      EntryWithPaOut out;
      return fs_->UnLink(ctx, parent, name, out);
    });
  }

  bool TryRmDir(Ino parent, const std::string& name) {
    return RetryOnConflict([&] {
      Context ctx;
      EntryWithPaOut out;
      return fs_->RmDir(ctx, parent, name, out);
    });
  }

  bool TrySetAttrMode(Ino ino, uint32_t mode) {
    return RetryOnConflict([&] {
      Context ctx;
      FileSystem::SetAttrParam param;
      param.to_set = kSetAttrMode;
      param.attr.set_fs_id(kTestFsId);
      param.attr.set_ino(ino);
      param.attr.set_mode(mode);
      EntryWithChunkOut out;
      return fs_->SetAttr(ctx, ino, param, out);
    });
  }

  // --- status-returning helpers (keep the error code for exact checks) ---
  Status LookupRaw(Ino parent, const std::string& name, EntryOut& out) {
    Context ctx;
    return fs_->Lookup(ctx, parent, name, out);
  }

  Status BatchCreateOne(Ino parent, const std::string& name,
                        EntriesWithPaOut& out) {
    Context ctx;
    FileSystem::MkNodParam param;
    param.parent = parent;
    param.name = name;
    param.mode = 0777;
    param.uid = 1;
    param.gid = 1;
    return fs_->BatchCreate(ctx, parent, {param}, out);
  }

  Status MkDirRaw(Ino parent, const std::string& name, EntryWithPaOut& out) {
    Context ctx;
    FileSystem::MkDirParam param;
    param.parent = parent;
    param.name = name;
    param.mode = 0777;
    param.uid = 1;
    param.gid = 1;
    return fs_->MkDir(ctx, param, out);
  }

  Status UnLinkRaw(Ino parent, const std::string& name, EntryWithPaOut& out) {
    Context ctx;
    return fs_->UnLink(ctx, parent, name, out);
  }

  Status RmDirRaw(Ino parent, const std::string& name, EntryWithPaOut& out) {
    Context ctx;
    return fs_->RmDir(ctx, parent, name, out);
  }

  // Read every child name of `dir` through the paged ReadDir path.
  std::set<std::string> SnapshotNames(Ino dir) {
    std::set<std::string> names;
    std::string last;
    while (true) {
      Context ctx;
      std::vector<EntryWithNameOut> entries;
      auto status = fs_->ReadDir(ctx, dir, last, 1000, false, entries);
      if (!status.ok()) break;
      for (const auto& entry : entries) names.insert(entry.name);
      if (entries.size() < 1000) break;
      last = entries.back().name;
    }
    return names;
  }

  KVStorageSPtr storage_;
  OperationProcessorSPtr processor_;
  WorkerSetSPtr worker_set_;
  FileSystemSPtr fs_;
};

// ---------------------------------------------------------------------------
// T1: version/state invariants for namespace write operations
// ---------------------------------------------------------------------------

TEST_F(FileSystemStateTest, CreateRootState) {
  auto inode = Inode(kTestRootIno);
  ASSERT_TRUE(inode != nullptr);
  EXPECT_EQ(pb::mds::FileType::DIRECTORY, inode->Type());
  EXPECT_EQ(1u, inode->BaseVersion());
  EXPECT_EQ(1u, inode->CompleteVersion());

  auto partition = Partition(kTestRootIno);
  ASSERT_TRUE(partition != nullptr);
  EXPECT_EQ("1 0", partition->VersionVec().ToString());
  ExpectInodePartitionVersionEqual(kTestRootIno);
}

TEST_F(FileSystemStateTest, MkNodAdvancesParentExactly) {
  const uint64_t nlink0 = Inode(kTestRootIno)->Nlink();
  const uint64_t base0 = Inode(kTestRootIno)->BaseVersion();

  for (int i = 0; i < 5; ++i) {
    Ino ino = MkNod(kTestRootIno, fmt::format("mn_file_{}", i));
    ASSERT_GT(ino, 0u);

    auto child = Inode(ino);
    ASSERT_TRUE(child != nullptr);
    EXPECT_EQ(pb::mds::FileType::FILE, child->Type());
    EXPECT_EQ(1u, child->Nlink());
    EXPECT_EQ(1u, child->BaseVersion());
    EXPECT_EQ(1u, child->CompleteVersion());

    auto root = Inode(kTestRootIno);
    EXPECT_EQ(base0 + i + 1, root->BaseVersion());
    EXPECT_EQ(base0 + i + 1, root->CompleteVersion());
    ExpectInodePartitionVersionEqual(kTestRootIno);

    Dentry dentry;
    EXPECT_TRUE(Partition(kTestRootIno)
                    ->Get(fmt::format("mn_file_{}", i), dentry)
                    .ok());
  }

  // creating a file does not change the parent directory's link count
  EXPECT_EQ(nlink0, Inode(kTestRootIno)->Nlink());
}

TEST_F(FileSystemStateTest, MkDirBumpsParentNlinkAndVersion) {
  const uint64_t nlink0 = Inode(kTestRootIno)->Nlink();
  const uint64_t base0 = Inode(kTestRootIno)->BaseVersion();

  Ino dir = MkDir(kTestRootIno, "mk_dir_a");
  ASSERT_GT(dir, 0u);

  auto child = Inode(dir);
  ASSERT_TRUE(child != nullptr);
  EXPECT_EQ(pb::mds::FileType::DIRECTORY, child->Type());
  EXPECT_EQ(static_cast<uint32_t>(kEmptyDirMinLinkNum), child->Nlink());
  EXPECT_EQ(1u, child->BaseVersion());
  EXPECT_EQ(1u, child->CompleteVersion());

  auto root = Inode(kTestRootIno);
  EXPECT_EQ(nlink0 + 1, root->Nlink());
  EXPECT_EQ(base0 + 1, root->BaseVersion());
  ExpectInodePartitionVersionEqual(kTestRootIno);
}

TEST_F(FileSystemStateTest, BatchMkNodAdvancesParentOnce) {
  const uint64_t base0 = Inode(kTestRootIno)->BaseVersion();

  std::vector<FileSystem::MkNodParam> params;
  for (int i = 0; i < 3; ++i) {
    FileSystem::MkNodParam param;
    param.parent = kTestRootIno;
    param.name = fmt::format("bmn_file_{}", i);
    param.mode = 0777;
    params.push_back(param);
  }

  Context ctx;
  EntriesWithPaOut out;
  ASSERT_TRUE(fs_->BatchMkNod(ctx, params, out).ok());
  ASSERT_EQ(3u, out.attrs.size());
  for (const auto& attr : out.attrs) {
    EXPECT_EQ(1u, attr.version());
    auto child = Inode(attr.ino());
    ASSERT_TRUE(child != nullptr);
    EXPECT_EQ(1u, child->CompleteVersion());
  }

  // a single batch bumps the parent once, not once per entry
  auto root = Inode(kTestRootIno);
  EXPECT_EQ(base0 + 1, root->BaseVersion());
  EXPECT_EQ(base0 + 1, root->CompleteVersion());
  ExpectInodePartitionVersionEqual(kTestRootIno);
}

TEST_F(FileSystemStateTest, BatchMkDirAdvancesParentOnceAndNlinkByN) {
  const uint64_t nlink0 = Inode(kTestRootIno)->Nlink();
  const uint64_t base0 = Inode(kTestRootIno)->BaseVersion();

  std::vector<FileSystem::MkDirParam> params;
  for (int i = 0; i < 4; ++i) {
    FileSystem::MkDirParam param;
    param.parent = kTestRootIno;
    param.name = fmt::format("bmd_dir_{}", i);
    param.mode = 0777;
    params.push_back(param);
  }

  Context ctx;
  EntriesWithPaOut out;
  ASSERT_TRUE(fs_->BatchMkDir(ctx, params, out).ok());
  ASSERT_EQ(4u, out.attrs.size());

  auto root = Inode(kTestRootIno);
  EXPECT_EQ(nlink0 + 4, root->Nlink());
  EXPECT_EQ(base0 + 1, root->BaseVersion());
  ExpectInodePartitionVersionEqual(kTestRootIno);
}

TEST_F(FileSystemStateTest, RmDirBumpsParentAndDropsChild) {
  Ino dir = MkDir(kTestRootIno, "rm_dir_a");
  ASSERT_TRUE(Partition(kTestRootIno) != nullptr);

  const uint64_t nlink0 = Inode(kTestRootIno)->Nlink();
  const uint64_t base0 = Inode(kTestRootIno)->BaseVersion();

  Context ctx;
  EntryWithPaOut out;
  ASSERT_TRUE(fs_->RmDir(ctx, kTestRootIno, "rm_dir_a", out).ok());

  auto root = Inode(kTestRootIno);
  EXPECT_EQ(nlink0 - 1, root->Nlink());
  EXPECT_EQ(base0 + 1, root->BaseVersion());
  ExpectInodePartitionVersionEqual(kTestRootIno);

  EXPECT_TRUE(Inode(dir) == nullptr)
      << "removed dir inode should leave the cache";
  Dentry dentry;
  EXPECT_FALSE(Partition(kTestRootIno)->Get("rm_dir_a", dentry).ok());
}

TEST_F(FileSystemStateTest, LinkAndUnLinkTrackNlinkVersionAndParents) {
  Ino dir = MkDir(kTestRootIno, "link_dir_a");
  LoadPartition(dir);
  Ino file = MkNod(kTestRootIno, "link_file_a");

  const uint64_t root_base0 = Inode(kTestRootIno)->BaseVersion();
  const uint64_t dir_base0 = Inode(dir)->BaseVersion();
  const uint64_t file_base0 = Inode(file)->BaseVersion();
  const uint64_t file_nlink0 = Inode(file)->Nlink();

  {
    Context ctx;
    EntryWithPaOut out;
    ASSERT_TRUE(fs_->Link(ctx, file, dir, "link_child", out).ok());

    auto child = Inode(file);
    ASSERT_TRUE(child != nullptr);
    EXPECT_EQ(file_nlink0 + 1, child->Nlink());
    EXPECT_EQ(file_base0 + 1, child->BaseVersion());
    EXPECT_EQ(2u, child->Parents().size());

    EXPECT_EQ(dir_base0 + 1, Inode(dir)->BaseVersion());
    EXPECT_EQ(root_base0, Inode(kTestRootIno)->BaseVersion());
    ExpectInodePartitionVersionEqual(dir);
  }

  {
    // remove one of the two links: inode survives with nlink-1
    Context ctx;
    EntryWithPaOut out;
    ASSERT_TRUE(fs_->UnLink(ctx, dir, "link_child", out).ok());

    auto child = Inode(file);
    ASSERT_TRUE(child != nullptr);
    EXPECT_EQ(file_nlink0, child->Nlink());
    EXPECT_FALSE(child->IsDeleted());
    EXPECT_EQ(1u, child->Parents().size());
    EXPECT_EQ(dir_base0 + 2, Inode(dir)->BaseVersion());
    ExpectInodePartitionVersionEqual(dir);
  }

  {
    // remove the last link: inode becomes deleted
    Context ctx;
    EntryWithPaOut out;
    ASSERT_TRUE(fs_->UnLink(ctx, kTestRootIno, "link_file_a", out).ok());

    auto child = Inode(file);
    ASSERT_TRUE(child != nullptr);
    EXPECT_EQ(0u, child->Nlink());
    EXPECT_TRUE(child->IsDeleted());
  }
}

TEST_F(FileSystemStateTest, SymlinkBumpsParentVersionNotNlink) {
  Ino dir = MkDir(kTestRootIno, "sym_dir_a");
  LoadPartition(dir);

  const uint64_t base0 = Inode(dir)->BaseVersion();
  const uint64_t nlink0 = Inode(dir)->Nlink();

  Ino link = Symlink(dir, "sym_link_a", "/some/target");
  ASSERT_GT(link, 0u);

  auto child = Inode(link);
  ASSERT_TRUE(child != nullptr);
  EXPECT_EQ(pb::mds::FileType::SYM_LINK, child->Type());
  EXPECT_EQ(1u, child->BaseVersion());
  EXPECT_EQ(1u, child->CompleteVersion());
  EXPECT_EQ("/some/target", child->Symlink());

  EXPECT_EQ(base0 + 1, Inode(dir)->BaseVersion());
  EXPECT_EQ(nlink0, Inode(dir)->Nlink());
  ExpectInodePartitionVersionEqual(dir);
}

TEST_F(FileSystemStateTest, SetAttrOnDirRefreshesPartitionVersion) {
  Ino dir = MkDir(kTestRootIno, "setattr_dir_a");
  LoadPartition(dir);

  const uint64_t dir_base0 = Inode(dir)->BaseVersion();

  Context ctx;
  FileSystem::SetAttrParam param;
  param.to_set = kSetAttrMode;
  param.attr.set_fs_id(kTestFsId);
  param.attr.set_ino(dir);
  param.attr.set_mode(0600);
  EntryWithChunkOut out;
  ASSERT_TRUE(fs_->SetAttr(ctx, dir, param, out).ok());
  EXPECT_EQ(0600u, out.attr.mode() & 0777);
  EXPECT_EQ(dir_base0 + 1, Inode(dir)->BaseVersion());
  ExpectInodePartitionVersionEqual(dir);

  // setattr on a file must not touch its parent directory version
  Ino file = MkNod(dir, "setattr_file_a");
  const uint64_t dir_after_mknod = Inode(dir)->BaseVersion();

  FileSystem::SetAttrParam file_param;
  file_param.to_set = kSetAttrMode;
  file_param.attr.set_fs_id(kTestFsId);
  file_param.attr.set_ino(file);
  file_param.attr.set_mode(0644);
  EntryWithChunkOut file_out;
  ASSERT_TRUE(fs_->SetAttr(ctx, file, file_param, file_out).ok());
  EXPECT_EQ(dir_after_mknod, Inode(dir)->BaseVersion());
}

TEST_F(FileSystemStateTest, SetAndRemoveXAttrOnDirRefreshPartitionVersion) {
  Ino dir = MkDir(kTestRootIno, "xattr_dir_a");
  LoadPartition(dir);

  const uint64_t base0 = Inode(dir)->BaseVersion();

  Context ctx;
  Inode::XAttrMap xattrs;
  xattrs["user.k"] = "v";
  EntryOut out;
  ASSERT_TRUE(fs_->SetXAttr(ctx, dir, xattrs, out).ok());
  EXPECT_EQ(base0 + 1, Inode(dir)->BaseVersion());
  ExpectInodePartitionVersionEqual(dir);

  std::string value;
  ASSERT_TRUE(fs_->GetXAttr(ctx, dir, "user.k", value).ok());
  EXPECT_EQ("v", value);

  EntryOut remove_out;
  ASSERT_TRUE(fs_->RemoveXAttr(ctx, dir, "user.k", remove_out).ok());
  EXPECT_EQ(base0 + 2, Inode(dir)->BaseVersion());
  EXPECT_TRUE(Inode(dir)->XAttr("user.k").empty());
  ExpectInodePartitionVersionEqual(dir);
}

TEST_F(FileSystemStateTest, RenameSameDirAdvancesParentOnce) {
  Ino dir = MkDir(kTestRootIno, "ren1_dir");
  MkNod(dir, "ren1_src");
  LoadPartition(dir);

  const uint64_t base0 = Inode(dir)->BaseVersion();

  Context ctx;
  FileSystem::RenameParam param;
  param.old_parent = dir;
  param.old_name = "ren1_src";
  param.new_parent = dir;
  param.new_name = "ren1_dst";
  FileSystem::RenameResult out;
  ASSERT_TRUE(fs_->Rename(ctx, param, out).ok());

  EXPECT_EQ(base0 + 1, Inode(dir)->BaseVersion());
  ExpectInodePartitionVersionEqual(dir);

  Dentry dentry;
  EXPECT_FALSE(Partition(dir)->Get("ren1_src", dentry).ok());
  EXPECT_TRUE(Partition(dir)->Get("ren1_dst", dentry).ok());
  EXPECT_EQ(dir, dentry.ParentIno());
}

TEST_F(FileSystemStateTest, RenameDiffDirAdvancesBothParents) {
  Ino src_dir = MkDir(kTestRootIno, "ren2_src");
  Ino dst_dir = MkDir(kTestRootIno, "ren2_dst");
  Ino file = MkNod(src_dir, "ren2_file");
  LoadPartition(src_dir);
  LoadPartition(dst_dir);

  const uint64_t src_base0 = Inode(src_dir)->BaseVersion();
  const uint64_t dst_base0 = Inode(dst_dir)->BaseVersion();

  Context ctx;
  FileSystem::RenameParam param;
  param.old_parent = src_dir;
  param.old_name = "ren2_file";
  param.new_parent = dst_dir;
  param.new_name = "ren2_file";
  FileSystem::RenameResult out;
  ASSERT_TRUE(fs_->Rename(ctx, param, out).ok());

  EXPECT_EQ(src_base0 + 1, Inode(src_dir)->BaseVersion());
  EXPECT_EQ(dst_base0 + 1, Inode(dst_dir)->BaseVersion());
  ExpectInodePartitionVersionEqual(src_dir);
  ExpectInodePartitionVersionEqual(dst_dir);

  auto child = Inode(file);
  ASSERT_TRUE(child != nullptr);
  auto parents = child->Parents();
  EXPECT_NE(parents.end(), std::find(parents.begin(), parents.end(), dst_dir));

  Dentry dentry;
  EXPECT_FALSE(Partition(src_dir)->Get("ren2_file", dentry).ok());
  EXPECT_TRUE(Partition(dst_dir)->Get("ren2_file", dentry).ok());
}

// ---------------------------------------------------------------------------
// T2: read paths and lazy partition loading
// ---------------------------------------------------------------------------

TEST_F(FileSystemStateTest, ReadPathsDoNotChangeVersions) {
  Ino dir = MkDir(kTestRootIno, "read_dir_a");
  Ino file = MkNod(dir, "read_file_a");
  Ino sym = Symlink(dir, "read_sym_a", "/read/target");
  LoadPartition(dir);

  auto inode_version = [&](Ino ino) {
    auto inode = Inode(ino);
    EXPECT_TRUE(inode != nullptr) << "inode " << ino << " not cached";
    if (inode == nullptr) return std::make_pair<uint64_t, uint64_t>(0, 0);
    return std::make_pair(inode->BaseVersion(), inode->CompleteVersion());
  };
  auto partition_version = [&](Ino ino) {
    auto partition = Partition(ino);
    EXPECT_TRUE(partition != nullptr) << "partition " << ino << " not cached";
    if (partition == nullptr) return std::make_pair<uint64_t, uint64_t>(0, 0);
    return std::make_pair(partition->VersionVec().BaseVersion(),
                          partition->VersionVec().CompleteVersion());
  };

  const std::vector<Ino> inodes = {kTestRootIno, dir, file, sym};
  std::map<Ino, std::pair<uint64_t, uint64_t>> inode_before;
  for (Ino ino : inodes) inode_before[ino] = inode_version(ino);

  const std::map<Ino, std::pair<uint64_t, uint64_t>> partition_before = {
      {kTestRootIno, partition_version(kTestRootIno)},
      {dir, partition_version(dir)},
  };

  Context ctx;
  {
    EntryOut out;
    ASSERT_TRUE(fs_->Lookup(ctx, kTestRootIno, "read_dir_a", out).ok());
  }
  {
    EntryOut out;
    ASSERT_TRUE(fs_->GetAttr(ctx, file, out).ok());
  }
  {
    Dentry dentry;
    ASSERT_TRUE(fs_->GetDentry(ctx, dir, "read_file_a", dentry).ok());
  }
  {
    std::vector<Dentry> dentries;
    ASSERT_TRUE(fs_->ListDentry(ctx, dir, "", 100, false, dentries).ok());
  }
  {
    std::vector<EntryWithNameOut> entries;
    ASSERT_TRUE(fs_->ReadDir(ctx, dir, "", 100, false, entries).ok());
  }
  {
    Inode::XAttrMap xattrs;
    ASSERT_TRUE(fs_->GetXAttr(ctx, file, xattrs).ok());
  }
  {
    std::string link;
    ASSERT_TRUE(fs_->ReadLink(ctx, sym, link).ok());
    EXPECT_EQ("/read/target", link);
  }
  {
    std::vector<EntryOut> outs;
    ASSERT_TRUE(fs_->BatchGetInode(ctx, {file, sym}, outs).ok());
    EXPECT_EQ(2u, outs.size());
  }
  {
    std::vector<pb::mds::XAttr> xattrs;
    ASSERT_TRUE(fs_->BatchGetXAttr(ctx, {file}, xattrs).ok());
    EXPECT_EQ(1u, xattrs.size());
  }

  for (Ino ino : inodes) {
    EXPECT_EQ(inode_before[ino], inode_version(ino))
        << "inode " << ino << " version changed";
  }
  for (const auto& [ino, before] : partition_before) {
    EXPECT_EQ(before, partition_version(ino))
        << "partition " << ino << " version changed";
  }
}

TEST_F(FileSystemStateTest, ShardVersionEqualsPartitionAtFetch) {
  Ino dir = MkDir(kTestRootIno, "shard_dir_a");
  ASSERT_TRUE(Partition(dir) == nullptr)
      << "fresh dir partition should not be cached";

  // create children while the partition is not cached: no shard is loaded, the
  // dentries only land in the store and as delta ops
  MkNod(dir, "s1");
  MkNod(dir, "s2");
  MkNod(dir, "s3");
  ASSERT_TRUE(Partition(dir) == nullptr)
      << "mknod must not cache the child partition";

  // now load the partition + shard through a read path
  LoadPartition(dir);

  ExpectInodePartitionVersionEqual(dir);

  Json::Value value;
  ASSERT_TRUE(fs_->DescribePartitionShard(dir, value).ok());
  const std::string partition_version = value["version"].asString();
  EXPECT_EQ(Partition(dir)->VersionVec().ToString(), partition_version);

  bool saw_loaded_shard = false;
  for (const auto& shard : value["shards"]) {
    if (!shard.isMember("id")) continue;  // shard not loaded yet
    saw_loaded_shard = true;
    // fetched from the same store snapshot: shard version == partition version
    EXPECT_EQ(partition_version, shard["version"].asString());
  }
  EXPECT_TRUE(saw_loaded_shard) << "no shard was loaded";

  // a further op advances the partition but the already-loaded shard lags
  MkNod(dir, "s4");
  Json::Value value_after;
  ASSERT_TRUE(fs_->DescribePartitionShard(dir, value_after).ok());
  const std::string partition_version_after = value_after["version"].asString();
  for (const auto& shard : value_after["shards"]) {
    if (!shard.isMember("id")) continue;
    EXPECT_NE(partition_version_after, shard["version"].asString());
  }
}

TEST_F(FileSystemStateTest, UncachedPartitionOnlyAdvancesInode) {
  Ino dir = MkDir(kTestRootIno, "uncached_dir_a");
  ASSERT_TRUE(Partition(dir) == nullptr);

  const uint64_t base0 = Inode(dir)->BaseVersion();
  MkNod(dir, "u1");

  // partition is still absent; only the inode side advanced
  EXPECT_TRUE(Partition(dir) == nullptr);
  EXPECT_EQ(base0 + 1, Inode(dir)->BaseVersion());

  // loading the partition reconciles both sides from the same store snapshot
  {
    Context ctx;
    EntryOut out;
    ASSERT_TRUE(fs_->Lookup(ctx, dir, "u1", out).ok());
  }
  ASSERT_TRUE(Partition(dir) != nullptr);
  ExpectInodePartitionVersionEqual(dir);
}

// ---------------------------------------------------------------------------
// T4: concurrent operations still keep inode and partition versions in sync
// ---------------------------------------------------------------------------

TEST_F(FileSystemStateTest, ConcurrentOpsKeepVersionConsistent) {
  constexpr int kThreads = 16;
  constexpr int kOpsPerThread = 50;
  constexpr int kTotal = kThreads * kOpsPerThread;

  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::atomic<int> ok_count{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      ready.fetch_add(1);
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < kOpsPerThread; ++i) {
        Context ctx;
        FileSystem::MkNodParam param;
        param.parent = kTestRootIno;
        param.name = fmt::format("cc_{}_{}", t, i);
        param.mode = 0777;
        EntryWithPaOut out;
        if (fs_->MkNod(ctx, param, out).ok()) ok_count.fetch_add(1);
      }
    });
  }

  while (ready.load(std::memory_order_acquire) < kThreads)
    std::this_thread::yield();
  go.store(true, std::memory_order_release);
  for (auto& thread : threads) thread.join();

  ASSERT_EQ(kTotal, ok_count.load()) << "some concurrent mknod failed";

  // load shards so Size() reflects the persisted dentries
  LoadPartition(kTestRootIno);
  EXPECT_EQ(static_cast<size_t>(kTotal), Partition(kTestRootIno)->Size());

  // inode and partition must agree bucket by bucket after the dust settles
  ExpectInodePartitionVersionEqual(kTestRootIno);

  auto iv = Inode(kTestRootIno)->VersionVec();
  // concurrency must have exercised the mutation (delta) path at least once,
  // otherwise this test proves nothing about the in-flight merge logic
  EXPECT_GT(iv.total_delta_version, 0u)
      << "mutation path was never exercised; run on a multi-core host";
  EXPECT_GT(iv.BaseVersion(), 1u);
  EXPECT_EQ(iv.BaseVersion() + iv.total_delta_version, iv.CompleteVersion());
}

// Long-running mixed-op concurrency with a checker thread that validates the
// state while operations are still in flight. Rename is deliberately left out
// of the mix: it updates the partition before the inode, so it would break the
// in-flight `partition <= inode` assertion (see ShardPartition/FileSystem
// comment in Rename). All other namespace ops publish the inode first.
//
// Duration defaults to 3000ms; override with LONG_TEST_SECONDS=<n> to run
// longer (e.g. LONG_TEST_SECONDS=120 for a soak run).
TEST_F(FileSystemStateTest, LongRunningConcurrentStateConsistency) {
  if (getenv("MANUAL_TEST") == nullptr) {
    GTEST_SKIP() << "Skip manual test case.";
  }

  const uint64_t run_ms = [] {
    if (const char* s = getenv("LONG_TEST_SECONDS")) {
      return static_cast<uint64_t>(std::stoull(s)) * 1000;
    }
    return static_cast<uint64_t>(3000);
  }();

  constexpr int kWorkers = 8;
  constexpr int kKeepersPerWorker = 10;

  // A file that must stay present, untouched, for the whole run.
  const std::string stable_name = "long_stable_file";
  Ino stable_ino = MkNod(kTestRootIno, stable_name);
  ASSERT_GT(stable_ino, 0u);
  LoadPartition(kTestRootIno);  // load root shard so reads hit the warm path

  std::atomic<bool> stop{false};
  std::atomic<int> ok_ops{0};
  std::atomic<int> checker_checks{0};
  std::atomic<int> checker_failures{0};

  // Periodic in-flight checker. Runs purely read-only paths and only uses
  // EXPECT_* (gtest assertions are safe to call from non-main threads).
  std::thread checker([&] {
    auto parse_version = [](const std::string& s) {
      const size_t space = s.find(' ');
      return std::make_pair(std::stoull(s.substr(0, space)),
                            std::stoull(s.substr(space + 1)));
    };
    auto inode_vec = [&] { return Inode(kTestRootIno)->VersionVec(); };
    // Version of the cached partition, or nullopt when it has been evicted.
    auto partition_version =
        [&]() -> std::optional<std::pair<uint64_t, uint64_t>> {
      auto partition = Partition(kTestRootIno);
      if (partition == nullptr) return std::nullopt;
      Json::Value value;
      partition->Dump(value);
      return parse_version(value["version"].asString());
    };

    auto last_inode = inode_vec();

    while (!stop.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      checker_checks.fetch_add(1);

      // 1. inode version vector is monotonic, bucket by bucket
      auto iv = inode_vec();
      if (iv.BaseVersion() < last_inode.BaseVersion() ||
          iv.CompleteVersion() < last_inode.CompleteVersion()) {
        checker_failures.fetch_add(1);
        ADD_FAILURE() << "root inode version went backwards: "
                      << last_inode.ToString() << " -> " << iv.ToString();
      }
      for (size_t i = 0; i < iv.delta_versions.size(); ++i) {
        if (iv.DeltaVersion(static_cast<uint32_t>(i)) <
            last_inode.DeltaVersion(static_cast<uint32_t>(i))) {
          checker_failures.fetch_add(1);
          ADD_FAILURE() << "root inode delta bucket " << i << " went backwards";
        }
      }

      // 2. the cached partition must never run ahead of the inode. It is
      // rebuilt from the store after eviction, which may yield a lower logical
      // version than the evicted cache entry, so only this direction holds
      // across evictions. Dump the cache entry directly: once evicted,
      // DescribePartitionShard would rebuild a base-only view instead.
      auto pv = partition_version();
      if (pv.has_value()) {
        uint64_t partition_complete = pv->first + pv->second;
        if (partition_complete > iv.CompleteVersion()) {
          checker_failures.fetch_add(1);
          ADD_FAILURE() << "root partition(" << partition_complete
                        << ") ahead of inode(" << iv.CompleteVersion() << ")";
        }
      }

      // 3. the untouched file stays readable with a stable version
      Context ctx;
      EntryOut out;
      auto status = fs_->Lookup(ctx, kTestRootIno, stable_name, out);
      if (!status.ok() || out.attr.ino() != stable_ino ||
          out.attr.version() != 1) {
        checker_failures.fetch_add(1);
        ADD_FAILURE() << "stable file changed: status=" << status.error_str()
                      << " ino=" << out.attr.ino()
                      << " version=" << out.attr.version();
      }

      last_inode = iv;
    }
  });

  // Randomly evict the root partition and/or its dir shards so the workers and
  // the checker keep re-exercising the cold, store-backed load path.
  std::atomic<int> cleaner_rounds{0};
  std::thread cleaner([&] {
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> gap_ms(1, 50);
    std::uniform_int_distribution<int> pick(0, 1);
    while (!stop.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(gap_ms(rng)));
      if (pick(rng) == 0) {
        fs_->Test_DeletePartitionFromCache(kTestRootIno);
      } else if (auto partition = Partition(kTestRootIno);
                 partition != nullptr) {
        partition->TEST_DeleteDirShard();
      }
      cleaner_rounds.fetch_add(1);
    }
  });

  std::vector<std::thread> workers;
  workers.reserve(kWorkers);
  for (int w = 0; w < kWorkers; ++w) {
    workers.emplace_back([&, w] {
      std::string prev;
      bool prev_is_dir = false;
      uint64_t i = 0;
      while (!stop.load(std::memory_order_acquire)) {
        const std::string name = fmt::format("lr_{}_{}", w, i);
        const bool is_dir = (i % 2) == 0;
        const bool created = is_dir ? TryMkDir(kTestRootIno, name)
                                    : TryMkNod(kTestRootIno, name);
        if (created) ok_ops.fetch_add(1);

        // extra concurrent traffic on the same parent inode
        if (i % 8 == 0 &&
            TrySetAttrMode(kTestRootIno, 0700 + static_cast<uint32_t>(w % 8))) {
          ok_ops.fetch_add(1);
        }

        if (!prev.empty()) {
          const bool removed = prev_is_dir ? TryRmDir(kTestRootIno, prev)
                                           : TryUnLink(kTestRootIno, prev);
          if (removed) ok_ops.fetch_add(1);
        }
        prev = name;
        prev_is_dir = is_dir;
        ++i;
      }

      // leave a clean, known-final set of entries behind
      if (!prev.empty()) {
        if (prev_is_dir) {
          TryRmDir(kTestRootIno, prev);
        } else {
          TryUnLink(kTestRootIno, prev);
        }
      }
      for (int j = 0; j < kKeepersPerWorker; ++j) {
        const std::string keeper = fmt::format("lrk_{}_{}", w, j);
        if (TryMkNod(kTestRootIno, keeper)) ok_ops.fetch_add(1);
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(run_ms));
  stop.store(true, std::memory_order_release);
  checker.join();
  for (auto& worker : workers) worker.join();
  cleaner.join();

  EXPECT_GT(ok_ops.load(), 0);
  EXPECT_GT(checker_checks.load(), 0) << "checker never ran";
  EXPECT_GT(cleaner_rounds.load(), 0) << "cleaner never ran";
  EXPECT_EQ(0, checker_failures.load()) << "in-flight state check failed";

  // quiescent: both sides must have converged and every expected dentry exists.
  // The cleaner may have dropped the partition last, so reload it first.
  LoadPartition(kTestRootIno);
  ExpectInodePartitionVersionEqual(kTestRootIno);
  auto iv = Inode(kTestRootIno)->VersionVec();
  EXPECT_GT(iv.total_delta_version, 0u) << "mutation path was never exercised";

  std::set<std::string> expected = {stable_name};
  for (int w = 0; w < kWorkers; ++w) {
    for (int j = 0; j < kKeepersPerWorker; ++j) {
      expected.insert(fmt::format("lrk_{}_{}", w, j));
    }
  }
  const auto actual = SnapshotNames(kTestRootIno);
  EXPECT_EQ(expected, actual) << "final dentry set mismatch";
}

// Long-running soak: each worker owns a private name and drives one full
//   Lookup(miss) -> create -> Lookup(hit, version==1) -> delete -> Lookup(miss)
// cycle per iteration, while a cleaner thread keeps evicting the root
// partition and its dir shards. Because the name is private every step has a
// single deterministic outcome, so a failed check is a real bug rather than a
// race on the name; all contention is on the shared root inode/partition.
//
// Duration defaults to 60s; LONG_TEST_SECONDS=<n> overrides it and
// LONG_TEST_SECONDS=0 runs until a check fails. The run stops early as soon as
// any check fails (fail -> ADD_FAILURE + fatal flag, every loop bails out).
//
// WARNING: this is a manual/soak test, it is skipped unless MANUAL_TEST is set.
TEST_F(FileSystemStateTest, LongRunningLookupCreateDeleteVersions) {
  if (getenv("MANUAL_TEST") == nullptr) {
    GTEST_SKIP() << "Skip manual test case.";
  }

  const uint64_t run_ms = [] {
    if (const char* s = getenv("LONG_TEST_SECONDS")) {
      return static_cast<uint64_t>(std::stoull(s)) * 1000;
    }
    return static_cast<uint64_t>(60) * 1000;
  }();

  constexpr int kFileWorkers = 4;
  constexpr int kDirWorkers = 4;

  std::atomic<bool> stop{false};
  std::atomic<bool> fatal{false};
  std::atomic<uint64_t> created_ops{0};
  std::atomic<uint64_t> deleted_ops{0};
  std::atomic<uint64_t> cleaner_rounds{0};

  // A failed check records the gtest failure and wakes every loop to bail out.
  auto Fail = [&](const std::string& msg) {
    ADD_FAILURE() << msg;
    fatal.store(true, std::memory_order_release);
  };

  // Retry the store conflict budget like a real client. When abort_on_stop is
  // set (the worker path) the loop bails as soon as the run is winding down, so
  // a 200-attempt loop cannot outlive the duration; the cleanup path passes
  // false so it still drains leftovers after stop is set.
  auto retry_write = [&](auto&& fn, bool abort_on_stop = true) -> Status {
    // Seed with a non-OK status: when the loop aborts before the first attempt
    // (stop/fatal already set) callers must not take it for success and read
    // the untouched output.
    Status last = Status(pb::error::ESTORE_MAYBE_RETRY, "retry aborted");
    for (int attempt = 0; attempt < 200; ++attempt) {
      if (fatal.load(std::memory_order_acquire)) return last;
      if (abort_on_stop && stop.load(std::memory_order_acquire)) return last;
      last = fn();
      if (last.ok() || last.error_code() != pb::error::ESTORE_MAYBE_RETRY)
        return last;
      std::this_thread::yield();
    }
    return last;
  };

  // One worker family: is_dir selects MkDir/RmDir + directory expectations,
  // otherwise BatchCreate/UnLink + file expectations.
  auto run_worker = [&](int w, bool is_dir) {
    const std::string name =
        is_dir ? fmt::format("ld_{}", w) : fmt::format("lf_{}", w);

    std::optional<AttrVersionVec> last_vec;
    uint64_t last_parent_version = 0;

    auto root_complete_version = [&]() -> uint64_t {
      auto inode = Inode(kTestRootIno);
      return (inode != nullptr) ? inode->CompleteVersion() : 0;
    };

    // Root version vector must never go backwards, and the cached partition
    // must never run ahead of the inode. The partition is sampled before the
    // inode on purpose: an op publishes the inode first, so any partition
    // update observed in the meantime is already reflected in the later inode
    // sample.
    auto check_root_versions = [&]() -> bool {
      auto partition = Partition(kTestRootIno);
      const bool has_partition = (partition != nullptr);
      const uint64_t partition_complete =
          has_partition ? partition->VersionVec().CompleteVersion() : 0;

      auto inode = Inode(kTestRootIno);
      if (inode == nullptr) {
        Fail(fmt::format("[{}] root inode left the cache", name));
        return false;
      }
      auto vec = inode->VersionVec();

      if (last_vec.has_value()) {
        if (vec.BaseVersion() < last_vec->BaseVersion() ||
            vec.CompleteVersion() < last_vec->CompleteVersion()) {
          Fail(fmt::format("[{}] root version went backwards: {} -> {}", name,
                           last_vec->ToString(), vec.ToString()));
          return false;
        }
        for (size_t i = 0; i < vec.delta_versions.size(); ++i) {
          const auto idx = static_cast<uint32_t>(i);
          if (vec.DeltaVersion(idx) < last_vec->DeltaVersion(idx)) {
            Fail(fmt::format("[{}] root delta bucket {} went backwards", name,
                             idx));
            return false;
          }
        }
      }
      last_vec = vec;

      if (has_partition && partition_complete > vec.CompleteVersion()) {
        Fail(fmt::format("[{}] root partition({}) ahead of inode({})", name,
                         partition_complete, vec.CompleteVersion()));
        return false;
      }

      return true;
    };

    while (!stop.load(std::memory_order_acquire) &&
           !fatal.load(std::memory_order_acquire)) {
      // 1. the private name must be absent after the previous cycle
      {
        EntryOut out;
        auto status = LookupRaw(kTestRootIno, name, out);
        if (status.ok()) {
          Fail(fmt::format(
              "[{}] step1: expected absent, found ino={} version={}", name,
              out.attr.ino(), out.attr.version()));
          return;
        }
        if (status.error_code() != pb::error::ENOT_FOUND) {
          Fail(fmt::format("[{}] step1: unexpected lookup error: {}", name,
                           status.error_str()));
          return;
        }
      }

      const uint64_t parent_before = root_complete_version();

      // 2. create (batch size one), then verify the fresh inode
      Ino ino = 0;
      uint64_t parent_after_create = 0;
      if (is_dir) {
        EntryWithPaOut out;
        auto status =
            retry_write([&] { return MkDirRaw(kTestRootIno, name, out); });
        if (!status.ok()) {
          if (stop.load(std::memory_order_acquire) ||
              fatal.load(std::memory_order_acquire))
            return;
          Fail(fmt::format("[{}] step2: mkdir failed: {}", name,
                           status.error_str()));
          return;
        }
        ino = out.attr.ino();
        parent_after_create = out.parent_attr.version();
        if (out.attr.version() != 1 ||
            out.attr.type() != pb::mds::FileType::DIRECTORY ||
            out.attr.nlink() != static_cast<uint32_t>(kEmptyDirMinLinkNum)) {
          Fail(fmt::format(
              "[{}] step2: bad new dir attr ino={} version={} type={} nlink={}",
              name, out.attr.ino(), out.attr.version(),
              static_cast<int>(out.attr.type()), out.attr.nlink()));
          return;
        }
      } else {
        EntriesWithPaOut out;
        auto status = retry_write(
            [&] { return BatchCreateOne(kTestRootIno, name, out); });
        if (!status.ok()) {
          if (stop.load(std::memory_order_acquire) ||
              fatal.load(std::memory_order_acquire))
            return;
          Fail(fmt::format("[{}] step2: batch create failed: {}", name,
                           status.error_str()));
          return;
        }
        if (out.attrs.size() != 1) {
          Fail(fmt::format("[{}] step2: batch create returned {} attrs", name,
                           out.attrs.size()));
          return;
        }
        ino = out.attrs[0].ino();
        parent_after_create = out.parent_attr.version();
        if (out.attrs[0].version() != 1 ||
            out.attrs[0].type() != pb::mds::FileType::FILE ||
            out.attrs[0].nlink() != 1) {
          Fail(fmt::format(
              "[{}] step2: bad new file attr ino={} version={} type={} "
              "nlink={}",
              name, out.attrs[0].ino(), out.attrs[0].version(),
              static_cast<int>(out.attrs[0].type()), out.attrs[0].nlink()));
          return;
        }
      }
      if (parent_after_create <= parent_before ||
          parent_after_create <= last_parent_version) {
        Fail(fmt::format(
            "[{}] step2: parent version did not advance: before={} returned={} "
            "prev={}",
            name, parent_before, parent_after_create, last_parent_version));
        return;
      }
      last_parent_version = parent_after_create;
      if (!check_root_versions()) return;
      created_ops.fetch_add(1, std::memory_order_relaxed);

      // 3. the created inode must be visible at exactly version one
      {
        EntryOut out;
        auto status = LookupRaw(kTestRootIno, name, out);
        if (!status.ok() || out.attr.ino() != ino || out.attr.version() != 1) {
          Fail(
              fmt::format("[{}] step3: lookup after create: status={} ino={} "
                          "expected_ino={} version={}",
                          name, status.error_str(), out.attr.ino(), ino,
                          out.attr.version()));
          return;
        }
      }

      // 4. delete it
      {
        EntryWithPaOut out;
        auto status = retry_write([&] {
          return is_dir ? RmDirRaw(kTestRootIno, name, out)
                        : UnLinkRaw(kTestRootIno, name, out);
        });
        if (!status.ok()) {
          if (stop.load(std::memory_order_acquire) ||
              fatal.load(std::memory_order_acquire))
            return;
          Fail(fmt::format("[{}] step4: {} failed: {}", name,
                           is_dir ? "rmdir" : "unlink", status.error_str()));
          return;
        }
        if (out.attr.ino() != ino) {
          Fail(fmt::format("[{}] step4: removed ino={} expected={}", name,
                           out.attr.ino(), ino));
          return;
        }
        if (out.parent_attr.version() <= last_parent_version) {
          Fail(fmt::format(
              "[{}] step4: parent version did not advance: returned={} prev={}",
              name, out.parent_attr.version(), last_parent_version));
          return;
        }
        last_parent_version = out.parent_attr.version();
      }
      if (!check_root_versions()) return;
      deleted_ops.fetch_add(1, std::memory_order_relaxed);

      // 5. it must be gone again, and reads must say exactly so
      {
        EntryOut out;
        auto status = LookupRaw(kTestRootIno, name, out);
        if (status.ok()) {
          Fail(fmt::format(
              "[{}] step5: expected absent after delete, found ino={}", name,
              out.attr.ino()));
          return;
        }
        if (status.error_code() != pb::error::ENOT_FOUND) {
          Fail(fmt::format("[{}] step5: unexpected lookup error: {}", name,
                           status.error_str()));
          return;
        }
      }
    }
  };

  // Randomly evict the root partition and/or its dir shards so the workers
  // keep re-exercising the cold, store-backed load path.
  std::thread cleaner([&] {
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> gap_ms(1, 50);
    std::uniform_int_distribution<int> pick(0, 1);
    while (!stop.load(std::memory_order_acquire) &&
           !fatal.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(gap_ms(rng)));
      if (pick(rng) == 0) {
        fs_->Test_DeletePartitionFromCache(kTestRootIno);
      } else if (auto partition = Partition(kTestRootIno);
                 partition != nullptr) {
        partition->TEST_DeleteDirShard();
      }
      cleaner_rounds.fetch_add(1, std::memory_order_relaxed);
    }
  });

  std::vector<std::thread> workers;
  workers.reserve(kFileWorkers + kDirWorkers);
  for (int w = 0; w < kFileWorkers; ++w)
    workers.emplace_back([&, w] { run_worker(w, false); });
  for (int w = 0; w < kDirWorkers; ++w)
    workers.emplace_back([&, w] { run_worker(w, true); });

  // LONG_TEST_SECONDS=0 means: run until a check fails.
  if (run_ms == 0) {
    while (!fatal.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  } else {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(run_ms);
    while (!fatal.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  stop.store(true, std::memory_order_release);
  for (auto& worker : workers) worker.join();
  cleaner.join();

  // best-effort: a stop can land mid-cycle, so leave a clean root behind
  for (int w = 0; w < kFileWorkers; ++w) {
    EntryWithPaOut out;
    retry_write(
        [&] { return UnLinkRaw(kTestRootIno, fmt::format("lf_{}", w), out); },
        false);
  }
  for (int w = 0; w < kDirWorkers; ++w) {
    EntryWithPaOut out;
    retry_write(
        [&] { return RmDirRaw(kTestRootIno, fmt::format("ld_{}", w), out); },
        false);
  }

  EXPECT_FALSE(fatal.load()) << "strict check failed during the run";

  // quiescent: after the last eviction both sides must have converged
  if (!fatal.load()) {
    LoadPartition(kTestRootIno);
    ExpectInodePartitionVersionEqual(kTestRootIno);
    EXPECT_EQ(0u, SnapshotNames(kTestRootIno).size())
        << "leftover entries in root";
  }

  EXPECT_GT(created_ops.load(), 0u) << "worker never created anything";
  EXPECT_GT(deleted_ops.load(), 0u) << "worker never deleted anything";
  EXPECT_GT(cleaner_rounds.load(), 0u) << "cleaner never ran";
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
