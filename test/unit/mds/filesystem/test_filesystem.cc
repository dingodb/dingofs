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

#include <absl/container/flat_hash_map.h>
#include <fcntl.h>
#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "gtest/gtest.h"
#include "mds/common/context.h"
#include "mds/common/trash.h"
#include "mds/coordinator/dummy_coordinator_client.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/id_generator.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dummy_storage.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {
namespace unit_test {

// Drain the in-memory dir-stat delta log (summed per ino) and clear it,
// mirroring what a flush consumes. The production flush uses GetFlushSnapshot +
// Compact so a delta arriving mid-flush survives; tests run with inplace
// workers so nothing races, and this drains everything for per-op delta
// assertions/isolation.
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

const std::string kFsAutoIncrementIdName = "dingofs-fs-id";
const std::string kInoAutoIncrementIdName = "dingofs-inode-id";
const std::string kSliceAutoIncrementIdName = "dingofs-slice-id";
const int64_t kFsTableId = 1234;
const int64_t kInodeTableId = 2345;
const int64_t kSliceTableId = 3456;

const uint64_t kRootIno = 1;

// Keep the test MDS ID below the reserved trash inode range when using the
// per-MDS inode layout.
const int64_t kMdsId = 1000;

static pb::mds::S3Info CreateS3Info() {
  pb::mds::S3Info s3_info;
  s3_info.set_ak("ak");
  s3_info.set_sk("sk");
  s3_info.set_endpoint("http://s3.com");
  s3_info.set_bucketname("bucket");

  return s3_info;
}

static pb::mds::FsInfo CreateFsInfo(
    uint32_t fs_id, const std::string& fs_name,
    dingofs::pb::mds::PartitionType partition_type) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_id(fs_id);
  fs_info.set_fs_name(fs_name);
  fs_info.set_fs_type(pb::mds::FsType::S3);
  fs_info.set_status(pb::mds::FsStatus::NORMAL);
  fs_info.set_block_size(1024 * 1024);
  fs_info.set_chunk_size(1024 * 1024 * 64);
  fs_info.set_enable_dir_stats(true);
  fs_info.set_owner("dengzh");
  fs_info.set_capacity(1024 * 1024 * 1024);
  fs_info.set_recycle_time_hour(24);
  *fs_info.mutable_extra()->mutable_s3_info() = CreateS3Info();
  auto* partition_policy = fs_info.mutable_partition_policy();
  partition_policy->set_type(partition_type);

  if (partition_type ==
      ::dingofs::pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    partition_policy->mutable_mono()->set_mds_id(kMdsId);

  } else {
    auto* hash = partition_policy->mutable_parent_hash();
    hash->set_bucket_num(8);
    pb::mds::HashPartition::BucketSet bucket_set;
    for (int i = 0; i < hash->bucket_num(); ++i) {
      bucket_set.add_bucket_ids(0);
    }
    (*hash->mutable_distributions())[kMdsId] = bucket_set;
  }

  return fs_info;
}

// test FileSystemSet
class FileSystemSetTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto coordinator_client = DummyCoordinatorClient::New();
    ASSERT_TRUE(coordinator_client->Init(""))
        << "init coordinator client fail.";

    auto fs_id_generator = CoorAutoIncrementIdGenerator::New(
        coordinator_client, kFsAutoIncrementIdName, kFsTableId, 20000, 8);
    ASSERT_TRUE(fs_id_generator->Init()) << "init fs id generator fail.";

    auto slice_id_generator = CoorAutoIncrementIdGenerator::NewShare(
        coordinator_client, kSliceAutoIncrementIdName, kSliceTableId, 20001, 8);
    ASSERT_TRUE(slice_id_generator->Init()) << "init fs id generator fail.";

    auto kv_storage = DummyStorage::New();
    ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

    operation_processor = OperationProcessor::New(kv_storage);
    ASSERT_TRUE(operation_processor->Init()) << "init mutation merger fail.";

    MDSMeta mds_meta;
    mds_meta.SetID(kMdsId);
    mds_meta.SetHost("127.0.0.1");
    mds_meta.SetPort(6666);
    mds_meta.SetState(MDSMeta::State::kInit);

    auto mds_meta_map = MDSMetaMap::New();
    fs_set = FileSystemSet::New(kv_storage, mds_meta, mds_meta_map,
                                operation_processor, nullptr);
    ASSERT_TRUE(fs_set->Init()) << "init fs set fail.";
  }

  static void TearDownTestSuite() {
    fs_set = nullptr;

    if (operation_processor != nullptr) {
      operation_processor->Stop();
      operation_processor = nullptr;
    }
  }

  void SetUp() override {}
  void TearDown() override {}

 public:
  static FileSystemSetSPtr fs_set;
  static OperationProcessorSPtr operation_processor;

  static FileSystemSetSPtr FsSet() { return fs_set; }
};

FileSystemSetSPtr FileSystemSetTest::fs_set = nullptr;
OperationProcessorSPtr FileSystemSetTest::operation_processor = nullptr;

// test FileSystem
class FileSystemTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto coordinator_client = DummyCoordinatorClient::New();
    ASSERT_TRUE(coordinator_client->Init(""))
        << "init coordinator client fail.";

    auto kv_storage = DummyStorage::New();
    ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

    // KV-backed id generators work with DummyStorage.
    auto inode_id_generator = NewInodeIdGenerator(1000, kv_storage);
    ASSERT_TRUE(inode_id_generator->Init()) << "init inode id generator fail.";

    auto slice_id_generator = NewSliceIdGenerator(kv_storage);
    ASSERT_TRUE(slice_id_generator->Init()) << "init slice id generator fail.";

    operation_processor = OperationProcessor::New(kv_storage);
    ASSERT_TRUE(operation_processor->Init()) << "init mutation merger fail.";

    // quota worker set (needed to avoid nullptr crash in AsyncUpdateDirUsage).
    auto quota_worker_set = SimpleWorkerSet::New(
        "fs_test_quota", 1, 1024, false, /*is_inplace_run=*/true);
    ASSERT_TRUE(quota_worker_set->Init()) << "init quota worker set fail.";

    pb::mds::FsInfo fs_info = CreateFsInfo(
        1000, "test_fs", pb::mds::PartitionType::MONOLITHIC_PARTITION);

    fs = FileSystem::New(kMdsId, FsInfo::New(fs_info),
                         std::move(inode_id_generator), slice_id_generator,
                         operation_processor, nullptr, nullptr,
                         quota_worker_set, quota_worker_set, nullptr);
    auto status = fs->CreateRoot();
    ASSERT_TRUE(status.ok())
        << "create root fail, error: " << status.error_str();
  }

  static void TearDownTestSuite() {
    fs = nullptr;

    if (operation_processor != nullptr) {
      operation_processor->Stop();
      operation_processor = nullptr;
    }
  }

  void SetUp() override {}
  void TearDown() override {}

 public:
  static FileSystemSPtr fs;
  static OperationProcessorSPtr operation_processor;

  static FileSystemSPtr Fs() { return fs; }
};

FileSystemSPtr FileSystemTest::fs = nullptr;
OperationProcessorSPtr FileSystemTest::operation_processor = nullptr;

TEST_F(FileSystemSetTest, CreateFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_id = 0;
  param.fs_name = "test_fs_for_create";
  param.block_size = 1024 * 1024;
  param.chunk_size = 64 * 1024 * 1024;
  param.fs_type = pb::mds::FsType::S3;
  param.partition_type = pb::mds::PartitionType::MONOLITHIC_PARTITION;
  param.candidate_mds_ids = {kMdsId};
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_dir_stats = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  pb::mds::FsInfo fs_info;
  auto status = fs_set->CreateFs(param, fs_info);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();

  int64_t fs_id = fs_info.fs_id();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  ASSERT_TRUE(fs_set->GetFileSystem(fs_id) != nullptr) << "get fs fail.";
}

TEST_F(FileSystemSetTest, GetFsInfo) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_id = 0;
  param.fs_name = "test_fs_for_get";
  param.block_size = 1024 * 1024;
  param.chunk_size = 64 * 1024 * 1024;
  param.fs_type = pb::mds::FsType::S3;
  param.partition_type = pb::mds::PartitionType::MONOLITHIC_PARTITION;
  param.candidate_mds_ids = {kMdsId};
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_dir_stats = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  pb::mds::FsInfo fs_info;
  auto status = fs_set->CreateFs(param, fs_info);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  int64_t fs_id = fs_info.fs_id();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
  ASSERT_EQ(fs_info.fs_name(), param.fs_name) << "fs name not equal.";
  ASSERT_EQ(fs_info.fs_type(), param.fs_type) << "fs type not equal.";
  ASSERT_EQ(fs_info.block_size(), param.block_size) << "block size not equal.";
  ASSERT_EQ(fs_info.enable_dir_stats(), param.enable_dir_stats)
      << "enable sum in dir not equal.";
  ASSERT_EQ(fs_info.owner(), param.owner) << "owner not equal.";
  ASSERT_EQ(fs_info.capacity(), param.capacity) << "capacity not equal.";
  ASSERT_EQ(fs_info.recycle_time_hour(), param.recycle_time_hour)
      << "recycle time hour not equal.";
}

TEST_F(FileSystemSetTest, DeleteFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_id = 0;
  param.fs_name = "test_fs_for_delete";
  param.block_size = 1024 * 1024;
  param.chunk_size = 64 * 1024 * 1024;
  param.fs_type = pb::mds::FsType::S3;
  param.partition_type = pb::mds::PartitionType::MONOLITHIC_PARTITION;
  param.candidate_mds_ids = {kMdsId};
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_dir_stats = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  pb::mds::FsInfo fs_info;
  auto status = fs_set->CreateFs(param, fs_info);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();

  int64_t fs_id = fs_info.fs_id();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  Context ctx;
  status = fs_set->DeleteFs(ctx, param.fs_name, false);
  ASSERT_TRUE(status.ok()) << "delete fs fail, error: " << status.error_str();

  ASSERT_EQ(nullptr, fs_set->GetFileSystem(fs_id));

  status = fs_set->GetFsInfo(ctx, param.fs_name, fs_info);
  ASSERT_TRUE(pb::error::ENOT_FOUND == status.error_code())
      << "not should found fs, error: " << status.error_str();
}

TEST_F(FileSystemSetTest, MountFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_id = 0;
  param.fs_name = "test_fs_for_mount";
  param.block_size = 1024 * 1024;
  param.chunk_size = 64 * 1024 * 1024;
  param.fs_type = pb::mds::FsType::S3;
  param.partition_type = pb::mds::PartitionType::MONOLITHIC_PARTITION;
  param.candidate_mds_ids = {kMdsId};
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_dir_stats = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  pb::mds::FsInfo fs_info;
  auto status = fs_set->CreateFs(param, fs_info);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();

  int64_t fs_id = fs_info.fs_id();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  Context ctx;
  pb::mds::MountPoint mount_point;
  mount_point.set_client_id("00000000-0000-0000-0000-000000000002");
  mount_point.set_hostname("localhost");
  mount_point.set_port(8080);
  mount_point.set_path("/mnt/dingofs");
  mount_point.set_cto(true);
  status = fs_set->MountFs(ctx, param.fs_name, mount_point);
  ASSERT_TRUE(status.ok()) << "mount fs fail, error: " << status.error_str();

  status = fs_set->GetFsInfo(ctx, param.fs_name, fs_info);
  ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
  ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
  ASSERT_EQ(1, fs_info.mount_points_size()) << "mount point size not equal.";
  auto actual_mount_point = fs_info.mount_points(0);
  ASSERT_EQ(mount_point.hostname(), actual_mount_point.hostname())
      << "hostname not equal.";
  ASSERT_EQ(mount_point.port(), actual_mount_point.port()) << "port not equal.";
  ASSERT_EQ(mount_point.path(), actual_mount_point.path()) << "path not equal.";
  ASSERT_EQ(mount_point.cto(), actual_mount_point.cto()) << "cto not equal.";
}

TEST_F(FileSystemSetTest, UnMountFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_id = 0;
  param.fs_name = "test_fs_for_unmount";
  param.block_size = 1024 * 1024;
  param.chunk_size = 64 * 1024 * 1024;
  param.fs_type = pb::mds::FsType::S3;
  param.partition_type = pb::mds::PartitionType::MONOLITHIC_PARTITION;
  param.candidate_mds_ids = {kMdsId};
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_dir_stats = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  pb::mds::FsInfo fs_info;
  auto status = fs_set->CreateFs(param, fs_info);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();

  int64_t fs_id = fs_info.fs_id();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  Context ctx;
  pb::mds::MountPoint mount_point;
  mount_point.set_client_id("00000000-0000-0000-0000-000000000001");
  mount_point.set_hostname("localhost");
  mount_point.set_port(8080);
  mount_point.set_path("/mnt/dingofs");
  mount_point.set_cto(true);
  status = fs_set->MountFs(ctx, param.fs_name, mount_point);
  ASSERT_TRUE(status.ok()) << "mount fs fail, error: " << status.error_str();

  status = fs_set->GetFsInfo(ctx, param.fs_name, fs_info);
  ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
  ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
  ASSERT_EQ(1, fs_info.mount_points_size()) << "mount point size not equal.";
  auto actual_mount_point = fs_info.mount_points(0);
  ASSERT_EQ(mount_point.hostname(), actual_mount_point.hostname())
      << "hostname not equal.";
  ASSERT_EQ(mount_point.port(), actual_mount_point.port()) << "port not equal.";
  ASSERT_EQ(mount_point.path(), actual_mount_point.path()) << "path not equal.";
  ASSERT_EQ(mount_point.cto(), actual_mount_point.cto()) << "cto not equal.";

  status = fs_set->UmountFs(ctx, param.fs_name, mount_point.client_id());
  ASSERT_TRUE(status.ok()) << "unmount fs fail, error: " << status.error_str();

  {
    pb::mds::FsInfo fs_info;
    status = fs_set->GetFsInfo(ctx, param.fs_name, fs_info);
    ASSERT_TRUE(status.ok())
        << "get fs info fail, error: " << status.error_str();
    ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
    ASSERT_EQ(0, fs_info.mount_points_size()) << "mount point size not equal.";
  }
}

TEST_F(FileSystemTest, CreateRoot) {
  auto fs = Fs();

  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  auto partition = partition_cache.Get(kRootIno);
  ASSERT_TRUE(partition != nullptr);

  auto inode = inode_cache.Get(kRootIno);
  ASSERT_TRUE(inode != nullptr);
  ASSERT_EQ(kRootIno, inode->Ino());
  ASSERT_EQ(pb::mds::FileType::DIRECTORY, inode->Type());
}

TEST_F(FileSystemTest, MkNod) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "mkdir_file";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 1;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

  auto partition = partition_cache.Get(param.parent);
  ASSERT_TRUE(partition != nullptr) << "get partition fail.";

  Dentry dentry;
  status = partition->Get(param.name, dentry);
  ASSERT_TRUE(status.ok()) << "get child fail, error: " << status.error_str();
  ASSERT_EQ(param.name, dentry.Name()) << "dentry name not equal.";
  ASSERT_EQ(param.parent, dentry.ParentIno()) << "dentry parent ino not equal.";

  InodeSPtr inode = inode_cache.Get(entry_out.attr.ino());
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";
  ASSERT_EQ(param.mode, inode->Mode()) << "inode mode not equal.";
  ASSERT_EQ(param.uid, inode->Uid()) << "inode uid not equal.";
  ASSERT_EQ(param.gid, inode->Gid()) << "inode gid not equal.";
  ASSERT_EQ(pb::mds::FileType::FILE, inode->Type()) << "inode type not equal.";
  ASSERT_EQ(0, inode->Length()) << "inode length not equal.";
  ASSERT_EQ(param.rdev, inode->Rdev()) << "inode rdev not equal.";
}

TEST_F(FileSystemTest, MkNodDuplicate) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "duplicate_file";

  EntryWithPaOut entry_out;
  ASSERT_TRUE(fs->MkNod(ctx, param, entry_out).ok());
  const auto first_ino = entry_out.attr.ino();

  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_EQ(pb::error::EEXISTED, status.error_code());
  ASSERT_EQ(first_ino, entry_out.attr.ino());
}

TEST_F(FileSystemTest, MkDir) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkDirParam param;
  param.parent = kRootIno;
  param.name = "mkdir_dir";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 0;

  EntryWithPaOut entry_out;
  auto status = fs->MkDir(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

  auto partition = partition_cache.Get(param.parent);
  ASSERT_TRUE(partition != nullptr) << "get partition fail.";

  Dentry dentry;
  status = partition->Get(param.name, dentry);
  ASSERT_TRUE(status.ok()) << "get child fail, error: " << status.error_str();
  ASSERT_EQ(param.name, dentry.Name()) << "dentry name not equal.";
  ASSERT_EQ(param.parent, dentry.ParentIno()) << "dentry parent ino not equal.";

  InodeSPtr inode = inode_cache.Get(entry_out.attr.ino());
  ASSERT_TRUE(status.ok()) << "get inode fail, error: " << status.error_str();
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";
  ASSERT_EQ(S_IFDIR | param.mode, inode->Mode()) << "inode mode not equal.";
  ASSERT_EQ(param.uid, inode->Uid()) << "inode uid not equal.";
  ASSERT_EQ(param.gid, inode->Gid()) << "inode gid not equal.";
  ASSERT_EQ(pb::mds::FileType::DIRECTORY, inode->Type())
      << "inode type not equal.";
  ASSERT_EQ(4096, inode->Length()) << "inode length not equal.";
  ASSERT_EQ(param.rdev, inode->Rdev()) << "inode rdev not equal.";
}

TEST_F(FileSystemTest, MkDirDuplicate) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam param;
  param.parent = kRootIno;
  param.name = "duplicate_dir";

  EntryWithPaOut entry_out;
  ASSERT_TRUE(fs->MkDir(ctx, param, entry_out).ok());
  const auto first_ino = entry_out.attr.ino();

  auto status = fs->MkDir(ctx, param, entry_out);
  ASSERT_EQ(pb::error::EEXISTED, status.error_code());
  ASSERT_EQ(first_ino, entry_out.attr.ino());
}

TEST_F(FileSystemTest, CreateInheritsSetgidParent) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkDirParam parent_param;
  parent_param.parent = kRootIno;
  parent_param.name = "setgid_parent";
  parent_param.mode = 02777;
  parent_param.uid = 1;
  parent_param.gid = 123;

  EntryWithPaOut parent_out;
  ASSERT_TRUE(fs->MkDir(ctx, parent_param, parent_out).ok());
  ASSERT_NE(parent_out.attr.mode() & S_ISGID, 0);

  FileSystem::MkDirParam dir_param;
  dir_param.parent = parent_out.attr.ino();
  dir_param.name = "dir";
  dir_param.mode = 0000;
  dir_param.uid = 1;
  dir_param.gid = 10000;

  EntryWithPaOut dir_out;
  ASSERT_TRUE(fs->MkDir(ctx, dir_param, dir_out).ok());
  EXPECT_EQ(dir_out.attr.gid(), parent_param.gid);
  EXPECT_NE(dir_out.attr.mode() & S_ISGID, 0);

  FileSystem::MkNodParam node_param;
  node_param.parent = parent_out.attr.ino();
  node_param.name = "node";
  node_param.mode = S_IFREG;
  node_param.uid = 1;
  node_param.gid = 10000;

  EntryWithPaOut node_out;
  ASSERT_TRUE(fs->MkNod(ctx, node_param, node_out).ok());
  EXPECT_EQ(node_out.attr.gid(), parent_param.gid);

  node_param.name = "created";
  node_param.session_id = "setgid-test-session";
  EntriesWithPaOut create_out;
  ASSERT_TRUE(
      fs->BatchCreate(ctx, parent_out.attr.ino(), {node_param}, create_out)
          .ok());
  ASSERT_EQ(create_out.attrs.size(), 1);
  EXPECT_EQ(create_out.attrs.front().gid(), parent_param.gid);
}

TEST_F(FileSystemTest, RmDir) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkDirParam param;
  param.parent = kRootIno;
  param.name = "rmdir_dir";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 0;

  EntryWithPaOut entry_out;
  auto status = fs->MkDir(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";
  int64_t ino = entry_out.attr.ino();

  auto partition = partition_cache.Get(param.parent);
  ASSERT_TRUE(partition != nullptr) << "get dentry fail.";

  Dentry dentry;
  status = partition->Get(param.name, dentry);
  ASSERT_TRUE(status.ok()) << "get child fail, error: " << status.error_str();
  ASSERT_EQ(param.name, dentry.Name()) << "dentry name not equal.";
  ASSERT_EQ(param.parent, dentry.ParentIno()) << "dentry parent ino not equal.";

  InodeSPtr inode = inode_cache.Get(ino);
  ASSERT_TRUE(status.ok()) << "get inode fail, error: " << status.error_str();
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";

  {
    EntryWithPaOut entry_out;
    status = fs->RmDir(ctx, param.parent, param.name, entry_out);
    ASSERT_TRUE(status.ok())
        << "remove dir fail, error: " << status.error_str();
    Ino ino = entry_out.attr.ino();

    auto partition = partition_cache.Get(ino);
    ASSERT_TRUE(partition == nullptr) << "get partition fail.";

    InodeSPtr inode = inode_cache.Get(ino);
    ASSERT_TRUE(inode == nullptr) << "get inode fail.";
  }
}

TEST_F(FileSystemTest, Link) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "link_file_source";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 1;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

  InodeSPtr inode = inode_cache.Get(entry_out.attr.ino());
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";

  {
    EntryWithPaOut entry_out;
    auto status =
        fs->Link(ctx, inode->Ino(), kRootIno, "link_file_hardlink", entry_out);
    ASSERT_TRUE(status.ok()) << "link fail, error: " << status.error_str();
    ASSERT_EQ(inode->Ino(), entry_out.attr.ino()) << "ino is invalid.";
    ASSERT_EQ(2, inode->Nlink()) << "nlink not equal.";
  }
}

TEST_F(FileSystemTest, RmDirNotFound) {
  auto fs = Fs();
  Context ctx;
  EntryWithPaOut entry_out;

  auto status = fs->RmDir(ctx, kRootIno, "missing_dir", entry_out);
  ASSERT_EQ(pb::error::ENOT_FOUND, status.error_code());
}

TEST_F(FileSystemTest, UnLink) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "unlink_file_source";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 1;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

  InodeSPtr inode = inode_cache.Get(entry_out.attr.ino());
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";

  {
    EntryWithPaOut entry_out;
    auto status = fs->Link(ctx, inode->Ino(), kRootIno, "unlink_file_hardlink",
                           entry_out);
    ASSERT_TRUE(status.ok()) << "link fail, error: " << status.error_str();
    ASSERT_EQ(inode->Ino(), entry_out.attr.ino()) << "ino is invalid.";
    ASSERT_EQ(2, inode->Nlink()) << "nlink not equal.";

    status = fs->UnLink(ctx, kRootIno, "unlink_file_hardlink", entry_out);
    ASSERT_TRUE(status.ok()) << "link fail, error: " << status.error_str();
    ASSERT_EQ(1, inode->Nlink()) << "nlink not equal.";
  }
}

TEST_F(FileSystemTest, UnLinkNotFound) {
  auto fs = Fs();
  Context ctx;
  EntryWithPaOut entry_out;

  auto status = fs->UnLink(ctx, kRootIno, "missing_file", entry_out);
  ASSERT_EQ(pb::error::ENOT_FOUND, status.error_code());
}

TEST_F(FileSystemTest, LinkDuplicate) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "duplicate_hardlink_source";

  EntryWithPaOut source_out;
  ASSERT_TRUE(fs->MkNod(ctx, param, source_out).ok());

  const std::string link_name = "duplicate_hardlink";
  EntryWithPaOut entry_out;
  ASSERT_TRUE(
      fs->Link(ctx, source_out.attr.ino(), kRootIno, link_name, entry_out)
          .ok());
  const auto first_ino = entry_out.attr.ino();

  auto status =
      fs->Link(ctx, source_out.attr.ino(), kRootIno, link_name, entry_out);
  ASSERT_EQ(pb::error::EEXISTED, status.error_code());
  ASSERT_EQ(first_ino, entry_out.attr.ino());
}

TEST_F(FileSystemTest, SymlinkDuplicate) {
  auto fs = Fs();
  Context ctx;
  const std::string link_name = "duplicate_symlink";

  EntryWithPaOut entry_out;
  ASSERT_TRUE(
      fs->Symlink(ctx, "/target", kRootIno, link_name, 1, 1, entry_out).ok());
  const auto first_ino = entry_out.attr.ino();

  auto status =
      fs->Symlink(ctx, "/target", kRootIno, link_name, 1, 1, entry_out);
  ASSERT_EQ(pb::error::EEXISTED, status.error_code());
  ASSERT_EQ(first_ino, entry_out.attr.ino());
}

TEST_F(FileSystemTest, SymlinkWithFile) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "symlink_with_file";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 1;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

  InodeSPtr inode = inode_cache.Get(entry_out.attr.ino());
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";

  {
    std::string symlink = fmt::format("/{}", param.name);
    std::string name = "symlink_file_link";
    EntryWithPaOut entry_out;
    auto status = fs->Symlink(ctx, symlink, kRootIno, name, 1, 3, entry_out);
    ASSERT_TRUE(status.ok())
        << "create symlink fail, error: " << status.error_str();
    ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

    InodeSPtr sym_inode = inode_cache.Get(entry_out.attr.ino());
    ASSERT_TRUE(sym_inode != nullptr) << "get inode fail.";
    ASSERT_EQ(pb::mds::FileType::SYM_LINK, sym_inode->Type())
        << "inode type not equal.";
    ASSERT_EQ(symlink, sym_inode->Symlink());
  }
}

TEST_F(FileSystemTest, SymlinkWithDir) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkDirParam param;
  param.parent = kRootIno;
  param.name = "symlink_with_dir";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 1;

  EntryWithPaOut entry_out;
  auto status = fs->MkDir(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

  InodeSPtr inode = inode_cache.Get(entry_out.attr.ino());
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";

  {
    std::string symlink = fmt::format("/{}", param.name);
    std::string name = "symlinkwithdir";
    EntryWithPaOut entry_out;
    auto status = fs->Symlink(ctx, symlink, kRootIno, name, 1, 3, entry_out);
    ASSERT_TRUE(status.ok())
        << "create symlink fail, error: " << status.error_str();
    ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

    InodeSPtr sym_inode = inode_cache.Get(entry_out.attr.ino());
    ASSERT_TRUE(sym_inode != nullptr) << "get inode fail.";
    ASSERT_EQ(pb::mds::FileType::SYM_LINK, sym_inode->Type())
        << "inode type not equal.";
    ASSERT_EQ(symlink, sym_inode->Symlink());
  }
}

TEST_F(FileSystemTest, ReadLink) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "readlink_file";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 1;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

  InodeSPtr inode = inode_cache.Get(entry_out.attr.ino());
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";

  {
    std::string symlink = fmt::format("/{}", param.name);
    std::string name = "symlinkwithfile";
    EntryWithPaOut entry_out;
    auto status = fs->Symlink(ctx, symlink, kRootIno, name, 1, 3, entry_out);
    ASSERT_TRUE(status.ok())
        << "create symlink fail, error: " << status.error_str();
    ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

    std::string actual_link;
    status = fs->ReadLink(ctx, entry_out.attr.ino(), actual_link);
    ASSERT_TRUE(status.ok())
        << "read symlink fail, error: " << status.error_str();
    ASSERT_EQ(symlink, actual_link) << "symlink is eq.";
  }
}

TEST_F(FileSystemTest, SetXAttr) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "set_xattr_file";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 1;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

  InodeSPtr inode = inode_cache.Get(entry_out.attr.ino());
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";

  Inode::XAttrMap xattr;
  xattr.insert({"key3", "value3"});
  xattr.insert({"key4", "value4"});
  EntryOut xattr_out;
  status = fs->SetXAttr(ctx, inode->Ino(), xattr, xattr_out);
  ASSERT_TRUE(status.ok()) << "set xattr fail, error: " << status.error_str();
}

TEST_F(FileSystemTest, GetXAttr) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  FileSystem::MkNodParam param;
  param.parent = kRootIno;
  param.name = "get_xattr_file";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 1;

  EntryWithPaOut entry_out;
  auto status = fs->MkNod(ctx, param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

  InodeSPtr inode = inode_cache.Get(entry_out.attr.ino());
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";

  Inode::XAttrMap xattr;
  xattr.insert({"key3", "value3"});
  xattr.insert({"key4", "value4"});
  EntryOut xattr_out;
  status = fs->SetXAttr(ctx, inode->Ino(), xattr, xattr_out);
  ASSERT_TRUE(status.ok()) << "set xattr fail, error: " << status.error_str();

  Inode::XAttrMap actual_xattr;
  status = fs->GetXAttr(ctx, inode->Ino(), actual_xattr);
  ASSERT_TRUE(status.ok()) << "get xattr fail, error: " << status.error_str();
  ASSERT_EQ(xattr.size(), actual_xattr.size()) << "xattr not equal.";

  std::string value;
  status = fs->GetXAttr(ctx, inode->Ino(), "key3", value);
  ASSERT_TRUE(status.ok()) << "get xattr fail, error: " << status.error_str();
  ASSERT_EQ("value3", value) << "xattr value not equal.";
}

// /
// |--dir1
// |  |--file1
// ======= after =====
// /
// |--dir1
// |  |--file2
// rename dir1/file1 to dir1/file2
TEST_F(FileSystemTest, RenameWithSameDir) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  uint64_t old_parent_ino;
  std::string old_name = "rename1_file1";
  {
    FileSystem::MkDirParam param;
    param.parent = kRootIno;
    param.name = "rename1_dir1";
    param.mode = 0777;
    param.uid = 1;
    param.gid = 3;
    param.rdev = 1;

    EntryWithPaOut entry_out;
    auto status = fs->MkDir(ctx, param, entry_out);
    ASSERT_TRUE(status.ok())
        << "create file fail, error: " << status.error_str();
    ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

    old_parent_ino = entry_out.attr.ino();
  }

  {
    FileSystem::MkNodParam param;
    param.parent = old_parent_ino;
    param.name = old_name;
    param.mode = 0777;
    param.uid = 1;
    param.gid = 3;
    param.rdev = 1;

    EntryWithPaOut entry_out;
    auto status = fs->MkNod(ctx, param, entry_out);
    ASSERT_TRUE(status.ok())
        << "create file fail, error: " << status.error_str();
    ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";
  }

  std::string new_name = "rename1_file2";
  uint64_t old_parent_version;
  uint64_t new_parent_version;

  FileSystem::RenameParam param;
  param.old_parent = old_parent_ino;
  param.old_name = old_name;
  param.new_parent = old_parent_ino;
  param.new_name = new_name;

  FileSystem::RenameResult rename_result;
  auto status = fs->Rename(ctx, param, rename_result);
  ASSERT_TRUE(status.ok()) << "rename fail, error: " << status.error_str();

  std::vector<EntryWithNameOut> entries;
  ASSERT_TRUE(fs->ReadDir(ctx, old_parent_ino, "", 0, false, entries).ok());
  auto partition = partition_cache.Get(old_parent_ino);
  ASSERT_TRUE(partition != nullptr);
  Dentry dentry;
  status = partition->Get(old_name, dentry);
  ASSERT_FALSE(status.ok())
      << "get old child fail, error: " << status.error_str();
  status = partition->Get(new_name, dentry);
  ASSERT_TRUE(status.ok()) << "get new child fail, error: "
                           << status.error_str();
  ASSERT_EQ(new_name, dentry.Name());
  ASSERT_EQ(old_parent_ino, dentry.ParentIno());
}

// /
// |--dir1
// |  |--file1
// |--dir2
// ======= after =====
// /
// |--dir1
// |--dir2
// |  |--file1
// rename dir1/file1 to dir2/file1
TEST_F(FileSystemTest, RenameWithDiffDir) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  Context ctx;

  uint64_t old_parent_ino;
  std::string old_name = "rename2_file01";
  {
    FileSystem::MkDirParam param;
    param.parent = kRootIno;
    param.name = "rename2_dir1";
    param.mode = 0777;
    param.uid = 1;
    param.gid = 3;
    param.rdev = 1;

    EntryWithPaOut entry_out;
    auto status = fs->MkDir(ctx, param, entry_out);
    ASSERT_TRUE(status.ok())
        << "create file fail, error: " << status.error_str();
    ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

    old_parent_ino = entry_out.attr.ino();
  }

  {
    FileSystem::MkNodParam param;
    param.parent = old_parent_ino;
    param.name = old_name;
    param.mode = 0777;
    param.uid = 1;
    param.gid = 3;
    param.rdev = 1;

    EntryWithPaOut entry_out;
    auto status = fs->MkNod(ctx, param, entry_out);
    ASSERT_TRUE(status.ok())
        << "create file fail, error: " << status.error_str();
    ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";
  }

  uint64_t new_parent_ino;
  {
    FileSystem::MkDirParam param;
    param.parent = kRootIno;
    param.name = "rename2_dir2";
    param.mode = 0777;
    param.uid = 1;
    param.gid = 3;
    param.rdev = 1;

    EntryWithPaOut entry_out;
    auto status = fs->MkDir(ctx, param, entry_out);
    ASSERT_TRUE(status.ok())
        << "create file fail, error: " << status.error_str();
    ASSERT_GT(entry_out.attr.ino(), 0) << "ino is invalid.";

    new_parent_ino = entry_out.attr.ino();
  }

  uint64_t old_parent_version;
  uint64_t new_parent_version;
  const std::string& new_name = old_name;

  FileSystem::RenameParam param;
  param.old_parent = old_parent_ino;
  param.old_name = old_name;
  param.new_parent = new_parent_ino;
  param.new_name = new_name;

  FileSystem::RenameResult rename_result;
  auto status = fs->Rename(ctx, param, rename_result);
  ASSERT_TRUE(status.ok()) << "rename fail, error: " << status.error_str();

  {
    std::vector<EntryWithNameOut> entries;
    ASSERT_TRUE(fs->ReadDir(ctx, old_parent_ino, "", 0, false, entries).ok());
    auto partition = partition_cache.Get(old_parent_ino);
    ASSERT_TRUE(partition != nullptr);
    Dentry dentry;
    auto status = partition->Get(old_name, dentry);
    ASSERT_FALSE(status.ok())
        << "get old child fail, error: " << status.error_str();
  }

  {
    std::vector<EntryWithNameOut> entries;
    ASSERT_TRUE(fs->ReadDir(ctx, new_parent_ino, "", 0, false, entries).ok());
    auto partition = partition_cache.Get(new_parent_ino);
    ASSERT_TRUE(partition != nullptr);
    Dentry dentry;
    auto status = partition->Get(new_name, dentry);
    ASSERT_TRUE(status.ok())
        << "get new child fail, error: " << status.error_str();
    ASSERT_EQ(new_name, dentry.Name());
    ASSERT_EQ(new_parent_ino, dentry.ParentIno());
  }
}

TEST_F(FileSystemSetTest, CreateFs_PropagatesEnableUidGidMapTrue) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "uidgid-on";
  param.fs_id =
      30001;  // explicit id to avoid auto-increment and id-reuse issues
  param.block_size = 1 << 20;
  param.chunk_size = 64 << 20;
  param.fs_type = pb::mds::FsType::S3;
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.owner = "tester";
  param.capacity = 1ULL << 30;
  param.partition_type = pb::mds::PartitionType::MONOLITHIC_PARTITION;
  param.candidate_mds_ids = {
      kMdsId};  // avoid random-select crash on empty mds_meta_map
  param.enable_uid_gid_map = true;

  pb::mds::FsInfo fs_info;
  auto status = fs_set->CreateFs(param, fs_info);
  ASSERT_TRUE(status.ok()) << "create fs fail: " << status.error_str();
  EXPECT_TRUE(fs_info.enable_uid_gid_map());
}

TEST_F(FileSystemSetTest, CreateFs_PropagatesEnableUidGidMapDefaultFalse) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "uidgid-off";
  param.fs_id =
      30002;  // explicit id to avoid auto-increment and id-reuse issues
  param.block_size = 1 << 20;
  param.chunk_size = 64 << 20;
  param.fs_type = pb::mds::FsType::S3;
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.owner = "tester";
  param.capacity = 1ULL << 30;
  param.partition_type = pb::mds::PartitionType::MONOLITHIC_PARTITION;
  param.candidate_mds_ids = {
      kMdsId};  // avoid random-select crash on empty mds_meta_map
  // enable_uid_gid_map intentionally left default (false)

  pb::mds::FsInfo fs_info;
  auto status = fs_set->CreateFs(param, fs_info);
  ASSERT_TRUE(status.ok()) << "create fs fail: " << status.error_str();
  EXPECT_FALSE(fs_info.enable_uid_gid_map());
}

TEST_F(FileSystemTest, CalcDirStat) {
  auto fs = Fs();
  Context ctx;
  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "calc_d";
  dp.mode = 0777;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  auto mkdir_status = fs->MkDir(ctx, dp, dout);
  ASSERT_TRUE(mkdir_status.ok()) << "mkdir fail: " << mkdir_status.error_str();
  ASSERT_GT(dout.attr.ino(), 0) << "mkdir ino invalid";
  Ino d = dout.attr.ino();

  auto mk = [&](const std::string& name, uint64_t len) {
    FileSystem::MkNodParam p;
    p.parent = d;
    p.name = name;
    p.mode = 0644;
    p.uid = 1;
    p.gid = 1;
    p.rdev = 0;
    EntryWithPaOut o;
    auto mn = fs->MkNod(ctx, p, o);
    EXPECT_TRUE(mn.ok()) << "mknod fail: " << mn.error_str();
    if (len > 0) {
      FileSystem::FlushFileParam fp;
      fp.length = len;
      EntryWithFileChangeOut fo;
      auto sa = fs->FlushFile(ctx, o.attr.ino(), fp, fo);
      EXPECT_TRUE(sa.ok()) << "flushfile fail: " << sa.error_str();
    }
  };
  mk("f1", 1000);
  mk("f2", 5000);

  DirStatEntry st;
  ASSERT_TRUE(fs->CalcDirStat(ctx, d, st).ok());
  EXPECT_EQ(st.inodes(), 2);
  EXPECT_EQ(st.length(), 6000);
}

// A same-directory hardlink (`ln f hl`) puts two dentries on one inode, so the
// inode number repeats in the dentry scan. CalcDirStat must not hand BatchGet a
// duplicate inode key (the store txn rejects duplicates) and must still charge
// the inode's length once per dentry (matching `find`).
TEST_F(FileSystemTest, CalcDirStatSameDirHardlink) {
  auto fs = Fs();
  Context ctx;
  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "calc_hl_d";
  dp.mode = 0777;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  ASSERT_TRUE(fs->MkDir(ctx, dp, dout).ok());
  Ino d = dout.attr.ino();

  FileSystem::MkNodParam p;
  p.parent = d;
  p.name = "f";
  p.mode = 0644;
  p.uid = 1;
  p.gid = 1;
  p.rdev = 0;
  EntryWithPaOut o;
  ASSERT_TRUE(fs->MkNod(ctx, p, o).ok());
  Ino f = o.attr.ino();
  FileSystem::FlushFileParam fp;
  fp.length = 7777;
  EntryWithFileChangeOut fo;
  ASSERT_TRUE(fs->FlushFile(ctx, f, fp, fo).ok());

  // hardlink the file under a second name in the SAME directory.
  EntryWithPaOut lo;
  ASSERT_TRUE(fs->Link(ctx, f, d, "f_hl", lo).ok());

  DirStatEntry st;
  auto status = fs->CalcDirStat(ctx, d, st);
  ASSERT_TRUE(status.ok()) << "CalcDirStat fail: " << status.error_str();
  EXPECT_EQ(st.inodes(), 2);         // two dentries (f, f_hl)
  EXPECT_EQ(st.length(), 7777 * 2);  // length charged once per dentry
}

// ADR-0003: RollbackFile conditionally shrinks the length back to the flush
// checkpoint -- only when checkpoint < current length <= the session's last
// write length. Otherwise it is a no-op (conservative for concurrent writers).
TEST_F(FileSystemTest, RollbackFileConditionalShrink) {
  auto fs = Fs();
  Context ctx;

  FileSystem::MkNodParam p;
  p.parent = kRootIno;
  p.name = "rollback_f";
  p.mode = 0644;
  p.uid = 1;
  p.gid = 1;
  p.rdev = 0;
  EntryWithPaOut o;
  ASSERT_TRUE(fs->MkNod(ctx, p, o).ok());
  Ino f = o.attr.ino();

  // push length to 1000 (checkpoint), then to 5000 (this round's writes).
  auto flush_to = [&](uint64_t len) {
    FileSystem::FlushFileParam fp;
    fp.length = len;
    EntryWithFileChangeOut fo;
    ASSERT_TRUE(fs->FlushFile(ctx, f, fp, fo).ok());
  };
  flush_to(1000);
  flush_to(5000);

  // rollback: checkpoint=1000, last write=5000 -> shrink to 1000.
  {
    FileSystem::RollbackFileParam rp;
    rp.last_write_length = 5000;
    rp.rollback_to_length = 1000;
    EntryWithFileChangeOut fo;
    ASSERT_TRUE(fs->RollbackFile(ctx, f, rp, fo).ok());
    EXPECT_EQ(fo.attr.length(), 1000);
    EXPECT_TRUE(fo.shrink_file);
  }

  // repeated rollback is a no-op: current length (1000) == checkpoint.
  {
    FileSystem::RollbackFileParam rp;
    rp.last_write_length = 5000;
    rp.rollback_to_length = 1000;
    EntryWithFileChangeOut fo;
    ASSERT_TRUE(fs->RollbackFile(ctx, f, rp, fo).ok());
    EXPECT_EQ(fo.attr.length(), 1000);
    EXPECT_FALSE(fo.shrink_file);
  }

  // another writer pushed the length beyond this session's last write:
  // rollback must give up (no shrink).
  flush_to(9000);
  {
    FileSystem::RollbackFileParam rp;
    rp.last_write_length = 5000;  // this session's last write
    rp.rollback_to_length = 1000;
    EntryWithFileChangeOut fo;
    ASSERT_TRUE(fs->RollbackFile(ctx, f, rp, fo).ok());
    EXPECT_EQ(fo.attr.length(), 9000);
    EXPECT_FALSE(fo.shrink_file);
  }
}

TEST_F(FileSystemTest, UpdateDirStatOnWrite) {
  auto fs = Fs();
  ASSERT_TRUE(fs->EnableDirStats());
  Context ctx;

  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "upd_d";
  dp.mode = 0777;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  ASSERT_TRUE(fs->MkDir(ctx, dp, dout).ok());
  Ino d = dout.attr.ino();
  DrainDirStats(fs->GetDirStatManager());  // drain whatever accumulated so far

  // mknod a file under d: +1 inode, +0 length.
  FileSystem::MkNodParam p;
  p.parent = d;
  p.name = "g";
  p.mode = 0644;
  p.uid = 1;
  p.gid = 1;
  p.rdev = 0;
  EntryWithPaOut o;
  ASSERT_TRUE(fs->MkNod(ctx, p, o).ok());
  {
    auto deltas = DrainDirStats(fs->GetDirStatManager());
    ASSERT_TRUE(deltas.count(d));
    EXPECT_EQ(deltas[d].inodes, 1);
    EXPECT_EQ(deltas[d].length, 0);
  }

  // set g's length to 5000 via FlushFile (a wired length-change site):
  // length 0->5000 => +5000 length, inodes unchanged.
  {
    FileSystem::FlushFileParam fp;
    fp.length = 5000;
    EntryWithFileChangeOut fo;
    ASSERT_TRUE(fs->FlushFile(ctx, o.attr.ino(), fp, fo).ok());
    auto deltas = DrainDirStats(fs->GetDirStatManager());
    ASSERT_TRUE(deltas.count(d));
    EXPECT_EQ(deltas[d].inodes, 0);
    EXPECT_EQ(deltas[d].length, 5000);
  }

  // unlink g (file length now 5000): -1 inode, -5000 length.
  {
    EntryWithPaOut uo;
    ASSERT_TRUE(fs->UnLink(ctx, d, "g", uo).ok());
    auto deltas = DrainDirStats(fs->GetDirStatManager());
    ASSERT_TRUE(deltas.count(d));
    EXPECT_EQ(deltas[d].inodes, -1);
    EXPECT_EQ(deltas[d].length, -5000);
  }

  // mkdir a subdir under d then rmdir it: subdir create gives d +1 inode/+0
  // length, rmdir gives d -1 inode/-0 length. Drain after create so the rmdir
  // delta is isolated.
  {
    FileSystem::MkDirParam sp;
    sp.parent = d;
    sp.name = "sub";
    sp.mode = 0777;
    sp.uid = 1;
    sp.gid = 1;
    sp.rdev = 0;
    EntryWithPaOut so;
    ASSERT_TRUE(fs->MkDir(ctx, sp, so).ok());
    {
      auto deltas = DrainDirStats(fs->GetDirStatManager());
      ASSERT_TRUE(deltas.count(d));
      EXPECT_EQ(deltas[d].inodes, 1);
      EXPECT_EQ(deltas[d].length, 0);
    }
    Ino sub_ino = 0;
    EntryWithPaOut ro;
    ASSERT_TRUE(fs->RmDir(ctx, d, "sub", ro).ok());
    sub_ino = ro.attr.ino();
    auto deltas = DrainDirStats(fs->GetDirStatManager());
    ASSERT_TRUE(deltas.count(d));
    EXPECT_EQ(deltas[d].inodes, -1);
    EXPECT_EQ(deltas[d].length, 0);
  }

  // symlink under d: +1 inode, +0 length (non-FILE).
  {
    EntryWithPaOut sym_out;
    ASSERT_TRUE(fs->Symlink(ctx, "/target", d, "lnk", 1, 1, sym_out).ok());
    auto deltas = DrainDirStats(fs->GetDirStatManager());
    ASSERT_TRUE(deltas.count(d));
    EXPECT_EQ(deltas[d].inodes, 1);
    EXPECT_EQ(deltas[d].length, 0);
  }
}

// Cross-check the accumulated single-level deltas against the CalcDirStat
// source-of-truth: build a small tree under a fresh dir, drain the buffer,
// perform the ops, sum the deltas, and assert they match a fresh CalcDirStat.
TEST_F(FileSystemTest, UpdateDirStatMatchesCalc) {
  auto fs = Fs();
  ASSERT_TRUE(fs->EnableDirStats());
  Context ctx;

  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "match_d";
  dp.mode = 0777;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  ASSERT_TRUE(fs->MkDir(ctx, dp, dout).ok());
  Ino d = dout.attr.ino();
  DrainDirStats(fs->GetDirStatManager());  // drain

  auto mk = [&](const std::string& name, uint64_t len) {
    FileSystem::MkNodParam p;
    p.parent = d;
    p.name = name;
    p.mode = 0644;
    p.uid = 1;
    p.gid = 1;
    p.rdev = 0;
    EntryWithPaOut o;
    ASSERT_TRUE(fs->MkNod(ctx, p, o).ok());
    if (len > 0) {
      FileSystem::FlushFileParam fp;
      fp.length = len;
      EntryWithFileChangeOut fo;
      ASSERT_TRUE(fs->FlushFile(ctx, o.attr.ino(), fp, fo).ok());
    }
  };
  mk("a", 1000);
  mk("b", 5000);

  auto deltas = DrainDirStats(fs->GetDirStatManager());
  ASSERT_TRUE(deltas.count(d));

  DirStatEntry st;
  ASSERT_TRUE(fs->CalcDirStat(ctx, d, st).ok());

  EXPECT_EQ(deltas[d].inodes, st.inodes());
  EXPECT_EQ(deltas[d].length, st.length());
}

TEST_F(FileSystemTest, FlushDirStats) {
  auto fs = Fs();
  ASSERT_TRUE(fs->EnableDirStats());
  Context ctx;
  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "flush_d";
  dp.mode = 0777;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  ASSERT_TRUE(fs->MkDir(ctx, dp, dout).ok());
  Ino d = dout.attr.ino();

  FileSystem::MkNodParam p;
  p.parent = d;
  p.name = "x";
  p.mode = 0644;
  p.uid = 1;
  p.gid = 1;
  p.rdev = 0;
  EntryWithPaOut o;
  ASSERT_TRUE(fs->MkNod(ctx, p, o).ok());

  ASSERT_TRUE(fs->GetDirStatManager().FlushDirStats().ok());

  DirStatEntry st;
  ASSERT_TRUE(fs->GetDirStatManager().GetDirStat(ctx, d, st).ok());
  EXPECT_EQ(st.inodes(), 1);
  EXPECT_EQ(st.length(), 0);  // one empty file
}

// MkDir writes a zero-valued dir-stat record in the SAME txn, so a freshly
// created directory is tracked from birth: it has a stored {0,0,0} record
// immediately, so the first flush takes the in-place increment path (delta
// applied on top of the seeded record) rather than missing->recompute.
TEST_F(FileSystemTest, MkDirSeedsZeroDirStat) {
  auto fs = Fs();
  ASSERT_TRUE(fs->EnableDirStats());
  Context ctx;
  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "seed_d";
  dp.mode = 0777;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  ASSERT_TRUE(fs->MkDir(ctx, dp, dout).ok());
  Ino d = dout.attr.ino();

  DirStatEntry seeded;
  ASSERT_TRUE(fs->GetDirStatManager().GetDirStat(ctx, d, seeded).ok());
  EXPECT_EQ(seeded.inodes(), 0);
  EXPECT_EQ(seeded.length(), 0);

  FileSystem::MkNodParam p;
  p.parent = d;
  p.name = "x";
  p.mode = 0644;
  p.uid = 1;
  p.gid = 1;
  p.rdev = 0;
  EntryWithPaOut o;
  ASSERT_TRUE(fs->MkNod(ctx, p, o).ok());
  ASSERT_TRUE(fs->GetDirStatManager().FlushDirStats().ok());

  DirStatEntry after;
  ASSERT_TRUE(fs->GetDirStatManager().GetDirStat(ctx, d, after).ok());
  EXPECT_EQ(after.inodes(), 1);
  EXPECT_EQ(after.length(), 0);
}

// BatchMkDir seeds a zero-valued dir-stat record for the directory inode(s) it
// creates, in the same txn as the inode write.
TEST_F(FileSystemTest, BatchMkDirSeedsZeroDirStat) {
  auto fs = Fs();
  ASSERT_TRUE(fs->EnableDirStats());
  Context ctx;

  std::vector<FileSystem::MkDirParam> params;
  for (const auto& name : {"bseed_a", "bseed_b"}) {
    FileSystem::MkDirParam p;
    p.parent = kRootIno;
    p.name = name;
    p.mode = 0777;
    p.uid = 1;
    p.gid = 1;
    p.rdev = 0;
    params.push_back(p);
  }
  EntriesWithPaOut out;
  ASSERT_TRUE(fs->BatchMkDir(ctx, params, out).ok());
  ASSERT_FALSE(out.attrs.empty());

  for (const auto& attr : out.attrs) {
    DirStatEntry st;
    ASSERT_TRUE(fs->GetDirStatManager().GetDirStat(ctx, attr.ino(), st).ok());
    EXPECT_EQ(st.inodes(), 0);
    EXPECT_EQ(st.length(), 0);
  }
}

TEST_F(FileSystemTest, SyncDirStatSingleLevelRepair) {
  auto fs = Fs();
  Context ctx;
  auto mkdir = [&](Ino parent, const std::string& name) -> Ino {
    FileSystem::MkDirParam p;
    p.parent = parent;
    p.name = name;
    p.mode = 0777;
    p.uid = 1;
    p.gid = 1;
    p.rdev = 0;
    EntryWithPaOut o;
    EXPECT_TRUE(fs->MkDir(ctx, p, o).ok());
    return o.attr.ino();
  };
  auto mkfile = [&](Ino parent, const std::string& name, uint64_t len) {
    FileSystem::MkNodParam p;
    p.parent = parent;
    p.name = name;
    p.mode = 0644;
    p.uid = 1;
    p.gid = 1;
    p.rdev = 0;
    EntryWithPaOut o;
    ASSERT_TRUE(fs->MkNod(ctx, p, o).ok());
    if (len > 0) {
      FileSystem::FlushFileParam fp;
      fp.length = len;
      EntryWithFileChangeOut fo;
      ASSERT_TRUE(fs->FlushFile(ctx, o.attr.ino(), fp, fo).ok());
    }
  };

  Ino s = mkdir(kRootIno, "sum_s");
  Ino a = mkdir(s, "a");
  mkfile(a, "f1", 1000);
  mkfile(s, "f2", 5000);

  // Single-level: s's direct children are {a (dir), f2 (file, 5000)} ->
  // inodes=2, dirs=1, length=5000 (f1 lives under a, not s). The stored stat
  // lags (seeded zero at MkDir plus buffered, un-flushed deltas), so a
  // read-only check reports one break for s carrying the recomputed (want)
  // value.
  std::vector<dir_stat::DirStatManager::DirStatMismatch> mismatches;
  ASSERT_TRUE(fs->GetDirStatManager()
                  .SyncDirStat(ctx, s, /*repair=*/false, mismatches)
                  .ok());
  ASSERT_EQ(mismatches.size(), 1u);
  EXPECT_EQ(mismatches[0].ino, s);
  EXPECT_EQ(mismatches[0].want.inodes(), 2);
  EXPECT_EQ(mismatches[0].want.dirs(), 1);
  EXPECT_EQ(mismatches[0].want.length(), 5000);

  // Repair persists the recomputed value (and discards the buffered delta), so
  // a subsequent read-only check is clean and the stored record equals
  // CalcDirStat.
  mismatches.clear();
  ASSERT_TRUE(fs->GetDirStatManager()
                  .SyncDirStat(ctx, s, /*repair=*/true, mismatches)
                  .ok());
  EXPECT_EQ(mismatches.size(), 1u);

  mismatches.clear();
  ASSERT_TRUE(fs->GetDirStatManager()
                  .SyncDirStat(ctx, s, /*repair=*/false, mismatches)
                  .ok());
  EXPECT_TRUE(mismatches.empty());

  DirStatEntry calc, stored;
  ASSERT_TRUE(fs->CalcDirStat(ctx, s, calc).ok());
  ASSERT_TRUE(fs->GetDirStatManager().GetDirStat(ctx, s, stored).ok());
  EXPECT_EQ(stored.inodes(), calc.inodes());
  EXPECT_EQ(stored.dirs(), calc.dirs());
  EXPECT_EQ(stored.length(), calc.length());
}

// Regression: GetAttr on a directory inode that has no stored record (deleted,
// or never created) must return ENOT_FOUND, not crash. The dir-branch scan in
// GetInodeAttrOperation comes back empty and previously left a default zero-ino
// attr that UpsertInodeCache rejects with LOG(FATAL) -- crashing the MDS on a
// GetAttr of any missing directory.
TEST_F(FileSystemTest, GetAttrMissingDirReturnsNotFound) {
  auto fs = Fs();
  Context ctx;

  // An odd inode is treated as a directory (IsDir); pick one well below the
  // allocated range so it has no inode key and is not cached -> store read.
  Ino missing_dir = 123456789;
  ASSERT_TRUE(IsDir(missing_dir));

  EntryOut got;
  auto status = fs->GetAttr(ctx, missing_dir, got);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_FOUND);
}

// Fallocate that extends a file must charge the growth to the parent dir-stat
// (and quota). Regression: the delta was computed by re-reading the cached
// inode AFTER UpsertInodeCache mutated it in place, yielding a constant 0 and
// silently dropping both the dir-stat and quota updates. The delta is now taken
// from the operation's txn (FallocateOperation::Result::length_delta).
TEST_F(FileSystemTest, FallocateChargesDirStat) {
  auto fs = Fs();
  ASSERT_TRUE(fs->EnableDirStats());
  Context ctx;
  auto mkdir = [&](Ino parent, const std::string& name) -> Ino {
    FileSystem::MkDirParam p;
    p.parent = parent;
    p.name = name;
    p.mode = 0777;
    p.uid = 1;
    p.gid = 1;
    p.rdev = 0;
    EntryWithPaOut o;
    EXPECT_TRUE(fs->MkDir(ctx, p, o).ok());
    return o.attr.ino();
  };
  auto mknod = [&](Ino parent, const std::string& name) -> Ino {
    FileSystem::MkNodParam p;
    p.parent = parent;
    p.name = name;
    p.mode = 0644;
    p.uid = 1;
    p.gid = 1;
    p.rdev = 0;
    EntryWithPaOut o;
    EXPECT_TRUE(fs->MkNod(ctx, p, o).ok());
    return o.attr.ino();
  };

  Ino d = mkdir(kRootIno, "fa_d");
  Ino f = mknod(d, "f");  // empty file, length 0

  // fallocate(mode=0) extends f from 0 to 4096.
  EntryWithChunkOut eo;
  ASSERT_TRUE(
      fs->Fallocate(ctx, f, /*mode=*/0, /*offset=*/0, /*len=*/4096, eo).ok());
  EXPECT_EQ(eo.attr.length(), 4096u);

  // The parent dir-stat length must reflect the 4096-byte growth (folding the
  // buffered delta). Without the fix this stays 0.
  DirStatEntry stat;
  ASSERT_TRUE(fs->GetDirStatManager()
                  .GetDirStat(ctx, d, stat, /*with_pending=*/true)
                  .ok());
  EXPECT_EQ(stat.length(), 4096);
  EXPECT_EQ(stat.inodes(), 1);  // just f
  EXPECT_EQ(stat.dirs(), 0);
}

// BatchUnLink must apply the aggregated dir-stat delta once (single lock) and
// account every removed child, not silently drop any.
TEST_F(FileSystemTest, BatchUnLinkAggregatesDirStatDelta) {
  auto fs = Fs();
  ASSERT_TRUE(fs->EnableDirStats());
  Context ctx;

  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "batch_unlink_d";
  dp.mode = 0777;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  ASSERT_TRUE(fs->MkDir(ctx, dp, dout).ok());
  Ino d = dout.attr.ino();

  std::vector<std::string> names = {"a", "b", "c"};
  for (const auto& name : names) {
    FileSystem::MkNodParam p;
    p.parent = d;
    p.name = name;
    p.mode = 0644;
    p.uid = 1;
    p.gid = 1;
    p.rdev = 0;
    EntryWithPaOut o;
    ASSERT_TRUE(fs->MkNod(ctx, p, o).ok());
  }

  DrainDirStats(fs->GetDirStatManager());  // drain create deltas

  EntriesWithPaOut out;
  ASSERT_TRUE(fs->BatchUnLink(ctx, d, names, out).ok());

  auto deltas = DrainDirStats(fs->GetDirStatManager());
  ASSERT_TRUE(deltas.count(d));
  EXPECT_EQ(deltas[d].inodes, -3);  // all three children removed
  EXPECT_EQ(deltas[d].length, 0);   // empty files
}

// ForEachDentry pages with last_name; the store-backed scan is inclusive of
// last_name, so without skip-first the page-boundary entry is counted twice.
// Force the store path via a bypass-cache context over >1 page of children.
TEST_F(FileSystemTest, CalcDirStatNoPageBoundaryDoubleCount) {
  auto fs = Fs();
  Context ctx;
  FileSystem::MkDirParam dp;
  dp.parent = kRootIno;
  dp.name = "page_d";
  dp.mode = 0777;
  dp.uid = 1;
  dp.gid = 1;
  dp.rdev = 0;
  EntryWithPaOut dout;
  ASSERT_TRUE(fs->MkDir(ctx, dp, dout).ok());
  Ino d = dout.attr.ino();

  // One more than the internal scan page size (1000) so the scan spans 2 pages.
  constexpr int kCount = 1001;
  for (int i = 0; i < kCount; ++i) {
    FileSystem::MkNodParam p;
    p.parent = d;
    // zero-padded names keep a deterministic sorted order across the boundary.
    p.name = fmt::format("f{:05d}", i);
    p.mode = 0644;
    p.uid = 1;
    p.gid = 1;
    p.rdev = 0;
    EntryWithPaOut o;
    ASSERT_TRUE(fs->MkNod(ctx, p, o).ok());
  }

  // bypass-cache forces ListDentryFromStore (the inclusive scan path).
  pb::mds::Context ce;
  ce.set_is_bypass_cache(true);
  Context bypass_ctx(ce, "test", "CalcDirStatNoPageBoundaryDoubleCount");

  DirStatEntry st;
  ASSERT_TRUE(fs->CalcDirStat(bypass_ctx, d, st).ok());
  EXPECT_EQ(st.inodes(),
            kCount);  // exactly kCount, not kCount + (boundary dups)
}

// Every worker owns a private directory.  The operation order is random, but
// each operation has a pre-created, independent fixture so failures are real
// filesystem failures rather than expected dependency races.
TEST_F(FileSystemTest, ChaosOperations) {
  if (getenv("SLOW_TEST") == nullptr) {
    GTEST_SKIP() << "Skip slow test case.";
  }

  auto fs = Fs();
  constexpr int kWorkerCount = 8;
  constexpr int kRoundCount = 1000;

  struct Fixture {
    std::string prefix;
    std::string file_name;
    std::string src_name;
    std::string dst_name;
    std::string unlink_name;
    std::string batch_unlink_a;
    std::string batch_unlink_b;
    std::string rmdir_name;
    std::string rename_src;
    std::string rename_dst;
    std::string readlink_name;
    Ino dir{0};
    Ino file{0};
    Ino src{0};
    Ino dst{0};
    Ino unlink_ino{0};
    Ino rmdir_ino{0};
    Ino rename_ino{0};
    Ino readlink_ino{0};
    uint64_t first_slice_id{0};
    uint64_t second_slice_id{0};
    std::string release_session;
  };

  std::vector<Fixture> fixtures(kWorkerCount * kRoundCount);
  auto make_dir = [&](Ino parent, const std::string& name) -> Ino {
    FileSystem::MkDirParam param;
    param.parent = parent;
    param.name = name;
    param.mode = 0755;
    param.uid = 1;
    param.gid = 1;
    EntryWithPaOut out;
    Context ctx("chaos-setup-" + name, "ChaosSetup");
    auto status = fs->MkDir(ctx, param, out);
    if (!status.ok()) {
      ADD_FAILURE() << "mkdir " << name << ": " << status.error_str();
      return 0;
    }
    return out.attr.ino();
  };
  auto make_file = [&](Ino parent, const std::string& name) -> Ino {
    FileSystem::MkNodParam param;
    param.parent = parent;
    param.name = name;
    param.mode = 0644;
    param.uid = 1;
    param.gid = 1;
    EntryWithPaOut out;
    Context ctx("chaos-setup-" + name, "ChaosSetup");
    auto status = fs->MkNod(ctx, param, out);
    if (!status.ok()) {
      ADD_FAILURE() << "mknod " << name << ": " << status.error_str();
      return 0;
    }
    return out.attr.ino();
  };

  for (int round = 0; round < kRoundCount; ++round) {
    for (int worker = 0; worker < kWorkerCount; ++worker) {
      auto& f = fixtures[round * kWorkerCount + worker];
      f.prefix = fmt::format("chaos_ops_{}_{}", round, worker);
      f.file_name = f.prefix + "_file";
      f.src_name = f.prefix + "_src";
      f.dst_name = f.prefix + "_dst";
      f.unlink_name = f.prefix + "_unlink";
      f.batch_unlink_a = f.prefix + "_batch_unlink_a";
      f.batch_unlink_b = f.prefix + "_batch_unlink_b";
      f.rmdir_name = f.prefix + "_rmdir";
      f.rename_src = f.prefix + "_rename_src";
      f.rename_dst = f.prefix + "_rename_dst";
      f.readlink_name = f.prefix + "_readlink";
      f.release_session = f.prefix + "_release_session";

      f.dir = make_dir(kRootIno, f.prefix);
      ASSERT_GT(f.dir, 0u);
      f.file = make_file(f.dir, f.file_name);
      f.src = make_file(f.dir, f.src_name);
      f.dst = make_file(f.dir, f.dst_name);
      f.unlink_ino = make_file(f.dir, f.unlink_name);
      ASSERT_GT(f.file, 0u);
      ASSERT_GT(f.src, 0u);
      ASSERT_GT(f.dst, 0u);
      ASSERT_GT(f.unlink_ino, 0u);
      ASSERT_GT(make_file(f.dir, f.batch_unlink_a), 0u);
      ASSERT_GT(make_file(f.dir, f.batch_unlink_b), 0u);
      f.rmdir_ino = make_dir(f.dir, f.rmdir_name);
      ASSERT_GT(f.rmdir_ino, 0u);
      f.rename_ino = make_file(f.dir, f.rename_src);
      ASSERT_GT(f.rename_ino, 0u);

      {
        EntryWithPaOut out;
        Context ctx("chaos-setup-" + f.readlink_name, "ChaosSetup");
        auto status = fs->Symlink(ctx, "/chaos-target", f.dir, f.readlink_name,
                                  1, 1, out);
        ASSERT_TRUE(status.ok()) << status.error_str();
        f.readlink_ino = out.attr.ino();
      }

      // Give the slice operations a stable chunk with two slices.  The IDs are
      // deliberately outside the test generator's normal range and unique per
      // worker.
      f.first_slice_id = (1ULL << 50) + (round * kWorkerCount + worker) * 100;
      f.second_slice_id = f.first_slice_id + 1;
      auto seed_slices = [&](Ino ino, uint64_t first_id, int count) {
        DeltaSliceEntry delta;
        delta.set_chunk_index(0);
        delta.set_curr_version(0);
        for (int i = 0; i < count; ++i) {
          auto* slice = delta.add_slices();
          slice->set_id(first_id + i);
          slice->set_off(0);
          slice->set_size(4096);
          slice->set_pos(i * 4096);
          slice->set_len(4096);
        }
        EntryWithChunkOut out;
        Context ctx(fmt::format("chaos-setup-slice-{}", ino), "ChaosSetup");
        auto status = fs->WriteSlice(ctx, ino, {delta}, out);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(out.attr.ino(), ino);
      };
      seed_slices(f.file, f.first_slice_id, 2);
      seed_slices(f.src, f.first_slice_id + 10, 1);

      {
        Inode::XAttrMap xattrs;
        xattrs["chaos.seed"] = "value";
        xattrs["chaos.remove"] = "value";
        EntryOut out;
        Context ctx("chaos-setup-xattr-" + std::to_string(worker),
                    "ChaosSetup");
        auto status = fs->SetXAttr(ctx, f.file, xattrs, out);
        ASSERT_TRUE(status.ok()) << status.error_str();
      }

      {
        FileSystem::OpenParam param;
        param.flags = O_RDWR;
        param.session_id = f.release_session;
        EntryOutForOpen out;
        Context ctx("chaos-setup-open-" + std::to_string(worker), "ChaosSetup");
        auto status = fs->Open(ctx, f.file, param, out);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(out.attr.ino(), f.file);
      }
    }
  }

  std::mutex failures_mutex;
  std::vector<std::string> failures;
  auto fail = [&](const std::string& message) {
    std::lock_guard<std::mutex> lock(failures_mutex);
    failures.push_back(message);
  };
  auto check_status = [&](int worker, const char* operation,
                          const Status& status) {
    if (!status.ok()) {
      fail(fmt::format("worker {} {}: {}", worker, operation,
                       status.error_str()));
      return false;
    }
    return true;
  };

  struct OperationContext {
    OperationContext(int worker, const char* name)
        : context(fmt::format("chaos-{}-{}", worker, name), "ChaosOperations") {
    }
    operator Context&() { return context; }
    Context context;
  };

  std::atomic<int> ready_workers{0};
  std::atomic<bool> start_workers{false};
  std::vector<std::thread> workers;
  workers.reserve(kWorkerCount);
  for (int worker = 0; worker < kWorkerCount; ++worker) {
    workers.emplace_back([&, worker] {
      ready_workers.fetch_add(1);
      while (!start_workers.load()) std::this_thread::yield();

      for (int round = 0; round < kRoundCount; ++round) {
        const auto& f = fixtures[round * kWorkerCount + worker];
        std::vector<int> operations(27);
        std::iota(operations.begin(), operations.end(), 0);
        std::mt19937 random(
            static_cast<uint32_t>(0xd1a0f5 + round * kWorkerCount + worker));
        std::shuffle(operations.begin(), operations.end(), random);

        for (int operation : operations) {
          auto context = [&](const char* name) {
            return OperationContext(worker, name);
          };
          switch (operation) {
            case 0: {  // Lookup
              EntryOut out;
              auto status =
                  fs->Lookup(context("lookup"), f.dir, f.file_name, out);
              if (check_status(worker, "Lookup", status) &&
                  out.attr.ino() != f.file) {
                fail(fmt::format("worker {} Lookup returned ino {} (want {})",
                                 worker, out.attr.ino(), f.file));
              }
              break;
            }
            case 1: {  // BatchCreate
              std::vector<FileSystem::MkNodParam> params(2);
              for (int i = 0; i < 2; ++i) {
                params[i].parent = f.dir;
                params[i].name = fmt::format("{}_batch_create_{}", f.prefix, i);
                params[i].mode = 0644;
              }
              EntriesWithPaOut out;
              auto status =
                  fs->BatchCreate(context("batch-create"), f.dir, params, out);
              if (check_status(worker, "BatchCreate", status)) {
                if (out.attrs.size() != 2) {
                  fail(fmt::format("worker {} BatchCreate returned {} attrs",
                                   worker, out.attrs.size()));
                }
                for (const auto& attr : out.attrs) {
                  if (attr.type() != pb::mds::FileType::FILE || attr.ino() == 0)
                    fail(fmt::format("worker {} BatchCreate returned bad attr",
                                     worker));
                }
              }
              break;
            }
            case 2: {  // MkNod
              FileSystem::MkNodParam param;
              param.parent = f.dir;
              param.name = f.prefix + "_mknod";
              param.mode = 0644;
              EntryWithPaOut out;
              auto status = fs->MkNod(context("mknod"), param, out);
              if (check_status(worker, "MkNod", status) &&
                  (out.attr.ino() == 0 ||
                   out.attr.type() != pb::mds::FileType::FILE)) {
                fail(fmt::format("worker {} MkNod returned bad attr", worker));
              }
              break;
            }
            case 3: {  // BatchMkNod
              std::vector<FileSystem::MkNodParam> params(2);
              for (int i = 0; i < 2; ++i) {
                params[i].parent = f.dir;
                params[i].name = fmt::format("{}_batch-mknod_{}", f.prefix, i);
                params[i].mode = 0644;
              }
              EntriesWithPaOut out;
              auto status = fs->BatchMkNod(context("batch-mknod"), params, out);
              if (check_status(worker, "BatchMkNod", status) &&
                  out.attrs.size() != 2) {
                fail(fmt::format("worker {} BatchMkNod returned {} attrs",
                                 worker, out.attrs.size()));
              }
              break;
            }
            case 4: {  // Open
              FileSystem::OpenParam param;
              param.flags = O_RDWR;
              param.session_id = f.prefix + "_open_session";
              EntryOutForOpen out;
              auto status = fs->Open(context("open"), f.file, param, out);
              if (check_status(worker, "Open", status) &&
                  out.attr.ino() != f.file) {
                fail(fmt::format("worker {} Open returned wrong ino", worker));
              }
              break;
            }
            case 5: {  // Release
              auto status =
                  fs->Release(context("release"), f.file, f.release_session);
              check_status(worker, "Release", status);
              break;
            }
            case 6: {  // FlushFile
              FileSystem::FlushFileParam param;
              param.length = 12345 + worker;
              EntryWithFileChangeOut out;
              auto status = fs->FlushFile(context("flush"), f.file, param, out);
              if (check_status(worker, "FlushFile", status) &&
                  out.attr.length() != param.length) {
                fail(fmt::format("worker {} FlushFile length {} (want {})",
                                 worker, out.attr.length(), param.length));
              }
              break;
            }
            case 7: {  // MkDir
              FileSystem::MkDirParam param;
              param.parent = f.dir;
              param.name = f.prefix + "_mkdir";
              param.mode = 0755;
              EntryWithPaOut out;
              auto status = fs->MkDir(context("mkdir"), param, out);
              if (check_status(worker, "MkDir", status) &&
                  out.attr.type() != pb::mds::FileType::DIRECTORY) {
                fail(fmt::format("worker {} MkDir returned non-directory",
                                 worker));
              }
              break;
            }
            case 8: {  // BatchMkDir
              std::vector<FileSystem::MkDirParam> params(2);
              for (int i = 0; i < 2; ++i) {
                params[i].parent = f.dir;
                params[i].name = fmt::format("{}_batch-mkdir_{}", f.prefix, i);
                params[i].mode = 0755;
              }
              EntriesWithPaOut out;
              auto status = fs->BatchMkDir(context("batch-mkdir"), params, out);
              if (check_status(worker, "BatchMkDir", status) &&
                  out.attrs.size() != 2) {
                fail(fmt::format("worker {} BatchMkDir returned {} attrs",
                                 worker, out.attrs.size()));
              }
              break;
            }
            case 9: {  // RmDir
              EntryWithPaOut out;
              auto status =
                  fs->RmDir(context("rmdir"), f.dir, f.rmdir_name, out);
              if (check_status(worker, "RmDir", status) &&
                  out.attr.ino() != f.rmdir_ino) {
                fail(fmt::format("worker {} RmDir returned wrong ino", worker));
              }
              break;
            }
            case 10: {  // ReadDir
              std::vector<EntryWithNameOut> entries;
              auto status =
                  fs->ReadDir(context("readdir"), f.dir, "", 0, false, entries);
              if (check_status(worker, "ReadDir", status)) {
                const bool found = std::any_of(
                    entries.begin(), entries.end(), [&](const auto& entry) {
                      return entry.name == f.file_name &&
                             entry.attr.ino() == f.file;
                    });
                if (!found)
                  fail(fmt::format("worker {} ReadDir missed file", worker));
              }
              break;
            }
            case 11: {  // Link
              EntryWithPaOut out;
              auto status = fs->Link(context("link"), f.file, f.dir,
                                     f.prefix + "_link", out);
              if (check_status(worker, "Link", status) &&
                  out.attr.ino() != f.file) {
                fail(fmt::format("worker {} Link returned wrong ino", worker));
              }
              break;
            }
            case 12: {  // UnLink
              EntryWithPaOut out;
              auto status =
                  fs->UnLink(context("unlink"), f.dir, f.unlink_name, out);
              if (check_status(worker, "UnLink", status) &&
                  out.attr.ino() != f.unlink_ino) {
                fail(
                    fmt::format("worker {} UnLink returned wrong ino", worker));
              }
              break;
            }
            case 13: {  // BatchUnLink
              EntriesWithPaOut out;
              auto status =
                  fs->BatchUnLink(context("batch-unlink"), f.dir,
                                  {f.batch_unlink_a, f.batch_unlink_b}, out);
              if (check_status(worker, "BatchUnLink", status) &&
                  out.attrs.size() != 2) {
                fail(fmt::format("worker {} BatchUnLink returned {} attrs",
                                 worker, out.attrs.size()));
              }
              break;
            }
            case 14: {  // Symlink
              EntryWithPaOut out;
              auto status =
                  fs->Symlink(context("symlink"), "/chaos-created-target",
                              f.dir, f.prefix + "_symlink", 1, 1, out);
              if (check_status(worker, "Symlink", status) &&
                  (out.attr.type() != pb::mds::FileType::SYM_LINK ||
                   out.attr.symlink() != "/chaos-created-target")) {
                fail(
                    fmt::format("worker {} Symlink returned bad attr", worker));
              }
              break;
            }
            case 15: {  // ReadLink
              std::string link;
              auto status =
                  fs->ReadLink(context("readlink"), f.readlink_ino, link);
              if (check_status(worker, "ReadLink", status) &&
                  link != "/chaos-target") {
                fail(fmt::format("worker {} ReadLink returned {}", worker,
                                 link));
              }
              break;
            }
            case 16: {  // SetAttr
              FileSystem::SetAttrParam param;
              param.to_set = kSetAttrMode;
              param.attr.set_fs_id(fs->FsId());
              param.attr.set_ino(f.file);
              param.attr.set_mode(0600);
              EntryWithChunkOut out;
              auto status = fs->SetAttr(context("setattr"), f.file, param, out);
              if (check_status(worker, "SetAttr", status) &&
                  out.attr.mode() != 0600) {
                fail(fmt::format("worker {} SetAttr mode {}", worker,
                                 out.attr.mode()));
              }
              break;
            }
            case 17: {  // GetAttr
              EntryOut out;
              auto status = fs->GetAttr(context("getattr"), f.file, out);
              if (check_status(worker, "GetAttr", status) &&
                  out.attr.ino() != f.file) {
                fail(fmt::format("worker {} GetAttr returned wrong ino",
                                 worker));
              }
              break;
            }
            case 18: {  // GetXAttr
              Inode::XAttrMap xattrs;
              auto status = fs->GetXAttr(context("getxattr"), f.file, xattrs);
              if (check_status(worker, "GetXAttr", status) &&
                  xattrs["chaos.seed"] != "value") {
                fail(fmt::format("worker {} GetXAttr lost seed", worker));
              }
              break;
            }
            case 19: {  // SetXAttr
              Inode::XAttrMap xattrs;
              xattrs["chaos.set"] = "value";
              EntryOut out;
              auto status =
                  fs->SetXAttr(context("setxattr"), f.file, xattrs, out);
              if (check_status(worker, "SetXAttr", status) &&
                  out.attr.xattrs().find("chaos.set") ==
                      out.attr.xattrs().end()) {
                fail(fmt::format("worker {} SetXAttr returned no value",
                                 worker));
              }
              break;
            }
            case 20: {  // RemoveXAttr
              EntryOut out;
              auto status = fs->RemoveXAttr(context("removexattr"), f.file,
                                            "chaos.remove", out);
              if (check_status(worker, "RemoveXAttr", status) &&
                  out.attr.xattrs().find("chaos.remove") !=
                      out.attr.xattrs().end()) {
                fail(fmt::format("worker {} RemoveXAttr kept value", worker));
              }
              break;
            }
            case 21: {  // Rename
              FileSystem::RenameParam param;
              param.old_parent = f.dir;
              param.old_name = f.rename_src;
              param.new_parent = f.dir;
              param.new_name = f.rename_dst;
              FileSystem::RenameResult out;
              auto status = fs->Rename(context("rename"), param, out);
              if (check_status(worker, "Rename", status)) {
                EntryOut renamed;
                auto lookup = fs->Lookup(context("rename-verify"), f.dir,
                                         f.rename_dst, renamed);
                if (check_status(worker, "Rename verify", lookup) &&
                    renamed.attr.ino() != f.rename_ino) {
                  fail(fmt::format("worker {} Rename returned ino {} (want {})",
                                   worker, renamed.attr.ino(), f.rename_ino));
                }
              }
              break;
            }
            case 22: {  // WriteSlice
              DeltaSliceEntry delta;
              delta.set_chunk_index(0);
              delta.set_curr_version(1);
              auto* slice = delta.add_slices();
              slice->set_id(f.first_slice_id + 2);
              slice->set_off(0);
              slice->set_size(1024);
              slice->set_pos(8192);
              slice->set_len(1024);
              EntryWithChunkOut out;
              auto status =
                  fs->WriteSlice(context("writeslice"), f.file, {delta}, out);
              if (check_status(worker, "WriteSlice", status) &&
                  (out.attr.ino() != f.file || out.chunks.empty())) {
                fail(fmt::format("worker {} WriteSlice returned bad result",
                                 worker));
              }
              break;
            }
            case 23: {  // ReadSlice
              ChunkDescriptor descriptor;
              descriptor.set_index(0);
              descriptor.set_version(1);
              std::vector<ChunkEntry> chunks;
              auto status = fs->ReadSlice(context("readslice"), f.file,
                                          {descriptor}, chunks);
              const bool found = std::any_of(
                  chunks.begin(), chunks.end(),
                  [](const auto& chunk) { return chunk.index() == 0; });
              if (check_status(worker, "ReadSlice", status) && !found) {
                fail(fmt::format("worker {} ReadSlice missed chunk", worker));
              }
              break;
            }
            case 24: {  // CopyFileRange
              FileSystem::CopyFileRangeParam param{.src_ino = f.src,
                                                   .dst_ino = f.dst,
                                                   .src_off = 0,
                                                   .dst_off = 0,
                                                   .len = 4096};
              EntryWithChunkOut out;
              auto status = fs->CopyFileRange(context("copy"), param, out);
              if (check_status(worker, "CopyFileRange", status) &&
                  (out.delta_bytes != 4096 || out.chunks.empty())) {
                fail(fmt::format("worker {} CopyFileRange copied {} bytes",
                                 worker, out.delta_bytes));
              }
              break;
            }
            case 25: {  // Fallocate
              EntryOut before;
              auto get_status =
                  fs->GetAttr(context("fallocate-before"), f.dst, before);
              if (!check_status(worker, "Fallocate GetAttr", get_status)) break;
              EntryWithChunkOut out;
              auto status = fs->Fallocate(context("fallocate"), f.dst, 0, 8192,
                                          4096, out);
              const uint64_t want =
                  std::max<uint64_t>(before.attr.length(), 12288);
              if (check_status(worker, "Fallocate", status) &&
                  out.attr.length() != want) {
                fail(fmt::format("worker {} Fallocate length {} (want {})",
                                 worker, out.attr.length(), want));
              }
              break;
            }
            case 26: {  // CompactChunk
              FileSystem::CompactChunkParam param;
              param.start_pos = 0;
              param.start_slice_id = f.first_slice_id;
              param.end_pos = 1;
              param.end_slice_id = f.second_slice_id;
              SliceEntry replacement;
              replacement.set_id(f.first_slice_id + 3);
              replacement.set_size(8192);
              replacement.set_pos(0);
              replacement.set_len(8192);
              param.new_slices.push_back(replacement);
              ChunkEntry out;
              auto status =
                  fs->CompactChunk(context("compact"), f.file, 0, param, out);
              if (check_status(worker, "CompactChunk", status) &&
                  (out.index() != 0 || out.slices_size() == 0)) {
                fail(fmt::format("worker {} CompactChunk returned bad chunk",
                                 worker));
              }
              break;
            }
            default:
              fail(fmt::format("worker {} unknown operation {}", worker,
                               operation));
          }
          std::this_thread::yield();
        }
      }
    });
  }
  while (ready_workers.load() != kWorkerCount) std::this_thread::yield();
  start_workers.store(true);
  for (auto& worker : workers) worker.join();

  std::string failure_message;
  for (const auto& failure : failures) failure_message += "\n" + failure;
  ASSERT_TRUE(failures.empty()) << failure_message;
}

// --- Performance test ---
// Run: FILESYSTEM_PERF=1 ./test_mds --gtest_filter=FileSystemTest.MkDirPerf
// Optionally override count: PERF_MKDIR_COUNT=1000000
// Note: DummyStorage keeps everything in memory, ~1KB+/dir; 1e8 dirs needs
// 100GB+ RAM, use PERF_MKDIR_COUNT to scale down on small machines.
TEST_F(FileSystemTest, MkDirPerf) {
  if (getenv("MANUAL_TEST") == nullptr) {
    GTEST_SKIP() << "Skip manual test case.";
  }

  uint64_t total = 100000000ULL;  // 1亿
  if (const char* s = getenv("PERF_MKDIR_COUNT")) total = std::stoull(s);

  auto fs = Fs();

  const uint64_t report_interval = 100000;
  auto start_us = utils::TimestampUs();
  uint64_t last_us = start_us;

  for (uint64_t i = 1; i <= total; ++i) {
    FileSystem::MkDirParam param;
    param.parent = kRootIno;
    param.name = fmt::format("perf_dir_{:012d}", i);
    param.mode = 0755;
    param.uid = 1;
    param.gid = 1;
    param.rdev = 0;

    {
      Context ctx;
      EntryOut entry_out;
      fs->Lookup(ctx, param.parent, param.name, entry_out);
    }

    Context ctx;
    EntryWithPaOut entry_pa_out;
    auto status = fs->MkDir(ctx, param, entry_pa_out);
    ASSERT_TRUE(status.ok())
        << fmt::format("mkdir {} fail: {}", param.name, status.error_str());

    if (i % report_interval == 0) {
      auto now_us = utils::TimestampUs();
      double interval_qps =
          report_interval * 1e6 / static_cast<double>(now_us - last_us);
      double avg_qps = i * 1e6 / static_cast<double>(now_us - start_us);
      fmt::print(
          "progress: {}/{} ({:.1f}%), interval qps: {:.0f}, avg qps: {:.0f}, "
          "elapsed: {:.1f}s\n",
          i, total, i * 100.0 / total, interval_qps, avg_qps,
          (now_us - start_us) / 1e6);
      last_us = now_us;

      std::string trace_str;
      auto trace_time = ctx.GetTrace().GetTime();
      for (auto& [name, elapsed] : trace_time.elapsed_times) {
        trace_str += fmt::format("{}:{}us ", name, elapsed);
      }

      fmt::print("trace: {}\n", trace_str);
    }
  }

  auto elapsed_us = utils::TimestampUs() - start_us;
  double qps = total * 1e6 / static_cast<double>(elapsed_us);
  fmt::print(
      "=== FileSystem::MkDir perf: total({}) elapsed({:.1f}s) qps({:.0f}) avg "
      "latency({:.1f}us)\n",
      total, elapsed_us / 1e6, qps, static_cast<double>(elapsed_us) / total);
}

TEST_F(FileSystemTest, MapPerf) {
  if (getenv("MANUAL_TEST") == nullptr) {
    GTEST_SKIP() << "Skip manual test case.";
  }

  absl::flat_hash_map<Ino, InodeSPtr> map;

  Ino start_ino = 10000000000000;

  const uint64_t report_interval = 100000;

  uint64_t start_us = utils::TimestampUs();
  uint64_t last_us = start_us;
  for (uint64_t i = 1; i <= 10000000; ++i) {
    AttrEntry attr;
    map.emplace(start_ino + i, std::make_shared<Inode>(attr));

    if (i % report_interval == 0) {
      auto now_us = utils::TimestampUs();
      double interval_qps =
          report_interval * 1e6 / static_cast<double>(now_us - last_us);
      double avg_qps = i * 1e6 / static_cast<double>(now_us - start_us);
      fmt::print(
          "progress: {}/{} ({:.1f}%), time({}u) interval qps: {:.0f}, avg "
          "qps: {:.0f}, elapsed: {:.1f}s\n",
          i, 10000000, i * 100.0 / 10000000, now_us - last_us, interval_qps,
          avg_qps, (now_us - start_us) / 1e6);
      last_us = now_us;
    }
  }

  uint64_t elapsed_time_us = utils::TimestampUs() - start_us;
  fmt::print("=== btree_map insert perf: total({}) elapsed({:.1f}s)\n",
             map.size(), elapsed_time_us / 1e6);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
