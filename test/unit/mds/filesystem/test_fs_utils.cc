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

#include "mds/filesystem/fs_utils.h"

#include <string>

#include "common/const.h"
#include "gtest/gtest.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/id_generator.h"
#include "mds/quota/quota.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {
namespace unit_test {

TEST(HashRouterTest, GetMDSRoutesEveryBucketToItsAssignedMdsId) {
  pb::mds::HashPartition hash_partition;
  hash_partition.set_bucket_num(4);
  auto& distributions = *hash_partition.mutable_distributions();
  // mds 100 owns buckets 0,2; mds 200 owns buckets 1,3.
  distributions[100].add_bucket_ids(0);
  distributions[100].add_bucket_ids(2);
  distributions[200].add_bucket_ids(1);
  distributions[200].add_bucket_ids(3);

  HashRouter router(hash_partition);

  EXPECT_EQ(router.GetMDS(0), 100);
  EXPECT_EQ(router.GetMDS(2), 100);
  EXPECT_EQ(router.GetMDS(1), 200);
  EXPECT_EQ(router.GetMDS(3), 200);
  // Routing must be a pure function of `ino % bucket_num`, so inos far
  // beyond bucket_num still land on the correct owner.
  EXPECT_EQ(router.GetMDS(4), 100);   // 4 % 4 == 0
  EXPECT_EQ(router.GetMDS(1001), 200);  // 1001 % 4 == 1
}

class FsUtilsTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    kv_storage = DummyStorage::New();
    ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

    const uint32_t kFsId = 4000;
    auto ino_id_generator = NewInodeIdGenerator(kFsId, kv_storage);
    ASSERT_TRUE(ino_id_generator->Init());
    auto slice_id_generator = NewSliceIdGenerator(kv_storage);
    ASSERT_TRUE(slice_id_generator->Init());

    operation_processor = OperationProcessor::New(kv_storage);
    ASSERT_TRUE(operation_processor->Init());

    quota_worker_set = SimpleWorkerSet::New("fs_utils_test_quota", 1, 1024, false, /*is_inplace_run=*/true);
    ASSERT_TRUE(quota_worker_set->Init());

    fs_info.set_fs_id(kFsId);
    fs_info.set_fs_name("fs_utils_test_fs");
    fs_info.set_fs_type(pb::mds::FsType::S3);
    fs_info.set_status(pb::mds::FsStatus::NORMAL);
    fs_info.set_block_size(1024 * 1024);
    fs_info.set_chunk_size(1024 * 1024 * 64);
    fs_info.set_owner("test_user");
    fs_info.set_capacity(1024 * 1024 * 1024);
    fs_info.set_recycle_time_hour(24);
    fs_info.set_trash_days(0);  // no trash -> GenDirJsonString skips ".trash"
    fs_info.set_create_time_s(0);
    fs_info.mutable_partition_policy()->set_type(pb::mds::PartitionType::MONOLITHIC_PARTITION);
    fs_info.mutable_partition_policy()->mutable_mono()->set_mds_id(1);

    fs = FileSystem::New(/*self_mds_id=*/1, FsInfo::New(fs_info), std::move(ino_id_generator), slice_id_generator,
                         kv_storage, operation_processor, nullptr, quota_worker_set, quota_worker_set, nullptr);
    ASSERT_TRUE(fs->CreateRoot().ok());
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

  static FileSystemSPtr fs;
  static KVStorageSPtr kv_storage;
  static OperationProcessorSPtr operation_processor;
  static WorkerSetSPtr quota_worker_set;
  static pb::mds::FsInfo fs_info;
};
FileSystemSPtr FsUtilsTest::fs = nullptr;
KVStorageSPtr FsUtilsTest::kv_storage = nullptr;
OperationProcessorSPtr FsUtilsTest::operation_processor = nullptr;
WorkerSetSPtr FsUtilsTest::quota_worker_set = nullptr;
pb::mds::FsInfo FsUtilsTest::fs_info;

TEST_F(FsUtilsTest, GenDirJsonStringListsChildrenOfRoot) {
  Context ctx;
  FileSystem::MkDirParam mkdir_param;
  mkdir_param.parent = kRootIno;
  mkdir_param.name = "a_dir";
  mkdir_param.mode = 0755;
  EntryWithPaOut dir_out;
  ASSERT_TRUE(fs->MkDir(ctx, mkdir_param, dir_out).ok());

  FileSystem::MkNodParam mknod_param;
  mknod_param.parent = kRootIno;
  mknod_param.name = "a_file";
  mknod_param.mode = 0644;
  EntryWithPaOut file_out;
  ASSERT_TRUE(fs->MkNod(ctx, mknod_param, file_out).ok());

  FsUtils fs_utils(operation_processor, fs_info);
  std::string json;
  ASSERT_TRUE(fs_utils.GenDirJsonString(kRootIno, json).ok());

  EXPECT_NE(json.find("\"a_dir\""), std::string::npos);
  EXPECT_NE(json.find("\"directory\""), std::string::npos);
  EXPECT_NE(json.find("\"a_file\""), std::string::npos);
  // trash_days == 0 in this fixture, so the synthesized ".trash" row must
  // not be added even though parent == kRootIno.
  EXPECT_EQ(json.find(".trash"), std::string::npos);
}

TEST_F(FsUtilsTest, GenDirJsonStringForVirtualRootParentReturnsRootEntry) {
  FsUtils fs_utils(operation_processor, fs_info);
  std::string json;
  ASSERT_TRUE(fs_utils.GenDirJsonString(kRootParentIno, json).ok());

  EXPECT_NE(json.find("\"name\":\"/\""), std::string::npos);
  EXPECT_NE(json.find("\"directory\""), std::string::npos);
}

TEST_F(FsUtilsTest, GetChunksReturnsEmptyForNodeWithoutSlices) {
  Context ctx;
  FileSystem::MkNodParam mknod_param;
  mknod_param.parent = kRootIno;
  mknod_param.name = "no_slice_file";
  mknod_param.mode = 0644;
  EntryWithPaOut file_out;
  ASSERT_TRUE(fs->MkNod(ctx, mknod_param, file_out).ok());

  FsUtils fs_utils(operation_processor, fs_info);
  std::vector<ChunkEntry> chunks;
  ASSERT_TRUE(fs_utils.GetChunks(fs_info.fs_id(), file_out.attr.ino(), chunks).ok());
  EXPECT_TRUE(chunks.empty());
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
