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

#include <cstdint>
#include <utility>
#include <vector>

#include "bthread/bthread.h"
#include "bthread/types.h"
#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/mutation_processor.h"
#include "mdsv2/storage/dummy_storage.h"

namespace dingofs {
namespace mdsv2 {
namespace unit_test {

const int64_t kFsId = 1000;
const int64_t kRootParentIno = 0;
const int64_t kRootIno = 1;

static pb::mdsv2::Inode GenInode(uint32_t fs_id, uint64_t ino, pb::mdsv2::FileType type) {
  pb::mdsv2::Inode inode;
  inode.set_ino(ino);
  inode.set_fs_id(fs_id);
  inode.set_length(0);
  inode.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  inode.set_uid(1008);
  inode.set_gid(1008);
  inode.set_rdev(0);
  inode.set_type(type);

  auto now_ns = Helper::TimestampNs();

  inode.set_atime(now_ns);
  inode.set_mtime(now_ns);
  inode.set_ctime(now_ns);

  if (type == pb::mdsv2::FileType::DIRECTORY) {
    inode.set_nlink(2);
  } else {
    inode.set_nlink(1);
  }

  return inode;
}

static pb::mdsv2::Dentry GenDentry(uint32_t fs_id, uint64_t parent, uint64_t ino, const std::string& name) {
  pb::mdsv2::Dentry dentry;
  dentry.set_fs_id(fs_id);
  dentry.set_parent_ino(parent);
  dentry.set_ino(ino);
  dentry.set_name(name);

  return dentry;
}

class MutationProcessorTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    kv_storage = DummyStorage::New();
    ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

 public:
  static KVStoragePtr kv_storage;
};

KVStoragePtr MutationProcessorTest::kv_storage = nullptr;

Status CreateRoot(MutationProcessor& mutation_processor) {
  MixMutation mix_mutation = {.fs_id = kFsId};

  bthread::CountdownEvent count_down(1);

  auto inode = GenInode(kFsId, kRootIno, pb::mdsv2::FileType::FILE);

  Trace trace;
  Operation operation(Operation::OpType::kCreateInode, kRootParentIno,
                      MetaDataCodec::EncodeInodeKey(kFsId, inode.ino()), &count_down, &trace);
  operation.SetCreateInode(std::move(inode));
  mix_mutation.operations.push_back(&operation);

  CHECK(mutation_processor.Commit(mix_mutation)) << "commit mutation fail.";

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status rpc_status = operation.status;

  LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(rpc_status.error_code()), rpc_status.error_str());

  return rpc_status;
}

TEST_F(MutationProcessorTest, CreateFileInode) {
  KVStoragePtr kv_storage = DummyStorage::New();
  ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

  MutationProcessor mutation_processor(kv_storage);
  ASSERT_TRUE(mutation_processor.Init()) << "init mutation processor fail.";

  bthread_usleep(1000 * 1000);

  // create file inode
  for (int i = 0; i < 1; ++i) {
    MixMutation mix_mutation = {.fs_id = kFsId};

    bthread::CountdownEvent count_down(1);

    auto inode = GenInode(kFsId, 20000 + i, pb::mdsv2::FileType::FILE);

    Trace trace;
    Operation operation(Operation::OpType::kCreateInode, inode.ino(), MetaDataCodec::EncodeInodeKey(kFsId, inode.ino()),
                        &count_down, &trace);
    operation.SetCreateInode(std::move(inode));
    mix_mutation.operations.push_back(&operation);

    ASSERT_TRUE(mutation_processor.Commit(mix_mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    butil::Status rpc_status = operation.status;

    LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                             rpc_status.error_str());
  }
}

TEST_F(MutationProcessorTest, CreateDirInode) {
  KVStoragePtr kv_storage = DummyStorage::New();
  ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

  MutationProcessor mutation_processor(kv_storage);
  ASSERT_TRUE(mutation_processor.Init()) << "init mutation processor fail.";

  bthread_usleep(1000 * 1000);

  // create dir inode
  for (int i = 0; i < 1000; ++i) {
    MixMutation mix_mutation = {.fs_id = kFsId};

    bthread::CountdownEvent count_down(1);

    auto inode = GenInode(kFsId, 30000 + i, pb::mdsv2::FileType::DIRECTORY);

    Trace trace;
    Operation operation(Operation::OpType::kCreateInode, inode.ino(), MetaDataCodec::EncodeInodeKey(kFsId, inode.ino()),
                        &count_down, &trace);
    operation.SetCreateInode(std::move(inode));
    mix_mutation.operations.push_back(&operation);

    ASSERT_TRUE(mutation_processor.Commit(mix_mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    butil::Status rpc_status = operation.status;

    LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                             rpc_status.error_str());
  }
}

TEST_F(MutationProcessorTest, UpdateInodeNlink) {
  KVStoragePtr kv_storage = DummyStorage::New();
  ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

  MutationProcessor mutation_processor(kv_storage);
  ASSERT_TRUE(mutation_processor.Init()) << "init mutation processor fail.";

  bthread_usleep(1000 * 1000);

  uint64_t ino = 20000;
  // create file inode
  {
    MixMutation mix_mutation = {.fs_id = kFsId};

    bthread::CountdownEvent count_down(1);

    auto inode = GenInode(kFsId, ino, pb::mdsv2::FileType::FILE);

    Trace trace;
    Operation operation(Operation::OpType::kCreateInode, inode.ino(), MetaDataCodec::EncodeInodeKey(kFsId, inode.ino()),
                        &count_down, &trace);
    operation.SetCreateInode(std::move(inode));
    mix_mutation.operations.push_back(&operation);

    ASSERT_TRUE(mutation_processor.Commit(mix_mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    butil::Status rpc_status = operation.status;

    LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                             rpc_status.error_str());
  }

  // update inode nlink
  for (int i = 0; i < 10; ++i) {
    MixMutation mix_mutation = {.fs_id = kFsId};

    bthread::CountdownEvent count_down(1);

    auto inode = GenInode(kFsId, ino, pb::mdsv2::FileType::FILE);

    uint64_t now_ns = Helper::TimestampNs();

    Trace trace;
    Operation operation(Operation::OpType::kUpdateInodeNlink, inode.ino(),
                        MetaDataCodec::EncodeInodeKey(kFsId, inode.ino()), &count_down, &trace);
    operation.SetUpdateInodeNlink(inode.ino(), 1, now_ns);
    mix_mutation.operations.push_back(&operation);

    ASSERT_TRUE(mutation_processor.Commit(mix_mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    butil::Status rpc_status = operation.status;

    LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                             rpc_status.error_str());
  }

  // batch update inode nlink
  {
    MixMutation mix_mutation = {.fs_id = kFsId};

    bthread::CountdownEvent count_down(1);

    auto inode = GenInode(kFsId, ino, pb::mdsv2::FileType::FILE);

    uint64_t now_ns = Helper::TimestampNs();

    Trace trace;
    for (int i = 0; i < 100; ++i) {
      Operation operation(Operation::OpType::kUpdateInodeNlink, inode.ino(),
                          MetaDataCodec::EncodeInodeKey(kFsId, inode.ino()), &count_down, &trace);
      operation.SetUpdateInodeNlink(inode.ino(), 1, now_ns);
      mix_mutation.operations.push_back(&operation);
    }

    ASSERT_TRUE(mutation_processor.Commit(mix_mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    for (auto& operation : mix_mutation.operations) {
      butil::Status rpc_status = operation->status;
      if (!rpc_status.ok()) {
        LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                                 rpc_status.error_str());
      }
    }
  }
}

TEST_F(MutationProcessorTest, DeleteInode) {
  KVStoragePtr kv_storage = DummyStorage::New();
  ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

  MutationProcessor mutation_processor(kv_storage);
  ASSERT_TRUE(mutation_processor.Init()) << "init mutation processor fail.";

  bthread_usleep(1000 * 1000);

  // delete inode
  for (int i = 0; i < 1000; ++i) {
    MixMutation mix_mutation = {.fs_id = kFsId};

    bthread::CountdownEvent count_down(1);

    auto inode = GenInode(kFsId, 20000, pb::mdsv2::FileType::FILE);

    Trace trace;
    Operation operation(Operation::OpType::kDeleteInode, inode.ino(), MetaDataCodec::EncodeInodeKey(kFsId, inode.ino()),
                        &count_down, &trace);
    operation.SetDeleteInode(inode.ino());
    mix_mutation.operations.push_back(&operation);

    ASSERT_TRUE(mutation_processor.Commit(mix_mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    butil::Status rpc_status = operation.status;

    LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                             rpc_status.error_str());
  }
}

TEST_F(MutationProcessorTest, MkNod) {
  KVStoragePtr kv_storage = DummyStorage::New();
  ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

  MutationProcessor mutation_processor(kv_storage);
  ASSERT_TRUE(mutation_processor.Init()) << "init mutation processor fail.";

  bthread_usleep(1000 * 1000);

  auto status = CreateRoot(mutation_processor);
  ASSERT_TRUE(status.ok()) << "create root fail.";

  // mknod
  for (int i = 0; i < 1; ++i) {
    MixMutation mix_mutation = {.fs_id = kFsId};

    bthread::CountdownEvent count_down(2);

    uint64_t now_ns = Helper::TimestampNs();

    uint64_t ino = 20000 + i;

    auto inode = GenInode(kFsId, ino, pb::mdsv2::FileType::FILE);

    Trace trace;
    Operation operation(Operation::OpType::kCreateInode, inode.ino(), MetaDataCodec::EncodeInodeKey(kFsId, inode.ino()),
                        &count_down, &trace);
    auto inode_copy = inode;
    operation.SetCreateInode(std::move(inode_copy));
    mix_mutation.operations.push_back(&operation);

    pb::mdsv2::Dentry dentry = GenDentry(kFsId, kRootIno, ino, "file_" + std::to_string(ino));
    Operation dentry_operation(Operation::OpType::kCreateDentry, dentry.parent_ino(),
                               MetaDataCodec::EncodeDentryKey(kFsId, dentry.parent_ino(), dentry.name()), &count_down,
                               &trace);
    dentry_operation.SetCreateDentry(std::move(dentry), now_ns);
    mix_mutation.operations.push_back(&dentry_operation);

    ASSERT_TRUE(mutation_processor.Commit(mix_mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    butil::Status rpc_status = operation.status;
    butil::Status rpc_dentry_status = dentry_operation.status;

    LOG(INFO) << fmt::format("rpc status: {} {} {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                             rpc_status.error_str(), pb::error::Errno_Name(rpc_dentry_status.error_code()),
                             rpc_dentry_status.error_str());
  }
}

TEST_F(MutationProcessorTest, MkDir) {
  KVStoragePtr kv_storage = DummyStorage::New();
  ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

  MutationProcessor mutation_processor(kv_storage);
  ASSERT_TRUE(mutation_processor.Init()) << "init mutation processor fail.";

  bthread_usleep(1000 * 1000);

  // mkdir
  for (int i = 0; i < 1000; ++i) {
    MixMutation mix_mutation = {.fs_id = kFsId};

    bthread::CountdownEvent count_down(2);

    uint64_t now_ns = Helper::TimestampNs();

    uint64_t ino = 20000 + i;

    auto inode = GenInode(kFsId, ino, pb::mdsv2::FileType::DIRECTORY);

    Trace trace;
    Operation operation(Operation::OpType::kCreateInode, inode.ino(), MetaDataCodec::EncodeInodeKey(kFsId, inode.ino()),
                        &count_down, &trace);
    auto inode_copy = inode;
    operation.SetCreateInode(std::move(inode_copy));
    mix_mutation.operations.push_back(&operation);

    pb::mdsv2::Dentry dentry = GenDentry(kFsId, kRootIno, ino, "dir_" + std::to_string(ino));
    Operation dentry_operation(Operation::OpType::kCreateDentry, dentry.parent_ino(),
                               MetaDataCodec::EncodeDentryKey(kFsId, dentry.parent_ino(), dentry.name()), &count_down,
                               &trace);
    dentry_operation.SetCreateDentry(std::move(dentry), now_ns);
    mix_mutation.operations.push_back(&dentry_operation);

    ASSERT_TRUE(mutation_processor.Commit(mix_mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    butil::Status rpc_status = operation.status;
    butil::Status rpc_dentry_status = dentry_operation.status;

    LOG(INFO) << fmt::format("rpc status: {} {} {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                             rpc_status.error_str(), pb::error::Errno_Name(rpc_dentry_status.error_code()),
                             rpc_dentry_status.error_str());
  }
}

}  // namespace unit_test
}  // namespace mdsv2
}  // namespace dingofs