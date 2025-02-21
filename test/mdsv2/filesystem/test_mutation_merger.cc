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

#include <bthread/bthread.h>
#include <bthread/types.h>

#include <cstdint>
#include <vector>

#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/filesystem/mutation_merger.h"
#include "mdsv2/storage/dummy_storage.h"

namespace dingofs {
namespace mdsv2 {
namespace unit_test {

const int64_t kFsId = 1000;

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

static pb::mdsv2::Dentry GenDentry(uint32_t fs_id, uint64_t ino, const std::string& name) {
  pb::mdsv2::Dentry dentry;
  dentry.set_fs_id(fs_id);
  dentry.set_ino(ino);
  dentry.set_name(name);

  return dentry;
}

class MutationMergerTest : public testing::Test {
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

KVStoragePtr MutationMergerTest::kv_storage = nullptr;

TEST_F(MutationMergerTest, Merge) {
  MutationMerger merger(kv_storage);

  ASSERT_TRUE(merger.Init());

  // just different inode
  {
    int num = 10;
    std::vector<Mutation> mutations;
    for (int i = 0; i < num; i++) {
      Mutation mutation;
      mutation.type = Mutation::Type::kFileInode;
      mutation.fs_id = kFsId;
      mutation.inode_op = {Mutation::OpType::kPut, GenInode(kFsId, 100000 + i, pb::mdsv2::FileType::FILE)};

      mutation.notification.count_down_event = nullptr;
      mutation.notification.status = nullptr;

      mutations.push_back(mutation);
    }

    std::map<MutationMerger::Key, MergeMutation> out_merge_mutations;
    merger.TestMerge(mutations, out_merge_mutations);

    ASSERT_EQ(num, out_merge_mutations.size());

    for (const auto& [key, merge_mutation] : out_merge_mutations) {
      ASSERT_EQ(Mutation::Type::kFileInode, merge_mutation.type);
      ASSERT_EQ(kFsId, merge_mutation.fs_id);

      const auto& inode = merge_mutation.inode_op.inode;
      ASSERT_EQ(key.ino, inode.ino());
      ASSERT_EQ(pb::mdsv2::FileType::FILE, inode.type());

      ASSERT_EQ(0, merge_mutation.dentry_ops.size());
      ASSERT_EQ(1, merge_mutation.notifications.size());
    }
  }

  // just same inode
  {
    int num = 10;
    std::vector<Mutation> mutations;
    for (int i = 0; i < num; i++) {
      Mutation mutation;
      mutation.type = Mutation::Type::kFileInode;
      mutation.fs_id = kFsId;
      mutation.inode_op = {Mutation::OpType::kPut, GenInode(kFsId, 100000, pb::mdsv2::FileType::FILE)};

      mutation.notification.count_down_event = nullptr;
      mutation.notification.status = nullptr;

      mutations.push_back(mutation);
    }

    std::map<MutationMerger::Key, MergeMutation> out_merge_mutations;
    merger.TestMerge(mutations, out_merge_mutations);

    ASSERT_EQ(1, out_merge_mutations.size());

    for (const auto& [key, merge_mutation] : out_merge_mutations) {
      ASSERT_EQ(Mutation::Type::kFileInode, merge_mutation.type);
      ASSERT_EQ(kFsId, merge_mutation.fs_id);

      const auto& inode = merge_mutation.inode_op.inode;
      ASSERT_EQ(key.ino, inode.ino());
      ASSERT_EQ(pb::mdsv2::FileType::FILE, inode.type());

      ASSERT_EQ(0, merge_mutation.dentry_ops.size());
      ASSERT_EQ(num, merge_mutation.notifications.size());
    }
  }

  // parent and dentry
  {
    int num = 10;
    uint64_t parent_ino = 100000;
    std::vector<Mutation> mutations;
    for (int i = 0; i < num; i++) {
      Mutation mutation;
      mutation.type = Mutation::Type::kParentWithDentry;
      mutation.fs_id = kFsId;
      mutation.inode_op = {Mutation::OpType::kPut, GenInode(kFsId, parent_ino, pb::mdsv2::FileType::DIRECTORY)};

      mutation.dentry_op = {Mutation::OpType::kPut, GenDentry(kFsId, 200000 + i, "file" + std::to_string(i))};

      mutation.notification.count_down_event = nullptr;
      mutation.notification.status = nullptr;

      mutations.push_back(mutation);
    }

    std::map<MutationMerger::Key, MergeMutation> out_merge_mutations;
    merger.TestMerge(mutations, out_merge_mutations);

    ASSERT_EQ(1, out_merge_mutations.size());
    auto it = out_merge_mutations.find({kFsId, parent_ino});
    ASSERT_TRUE(it != out_merge_mutations.end());

    auto& merge_mutation = it->second;
    ASSERT_EQ(Mutation::Type::kParentWithDentry, merge_mutation.type);
    ASSERT_EQ(kFsId, merge_mutation.fs_id);

    const auto& inode = merge_mutation.inode_op.inode;
    ASSERT_EQ(parent_ino, inode.ino());
    ASSERT_EQ(pb::mdsv2::FileType::DIRECTORY, inode.type());

    ASSERT_EQ(num, merge_mutation.dentry_ops.size());
    ASSERT_EQ(num, merge_mutation.notifications.size());
  }

  // mix
  {
    std::vector<Mutation> mutations;

    uint64_t ino = 100000;
    {
      Mutation mutation;
      mutation.type = Mutation::Type::kFileInode;
      mutation.fs_id = kFsId;
      mutation.inode_op = {Mutation::OpType::kPut, GenInode(kFsId, ino, pb::mdsv2::FileType::FILE)};

      mutation.notification.count_down_event = nullptr;
      mutation.notification.status = nullptr;
      mutations.push_back(mutation);
    }

    {
      uint64_t dentry_ino = 200000;
      Mutation mutation;
      mutation.type = Mutation::Type::kParentWithDentry;
      mutation.fs_id = kFsId;
      mutation.inode_op = {Mutation::OpType::kPut, GenInode(kFsId, ino, pb::mdsv2::FileType::DIRECTORY)};

      mutation.dentry_op = {Mutation::OpType::kPut, GenDentry(kFsId, dentry_ino, "file" + std::to_string(dentry_ino))};

      mutation.notification.count_down_event = nullptr;
      mutation.notification.status = nullptr;
    }
  }

  // mix delete
  {
    std::vector<Mutation> mutations;

    uint64_t ino = 100000;
    {
      Mutation mutation;
      mutation.type = Mutation::Type::kFileInode;
      mutation.fs_id = kFsId;
      mutation.inode_op = {Mutation::OpType::kPut, GenInode(kFsId, ino, pb::mdsv2::FileType::FILE)};

      mutation.notification.count_down_event = nullptr;
      mutation.notification.status = nullptr;
      mutations.push_back(mutation);
    }

    {
      uint64_t dentry_ino = 200000;
      Mutation mutation;
      mutation.type = Mutation::Type::kParentWithDentry;
      mutation.fs_id = kFsId;
      mutation.inode_op = {Mutation::OpType::kPut, GenInode(kFsId, ino, pb::mdsv2::FileType::DIRECTORY)};

      mutation.dentry_op = {Mutation::OpType::kPut, GenDentry(kFsId, dentry_ino, "file" + std::to_string(dentry_ino))};

      mutation.notification.count_down_event = nullptr;
      mutation.notification.status = nullptr;
    }

    {
      Mutation mutation;
      mutation.type = Mutation::Type::kFileInode;
      mutation.fs_id = kFsId;
      mutation.inode_op = {Mutation::OpType::kDelete, GenInode(kFsId, ino, pb::mdsv2::FileType::FILE)};

      mutation.notification.count_down_event = nullptr;
      mutation.notification.status = nullptr;
      mutations.push_back(mutation);
    }

    {
      uint64_t dentry_ino = 200001;
      Mutation mutation;
      mutation.type = Mutation::Type::kParentWithDentry;
      mutation.fs_id = kFsId;
      mutation.inode_op = {Mutation::OpType::kPut, GenInode(kFsId, ino, pb::mdsv2::FileType::DIRECTORY)};

      mutation.dentry_op = {Mutation::OpType::kPut, GenDentry(kFsId, dentry_ino, "file" + std::to_string(dentry_ino))};

      mutation.notification.count_down_event = nullptr;
      mutation.notification.status = nullptr;
    }
  }

  merger.Destroy();
}

TEST_F(MutationMergerTest, CommitMutation) {
  MutationMerger merger(kv_storage);

  ASSERT_TRUE(merger.Init());

  bthread_usleep(1000 * 1000);

  // single file inode mutation
  for (int i = 0; i < 1000; ++i) {
    bthread::CountdownEvent count_down(1);

    butil::Status rpc_status;
    auto inode = GenInode(kFsId, 20000, pb::mdsv2::FileType::FILE);
    Mutation file_mutation(kFsId, {Mutation::OpType::kPut, inode}, &count_down, &rpc_status);

    ASSERT_TRUE(merger.CommitMutation(file_mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                             rpc_status.error_str());
  }

  // multiple file inode mutation
  for (int i = 0; i < 1000; ++i) {
    int batch_size = 10;
    bthread::CountdownEvent count_down(batch_size);

    std::vector<butil::Status> rpc_status(batch_size);
    std::vector<Mutation> mutations;
    for (int j = 0; j < batch_size; ++j) {
      auto inode = GenInode(kFsId, 20000 + j, pb::mdsv2::FileType::FILE);
      mutations.emplace_back(kFsId, Mutation::InodeOperation{Mutation::OpType::kPut, inode}, &count_down,
                             &rpc_status[j]);
    }

    ASSERT_TRUE(merger.CommitMutation(mutations)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    for (auto& status : rpc_status) {
      LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(status.error_code()), status.error_str());
    }
  }

  // multiple same file inode mutation
  for (int i = 0; i < 1000; ++i) {
    int batch_size = 10;
    bthread::CountdownEvent count_down(batch_size);

    std::vector<butil::Status> rpc_status(batch_size);
    std::vector<Mutation> mutations;
    for (int j = 0; j < batch_size; ++j) {
      auto inode = GenInode(kFsId, 20000, pb::mdsv2::FileType::FILE);
      mutations.emplace_back(kFsId, Mutation::InodeOperation{Mutation::OpType::kPut, inode}, &count_down,
                             &rpc_status[j]);
    }

    ASSERT_TRUE(merger.CommitMutation(mutations)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    for (auto& status : rpc_status) {
      LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(status.error_code()), status.error_str());
    }
  }

  // single parent/dentry mutation
  for (int i = 0; i < 1000; ++i) {
    bthread::CountdownEvent count_down(1);

    butil::Status rpc_status;
    auto inode = GenInode(kFsId, 20000, pb::mdsv2::FileType::FILE);
    auto dentry = GenDentry(kFsId, 30000, "file" + std::to_string(30000));

    Mutation mutation(kFsId, {Mutation::OpType::kPut, inode}, {Mutation::OpType::kDelete, dentry}, &count_down,
                      &rpc_status);

    ASSERT_TRUE(merger.CommitMutation(mutation)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(rpc_status.error_code()),
                             rpc_status.error_str());
  }

  // multiple parent/dentry mutation
  for (int i = 0; i < 1000; ++i) {
    int batch_size = 10;
    bthread::CountdownEvent count_down(batch_size);

    std::vector<butil::Status> rpc_status(batch_size);
    std::vector<Mutation> mutations;
    for (int j = 0; j < batch_size; ++j) {
      auto inode = GenInode(kFsId, 20000 + j, pb::mdsv2::FileType::FILE);
      auto dentry = GenDentry(kFsId, 30000 + j, "file" + std::to_string(30000 + j));

      mutations.emplace_back(kFsId, Mutation::InodeOperation{Mutation::OpType::kPut, inode},
                             Mutation::DentryOperation{Mutation::OpType::kPut, dentry}, &count_down, &rpc_status[j]);
    }

    ASSERT_TRUE(merger.CommitMutation(mutations)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    for (auto& status : rpc_status) {
      LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(status.error_code()), status.error_str());
    }
  }

  // multiple same parent/dentry mutation
  for (int i = 0; i < 1000; ++i) {
    int batch_size = 10;
    bthread::CountdownEvent count_down(batch_size);

    std::vector<butil::Status> rpc_status(batch_size);
    std::vector<Mutation> mutations;
    for (int j = 0; j < batch_size; ++j) {
      auto inode = GenInode(kFsId, 20000, pb::mdsv2::FileType::FILE);
      auto dentry = GenDentry(kFsId, 30000 + j, "file" + std::to_string(30000 + j));

      mutations.emplace_back(kFsId, Mutation::InodeOperation{Mutation::OpType::kPut, inode},
                             Mutation::DentryOperation{Mutation::OpType::kPut, dentry}, &count_down, &rpc_status[j]);
    }

    ASSERT_TRUE(merger.CommitMutation(mutations)) << "commit mutation fail.";

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    for (auto& status : rpc_status) {
      LOG(INFO) << fmt::format("rpc status: {} {}", pb::error::Errno_Name(status.error_code()), status.error_str());
    }
  }

  merger.Destroy();
}

}  // namespace unit_test
}  // namespace mdsv2
}  // namespace dingofs