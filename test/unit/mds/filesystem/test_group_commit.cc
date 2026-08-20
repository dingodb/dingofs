// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include <bthread/countdown_event.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <set>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "mds/common/codec.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"
#include "mds/filesystem/dentry.h"
#include "mds/filesystem/inode.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_store_operation_max_inflight_per_key);

namespace unit_test {

namespace {

constexpr uint32_t kFsId = 1000;
constexpr Ino kParentIno = 1;

AttrEntry MakeDirAttr(Ino ino) {
  AttrEntry attr;
  attr.set_fs_id(kFsId);
  attr.set_ino(ino);
  attr.set_mode(S_IFDIR | 0755);
  attr.set_type(pb::mds::FileType::DIRECTORY);
  attr.set_nlink(2);
  attr.set_version(1);
  return attr;
}

AttrEntry MakeFileAttr(Ino ino) {
  AttrEntry attr;
  attr.set_fs_id(kFsId);
  attr.set_ino(ino);
  attr.set_mode(S_IFREG | 0644);
  attr.set_type(pb::mds::FileType::FILE);
  attr.set_nlink(1);
  attr.add_parents(kParentIno);
  attr.set_version(1);
  return attr;
}

}  // namespace

class GroupCommitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    kv_storage_ = DummyStorage::New();
    ASSERT_TRUE(kv_storage_->Init("")) << "init kv storage fail.";

    processor_ = OperationProcessor::New(kv_storage_);
    ASSERT_TRUE(processor_->Init()) << "init operation processor fail.";

    // the batch transaction reads the parent inode, so it must exist
    ASSERT_TRUE(kv_storage_
                    ->Put(KVStorage::WriteOption(),
                          MetaCodec::EncodeInodeKey(kFsId, kParentIno),
                          MetaCodec::EncodeInodeValue(MakeDirAttr(kParentIno)))
                    .ok());
  }

  void TearDown() override {
    if (processor_ != nullptr) {
      processor_->Stop();
      processor_ = nullptr;
    }
    kv_storage_ = nullptr;
  }

  KVStorageSPtr kv_storage_;
  OperationProcessorSPtr processor_;
};

// Group commit parks operations that exceed the per-key in-flight limit and
// drains them from the transaction bthread. Under that pressure every
// concurrent create must still be executed exactly once, and the processor
// must not leak parked operations (otherwise the countdown never reaches 0).
TEST_F(GroupCommitTest, ConcurrentMkNodUnderParkingPressure) {
  constexpr int kOpNum = 256;

  auto run_workload = [&](uint32_t max_inflight, int ino_base) {
    FLAGS_mds_store_operation_max_inflight_per_key = max_inflight;

    std::vector<Trace> traces(kOpNum);
    std::vector<Status> statuses(kOpNum);
    bthread::CountdownEvent done(kOpNum);
    auto parent_inode = Inode::New(MakeDirAttr(kParentIno));

    std::vector<std::thread> threads;
    threads.reserve(kOpNum);
    for (int i = 0; i < kOpNum; i++) {
      threads.emplace_back([&, i]() {
        const Ino ino = ino_base + i;
        Dentry dentry(kFsId, "f" + std::to_string(ino), kParentIno, ino,
                      pb::mds::FileType::FILE, 0);
        auto attr = MakeFileAttr(ino);

        MkNodOperation operation(traces[i], parent_inode, dentry, attr);
        operation.SetEvent(&done);
        EXPECT_TRUE(processor_->RunBatched(&operation));
        done.wait();  // no operation may be dropped, otherwise this hangs
        statuses[i] = operation.GetStatus();
      });
    }
    for (auto& thread : threads) thread.join();

    std::set<uint64_t> txn_ids;
    for (int i = 0; i < kOpNum; i++) {
      const Ino ino = ino_base + i;
      EXPECT_TRUE(statuses[i].ok())
          << "op " << i << " fail: " << statuses[i].error_str();

      // every dentry the operation claimed to create is readable
      std::string value;
      EXPECT_TRUE(kv_storage_
                      ->Get(MetaCodec::EncodeDentryKey(
                                kFsId, kParentIno, "f" + std::to_string(ino)),
                            value)
                      .ok())
          << "dentry of ino " << ino << " missing";

      // an operation must be committed by exactly one transaction
      EXPECT_EQ(traces[i].GetTxns().size(), 1u)
          << "op " << i << " executed " << traces[i].GetTxns().size()
          << " times";

      for (const auto& txn : traces[i].GetTxns()) txn_ids.insert(txn.txn_id);
    }
    return txn_ids.size();
  };

  int ino_base = 1000;
  for (uint32_t max_inflight : {1U, 2U, 1000000U}) {
    const size_t txn_num = run_workload(max_inflight, ino_base);
    EXPECT_GT(txn_num, 0u);
    EXPECT_LE(txn_num, static_cast<size_t>(kOpNum))
        << "max_inflight=" << max_inflight;
    ino_base += 1000;
  }
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
