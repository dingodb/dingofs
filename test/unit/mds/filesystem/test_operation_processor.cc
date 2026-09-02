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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "mds/common/codec.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_store_operation_batch_size);
DECLARE_uint32(mds_store_operation_dispatcher_num);
DECLARE_uint32(mds_store_operation_max_inflight_per_key);
DECLARE_uint32(mds_txn_max_retry_times);

namespace unit_test {
namespace {

constexpr uint32_t kFsId = 100;
constexpr Ino kDirIno = 1;

Status RetryStatus() { return Status(pb::error::ESTORE_MAYBE_RETRY, "retry"); }

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

bool WaitFor(bthread::CountdownEvent& event) {
  timespec deadline;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 3;
  return event.timed_wait(deadline) == 0;
}

class Gate {
 public:
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    entered_ = true;
    cond_.notify_all();
    cond_.wait(lock, [this] { return open_; });
  }

  bool WaitUntilEntered() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cond_.wait_for(lock, std::chrono::seconds(3),
                          [this] { return entered_; });
  }

  void Open() {
    std::lock_guard<std::mutex> lock(mutex_);
    open_ = true;
    cond_.notify_all();
  }

  ~Gate() { Open(); }

 private:
  std::mutex mutex_;
  std::condition_variable cond_;
  bool entered_{false};
  bool open_{false};
};

class ScriptedStorage;

class ScriptedTxn : public Txn {
 public:
  ScriptedTxn(TxnUPtr txn, ScriptedStorage* storage)
      : txn_(std::move(txn)), storage_(storage) {}

  int64_t ID() const override { return txn_->ID(); }
  Status Put(const std::string& key, const std::string& value) override {
    return txn_->Put(key, value);
  }
  Status PutIfAbsent(const std::string& key,
                     const std::string& value) override {
    return txn_->PutIfAbsent(key, value);
  }
  Status Delete(const std::string& key) override { return txn_->Delete(key); }
  Status Get(const std::string& key, std::string& value) override {
    return txn_->Get(key, value);
  }
  Status BatchGet(const std::vector<std::string>& keys,
                  std::vector<KeyValue>& kvs) override;
  Status Scan(const Range& range, uint64_t limit,
              std::vector<KeyValue>& kvs) override {
    return txn_->Scan(range, limit, kvs);
  }
  Status Scan(const Range& range, ScanHandlerType handler) override {
    return txn_->Scan(range, handler);
  }
  Status Scan(const Range& range,
              std::function<bool(KeyValue&)> handler) override {
    return txn_->Scan(range, handler);
  }
  Status Commit() override;
  Trace::Txn GetTrace() override { return txn_->GetTrace(); }

 private:
  TxnUPtr txn_;
  ScriptedStorage* storage_;
};

class ScriptedStorage : public DummyStorage {
 public:
  TxnUPtr NewTxn(
      Txn::IsolationLevel isolation_level = Txn::kSnapshotIsolation) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (new_txn_failures_ > 0) {
        --new_txn_failures_;
        return nullptr;
      }
    }
    return std::make_unique<ScriptedTxn>(DummyStorage::NewTxn(isolation_level),
                                         this);
  }

  Status IsExistTable(const std::string& start_key,
                      const std::string& end_key) override {
    std::lock_guard<std::mutex> lock(mutex_);
    checked_start_ = start_key;
    checked_end_ = end_key;
    return check_table_status_;
  }

  Status CreateTable(const std::string& name, const TableOption& option,
                     int64_t& table_id) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      created_name_ = name;
      created_option_ = option;
      if (!create_table_status_.ok()) return create_table_status_;
    }
    return DummyStorage::CreateTable(name, option, table_id);
  }

  void SetNewTxnFailures(uint32_t failures) {
    std::lock_guard<std::mutex> lock(mutex_);
    new_txn_failures_ = failures;
  }

  void SetBatchGetStatuses(std::vector<Status> statuses) {
    std::lock_guard<std::mutex> lock(mutex_);
    batch_get_statuses_ = {statuses.begin(), statuses.end()};
  }

  void SetCommitStatuses(std::vector<Status> statuses) {
    std::lock_guard<std::mutex> lock(mutex_);
    commit_statuses_ = {statuses.begin(), statuses.end()};
  }

  void SetCheckTableStatus(const Status& status) {
    std::lock_guard<std::mutex> lock(mutex_);
    check_table_status_ = status;
  }

  void SetCreateTableStatus(const Status& status) {
    std::lock_guard<std::mutex> lock(mutex_);
    create_table_status_ = status;
  }

  Status TakeBatchGetStatus() { return TakeStatus(batch_get_statuses_); }
  Status TakeCommitStatus() { return TakeStatus(commit_statuses_); }

  std::string CheckedStart() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return checked_start_;
  }
  std::string CheckedEnd() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return checked_end_;
  }
  std::string CreatedName() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return created_name_;
  }
  TableOption CreatedOption() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return created_option_;
  }

 private:
  Status TakeStatus(std::deque<Status>& statuses) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (statuses.empty()) return Status::OK();
    Status status = statuses.front();
    statuses.pop_front();
    return status;
  }

  mutable std::mutex mutex_;
  uint32_t new_txn_failures_{0};
  std::deque<Status> batch_get_statuses_;
  std::deque<Status> commit_statuses_;
  Status check_table_status_;
  Status create_table_status_;
  std::string checked_start_;
  std::string checked_end_;
  std::string created_name_;
  TableOption created_option_;
};

Status ScriptedTxn::BatchGet(const std::vector<std::string>& keys,
                             std::vector<KeyValue>& kvs) {
  Status status = storage_->TakeBatchGetStatus();
  return status.ok() ? txn_->BatchGet(keys, kvs) : status;
}

Status ScriptedTxn::Commit() {
  Status status = storage_->TakeCommitStatus();
  return status.ok() ? txn_->Commit() : status;
}

class TestOperation : public Operation {
 public:
  TestOperation(Trace& trace, OpType type, uint32_t fs_id, Ino ino)
      : Operation(trace), type_(type), fs_id_(fs_id), ino_(ino) {}

  OpType GetOpType() const override { return type_; }
  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override {
    ++run_calls_;
    return run_handler_ ? run_handler_(txn) : Next(run_statuses_, run_index_);
  }

  Status RunInBatch(TxnUPtr&, BatchSharedParam&) override {
    ++batch_calls_;
    return Next(batch_statuses_, batch_index_);
  }

  void SetResultAttr(BatchSharedParam&) override {}

  void SetRunStatuses(std::vector<Status> statuses) {
    run_statuses_ = std::move(statuses);
  }
  void SetBatchStatuses(std::vector<Status> statuses) {
    batch_statuses_ = std::move(statuses);
  }
  void SetRunHandler(std::function<Status(TxnUPtr&)> handler) {
    run_handler_ = std::move(handler);
  }

  uint32_t RunCalls() const { return run_calls_.load(); }
  uint32_t BatchCalls() const { return batch_calls_.load(); }

 private:
  static Status Next(const std::vector<Status>& statuses, size_t& index) {
    return index < statuses.size() ? statuses[index++] : Status::OK();
  }

  OpType type_;
  uint32_t fs_id_;
  Ino ino_;
  std::vector<Status> run_statuses_;
  std::vector<Status> batch_statuses_;
  size_t run_index_{0};
  size_t batch_index_{0};
  std::function<Status(TxnUPtr&)> run_handler_;
  std::atomic<uint32_t> run_calls_{0};
  std::atomic<uint32_t> batch_calls_{0};
};

class OperationProcessorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    old_batch_size_ = FLAGS_mds_store_operation_batch_size;
    old_dispatcher_num_ = FLAGS_mds_store_operation_dispatcher_num;
    old_max_inflight_ = FLAGS_mds_store_operation_max_inflight_per_key;
    old_max_retries_ = FLAGS_mds_txn_max_retry_times;

    FLAGS_mds_store_operation_batch_size = 8;
    FLAGS_mds_store_operation_dispatcher_num = 1;
    FLAGS_mds_store_operation_max_inflight_per_key = 1;
    FLAGS_mds_txn_max_retry_times = 1;

    storage_ = std::make_shared<ScriptedStorage>();
    ASSERT_TRUE(storage_->Init(""));
  }

  void TearDown() override {
    if (processor_ != nullptr) processor_->Stop();

    FLAGS_mds_store_operation_batch_size = old_batch_size_;
    FLAGS_mds_store_operation_dispatcher_num = old_dispatcher_num_;
    FLAGS_mds_store_operation_max_inflight_per_key = old_max_inflight_;
    FLAGS_mds_txn_max_retry_times = old_max_retries_;
  }

  void Start() {
    processor_ = OperationProcessor::New(storage_);
    ASSERT_TRUE(processor_->Init());
  }

  void SeedDir(Ino ino = kDirIno) {
    ASSERT_TRUE(storage_
                    ->Put(KVStorage::WriteOption(),
                          MetaCodec::EncodeInodeKey(kFsId, ino),
                          MetaCodec::EncodeInodeValue(MakeDirAttr(ino)))
                    .ok());
  }

  void SetBatchGate(Gate& gate) {
    processor_ = OperationProcessor::New(storage_);
    processor_->SetBeforeBatchDrainHookForTest([&gate] { gate.Wait(); });
    ASSERT_TRUE(processor_->Init());
  }

  std::shared_ptr<ScriptedStorage> storage_;
  OperationProcessorSPtr processor_;

 private:
  uint32_t old_batch_size_{0};
  uint32_t old_dispatcher_num_{0};
  uint32_t old_max_inflight_{0};
  uint32_t old_max_retries_{0};
};

TEST_F(OperationProcessorTest, LifecycleAccessorsAndRejectAfterStop) {
  Start();
  EXPECT_EQ(processor_->GetKVStorage(), storage_);
  EXPECT_EQ(processor_->GetSelfPtr(), processor_);

  ASSERT_TRUE(processor_->Stop());

  Trace batch_trace;
  TestOperation batch_op(batch_trace, Operation::OpType::kUpdateAttr, kFsId,
                         kDirIno);
  EXPECT_FALSE(processor_->RunBatched(&batch_op));

  Trace async_trace;
  auto async_op = std::make_shared<TestOperation>(
      async_trace, Operation::OpType::kGetFs, kFsId, kDirIno);
  EXPECT_FALSE(processor_->AsyncRun(async_op, nullptr));
  EXPECT_EQ(async_op->RunCalls(), 0u);
}

TEST_F(OperationProcessorTest, RunAloneRetriesAndPropagatesFailures) {
  Start();

  Trace success_trace;
  TestOperation success(success_trace, Operation::OpType::kGetFs, kFsId,
                        kDirIno);
  storage_->SetCommitStatuses({RetryStatus(), Status::OK()});
  ASSERT_TRUE(processor_->RunAlone(&success).ok());
  EXPECT_EQ(success.RunCalls(), 2u);
  EXPECT_EQ(success_trace.GetTxns().size(), 2u);

  Trace failure_trace;
  TestOperation failure(failure_trace, Operation::OpType::kGetFs, kFsId,
                        kDirIno);
  storage_->SetCommitStatuses({RetryStatus(), RetryStatus()});
  Status status = processor_->RunAlone(&failure);
  EXPECT_EQ(status.error_code(), pb::error::ESTORE_MAYBE_RETRY);
  EXPECT_EQ(failure.GetStatus().error_code(), pb::error::ESTORE_MAYBE_RETRY);

  Trace new_txn_trace;
  TestOperation new_txn_failure(new_txn_trace, Operation::OpType::kGetFs, kFsId,
                                kDirIno);
  storage_->SetNewTxnFailures(2);
  status = processor_->RunAlone(&new_txn_failure);
  EXPECT_EQ(status.error_code(), pb::error::EBACKEND_STORE);
  EXPECT_EQ(new_txn_failure.GetStatus().error_code(),
            pb::error::EBACKEND_STORE);
}

TEST_F(OperationProcessorTest, AsyncRunCallsHandlerOnlyAfterSuccess) {
  Start();

  bthread::CountdownEvent callback_done(1);
  std::atomic<uint32_t> callback_count{0};
  Trace success_trace;
  auto success = std::make_shared<TestOperation>(
      success_trace, Operation::OpType::kGetFs, kFsId, kDirIno);
  ASSERT_TRUE(processor_->AsyncRun(success, [&](OperationSPtr) {
    ++callback_count;
    callback_done.signal();
  }));
  ASSERT_TRUE(WaitFor(callback_done));
  EXPECT_EQ(callback_count.load(), 1u);
  EXPECT_EQ(success->RunCalls(), 1u);

  Gate failure_gate;
  std::atomic<uint32_t> failure_callback_count{0};
  Trace failure_trace;
  auto failure = std::make_shared<TestOperation>(
      failure_trace, Operation::OpType::kGetFs, kFsId, kDirIno);
  failure->SetRunHandler([&](TxnUPtr&) {
    failure_gate.Wait();
    return Status(pb::error::EINTERNAL, "operation failed");
  });
  ASSERT_TRUE(processor_->AsyncRun(
      failure, [&](OperationSPtr) { ++failure_callback_count; }));
  ASSERT_TRUE(failure_gate.WaitUntilEntered());
  EXPECT_EQ(failure_callback_count.load(), 0u);
  failure_gate.Open();

  bthread::CountdownEvent sentinel_done(1);
  Trace sentinel_trace;
  auto sentinel = std::make_shared<TestOperation>(
      sentinel_trace, Operation::OpType::kGetFs, kFsId, kDirIno);
  ASSERT_TRUE(processor_->AsyncRun(
      sentinel, [&](OperationSPtr) { sentinel_done.signal(); }));
  ASSERT_TRUE(WaitFor(sentinel_done));
  EXPECT_EQ(failure_callback_count.load(), 0u);
  EXPECT_EQ(failure->GetStatus().error_code(), pb::error::EINTERNAL);
}

TEST_F(OperationProcessorTest,
       BatchedOperationsShareTransactionWhenGateCollectsThem) {
  SeedDir();
  Gate gate;
  SetBatchGate(gate);

  Trace first_trace;
  Trace second_trace;
  TestOperation first(first_trace, Operation::OpType::kUpdateAttr, kFsId,
                      kDirIno);
  TestOperation second(second_trace, Operation::OpType::kUpdateAttr, kFsId,
                       kDirIno);
  bthread::CountdownEvent done(2);
  first.SetEvent(&done);
  second.SetEvent(&done);

  ASSERT_TRUE(processor_->RunBatched(&first));
  ASSERT_TRUE(gate.WaitUntilEntered());
  ASSERT_TRUE(processor_->RunBatched(&second));
  gate.Open();
  ASSERT_TRUE(WaitFor(done));

  ASSERT_TRUE(first.GetStatus().ok());
  ASSERT_TRUE(second.GetStatus().ok());
  ASSERT_EQ(first_trace.GetTxns().size(), 1u);
  ASSERT_EQ(second_trace.GetTxns().size(), 1u);
  EXPECT_EQ(first_trace.GetTxns()[0].txn_id, second_trace.GetTxns()[0].txn_id);
}

TEST_F(OperationProcessorTest, BatchedOperationsWithDifferentKeysComplete) {
  SeedDir(kDirIno);
  SeedDir(3);
  Start();

  Trace first_trace;
  Trace second_trace;
  TestOperation first(first_trace, Operation::OpType::kUpdateAttr, kFsId,
                      kDirIno);
  TestOperation second(second_trace, Operation::OpType::kUpdateAttr, kFsId, 3);
  bthread::CountdownEvent done(2);
  first.SetEvent(&done);
  second.SetEvent(&done);

  ASSERT_TRUE(processor_->RunBatched(&first));
  ASSERT_TRUE(processor_->RunBatched(&second));
  ASSERT_TRUE(WaitFor(done));
  EXPECT_TRUE(first.GetStatus().ok());
  EXPECT_TRUE(second.GetStatus().ok());
  EXPECT_EQ(first_trace.GetTxns().size(), 1u);
  EXPECT_EQ(second_trace.GetTxns().size(), 1u);
}

TEST_F(OperationProcessorTest, BatchedOperationFailureDoesNotAbortPeers) {
  SeedDir();
  Gate gate;
  SetBatchGate(gate);

  Trace failed_trace;
  Trace success_trace;
  TestOperation failed(failed_trace, Operation::OpType::kUpdateAttr, kFsId,
                       kDirIno);
  failed.SetBatchStatuses({Status(pb::error::EINTERNAL, "operation failed")});
  TestOperation success(success_trace, Operation::OpType::kUpdateAttr, kFsId,
                        kDirIno);
  bthread::CountdownEvent done(2);
  failed.SetEvent(&done);
  success.SetEvent(&done);

  ASSERT_TRUE(processor_->RunBatched(&failed));
  ASSERT_TRUE(gate.WaitUntilEntered());
  ASSERT_TRUE(processor_->RunBatched(&success));
  gate.Open();
  ASSERT_TRUE(WaitFor(done));

  EXPECT_EQ(failed.GetStatus().error_code(), pb::error::EINTERNAL);
  EXPECT_TRUE(success.GetStatus().ok());
  EXPECT_EQ(failed.BatchCalls(), 1u);
  EXPECT_EQ(success.BatchCalls(), 1u);
}

TEST_F(OperationProcessorTest, BatchedRetryResetsOperationStatus) {
  SeedDir();
  storage_->SetCommitStatuses({RetryStatus(), Status::OK()});
  Start();

  Trace trace;
  TestOperation operation(trace, Operation::OpType::kUpdateAttr, kFsId,
                          kDirIno);
  operation.SetBatchStatuses(
      {Status(pb::error::ENOT_FOUND, "stale prefetch"), Status::OK()});
  bthread::CountdownEvent done(1);
  operation.SetEvent(&done);

  ASSERT_TRUE(processor_->RunBatched(&operation));
  ASSERT_TRUE(WaitFor(done));
  EXPECT_TRUE(operation.GetStatus().ok());
  EXPECT_EQ(operation.BatchCalls(), 2u);
}

TEST_F(OperationProcessorTest,
       BatchedTransactionFailureNotifiesEveryOperation) {
  SeedDir();
  storage_->SetBatchGetStatuses({RetryStatus(), RetryStatus()});
  Gate gate;
  SetBatchGate(gate);

  Trace first_trace;
  Trace second_trace;
  TestOperation first(first_trace, Operation::OpType::kUpdateAttr, kFsId,
                      kDirIno);
  TestOperation second(second_trace, Operation::OpType::kUpdateAttr, kFsId,
                       kDirIno);
  bthread::CountdownEvent done(2);
  first.SetEvent(&done);
  second.SetEvent(&done);

  ASSERT_TRUE(processor_->RunBatched(&first));
  ASSERT_TRUE(gate.WaitUntilEntered());
  ASSERT_TRUE(processor_->RunBatched(&second));
  gate.Open();
  ASSERT_TRUE(WaitFor(done));

  EXPECT_EQ(first.GetStatus().error_code(), pb::error::ESTORE_MAYBE_RETRY);
  EXPECT_EQ(second.GetStatus().error_code(), pb::error::ESTORE_MAYBE_RETRY);
}

TEST_F(OperationProcessorTest,
       RmDirAllowsConcurrentDirMutationToUseMutationKey) {
  SeedDir();
  Start();

  Gate rmdir_gate;
  Trace rmdir_trace;
  TestOperation rmdir(rmdir_trace, Operation::OpType::kRmDir, kFsId, kDirIno);
  rmdir.SetRunHandler([&](TxnUPtr&) {
    rmdir_gate.Wait();
    return Status::OK();
  });
  Status rmdir_status;
  std::thread rmdir_thread(
      [&] { rmdir_status = processor_->RunAlone(&rmdir); });
  ASSERT_TRUE(rmdir_gate.WaitUntilEntered());

  Trace batch_trace;
  TestOperation batch(batch_trace, Operation::OpType::kMkNod, kFsId, kDirIno);
  bthread::CountdownEvent done(1);
  batch.SetEvent(&done);
  const bool submitted = processor_->RunBatched(&batch);
  EXPECT_TRUE(submitted);
  const bool completed = submitted && WaitFor(done);
  EXPECT_TRUE(completed);

  if (completed) {
    std::string value;
    ASSERT_TRUE(
        storage_->Get(MetaCodec::EncodeInodeKey(kFsId, kDirIno), value).ok());
    EXPECT_EQ(MetaCodec::DecodeInodeValue(value).version(), 1u);
    EXPECT_TRUE(
        storage_
            ->Get(MetaCodec::EncodeDirInodeMutationKey(kFsId, kDirIno, 1),
                  value)
            .ok());
    EXPECT_TRUE(batch.GetStatus().ok());
  }

  rmdir_gate.Open();
  rmdir_thread.join();
  EXPECT_TRUE(rmdir_status.ok());
}

TEST_F(OperationProcessorTest, TableHelpersForwardArgumentsAndStatuses) {
  Start();
  const Range range{"a", "z"};

  EXPECT_TRUE(processor_->CheckTable(range).ok());
  EXPECT_EQ(storage_->CheckedStart(), range.start);
  EXPECT_EQ(storage_->CheckedEnd(), range.end);

  storage_->SetCheckTableStatus(Status(pb::error::ENOT_FOUND, "missing"));
  Status status = processor_->CheckTable(range);
  EXPECT_EQ(status.error_code(), pb::error::ENOT_FOUND);
  EXPECT_EQ(storage_->CheckedStart(), range.start);
  EXPECT_EQ(storage_->CheckedEnd(), range.end);

  int64_t table_id = 0;
  ASSERT_TRUE(processor_->CreateTable("table", range, table_id).ok());
  EXPECT_GT(table_id, 0);
  EXPECT_EQ(storage_->CreatedName(), "table");
  EXPECT_EQ(storage_->CreatedOption().start_key, range.start);
  EXPECT_EQ(storage_->CreatedOption().end_key, range.end);

  storage_->SetCreateTableStatus(
      Status(pb::error::EBACKEND_STORE, "backend failed"));
  status = processor_->CreateTable("failed", range, table_id);
  EXPECT_EQ(status.error_code(), pb::error::EINTERNAL);
}

}  // namespace
}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
