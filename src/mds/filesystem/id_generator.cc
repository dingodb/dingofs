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

#include "mds/filesystem/id_generator.h"

#include <algorithm>
#include <cstdint>
#include <string>

#include "brpc/reloadable_flags.h"
#include "bthread/bthread.h"
#include "bthread/mutex.h"
#include "butil/compiler_specific.h"
#include "common/logging.h"
#include "dingofs/error.pb.h"
#include "fmt/format.h"
#include "gflags/gflags_declare.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"
#include "mds/common/status.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_txn_max_retry_times);

const std::string kFsAutoIncrementIdName = "dingofs-fs-id";
static const int64_t kFsTableId = 1000;
static const int64_t kFsIdBatchSize = 2;
static const int64_t kFsIdStartId = 1e4;  // 10 thousand

// all file systems share the same slice id generator
const std::string kSliceAutoIncrementIdName = "dingofs-slice-id";
static const int64_t kSliceTableId = 1002;
static const int64_t kSliceIdStartId = 1e10;  // 10 billion

// each file system has its own inode id generator.
// Sub-trash directories do NOT use a separate generator: their ino is derived
// from this allocator + a kTrashInodeId offset inside BuildTrashMove.
const std::string kInoAutoIncrementIdName = "dingofs-inode-id";
static const int64_t kInoStartId = 2e10;  // 20 billion

DEFINE_uint32(mds_slice_id_generator_batch_size, 100000, "slice id generator batch size.");
DEFINE_validator(mds_slice_id_generator_batch_size, brpc::PassValidate);

DEFINE_uint32(mds_ino_generator_batch_size, 100000, "ino generator batch size.");
DEFINE_validator(mds_ino_generator_batch_size, brpc::PassValidate);

// all mds share the same inode generator
DEFINE_bool(mds_ino_generator_share_enable, false, "Inode generator share enable.");
DEFINE_bool(mds_slice_id_generator_share_enable, true, "Slice ID generator share enable.");

static uint32_t CalWaitTimeUs(int retry) {
  // exponential backoff
  return Helper::GenerateRealRandomInteger(1000, 5000) * (1 << retry);
}

static bool IsRetry(uint32_t& retry) {
  if (++retry <= FLAGS_mds_txn_max_retry_times) {
    bthread_usleep(CalWaitTimeUs(retry));
    return true;
  }
  return false;
}

CoorAutoIncrementIdGenerator::CoorAutoIncrementIdGenerator(CoordinatorClientSPtr client, const std::string& name,
                                                           int64_t table_id, uint64_t start_id, uint32_t batch_size)
    : client_(client), name_(name), table_id_(table_id), start_id_(start_id), batch_size_(batch_size) {
  next_id_ = start_id;
  bundle_ = start_id;
  bundle_end_ = start_id;

  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

CoorAutoIncrementIdGenerator::~CoorAutoIncrementIdGenerator() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail.";
}

bool CoorAutoIncrementIdGenerator::Init() {
  auto status = IsExistAutoIncrement();
  if (status.ok()) {
    LOG(INFO) << fmt::format("[idalloc.{}] autoincrement table exist.", name_);
    return true;
  }

  if (status.error_code() != pb::error::ENOT_FOUND) {
    LOG(ERROR) << fmt::format("[idalloc.{}] check autoincrement table fail, status({}).", name_, status.error_str());
    return false;
  }

  LOG(INFO) << fmt::format("[idalloc.{}] autoincrement table not exist.", name_);

  status = CreateAutoIncrement();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[idalloc.{}] create autoincrement table fail, error({}).", name_, status.error_str());
    return false;
  }

  return true;
}

bool CoorAutoIncrementIdGenerator::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);

  auto status = DeleteAutoIncrement();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[idalloc.{}] destroy autoincrement table fail, status({}).", name_, status.error_str());
    return false;
  }

  is_destroyed_ = true;

  return true;
}

bool CoorAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t& id) { return GenID(num, 0, id); }

bool CoorAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (is_destroyed_) {
    LOG(ERROR) << fmt::format("[idalloc.{}] id generator is destroyed.", name_);
    return false;
  }

  next_id_ = std::max(next_id_, min_slice_id);

  if (next_id_ + num > bundle_end_) {
    auto status = AllocateIds(std::max(num, batch_size_));
    if (!status.ok()) {
      return false;
    }
  }

  id = next_id_;
  next_id_ += num;

  LOG(INFO) << fmt::format("[idalloc.{}] alloc id {},{} bundle[{}, {}).", name_, id, num, bundle_, bundle_end_);

  return true;
}

std::string CoorAutoIncrementIdGenerator::Describe() {
  return fmt::format("[coordinator] name({}) start_id({}) batch_size({}) range[{}, {}) next_id({})", name_, start_id_,
                     batch_size_, bundle_, bundle_end_, next_id_);
}

Status CoorAutoIncrementIdGenerator::IsExistAutoIncrement() {
  int64_t start_id = -1;
  return client_->GetAutoIncrement(table_id_, start_id);
}

Status CoorAutoIncrementIdGenerator::CreateAutoIncrement() {
  LOG(INFO) << fmt::format("[idalloc.{}] create autoincrement table, start_id({}).", name_, start_id_);
  return client_->CreateAutoIncrement(table_id_, start_id_);
}

Status CoorAutoIncrementIdGenerator::DeleteAutoIncrement() {
  LOG(INFO) << fmt::format("[idalloc.{}] delete autoincrement table, start_id({}).", name_, start_id_);
  return client_->DeleteAutoIncrement(table_id_);
}

Status CoorAutoIncrementIdGenerator::AllocateIds(uint32_t num) {
  Status status;
  utils::Duration duration;
  int64_t bundle = 0;
  int64_t bundle_end = 0;
  do {
    status = client_->GenerateAutoIncrement(table_id_, num, bundle, bundle_end);
    if (!status.ok()) {
      break;
    }

    CHECK(bundle >= 0 && bundle_end >= 0) << "bundle id is negative.";
  } while (static_cast<uint64_t>(bundle) < next_id_);

  if (status.ok()) {
    bundle_ = static_cast<uint64_t>(bundle);
    next_id_ = static_cast<uint64_t>(bundle);
    bundle_end_ = static_cast<uint64_t>(bundle_end);
  }

  LOG(INFO) << fmt::format("[idalloc.{}][{}us] take bundle id, bundle[{},{}) num({}) status({}).", name_,
                           duration.ElapsedUs(), bundle_, bundle_end_, num, status.error_str());

  return status;
}

StoreAutoIncrementIdGenerator::StoreAutoIncrementIdGenerator(KVStorageSPtr kv_storage, const std::string& name,
                                                             int64_t start_id, int batch_size)
    : kv_storage_(kv_storage),
      name_(name),
      key_(MetaCodec::EncodeAutoIncrementIDKey(name)),
      next_id_(start_id),
      last_alloc_id_(start_id),
      batch_size_(batch_size) {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

StoreAutoIncrementIdGenerator::~StoreAutoIncrementIdGenerator() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail.";
}

bool StoreAutoIncrementIdGenerator::Init() {
  uint64_t alloc_id = 0;
  auto status = GetOrPutAllocId(alloc_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[idalloc.{}] init get alloc id fail, status({}).", name_, status.error_cstr());
    return false;
  }

  next_id_.store(alloc_id, std::memory_order_relaxed);
  last_alloc_id_.store(alloc_id, std::memory_order_relaxed);

  return true;
}

bool StoreAutoIncrementIdGenerator::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);

  is_destroyed_.store(true, std::memory_order_release);

  auto status = DestroyId();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[idalloc.{}] destroy autoincrement table fail, status({}).", name_, status.error_str());
    return false;
  }

  return true;
}

bool StoreAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t& id) { return GenID(num, 0, id); }

bool StoreAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) {
  if (BAIDU_UNLIKELY(num == 0)) {
    LOG(ERROR) << fmt::format("[idalloc.{}] num cant not 0.", name_);
    return false;
  }

  do {
    // Lock-free bump allocator. The steady-state path is a single CAS on next_id_;
    // only bundle refill (rare) touches storage, and refill is non-blocking: just
    // one elected thread does the IO while others back off and retry the fast path.
    constexpr int kMaxAttempts = 10;  // livelock guard
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
      if (BAIDU_UNLIKELY(is_destroyed_.load(std::memory_order_acquire))) {
        LOG(ERROR) << fmt::format("[idalloc.{}] id generator is destroyed.", name_);
        return false;
      }

      uint64_t cur = next_id_.load(std::memory_order_acquire);
      uint64_t start = std::max(cur, min_slice_id);

      if (start + num <= last_alloc_id_.load(std::memory_order_acquire)) {
        // Fast path: claim [start, start + num) atomically.
        if (next_id_.compare_exchange_weak(cur, start + num, std::memory_order_acq_rel, std::memory_order_relaxed)) {
          id = start;
          LOG_DEBUG << fmt::format("[idalloc.{}] alloc id({}) num({}).", name_, id, num);
          return true;
        }
        // Contended: another thread advanced next_id_, retry.
        continue;
      }

      // Bundle exhausted: elect a single refiller via trylock so waiters never
      // block on the storage IO.
      if (bthread_mutex_trylock(&mutex_) == 0) {
        Status status;
        // Double-check under the lock: another thread may have just refilled.
        uint64_t c2 = next_id_.load(std::memory_order_acquire);
        uint64_t s2 = std::max(c2, min_slice_id);
        if (s2 + num > last_alloc_id_.load(std::memory_order_acquire)) {
          status = AllocateIds(std::max(num, batch_size_), min_slice_id);
        }
        bthread_mutex_unlock(&mutex_);

        if (!status.ok()) {
          LOG(ERROR) << fmt::format("[idalloc.{}] allocate id fail, {}.", name_, status.error_str());
          return false;
        }
      } else {
        // Another thread is refilling; back off and retry the fast path.
        bthread_yield();
        // bthread_usleep(10);
      }
    }

    LOG(WARNING) << fmt::format("[idalloc.{}] gen id give up after {} attempts.", name_, kMaxAttempts);

    // sleep 3ms
    bthread_usleep(500);

  } while (true);

  return false;
}

std::string StoreAutoIncrementIdGenerator::Describe() {
  return fmt::format("[store] name({}) batch_size({}) last_alloc_id({}) next_id({})", name_, batch_size_,
                     last_alloc_id_.load(std::memory_order_acquire), next_id_.load(std::memory_order_acquire));
}

Status StoreAutoIncrementIdGenerator::GetOrPutAllocId(uint64_t& alloc_id) {
  Status status;
  uint32_t retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    std::string value;
    status = txn->Get(key_, value);
    if (!status.ok()) {
      if (status.error_code() != pb::error::ENOT_FOUND) {
        break;
      }
      alloc_id = 0;

    } else {
      MetaCodec::DecodeAutoIncrementIDValue(value, alloc_id);
    }

    if (alloc_id < last_alloc_id_.load(std::memory_order_relaxed)) {
      alloc_id = last_alloc_id_.load(std::memory_order_relaxed);
      txn->Put(key_, MetaCodec::EncodeAutoIncrementIDValue(alloc_id));
    }

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_mds_txn_max_retry_times);

  return status;
}

Status StoreAutoIncrementIdGenerator::AllocateIds(uint32_t size, uint64_t floor) {
  utils::Duration duration;
  Status status;
  uint32_t retry = 0;
  // Cover the floor (min_slice_id) and any range already handed out.
  uint64_t start_alloc_id =
      std::max({next_id_.load(std::memory_order_acquire), last_alloc_id_.load(std::memory_order_acquire), floor});
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    uint64_t alloced_id = 0;
    std::string value;
    status = txn->Get(key_, value);
    if (!status.ok()) {
      if (status.error_code() != pb::error::ENOT_FOUND) {
        break;
      }

    } else {
      MetaCodec::DecodeAutoIncrementIDValue(value, alloced_id);
    }

    start_alloc_id = std::max(alloced_id, start_alloc_id);
    txn->Put(key_, MetaCodec::EncodeAutoIncrementIDValue(start_alloc_id + size));

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (IsRetry(retry));

  if (status.ok()) {
    // Publish next_id_ before last_alloc_id_ so a fast-path reader that observes
    // the grown last_alloc_id_ also observes a consistent bundle start (R6).
    next_id_.store(start_alloc_id, std::memory_order_release);
    last_alloc_id_.store(start_alloc_id + size, std::memory_order_release);
  }

  LOG(INFO) << fmt::format("[idalloc.{}][{}us] take bundle id, bundle[{},{}) size({}) status({}).", name_,
                           duration.ElapsedUs(), start_alloc_id, start_alloc_id + size, size, status.error_str());

  return status;
}

Status StoreAutoIncrementIdGenerator::DestroyId() {
  Status status;
  uint32_t retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    txn->Delete(key_);

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_mds_txn_max_retry_times);

  return status;
}

ShardStoreAutoIncrementIdGenerator::ShardStoreAutoIncrementIdGenerator(KVStorageSPtr kv_storage,
                                                                       const std::string& name, int64_t start_id,
                                                                       int batch_size)
    : kv_storage_(kv_storage),
      name_(name),
      key_(MetaCodec::EncodeAutoIncrementIDKey(name)),
      start_id_(start_id),
      batch_size_(batch_size) {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
  shard_bundles_.iterateWLock([&](Bunlde& bundle) {
    bundle.next_id_ = start_id;
    bundle.last_alloc_id_ = start_id;
  });
}

bool ShardStoreAutoIncrementIdGenerator::Init() {
  uint64_t alloc_id = 0;
  auto status = GetOrPutAllocId(alloc_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[idalloc.{}] init get alloc id fail, status({}).", name_, status.error_cstr());
    return false;
  }

  shard_bundles_.iterateWLock([&](Bunlde& bundle) {
    bundle.next_id_ = alloc_id;
    bundle.last_alloc_id_ = alloc_id;
  });

  return true;
}

bool ShardStoreAutoIncrementIdGenerator::Destroy() {
  auto status = DestroyId();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[idalloc.{}] destroy autoincrement table fail, status({}).", name_, status.error_str());
    return false;
  }

  return true;
}

bool ShardStoreAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t& id) { return GenID(num, 0, id); }

bool ShardStoreAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) {
  if (BAIDU_UNLIKELY(num == 0)) {
    LOG(ERROR) << fmt::format("[idalloc.{}] num cant not 0.", name_);
    return false;
  }

  size_t shard_pos = next_shard_pos_.fetch_add(1, std::memory_order_relaxed) % kShardNum;

  bool ret = true;
  shard_bundles_.withWLockAt(
      [&](Bunlde& bundle) mutable {
        bundle.next_id_ = std::max(bundle.next_id_, min_slice_id);

        if (bundle.next_id_ + num > bundle.last_alloc_id_) {
          auto status = AllocateIds(bundle, std::max(num, batch_size_));
          if (!status.ok()) {
            LOG(ERROR) << fmt::format("[idalloc.{}] allocate id fail, {}.", name_, status.error_str());
            ret = false;
          }
        }

        // allocate id
        if (ret) {
          id = bundle.next_id_;
          bundle.next_id_ += num;
        }
      },
      shard_pos);

  LOG(INFO) << fmt::format("[idalloc.{}] alloc id({}) num({}).", name_, id, num);

  return ret;
}

std::string ShardStoreAutoIncrementIdGenerator::Describe() {
  std::string desc;
  shard_bundles_.iterate(
      [&](const Bunlde& bundle) { desc += fmt::format("[{},{}),", bundle.next_id_, bundle.last_alloc_id_); });

  return fmt::format("[store] name({}) batch_size({}) bundles({})", name_, batch_size_, desc);
}

Status ShardStoreAutoIncrementIdGenerator::GetOrPutAllocId(uint64_t& alloc_id) {
  Status status;
  uint32_t retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    std::string value;
    status = txn->Get(key_, value);
    if (!status.ok()) {
      if (status.error_code() != pb::error::ENOT_FOUND) {
        break;
      }
      alloc_id = 0;

    } else {
      MetaCodec::DecodeAutoIncrementIDValue(value, alloc_id);
    }

    if (alloc_id < start_id_) {
      alloc_id = start_id_;
      txn->Put(key_, MetaCodec::EncodeAutoIncrementIDValue(alloc_id));
    }

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_mds_txn_max_retry_times);

  return status;
}

Status ShardStoreAutoIncrementIdGenerator::AllocateIds(Bunlde& bundle, uint32_t size) {
  BAIDU_SCOPED_LOCK(mutex_);

  utils::Duration duration;
  Status status;
  uint32_t retry = 0;
  uint64_t start_alloc_id = std::max(bundle.next_id_, bundle.last_alloc_id_);
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    uint64_t alloced_id = 0;
    std::string value;
    status = txn->Get(key_, value);
    if (!status.ok()) {
      if (status.error_code() != pb::error::ENOT_FOUND) {
        break;
      }

    } else {
      MetaCodec::DecodeAutoIncrementIDValue(value, alloced_id);
    }

    start_alloc_id = std::max(alloced_id, start_alloc_id);
    txn->Put(key_, MetaCodec::EncodeAutoIncrementIDValue(start_alloc_id + size));

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_mds_txn_max_retry_times);

  if (status.ok()) {
    bundle.last_alloc_id_ = start_alloc_id + size;
    bundle.next_id_ = start_alloc_id;
  }

  LOG(INFO) << fmt::format("[idalloc.{}][{}us] take bundle id, bundle[{},{}) size({}) status({}).", name_,
                           duration.ElapsedUs(), bundle.next_id_, bundle.last_alloc_id_, size, status.error_str());

  return status;
}

Status ShardStoreAutoIncrementIdGenerator::DestroyId() {
  Status status;
  uint32_t retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    txn->Delete(key_);

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_mds_txn_max_retry_times);

  return status;
}

IdGeneratorUPtr NewFsIdGenerator(CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";

  return CoorAutoIncrementIdGenerator::New(coordinator_client, kFsAutoIncrementIdName, kFsTableId, kFsIdStartId,
                                           kFsIdBatchSize);
}

IdGeneratorUPtr NewFsIdGenerator(KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  return StoreAutoIncrementIdGenerator::New(kv_storage, kFsAutoIncrementIdName, kFsIdStartId, kFsIdBatchSize);
}

IdGeneratorUPtr NewInodeIdGenerator(uint32_t fs_id, CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";

  std::string name = fmt::format("{}-{}", kInoAutoIncrementIdName, fs_id);

  return CoorAutoIncrementIdGenerator::New(coordinator_client, name, fs_id, kInoStartId,
                                           FLAGS_mds_ino_generator_batch_size);
}

IdGeneratorUPtr NewInodeIdGenerator(uint32_t fs_id, uint64_t mds_id, CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";

  std::string name = fmt::format("{}-{}-{}", kInoAutoIncrementIdName, fs_id, mds_id);

  return CoorAutoIncrementIdGenerator::New(coordinator_client, name, fs_id, kInoStartId,
                                           FLAGS_mds_ino_generator_batch_size);
}

IdGeneratorUPtr NewInodeIdGenerator(uint32_t fs_id, KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  std::string name = fmt::format("{}-{}", kInoAutoIncrementIdName, fs_id);

  return StoreAutoIncrementIdGenerator::New(kv_storage, name, kInoStartId, FLAGS_mds_ino_generator_batch_size);
}

IdGeneratorUPtr NewInodeIdGenerator(uint32_t fs_id, uint64_t mds_id, KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  std::string name = fmt::format("{}-{}-{}", kInoAutoIncrementIdName, fs_id, mds_id);

  return StoreAutoIncrementIdGenerator::New(kv_storage, name, kInoStartId, FLAGS_mds_ino_generator_batch_size);
}

IdGeneratorSPtr NewSliceIdGenerator(CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";
  return CoorAutoIncrementIdGenerator::NewShare(coordinator_client, kSliceAutoIncrementIdName, kSliceTableId,
                                                kSliceIdStartId, FLAGS_mds_slice_id_generator_batch_size);
}

IdGeneratorSPtr NewSliceIdGenerator(uint64_t mds_id, CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";

  std::string name = fmt::format("{}-{}", kSliceAutoIncrementIdName, mds_id);

  return CoorAutoIncrementIdGenerator::NewShare(coordinator_client, name, kSliceTableId, kSliceIdStartId,
                                                FLAGS_mds_slice_id_generator_batch_size);
}

IdGeneratorSPtr NewSliceIdGenerator(KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  return StoreAutoIncrementIdGenerator::NewShare(kv_storage, kSliceAutoIncrementIdName, kSliceIdStartId,
                                                 FLAGS_mds_slice_id_generator_batch_size);
}

IdGeneratorSPtr NewSliceIdGenerator(uint64_t mds_id, KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  std::string name = fmt::format("{}-{}", kSliceAutoIncrementIdName, mds_id);

  return StoreAutoIncrementIdGenerator::NewShare(kv_storage, name, kSliceIdStartId,
                                                 FLAGS_mds_slice_id_generator_batch_size);
}

void DestroyInodeIdGenerator(uint32_t fs_id, CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";
  std::string name = fmt::format("{}-{}", kInoAutoIncrementIdName, fs_id);

  auto id_generator = CoorAutoIncrementIdGenerator::New(coordinator_client, name, fs_id, kInoStartId,
                                                        FLAGS_mds_ino_generator_batch_size);
  id_generator->Destroy();
}

void DestroyInodeIdGenerator(uint32_t fs_id, KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  std::string name = fmt::format("{}-{}", kInoAutoIncrementIdName, fs_id);

  auto id_generator =
      StoreAutoIncrementIdGenerator::New(kv_storage, name, kInoStartId, FLAGS_mds_ino_generator_batch_size);
  id_generator->Destroy();
}

}  // namespace mds
}  // namespace dingofs