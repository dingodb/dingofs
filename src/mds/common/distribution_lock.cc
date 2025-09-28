// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mds/common/distribution_lock.h"

#include <cstddef>
#include <cstdint>
#include <string>

#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/status.h"
#include "mds/coordinator/coordinator_client.h"

namespace dingofs {
namespace mds {

DEFINE_uint64(mds_distribution_lock_lease_ttl_ms, 18000, "distribution lock lease ttl.");
DEFINE_uint32(mds_distribution_lock_scan_size, 1024, "distribution lock scan size.");

DECLARE_uint32(mds_txn_max_retry_times);

CoorDistributionLock::CoorDistributionLock(CoordinatorClientSPtr coordinator_client, const std::string& lock_prefix,
                                           int64_t mds_id)
    : coordinator_client_(coordinator_client), lock_prefix_(lock_prefix), mds_id_(mds_id) {}

CoorDistributionLockSPtr CoorDistributionLock::GetSelfPtr() {
  return std::dynamic_pointer_cast<CoorDistributionLock>(shared_from_this());
}

bool CoorDistributionLock::Init() {
  DINGO_LOG(INFO) << fmt::format("[dlock.{}] init dlock.", LockKey());

  // create lease
  auto status = CreateLease(lease_id_);
  if (!status.ok()) {
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("[dlock.{}] lease id({}).", LockKey(), lease_id_);

  // launch renew lease at background
  if (!LaunchRenewLease()) {
    return false;
  }

  // delete already exist key
  status = DeleteLockKey(LockKey());
  if (!status.ok()) {
    return false;
  }

  // put key with lease
  status = PutLockKey(LockKey(), lease_id_);
  if (!status.ok()) {
    return false;
  }

  if (!LaunchCheckLock()) {
    return false;
  }

  return true;
}

void CoorDistributionLock::Destroy() {
  bool expect = false;
  if (!is_stop_.compare_exchange_strong(expect, true)) {
    return;
  }

  DINGO_LOG(INFO) << fmt::format("[dlock.{}] destroy dlock.", LockKey());

  StopRenewLease();
  DeleteLease(lease_id_);

  StopCheckLock();
}

bool CoorDistributionLock::IsLocked() { return is_locked_.load(); }

std::string CoorDistributionLock::LockKey() { return fmt::format("{}/{}", lock_prefix_, mds_id_); }

Status CoorDistributionLock::CreateLease(int64_t& lease_id) {
  int64_t ttl = 0;
  auto status = coordinator_client_->LeaseGrant(0, FLAGS_mds_distribution_lock_lease_ttl_ms, lease_id, ttl);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] lease grant fail, error: {}", LockKey(), status.error_str());
    return Status(pb::error::ECOORDINATOR, status.error_str());
  }

  return Status::OK();
}

Status CoorDistributionLock::RenewLease(int64_t lease_id) {
  DINGO_LOG(INFO) << fmt::format("[dlock.{}] renew lease id({}).", LockKey(), lease_id);

  int64_t ttl = 0;
  auto status = coordinator_client_->LeaseRenew(lease_id, ttl);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] lease renew fail, error: {}", LockKey(), status.error_str());
    return Status(pb::error::ECOORDINATOR, status.error_str());
  }

  return Status::OK();
}

Status CoorDistributionLock::DeleteLease(int64_t lease_id) {
  auto status = coordinator_client_->LeaseRevoke(lease_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] lease delete fail, error: {}", LockKey(), status.error_str());
    return Status(pb::error::ECOORDINATOR, status.error_str());
  }

  return Status::OK();
}

Status CoorDistributionLock::DeleteLockKey(const std::string& key) {
  CoordinatorClient::Options options;
  CoordinatorClient::Range range;
  range.start = key;

  int64_t deleted_count;
  std::vector<CoordinatorClient::KVWithExt> prev_kvs;
  auto status = coordinator_client_->KvDeleteRange(options, range, deleted_count, prev_kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] delete fail, {}", LockKey(), status.error_str());
    return Status(pb::error::ECOORDINATOR, status.error_str());
  }

  return Status::OK();
}

Status CoorDistributionLock::PutLockKey(const std::string& key, int64_t lease_id) {
  CHECK(lease_id > 0) << "lease_id is invalid.";

  CoordinatorClient::Options options;
  options.lease_id = lease_id;
  options.ignore_lease = false;
  options.ignore_value = false;
  options.need_prev_kv = false;
  CoordinatorClient::KVPair kv;
  kv.key = key;
  kv.value = "lock";

  CoordinatorClient::KVWithExt prev_kv;
  auto status = coordinator_client_->KvPut(options, kv, prev_kv);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] put fail, lease_id({}) {}", LockKey(), lease_id, status.error_str());
    return Status(pb::error::ECOORDINATOR, status.error_str());
  }

  return Status::OK();
}

Status CoorDistributionLock::CheckLock(std::string& watch_key, int64_t& watch_revision) {
  DINGO_LOG(INFO) << fmt::format("[dlock.{}] check lock...", LockKey());

  CoordinatorClient::Options options;
  options.keys_only = true;
  options.count_only = false;

  CoordinatorClient::Range range;
  range.start = lock_prefix_;
  range.end = Helper::PrefixNext(lock_prefix_);

  std::vector<CoordinatorClient::KVWithExt> kvs;
  bool more;
  int64_t count;
  auto status = coordinator_client_->KvRange(options, range, FLAGS_mds_distribution_lock_scan_size, kvs, more, count);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] scan range fail, error: {}", LockKey(), status.error_str());
    return Status(pb::error::ECOORDINATOR, status.error_str());
  }

  if (kvs.empty()) {
    return Status(pb::error::ENOT_FOUND, "lock key not found.");
  }

  // sort by mod_revision
  std::sort(kvs.begin(), kvs.end(),  // NOLINT
            [](const CoordinatorClient::KVWithExt& a, const CoordinatorClient::KVWithExt& b) {
              return a.mod_revision < b.mod_revision;
            });

  // debug log
  for (auto& kv : kvs) {
    DINGO_LOG(INFO) << fmt::format("[dlock.{}] key({}) revision({}).", LockKey(), Helper::StringToHex(kv.kv.key),
                                   kv.mod_revision);
  }

  std::string lock_key = LockKey();
  size_t index = 0;
  for (uint32_t i = 0; i < kvs.size(); ++i) {
    auto& kv = kvs[i];
    if (kv.kv.key == lock_key) {
      index = i;
      break;
    }
  }

  // own lock
  if (index == 0) {
    return Status::OK();
  }

  // not own lock
  size_t watch_index = index - 1;
  watch_key = kvs[watch_index].kv.key;
  watch_revision = kvs[watch_index].mod_revision;
  DINGO_LOG(INFO) << fmt::format("[dlock.{}] watch key({}) revision({}).", LockKey(), Helper::StringToHex(watch_key),
                                 watch_revision);
  CHECK(!watch_key.empty()) << "watch key is empty.";

  return Status::OK();
}

Status CoorDistributionLock::Watch(const std::string& watch_key, int64_t watch_revision) {
  CHECK(!watch_key.empty()) << "watch key is empty.";

  DINGO_LOG(INFO) << fmt::format("[dlock.{}] watch key({}) revision({}).", LockKey(), Helper::StringToHex(watch_key),
                                 watch_revision);

  CoordinatorClient::WatchOut out;
  auto status = coordinator_client_->Watch(watch_key, watch_revision, out);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] watch fail, error: {}", LockKey(), status.error_str());
    return Status(pb::error::ECOORDINATOR, status.error_str());
  }

  // received event
  DINGO_LOG(INFO) << fmt::format("[dlock.{}] received watch event.", LockKey());
  for (auto& envent : out.events) {
    DINGO_LOG(INFO) << fmt::format("[dlock.{}] watch event, type({}) key({}) lease({}) revision({}).", LockKey(),
                                   CoordinatorClient::EventTypeName(envent.type), Helper::StringToHex(envent.kv.kv.key),
                                   envent.kv.lease, envent.kv.mod_revision);
  }

  return Status::OK();
}

bool CoorDistributionLock::LaunchRenewLease() {
  struct Params {
    CoorDistributionLockSPtr self;
  };

  Params* param = new Params();
  param->self = GetSelfPtr();

  // bthread_attr_t attr;
  if (bthread_start_background(
          &lease_th_, nullptr,
          [](void* arg) -> void* {
            Params* param = static_cast<Params*>(arg);
            auto self = param->self;

            for (;;) {
              if (self->is_stop_.load()) {
                break;
              }

              self->RenewLease(self->lease_id_);

              bthread_usleep(FLAGS_mds_distribution_lock_lease_ttl_ms * 1000 / 2);
            }

            delete param;
            return nullptr;
          },
          (void*)param) != 0) {
    DINGO_LOG(FATAL) << "bthread_start_background fail.";
    delete param;
    return false;
  }

  return true;
}

void CoorDistributionLock::StopRenewLease() {
  if (lease_th_ == 0) {
    return;
  }

  if (bthread_stop(lease_th_) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] bthread_stop fail.", LockKey());
  }

  if (bthread_join(lease_th_, nullptr) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] bthread_join fail.", LockKey());
  }
}

bool CoorDistributionLock::LaunchCheckLock() {
  struct Params {
    CoorDistributionLockSPtr self;
  };

  Params* param = new Params();
  param->self = GetSelfPtr();

  if (bthread_start_background(
          &check_lock_th_, nullptr,
          [](void* arg) -> void* {
            Params* param = static_cast<Params*>(arg);

            param->self->CheckLock();

            delete param;
            return nullptr;
          },
          (void*)param) != 0) {
    DINGO_LOG(FATAL) << "bthread_start_background fail.";
    delete param;
    return false;
  }

  return true;
}

void CoorDistributionLock::CheckLock() {
  std::string lock_key = LockKey();

  for (;;) {
    if (is_stop_.load()) {
      break;
    }

    std::string watch_key;
    int64_t watch_revision;
    auto status = CheckLock(watch_key, watch_revision);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[dlock.{}] check lock fail, {}", lock_key, status.error_str());
      bthread_usleep(4 * 1000 * 1000);
      continue;
    }

    // own lock
    if (watch_key.empty()) {
      DINGO_LOG(INFO) << fmt::format("[dlock.{}] mds({}) own lock.", lock_key, mds_id_);

      is_locked_.store(true);
      bthread_usleep(FLAGS_mds_distribution_lock_lease_ttl_ms * 1000 / 3);

    } else {
      // not own lock, watch key
      Watch(watch_key, watch_revision);
    }
  }
}

void CoorDistributionLock::StopCheckLock() {
  if (check_lock_th_ == 0) {
    return;
  }

  if (bthread_stop(check_lock_th_) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] bthread_stop fail.", LockKey());
  }

  if (bthread_join(check_lock_th_, nullptr) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] bthread_join fail.", LockKey());
  }
}

StoreDistributionLock::StoreDistributionLock(KVStorageSPtr kv_storage, const std::string& name, int64_t mds_id)
    : kv_storage_(kv_storage), name_(name), mds_id_(mds_id) {}

StoreDistributionLockPtr StoreDistributionLock::GetSelfPtr() {
  return std::dynamic_pointer_cast<StoreDistributionLock>(shared_from_this());
}

bool StoreDistributionLock::Init() {
  // launch renew lease at background
  return LaunchRenewLease();
}

void StoreDistributionLock::Destroy() {
  // just run once
  bool expect = false;
  if (!is_stop_.compare_exchange_strong(expect, true)) {
    return;
  }

  DINGO_LOG(INFO) << fmt::format("[dlock.{}] destroy dlock.", LockKey());

  StopRenewLease();
}

std::string StoreDistributionLock::LockKey() { return name_; }

bool StoreDistributionLock::IsLocked() {
  if (!is_locked_.load()) {
    return false;
  }

  // silence period, avoid multi-owner
  uint64_t now_ms = Helper::TimestampMs();
  if (now_ms < last_lock_time_ms_.load() + FLAGS_mds_distribution_lock_lease_ttl_ms) {
    return false;
  }

  return is_locked_.load();
}

Status StoreDistributionLock::RenewLease() {
  DINGO_LOG(DEBUG) << fmt::format("[dlock.{}] renew lease.", LockKey());

  std::string state;
  int64_t owner_mds_id = 0;
  Status status;
  uint32_t retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    std::string value;
    const std::string key = MetaCodec::EncodeLockKey(name_);
    status = txn->Get(key, value);
    if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
      break;
    }

    bool need_update_last_lock_time = false;
    uint64_t now_ms = Helper::TimestampMs();
    uint64_t epoch = 0;
    uint64_t expire_time_ms = 0;
    if (status.ok()) {
      // already exist lock owner
      MetaCodec::DecodeLockValue(value, owner_mds_id, epoch, expire_time_ms);

      // self is lock owner
      if (owner_mds_id == mds_id_) {
        is_locked_.store(true);
        expire_time_ms = now_ms + FLAGS_mds_distribution_lock_lease_ttl_ms;
        state = "OwnLock";

      } else {
        is_locked_.store(false);
        if (now_ms <= expire_time_ms) {
          // self is not lock owner, and lock is not expired
          state = "NotOwnLock";
          break;

        } else {
          // self is not lock owner, but lock is expired
          expire_time_ms = now_ms + FLAGS_mds_distribution_lock_lease_ttl_ms;
          need_update_last_lock_time = true;
          state = "ExpiredLock";
        }
      }

    } else if (status.error_code() != pb::error::ENOT_FOUND) {
      // not exist lock owner
      expire_time_ms = now_ms + FLAGS_mds_distribution_lock_lease_ttl_ms;
      need_update_last_lock_time = true;
      state = "NotExistLockOwner";
    }

    epoch = (owner_mds_id != mds_id_) ? epoch + 1 : epoch;
    txn->Put(key, MetaCodec::EncodeLockValue(mds_id_, epoch, expire_time_ms));
    status = txn->Commit();
    if (status.ok()) {
      is_locked_.store(true);
      if (need_update_last_lock_time) last_lock_time_ms_.store(now_ms);
      state += ":Wined";
      break;

    } else if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_mds_txn_max_retry_times);

  DINGO_LOG(INFO) << fmt::format("[dlock.{}] renew lease finish, owner({}) state({}) retry({}) {}.", LockKey(),
                                 owner_mds_id, state, retry, status.error_str());

  return status;
}

bool StoreDistributionLock::LaunchRenewLease() {
  struct Params {
    StoreDistributionLockPtr self;
  };

  Params* param = new Params();
  param->self = GetSelfPtr();

  // bthread_attr_t attr;
  if (bthread_start_background(
          &lease_th_, nullptr,
          [](void* arg) -> void* {
            Params* param = static_cast<Params*>(arg);
            auto self = param->self;

            for (;;) {
              if (self->is_stop_.load()) {
                break;
              }

              self->RenewLease();

              bthread_usleep(FLAGS_mds_distribution_lock_lease_ttl_ms * 1000 / 3);
            }

            delete param;
            return nullptr;
          },
          (void*)param) != 0) {
    DINGO_LOG(FATAL) << "bthread_start_background fail.";
    delete param;
    return false;
  }

  return true;
}

void StoreDistributionLock::StopRenewLease() {
  if (lease_th_ == 0) {
    return;
  }

  if (bthread_stop(lease_th_) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] bthread_stop fail.", LockKey());
  }

  if (bthread_join(lease_th_, nullptr) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] bthread_join fail.", LockKey());
  }
}

Status StoreDistributionLock::GetAllLockInfo(OperationProcessorSPtr operation_processor,
                                             std::vector<LockEntry>& lock_entries) {
  Trace trace;
  ScanLockOperation operation(trace);

  auto status = operation_processor->RunAlone(&operation);
  if (!status.ok()) return status;

  auto& result = operation.GetResult();

  for (auto& kv : result.kvs) {
    LockEntry lock_entry;
    MetaCodec::DecodeLockKey(kv.key, lock_entry.name);
    MetaCodec::DecodeLockValue(kv.value, lock_entry.owner, lock_entry.epoch, lock_entry.expire_time_ms);

    lock_entries.push_back(lock_entry);
  }

  return Status::OK();
}

}  // namespace mds
}  // namespace dingofs
