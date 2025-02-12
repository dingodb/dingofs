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

#include "mdsv2/common/distribution_lock.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/coordinator/coordinator_client.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_uint32(distribution_lock_lease_ttl_ms, 10000, "distribution lock lease ttl.");
DEFINE_uint32(distribution_lock_scan_size, 1024, "distribution lock scan size.");

CoorDistributionLock::CoorDistributionLock(CoordinatorClientPtr coordinator_client, const std::string& lock_prefix,
                                           int64_t mds_id)
    : coordinator_client_(coordinator_client), lock_prefix_(lock_prefix), mds_id_(mds_id) {}

CoorDistributionLockPtr CoorDistributionLock::GetSelfPtr() {
  return std::dynamic_pointer_cast<CoorDistributionLock>(shared_from_this());
}

bool CoorDistributionLock::Init() {
  // create lease
  auto status = CreateLease(lease_id_);
  if (!status.ok()) {
    return false;
  }

  // launch renew lease at background
  if (!LaunchRenewLease()) {
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
  StopRenewLease();
  DeleteLease(lease_id_);
}

bool CoorDistributionLock::IsLocked() { return is_locked_.load(); }

std::string CoorDistributionLock::LockKey() { return fmt::format("{}/{}", lock_prefix_, mds_id_); }

Status CoorDistributionLock::CreateLease(int64_t& lease_id) {
  int64_t ttl = 0;
  auto status = coordinator_client_->LeaseGrant(0, FLAGS_distribution_lock_lease_ttl_ms, lease_id, ttl);
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

Status CoorDistributionLock::PutLockKey(const std::string& key, int64_t lease_id) {
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
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] put fail, error: {}", LockKey(), status.error_str());
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
  range.start_key = lock_prefix_;
  range.end_key = Helper::PrefixNext(lock_prefix_);

  std::vector<CoordinatorClient::KVWithExt> kvs;
  bool more;
  int64_t count;
  auto status = coordinator_client_->KvRange(options, range, FLAGS_distribution_lock_scan_size, kvs, more, count);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[dlock.{}] scan range fail, error: {}", LockKey(), status.error_str());
    return Status(pb::error::ECOORDINATOR, status.error_str());
  }
  DINGO_LOG(INFO) << fmt::format("[dlock.{}] scan range, count({}).", LockKey(), kvs.size());

  if (kvs.empty()) {
    return Status(pb::error::ENOT_FOUND, "lock key not found.");
  }

  // sort by mod_revision
  std::sort(kvs.begin(), kvs.end(), [](const CoordinatorClient::KVWithExt& a, const CoordinatorClient::KVWithExt& b) {
    return a.mod_revision < b.mod_revision;
  });

  std::string lock_key = LockKey();
  size_t index = 0;
  for (int i = 0; i < kvs.size(); ++i) {
    auto& kv = kvs[i];
    DINGO_LOG(INFO) << fmt::format("[dlock.{}] scan, key({}/{}) revision({}).", lock_key, Helper::StringToHex(lock_key),
                                   Helper::StringToHex(kv.kv.key), kv.mod_revision);
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
    CoorDistributionLockPtr self;
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
              self->RenewLease(self->lease_id_);

              bthread_usleep(FLAGS_distribution_lock_lease_ttl_ms * 1000 / 2);
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
    CoorDistributionLockPtr self;
  };

  Params* param = new Params();
  param->self = GetSelfPtr();

  if (bthread_start_background(
          &check_lock_th_, nullptr,
          [](void* arg) -> void* {
            Params* param = static_cast<Params*>(arg);
            auto self = param->self;

            std::string lock_key = self->LockKey();

            for (;;) {
              std::string watch_key;
              int64_t watch_revision;
              auto status = self->CheckLock(watch_key, watch_revision);
              if (!status.ok()) {
                LOG(ERROR) << fmt::format("[dlock.{}] check lock fail, {}", lock_key, status.error_str());
                bthread_usleep(4 * 1000 * 1000);
                continue;
              }

              // own lock
              if (watch_key.empty()) {
                LOG(INFO) << fmt::format("[dlock.{}] mds({}) own lock.", lock_key, self->mds_id_);

                self->is_locked_.store(true);
                bthread_usleep(FLAGS_distribution_lock_lease_ttl_ms * 1000 / 2);

              } else {
                // not own lock, watch key
                self->Watch(watch_key, watch_revision);
              }
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

}  // namespace mdsv2
}  // namespace dingofs
