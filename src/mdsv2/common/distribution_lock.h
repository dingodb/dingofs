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

#ifndef DINGOFS_MDSV2_COMMON_DISTRIBUTION_H_
#define DINGOFS_MDSV2_COMMON_DISTRIBUTION_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "bthread/bthread.h"
#include "dingosdk/version.h"
#include "mdsv2/common/status.h"
#include "mdsv2/coordinator/coordinator_client.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class DistributionLock : public std::enable_shared_from_this<DistributionLock> {
 public:
  DistributionLock() = default;
  virtual ~DistributionLock() = default;

  virtual bool Init() = 0;
  virtual void Destroy() = 0;

  virtual std::string LockKey() = 0;
  virtual bool IsLocked() = 0;
};

using DistributionLockPtr = std::shared_ptr<DistributionLock>;

class CoorDistributionLock;
using CoorDistributionLockPtr = std::shared_ptr<CoorDistributionLock>;

class StoreDistributionLock;
using StoreDistributionLockPtr = std::shared_ptr<StoreDistributionLock>;

class CoorDistributionLock : public DistributionLock {
 public:
  CoorDistributionLock(CoordinatorClientPtr coordinator_client, const std::string& lock_prefix, int64_t mds_id);
  ~CoorDistributionLock() override = default;

  static CoorDistributionLockPtr New(CoordinatorClientPtr coordinator_client, const std::string& lock_prefix,
                                     int64_t mds_id) {
    return std::make_shared<CoorDistributionLock>(coordinator_client, lock_prefix, mds_id);
  }

  CoorDistributionLockPtr GetSelfPtr();

  bool Init() override;
  void Destroy() override;

  std::string LockKey() override;
  bool IsLocked() override;

 private:
  Status CreateLease(int64_t& lease_id);
  Status RenewLease(int64_t lease_id);
  Status DeleteLease(int64_t lease_id);

  Status DeleteLockKey(const std::string& key);
  Status PutLockKey(const std::string& key, int64_t lease_id);
  Status CheckLock(std::string& watch_key, int64_t& watch_revision);

  bool LaunchRenewLease();
  void StopRenewLease();

  Status Watch(const std::string& watch_key, int64_t watch_revision);

  bool LaunchCheckLock();
  void CheckLock();
  void StopCheckLock();

  std::atomic<bool> is_stop_{false};

  std::string addr_;

  int64_t mds_id_;
  std::string lock_prefix_;

  int64_t lease_id_{0};
  // for renew lease
  bthread_t lease_th_{0};

  std::atomic<bool> is_locked_{false};
  bthread_t check_lock_th_{0};

  CoordinatorClientPtr coordinator_client_;
};

// use store transaction implement distribution lock
class StoreDistributionLock : public DistributionLock {
 public:
  StoreDistributionLock(KVStoragePtr kv_storage, const std::string& name, int64_t mds_id);
  ~StoreDistributionLock() override = default;

  static StoreDistributionLockPtr New(KVStoragePtr kv_storage, const std::string& name, int64_t mds_id) {
    return std::make_shared<StoreDistributionLock>(kv_storage, name, mds_id);
  }

  StoreDistributionLockPtr GetSelfPtr();

  bool Init() override;
  void Destroy() override;
  std::string LockKey() override;
  bool IsLocked() override;

 private:
  Status RenewLease();
  bool LaunchRenewLease();
  void StopRenewLease();

  const std::string name_;
  int64_t mds_id_;

  std::atomic<bool> is_stop_{false};

  bthread_t lease_th_{0};

  std::atomic<bool> is_locked_{false};
  std::atomic<uint64_t> last_lock_time_ms_{0};

  KVStoragePtr kv_storage_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_COMMON_DISTRIBUTION_H_