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

#ifndef DINGOFS_MDS_COMMON_DISTRIBUTION_H_
#define DINGOFS_MDS_COMMON_DISTRIBUTION_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "dingosdk/version.h"
#include "mds/common/status.h"
#include "mds/coordinator/coordinator_client.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {

class DistributionLock : public std::enable_shared_from_this<DistributionLock> {
 public:
  DistributionLock() = default;
  virtual ~DistributionLock() = default;

  virtual bool Init() = 0;
  virtual void Destroy() = 0;

  virtual std::string LockKey() = 0;
  virtual bool IsLocked() = 0;
};

using DistributionLockSPtr = std::shared_ptr<DistributionLock>;

class CoorDistributionLock;
using CoorDistributionLockSPtr = std::shared_ptr<CoorDistributionLock>;

class StoreDistributionLock;
using StoreDistributionLockPtr = std::shared_ptr<StoreDistributionLock>;

class CoorDistributionLock : public DistributionLock {
 public:
  CoorDistributionLock(CoordinatorClientSPtr coordinator_client, const std::string& lock_prefix, int64_t mds_id);
  ~CoorDistributionLock() override = default;

  static CoorDistributionLockSPtr New(CoordinatorClientSPtr coordinator_client, const std::string& lock_prefix,
                                      int64_t mds_id) {
    return std::make_shared<CoorDistributionLock>(coordinator_client, lock_prefix, mds_id);
  }

  CoorDistributionLockSPtr GetSelfPtr();

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

  CoordinatorClientSPtr coordinator_client_;
};

// use store transaction implement distribution lock
class StoreDistributionLock : public DistributionLock {
 public:
  StoreDistributionLock(KVStorageSPtr kv_storage, const std::string& name, int64_t mds_id);
  ~StoreDistributionLock() override = default;

  static StoreDistributionLockPtr New(KVStorageSPtr kv_storage, const std::string& name, int64_t mds_id) {
    return std::make_shared<StoreDistributionLock>(kv_storage, name, mds_id);
  }

  StoreDistributionLockPtr GetSelfPtr();

  bool Init() override;
  void Destroy() override;
  std::string LockKey() override;
  bool IsLocked() override;

  struct LockEntry {
    std::string name;
    int64_t owner{0};
    uint64_t epoch{0};
    uint64_t expire_time_ms{0};
  };

  static Status GetAllLockInfo(OperationProcessorSPtr operation_processor, std::vector<LockEntry>& lock_entries);

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

  KVStorageSPtr kv_storage_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_DISTRIBUTION_H_