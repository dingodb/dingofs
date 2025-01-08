/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2021-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_METASERVER_TRANSACTION_H_
#define DINGOFS_SRC_METASERVER_TRANSACTION_H_

#include <memory>
#include <vector>

#include "metaserver/dentry_storage.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace metaserver {
class RenameTx {
 public:
  RenameTx() = default;

  RenameTx(const std::vector<pb::metaserver::Dentry>& dentrys,
           std::shared_ptr<DentryStorage> storage);

  bool Prepare();

  bool Commit();

  bool Rollback();

  uint64_t GetTxId();

  uint64_t GetTxSequence();

  std::vector<pb::metaserver::Dentry>* GetDentrys();

  bool operator==(const RenameTx& rhs);

  friend std::ostream& operator<<(std::ostream& os, const RenameTx& renameTx);

 private:
  uint64_t txId_;

  // for prevent the stale transaction
  uint64_t txSequence_;

  std::vector<pb::metaserver::Dentry> dentrys_;

  std::shared_ptr<DentryStorage> storage_;
};

class TxManager {
 public:
  explicit TxManager(std::shared_ptr<DentryStorage> storage);

  pb::metaserver::MetaStatusCode HandleRenameTx(
      const std::vector<pb::metaserver::Dentry>& dentrys);

  pb::metaserver::MetaStatusCode PreCheck(
      const std::vector<pb::metaserver::Dentry>& dentrys);

  bool InsertPendingTx(const RenameTx& tx);

  bool FindPendingTx(RenameTx* pendingTx);

  void DeletePendingTx();

  bool HandlePendingTx(uint64_t txId, RenameTx* pendingTx);

 private:
  utils::RWLock rwLock_;

  std::shared_ptr<DentryStorage> storage_;

  RenameTx EMPTY_TX, pendingTx_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_TRANSACTION_H_
