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
 * Project: dingo
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef DINGOFS_SRC_METASERVER_DENTRY_STORAGE_H_
#define DINGOFS_SRC_METASERVER_DENTRY_STORAGE_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/container/btree_set.h"
#include "proto/metaserver.pb.h"
#include "metaserver/storage/converter.h"
#include "metaserver/storage/storage.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {

namespace pb {
namespace metaserver {
#define EQUAL(a) (lhs.a() == rhs.a())
#define LESS(a) (lhs.a() < rhs.a())
#define LESS2(a, b) (EQUAL(a) && LESS(b))
#define LESS3(a, b, c) (EQUAL(a) && LESS2(b, c))
#define LESS4(a, b, c, d) (EQUAL(a) && LESS3(b, c, d))

inline bool operator==(const Dentry& lhs, const Dentry& rhs) {
  return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) && EQUAL(txid) &&
         EQUAL(inodeid) && EQUAL(flag);
}

inline bool operator<(const Dentry& lhs, const Dentry& rhs) {
  return LESS(fsid) || LESS2(fsid, parentinodeid) ||
         LESS3(fsid, parentinodeid, name) ||
         LESS4(fsid, parentinodeid, name, txid);
}

}  // namespace metaserver
}  // namespace pb

namespace metaserver {

using BTree = absl::btree_set<pb::metaserver::Dentry>;

class DentryVector {
 public:
  explicit DentryVector(pb::metaserver::DentryVec* vec);

  void Insert(const pb::metaserver::Dentry& dentry);

  void Delete(const pb::metaserver::Dentry& dentry);

  void Merge(const pb::metaserver::DentryVec& src);

  void Filter(uint64_t maxTxId, BTree* btree);

  void Confirm(uint64_t* count);

 private:
  pb::metaserver::DentryVec* vec_;
  uint64_t nPendingAdd_;
  uint64_t nPendingDel_;
};

class DentryList {
 public:
  DentryList(std::vector<pb::metaserver::Dentry>* list, uint32_t limit,
             const std::string& exclude, uint64_t maxTxId, bool onlyDir);

  void PushBack(pb::metaserver::DentryVec* vec);

  uint32_t Size();

  bool IsFull();

 private:
  std::vector<pb::metaserver::Dentry>* list_;
  uint32_t size_;
  uint32_t limit_;
  std::string exclude_;
  uint64_t maxTxId_;
  bool onlyDir_;
};

class DentryStorage {
 public:
  enum class TX_OP_TYPE {
    PREPARE,
    COMMIT,
    ROLLBACK,
  };

 public:
  DentryStorage(std::shared_ptr<storage::KVStorage> kvStorage,
                std::shared_ptr<storage::NameGenerator> nameGenerator,
                uint64_t nDentry);

  pb::metaserver::MetaStatusCode Insert(const pb::metaserver::Dentry& dentry);

  // only for loadding from snapshot
  pb::metaserver::MetaStatusCode Insert(const pb::metaserver::DentryVec& vec,
                                        bool merge);

  pb::metaserver::MetaStatusCode Delete(const pb::metaserver::Dentry& dentry);

  pb::metaserver::MetaStatusCode Get(pb::metaserver::Dentry* dentry);

  pb::metaserver::MetaStatusCode List(
      const pb::metaserver::Dentry& dentry,
      std::vector<pb::metaserver::Dentry>* dentrys, uint32_t limit,
      bool onlyDir = false);

  pb::metaserver::MetaStatusCode HandleTx(TX_OP_TYPE type,
                                          const pb::metaserver::Dentry& dentry);

  std::shared_ptr<storage::Iterator> GetAll();

  size_t Size();

  bool Empty();

  pb::metaserver::MetaStatusCode Clear();

 private:
  std::string DentryKey(const pb::metaserver::Dentry& entry);

  bool CompressDentry(pb::metaserver::DentryVec* vec, BTree* dentrys);

  pb::metaserver::MetaStatusCode Find(const pb::metaserver::Dentry& in,
                                      pb::metaserver::Dentry* out,
                                      pb::metaserver::DentryVec* vec,
                                      bool compress);

 private:
  utils::RWLock rwLock_;
  std::shared_ptr<storage::KVStorage> kvStorage_;
  std::string table4Dentry_;
  uint64_t nDentry_;
  storage::Converter conv_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_DENTRY_STORAGE_H_
