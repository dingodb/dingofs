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
 * Created Date: 2021-08-31
 * Author: xuchaojie
 */

#ifndef DINGOFS_SRC_METASERVER_TRASH_H_
#define DINGOFS_SRC_METASERVER_TRASH_H_

#include <cstdint>
#include <list>
#include <memory>
#include <unordered_map>

#include "dataaccess/accesser_common.h"
#include "dataaccess/block_accesser_factory.h"
#include "metaserver/inode_storage.h"
#include "metaserver/s3/metaserver_s3_adaptor.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/concurrent/concurrent.h"
#include "utils/configuration.h"

namespace dingofs {
namespace metaserver {

struct TrashItem {
  uint32_t fsId;
  uint64_t inodeId;
  uint32_t dtime;
  TrashItem() : fsId(0), inodeId(0), dtime(0) {}
};

struct TrashOption {
  uint32_t scanPeriodSec;
  uint32_t expiredAfterSec;
  dataaccess::BlockAccessOptions block_access_options;
  std::shared_ptr<dataaccess::BlockAccesserFactory> block_accesser_factory;
  S3ClientAdaptorOption s3_client_adaptor_option;
  std::shared_ptr<stub::rpcclient::MdsClient> mdsClient;

  TrashOption() : scanPeriodSec(0), expiredAfterSec(0), mdsClient(nullptr) {}

  void InitTrashOptionFromConf(std::shared_ptr<utils::Configuration> conf);
};

class Trash {
 public:
  Trash() {}
  virtual ~Trash() {}

  virtual void Init(const TrashOption& option) = 0;

  virtual void Add(uint32_t fsId, uint64_t inodeId, uint32_t dtime) = 0;

  virtual void ListItems(std::list<TrashItem>* items) = 0;

  virtual void ScanTrash() = 0;

  virtual void StopScan() = 0;

  virtual bool IsStop() = 0;
};

class TrashImpl : public Trash {
 public:
  explicit TrashImpl(const std::shared_ptr<InodeStorage>& inodeStorage)
      : inodeStorage_(inodeStorage) {}

  ~TrashImpl() {}

  void Init(const TrashOption& option) override;

  void Add(uint32_t fsId, uint64_t inodeId, uint32_t dtime) override;

  void ListItems(std::list<TrashItem>* items) override;

  void ScanTrash() override;

  void StopScan() override;

  bool IsStop() override;

 private:
  bool NeedDelete(const TrashItem& item);

  pb::metaserver::MetaStatusCode DeleteInodeAndData(const TrashItem& item);

  uint64_t GetFsRecycleTimeHour(uint32_t fsId);

  TrashOption options_;

  std::shared_ptr<InodeStorage> inodeStorage_;
  std::shared_ptr<stub::rpcclient::MdsClient> mdsClient_;
  std::unordered_map<uint32_t, pb::mds::FsInfo> fsInfoMap_;

  std::list<TrashItem> trashItems_;

  mutable utils::Mutex itemsMutex_;

  mutable utils::Mutex scanMutex_;

  bool isStop_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_TRASH_H_
