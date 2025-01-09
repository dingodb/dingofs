/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-03-16
 * Author: Jingli Chen (Wine93)
 */

#include <map>
#include <memory>
#include <string>

#include "metaserver/common/types.h"
#include "metaserver/partition.h"
#include "metaserver/storage/converter.h"
#include "metaserver/storage/dumpfile.h"
#include "metaserver/storage/iterator.h"
#include "metaserver/storage/storage.h"

#ifndef DINGOFS_SRC_METASERVER_METASTORE_FSTREAM_H_
#define DINGOFS_SRC_METASERVER_METASTORE_FSTREAM_H_

namespace dingofs {
namespace metaserver {

using PartitionMap = std::map<uint32_t, std::shared_ptr<Partition>>;

class MetaStoreFStream {
 public:
  MetaStoreFStream(PartitionMap* partitionMap,
                   std::shared_ptr<storage::KVStorage> kvStorage, PoolId poolId,
                   CopysetId copysetId);

  bool Load(const std::string& pathname, uint8_t* version);

  bool Save(const std::string& path, storage::DumpFileClosure* done = nullptr);

 private:
  bool LoadPartition(uint32_t partitionId, const std::string& key,
                     const std::string& value);

  bool LoadInode(uint32_t partitionId, const std::string& key,
                 const std::string& value);

  bool LoadDentry(uint8_t version, uint32_t partitionId, const std::string& key,
                  const std::string& value);

  bool LoadPendingTx(uint32_t partitionId, const std::string& key,
                     const std::string& value);

  bool LoadInodeS3ChunkInfoList(uint32_t partitionId, const std::string& key,
                                const std::string& value);

  bool LoadVolumeExtentList(uint32_t partitionId, const std::string& key,
                            const std::string& value);

  std::shared_ptr<storage::Iterator> NewPartitionIterator();

  std::shared_ptr<storage::Iterator> NewInodeIterator(
      std::shared_ptr<Partition> partition);

  std::shared_ptr<storage::Iterator> NewDentryIterator(
      std::shared_ptr<Partition> partition);

  std::shared_ptr<storage::Iterator> NewPendingTxIterator(
      std::shared_ptr<Partition> partition);

  std::shared_ptr<storage::Iterator> NewInodeS3ChunkInfoListIterator(
      std::shared_ptr<Partition> partition);

  std::shared_ptr<storage::Iterator> NewVolumeExtentListIterator(
      Partition* partition);

 private:
  std::shared_ptr<Partition> GetPartition(uint32_t partitionId);

  PartitionMap* partitionMap_;
  std::shared_ptr<storage::KVStorage> kvStorage_;
  std::shared_ptr<storage::Converter> conv_;

  PoolId poolId_ = 0;
  CopysetId copysetId_ = 0;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_METASTORE_FSTREAM_H_
