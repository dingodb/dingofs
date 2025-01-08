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

#ifndef DINGOFS_SRC_METASERVER_INODE_STORAGE_H_
#define DINGOFS_SRC_METASERVER_INODE_STORAGE_H_

#include <list>
#include <memory>
#include <string>

#include "proto/metaserver.pb.h"
#include "metaserver/storage/converter.h"
#include "metaserver/storage/storage.h"
#include "utils/concurrent/rw_lock.h"

namespace dingofs {
namespace metaserver {

using Transaction = std::shared_ptr<storage::StorageTransaction>;

using S3ChunkInfoMap =
    google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>;

class InodeStorage {
 public:
  InodeStorage(std::shared_ptr<storage::KVStorage> kvStorage,
               std::shared_ptr<storage::NameGenerator> nameGenerator,
               uint64_t nInode);

  /**
   * @brief insert inode to storage
   * @param[in] inode: the inode want to insert
   * @return If inode exist, return INODE_EXIST; else insert and return OK
   */
  pb::metaserver::MetaStatusCode Insert(const pb::metaserver::Inode& inode);

  /**
   * @brief get inode from storage
   * @param[in] key: the key of inode want to get
   * @param[out] inode: the inode got
   * @return If inode not exist, return NOT_FOUND; else return OK
   */
  pb::metaserver::MetaStatusCode Get(const storage::Key4Inode& key,
                                     pb::metaserver::Inode* inode);

  /**
   * @brief get inode attribute from storage
   * @param[in] key: the key of inode want to get
   * @param[out] attr: the inode attribute got
   * @return If inode not exist, return NOT_FOUND; else return OK
   */
  pb::metaserver::MetaStatusCode GetAttr(const storage::Key4Inode& key,
                                         pb::metaserver::InodeAttr* attr);

  /**
   * @brief get inode extended attributes from storage
   * @param[in] key: the key of inode want to get
   * @param[out] attr: the inode extended attribute got
   * @return If inode not exist, return NOT_FOUND; else return OK
   */
  pb::metaserver::MetaStatusCode GetXAttr(const storage::Key4Inode& key,
                                          pb::metaserver::XAttr* xattr);

  /**
   * @brief delete inode from storage
   * @param[in] key: the key of inode want to delete
   * @return If inode not exist, return NOT_FOUND; else return OK
   */
  pb::metaserver::MetaStatusCode Delete(const storage::Key4Inode& key);

  /**
   * @brief update inode from storage
   * @param[in] inode: the inode want to update
   * @return If inode not exist, return NOT_FOUND; else replace and return OK
   */
  pb::metaserver::MetaStatusCode Update(const pb::metaserver::Inode& inode);

  std::shared_ptr<storage::Iterator> GetAllInode();

  bool GetAllInodeId(std::list<uint64_t>* ids);

  // NOTE: the return size is accurate under normal cluster status,
  // but under abnormal status, the return size maybe less than
  // the real value for delete the unexist inode multi-times.
  size_t Size();

  bool Empty();

  pb::metaserver::MetaStatusCode Clear();

  // s3chunkinfo
  pb::metaserver::MetaStatusCode ModifyInodeS3ChunkInfoList(
      uint32_t fsId, uint64_t inodeId, uint64_t chunkIndex,
      const pb::metaserver::S3ChunkInfoList* list2add,
      const pb::metaserver::S3ChunkInfoList* list2del);

  pb::metaserver::MetaStatusCode PaddingInodeS3ChunkInfo(int32_t fsId,
                                                         uint64_t inodeId,
                                                         S3ChunkInfoMap* m,
                                                         uint64_t limit = 0);

  std::shared_ptr<storage::Iterator> GetInodeS3ChunkInfoList(uint32_t fsId,
                                                             uint64_t inodeId);

  std::shared_ptr<storage::Iterator> GetAllS3ChunkInfoList();

  // volume extent
  std::shared_ptr<storage::Iterator> GetAllVolumeExtentList();

  pb::metaserver::MetaStatusCode UpdateVolumeExtentSlice(
      uint32_t fsId, uint64_t inodeId,
      const pb::metaserver::VolumeExtentSlice& slice);

  pb::metaserver::MetaStatusCode GetAllVolumeExtent(
      uint32_t fsId, uint64_t inodeId,
      pb::metaserver::VolumeExtentList* extents);

  pb::metaserver::MetaStatusCode GetVolumeExtentByOffset(
      uint32_t fsId, uint64_t inodeId, uint64_t offset,
      pb::metaserver::VolumeExtentSlice* slice);

  pb::metaserver::MetaStatusCode AddS3ChunkInfoList(
      std::shared_ptr<storage::StorageTransaction> txn, uint32_t fsId,
      uint64_t inodeId, uint64_t chunkIndex,
      const pb::metaserver::S3ChunkInfoList* list2add);

 private:
  pb::metaserver::MetaStatusCode UpdateInodeS3MetaSize(Transaction txn,
                                                       uint32_t fsId,
                                                       uint64_t inodeId,
                                                       uint64_t size4add,
                                                       uint64_t size4del);

  uint64_t GetInodeS3MetaSize(uint32_t fsId, uint64_t inodeId);

  pb::metaserver::MetaStatusCode DelS3ChunkInfoList(
      std::shared_ptr<storage::StorageTransaction> txn, uint32_t fsId,
      uint64_t inodeId, uint64_t chunkIndex,
      const pb::metaserver::S3ChunkInfoList* list2del);

  // FIXME: please remove this lock, because we has locked each inode
  // in inode manager, this lock only for proetct storage, but now we
  // use rocksdb storage, it support write in parallel.
  utils::PthreadRWLock rwLock_;
  std::shared_ptr<storage::KVStorage> kvStorage_;
  std::string table4Inode_;
  std::string table4S3ChunkInfo_;
  std::string table4VolumeExtent_;
  std::string table4InodeAuxInfo_;
  size_t nInode_;
  storage::Converter conv_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_INODE_STORAGE_H_
