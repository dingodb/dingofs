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

#ifndef DINGOFS_SRC_METASERVER_INODE_MANAGER_H_
#define DINGOFS_SRC_METASERVER_INODE_MANAGER_H_

#include <list>
#include <memory>
#include <string>
#include <vector>

#include "absl/types/optional.h"
#include "dingofs/metaserver.pb.h"
#include "metaserver/inode_storage.h"
#include "metaserver/trash/trash.h"
#include "utils/concurrent/name_lock.h"

namespace dingofs {
namespace metaserver {

using FileType2InodeNumMap =
    ::google::protobuf::Map<::google::protobuf::int32,
                            ::google::protobuf::uint64>;

struct InodeParam {
  uint32_t fsId;
  uint64_t length;
  uint32_t uid;
  uint32_t gid;
  uint32_t mode;
  pb::metaserver::FsFileType type;
  std::string symlink;
  uint64_t rdev;
  uint64_t parent;
  absl::optional<struct timespec> timestamp;
};

class InodeManager {
 public:
  InodeManager(const std::shared_ptr<InodeStorage>& inodeStorage,
               const std::shared_ptr<Trash>& trash,
               FileType2InodeNumMap* type2InodeNum)
      : inodeStorage_(inodeStorage),
        trash_(trash),
        type2InodeNum_(type2InodeNum) {}

  pb::metaserver::MetaStatusCode CreateInode(uint64_t inodeId,
                                             const InodeParam& param,
                                             pb::metaserver::Inode* inode);
  pb::metaserver::MetaStatusCode CreateRootInode(const InodeParam& param);

  pb::metaserver::MetaStatusCode CreateManageInode(
      const InodeParam& param, pb::metaserver::ManageInodeType manageType,
      pb::metaserver::Inode* inode);

  pb::metaserver::MetaStatusCode GetInode(uint32_t fs_id, uint64_t inode_id,
                                          pb::metaserver::Inode* inode,
                                          bool padding_s3_chunk_info = false);

  pb::metaserver::MetaStatusCode GetInodeAttr(uint32_t fsId, uint64_t inodeId,
                                              pb::metaserver::InodeAttr* attr);

  pb::metaserver::MetaStatusCode GetXAttr(uint32_t fsId, uint64_t inodeId,
                                          pb::metaserver::XAttr* xattr);

  pb::metaserver::MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeId);

  pb::metaserver::MetaStatusCode UpdateInode(
      const pb::metaserver::UpdateInodeRequest& request);

  pb::metaserver::MetaStatusCode GetOrModifyS3ChunkInfo(
      uint32_t fsId, uint64_t inodeId, const S3ChunkInfoMap& map2add,
      const S3ChunkInfoMap& map2del, bool returnS3ChunkInfoMap,
      std::shared_ptr<storage::Iterator>* iterator4InodeS3Meta);

  pb::metaserver::MetaStatusCode PaddingInodeS3ChunkInfo(int32_t fsId,
                                                         uint64_t inodeId,
                                                         S3ChunkInfoMap* m,
                                                         uint64_t limit = 0);

  pb::metaserver::MetaStatusCode UpdateInodeWhenCreateOrRemoveSubNode(
      uint32_t fsId, uint64_t inodeId, pb::metaserver::FsFileType type,
      bool isCreate);

  pb::metaserver::MetaStatusCode InsertInode(
      const pb::metaserver::Inode& inode);

  bool GetInodeIdList(std::list<uint64_t>* inodeIdList);

  // Update one or more volume extent slice
  pb::metaserver::MetaStatusCode UpdateVolumeExtent(
      uint32_t fsId, uint64_t inodeId,
      const pb::metaserver::VolumeExtentList& extents);

  // Update only one volume extent slice
  pb::metaserver::MetaStatusCode UpdateVolumeExtentSlice(
      uint32_t fsId, uint64_t inodeId,
      const pb::metaserver::VolumeExtentSlice& slice);

  pb::metaserver::MetaStatusCode GetVolumeExtent(
      uint32_t fsId, uint64_t inodeId, const std::vector<uint64_t>& slices,
      pb::metaserver::VolumeExtentList* extents);

 private:
  void GenerateInodeInternal(uint64_t inodeId, const InodeParam& param,
                             pb::metaserver::Inode* inode);

  bool AppendS3ChunkInfo(uint32_t fsId, uint64_t inodeId, S3ChunkInfoMap added);

  static std::string GetInodeLockName(uint32_t fsId, uint64_t inodeId) {
    return std::to_string(fsId) + "_" + std::to_string(inodeId);
  }

  pb::metaserver::MetaStatusCode UpdateVolumeExtentSliceLocked(
      uint32_t fsId, uint64_t inodeId,
      const pb::metaserver::VolumeExtentSlice& slice);

  std::shared_ptr<InodeStorage> inodeStorage_;
  std::shared_ptr<Trash> trash_;
  FileType2InodeNumMap* type2InodeNum_;

  utils::NameLock inodeLock_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_INODE_MANAGER_H_
