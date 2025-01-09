
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
 * @Project: dingo
 * @Date: Fri Jul 23 16:37:33 CST 2021
 * @Author: wuhanqing
 */

#ifndef DINGOFS_SRC_MDS_FS_INFO_WRAPPER_H_
#define DINGOFS_SRC_MDS_FS_INFO_WRAPPER_H_

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "dingofs/mds.pb.h"

namespace dingofs {
namespace mds {

// A wrapper for proto FsInfo
class FsInfoWrapper {
  friend class PersisKVStorage;

 public:
  FsInfoWrapper() = default;

  explicit FsInfoWrapper(const pb::mds::FsInfo& fs_info) : fsInfo_(fs_info) {}

  explicit FsInfoWrapper(pb::mds::FsInfo&& fs_info)
      : fsInfo_(std::move(fs_info)) {}

  FsInfoWrapper(const pb::mds::CreateFsRequest* request, uint64_t fs_id,
                uint64_t root_inode_id);

  FsInfoWrapper(const FsInfoWrapper& other) = default;
  FsInfoWrapper& operator=(const FsInfoWrapper& other) = default;

  FsInfoWrapper(FsInfoWrapper&& other) noexcept = default;
  FsInfoWrapper& operator=(FsInfoWrapper&& other) noexcept = default;

  void SetFsType(pb::common::FSType type) { fsInfo_.set_fstype(type); }

  void SetStatus(pb::mds::FsStatus status) { fsInfo_.set_status(status); }

  void SetFsName(const std::string& name) { fsInfo_.set_fsname(name); }

  void SetCapacity(uint64_t capacity) { fsInfo_.set_capacity(capacity); }

  void SetOwner(const std::string& owner) { fsInfo_.set_owner(owner); }

  pb::common::FSType GetFsType() const { return fsInfo_.fstype(); }

  pb::mds::FsStatus GetStatus() const { return fsInfo_.status(); }

  std::string GetFsName() const { return fsInfo_.fsname(); }

  uint64_t GetFsId() const { return fsInfo_.fsid(); }

  uint64_t GetBlockSize() const { return fsInfo_.blocksize(); }

  uint64_t GetCapacity() const { return fsInfo_.capacity(); }

  std::string GetOwner() const { return fsInfo_.owner(); }

  bool IsMountPointEmpty() const { return fsInfo_.mountpoints_size() == 0; }

  bool IsMountPointExist(const pb::mds::Mountpoint& mp) const;

  bool IsMountPointConflict(const pb::mds::Mountpoint& mp) const;

  void AddMountPoint(const pb::mds::Mountpoint& mp);

  pb::mds::FSStatusCode DeleteMountPoint(const pb::mds::Mountpoint& mp);

  std::vector<pb::mds::Mountpoint> MountPoints() const;

  const pb::mds::FsInfo& ProtoFsInfo() const& { return fsInfo_; }

  pb::mds::FsInfo ProtoFsInfo() && { return std::move(fsInfo_); }

  const pb::mds::FsDetail& GetFsDetail() const { return fsInfo_.detail(); }

  bool GetEnableSumInDir() const { return fsInfo_.enablesumindir(); }

  uint64_t IncreaseFsTxSequence(const std::string& owner) {
    if (!fsInfo_.has_txowner() || fsInfo_.txowner() != owner) {
      uint64_t tx_sequence = 0;
      if (fsInfo_.has_txsequence()) {
        tx_sequence = fsInfo_.txsequence();
      }
      fsInfo_.set_txsequence(tx_sequence + 1);
      fsInfo_.set_txowner(owner);
    }
    return fsInfo_.txsequence();
  }

  uint64_t GetFsTxSequence() {
    return fsInfo_.has_txsequence() ? fsInfo_.txsequence() : 0;
  }

  void SetVolumeSize(uint64_t size);

 private:
  pb::mds::FsInfo fsInfo_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_FS_INFO_WRAPPER_H_
