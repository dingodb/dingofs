// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDV2_FILESYSTEM_H_
#define DINGOFS_MDV2_FILESYSTEM_H_

#include "curvefs/proto/mdsv2.pb.h"
#include "curvefs/src/mdsv2/common/status.h"
#include "curvefs/src/mdsv2/storage/storage.h"

namespace dingofs {

namespace mdsv2 {

class FileSystem {
 public:
  FileSystem() = default;
  ~FileSystem() = default;

  Status CreateFs(const pb::mds::FsInfo& fs_info);
  Status MountFs(const std::string& fs_name, const pb::mds::MountPoint& mount_point);
  Status UmountFs(const std::string& fs_name, const pb::mds::MountPoint& mount_point);
  Status DeleteFs(const std::string& fs_name);
  Status GetFsInfo(const std::string& fs_name, pb::mds::FsInfo& fs_info);

 private:
  KVStoragePtr kv_storage_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_H_