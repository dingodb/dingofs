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

#include <memory>

#include "dingofs/proto/mdsv2.pb.h"
#include "dingofs/src/mdsv2/common/status.h"
#include "dingofs/src/mdsv2/filesystem/id_generator.h"
#include "dingofs/src/mdsv2/storage/storage.h"

namespace dingofs {

namespace mdsv2 {

class FileSystem;
using FileSystemPtr = std::shared_ptr<FileSystem>;

class FileSystem {
 public:
  FileSystem(KVStorage* kv_storage) : kv_storage_(kv_storage){};
  ~FileSystem() = default;

  static FileSystemPtr New(KVStorage* kv_storage) { return std::make_shared<FileSystem>(kv_storage); }

  bool Init();

  Status CreateFs(const pb::mds::FsInfo& fs_info);
  Status MountFs(const std::string& fs_name, const pb::mds::MountPoint& mount_point);
  Status UmountFs(const std::string& fs_name, const pb::mds::MountPoint& mount_point);
  Status DeleteFs(const std::string& fs_name);
  Status GetFsInfo(const std::string& fs_name, pb::mds::FsInfo& fs_info);

 private:
  Status CreateFsTable();
  bool IsExistFsTable();

  // here not free
  KVStorage* kv_storage_{nullptr};
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_H_