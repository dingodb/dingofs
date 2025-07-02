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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_DIR_ITERATOR_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_DIR_ITERATOR_H_

#include <json/value.h>

#include <cstdint>

#include "client/vfs/meta/v2/mds_client.h"
#include "client/meta/vfs_meta.h"
#include "common/status.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class DirIterator;
using DirIteratorSPtr = std::shared_ptr<DirIterator>;

// used by read dir
class DirIterator {
 public:
  DirIterator(MDSClientPtr mds_client, Ino ino)
      : mds_client_(mds_client), ino_(ino) {}

  static DirIteratorSPtr New(MDSClientPtr mds_client, Ino ino) {
    return std::make_shared<DirIterator>(mds_client, ino);
  }

  Status Seek();
  bool Valid();
  DirEntry GetValue(bool with_attr);
  void Next();

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  Ino ino_;
  // last file/dir name, used to read next batch
  std::string last_name_;
  bool with_attr_{false};

  uint32_t offset_{0};
  // stash entry for read dir
  std::vector<DirEntry> entries_;

  MDSClientPtr mds_client_;
};

class DirIteratorManager {
 public:
  DirIteratorManager() = default;
  ~DirIteratorManager() = default;

  void Put(uint64_t fh, DirIteratorSPtr dir_iterator);
  DirIteratorSPtr Get(uint64_t fh);
  void Delete(uint64_t fh);

  bool Dump(Json::Value& value);
  bool Load(MDSClientPtr mds_client, const Json::Value& value);

 private:
  utils::RWLock lock_;
  // fh -> DirIteratorSPtr
  std::map<uint64_t, DirIteratorSPtr> dir_iterator_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_DIR_ITERATOR_H_