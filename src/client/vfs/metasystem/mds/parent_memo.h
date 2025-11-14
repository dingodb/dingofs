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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_PARENT_MEMO_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_PARENT_MEMO_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class ParentMemo;
using ParentMemoSPtr = std::shared_ptr<ParentMemo>;

class ParentMemo {
 public:
  ParentMemo();
  ~ParentMemo() = default;

  static ParentMemoSPtr New() { return std::make_shared<ParentMemo>(); }

  bool GetParent(Ino ino, Ino& parent);
  bool GetVersion(Ino ino, uint64_t& version);
  std::vector<uint64_t> GetAncestors(uint64_t ino);
  bool GetRenameRefCount(Ino ino, int32_t& rename_ref_count);

  void Upsert(Ino ino, Ino parent);
  void UpsertVersion(Ino ino, uint64_t version);
  void UpsertVersionAndRenameRefCount(Ino ino, uint64_t version);
  void Upsert(Ino ino, Ino parent, uint64_t version);
  void Delete(Ino ino);
  void DecRenameRefCount(Ino ino);

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  struct Entry {
    Ino parent;
    uint64_t version;
    int32_t rename_ref_count{0};
  };

  utils::RWLock lock_;
  // ino -> entry
  std::unordered_map<Ino, Entry> ino_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_PARENT_MEMO_H_