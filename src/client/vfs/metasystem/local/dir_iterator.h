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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_LOCAL_DIR_ITERATOR_H_
#define DINGOFS_SRC_CLIENT_VFS_META_LOCAL_DIR_ITERATOR_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "json/value.h"
#include "leveldb/iterator.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace local {

class DirIterator;
using DirIteratorSPtr = std::shared_ptr<DirIterator>;

// used by read dir
class DirIterator {
 public:
  DirIterator(Ino ino, uint64_t fh, std::vector<mds::DentryEntry>&& dentries)
      : ino_(ino), fh_(fh), dentries_(std::move(dentries)) {}
  ~DirIterator() = default;

  static DirIteratorSPtr New(Ino ino, uint64_t fh,
                             std::vector<mds::DentryEntry>&& dentries) {
    return std::make_shared<DirIterator>(ino, fh, std::move(dentries));
  }

  Ino INo() const { return ino_; }
  uint64_t Fh() const { return fh_; }

  void Remember(uint64_t off);

  Status GetValue(ContextSPtr& ctx, uint64_t off, DirEntry& dir_entry);

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  const Ino ino_;
  const uint64_t fh_;

  std::vector<mds::DentryEntry> dentries_;

  // stat
  std::vector<uint64_t> offset_stats_;
};

class DirIteratorManager {
 public:
  DirIteratorManager() = default;
  ~DirIteratorManager() = default;

  using DirIteratorSet = std::vector<DirIteratorSPtr>;
  using PutFunc = std::function<void(const DirIteratorSet&)>;

  void PutWithFunc(Ino ino, uint64_t fh, DirIteratorSPtr& dir_iterator,
                   PutFunc&& f);

  DirIteratorSPtr Get(Ino ino, uint64_t fh);
  void Delete(Ino ino, uint64_t fh);

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  utils::RWLock lock_;
  std::unordered_map<Ino, DirIteratorSet> dir_iterator_map_;
};

}  // namespace local
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_LOCAL_DIR_ITERATOR_H_