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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_DIR_ITERATOR_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_DIR_ITERATOR_H_

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
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class DirIterator;
using DirIteratorSPtr = std::shared_ptr<DirIterator>;

// used by read dir
class DirIterator {
 public:
  DirIterator(MDSClientSPtr mds_client, Ino ino, uint64_t fh)
      : mds_client_(mds_client), ino_(ino), fh_(fh) {
    last_fetch_time_ns_ = utils::TimestampNs();
  }
  ~DirIterator();

  static DirIteratorSPtr New(MDSClientSPtr mds_client, Ino ino, uint64_t fh) {
    return std::make_shared<DirIterator>(mds_client, ino, fh);
  }

  Ino INo() const { return ino_; }
  uint64_t Fh() const { return fh_; }

  void Remember(uint64_t off);

  Status GetValue(ContextSPtr& ctx, uint64_t off, bool with_attr,
                  DirEntry& dir_entry);

  uint64_t LastFetchTimeNs() const { return last_fetch_time_ns_.load(); }

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  Status Fetch(ContextSPtr& ctx);

  const Ino ino_;
  const uint64_t fh_;
  bool with_attr_{true};

  uint64_t offset_{0};
  std::string last_name_;
  std::vector<DirEntry> entries_;

  bool is_fetch_{false};
  std::atomic<uint64_t> last_fetch_time_ns_{0};

  MDSClientSPtr mds_client_;

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
  bool Load(MDSClientSPtr mds_client, const Json::Value& value);

 private:
  utils::RWLock lock_;
  std::unordered_map<Ino, DirIteratorSet> dir_iterator_map_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_DIR_ITERATOR_H_