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

#ifndef DINGOFS_MDV2_FILESYSTEM_DIR_READER_H_
#define DINGOFS_MDV2_FILESYSTEM_DIR_READER_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "client/vfs/vfs_meta.h"
#include "dingofs/mdsv2.pb.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class DirReader {
 public:
  DirReader(uint64_t start_id) : id_generator_(start_id) {}
  ~DirReader() = default;

  using Entry = pb::mdsv2::ReadDirResponse::Entry;

  struct State {
    uint64_t ino{0};
    std::string last_name;
    bool is_end{false};
    std::vector<Entry> entries;
    uint32_t offset{0};
  };
  using StatePtr = std::shared_ptr<State>;

  uint64_t NewState(uint64_t ino);
  StatePtr GetState(uint64_t fh);
  void DeleteState(uint64_t fh);

 private:
  uint64_t GenID();

  std::atomic<uint64_t> id_generator_;

  utils::RWLock lock_;
  std::map<uint64_t, StatePtr> state_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_DIR_READER_H_
