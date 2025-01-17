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

#ifndef DINGOFS_MDV2_FILESYSTEM_FILE_H_
#define DINGOFS_MDV2_FILESYSTEM_FILE_H_

#include <cstdint>
#include <map>
#include <vector>

#include "mdsv2/filesystem/inode.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

class OpenFiles {
 public:
  OpenFiles() = default;
  ~OpenFiles() = default;

  struct State {
    InodePtr inode;
    uint32_t ref_count{0};
  };

  void Open(uint64_t ino, InodePtr inode);
  InodePtr IsOpened(uint64_t ino);

  void Close(uint64_t ino);
  void CloseAll();

  std::vector<State> GetAllState();

 private:
  utils::RWLock lock_;
  std::map<uint64_t, State> file_map_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_FILE_H_