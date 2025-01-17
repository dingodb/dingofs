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

#include "client/filesystemv2/dir_reader.h"

#include <cstdint>

namespace dingofs {
namespace client {
namespace filesystem {

uint64_t DirReader::GenID() { return id_generator_.fetch_add(1); }

uint64_t DirReader::NewState(uint64_t ino) {
  utils::WriteLockGuard lk(lock_);

  uint64_t id = GenID();

  auto state = std::make_shared<State>();
  state->ino = ino;
  state_map_[id] = state;

  return id;
}

DirReader::StatePtr DirReader::GetState(uint64_t fh) {
  utils::ReadLockGuard lk(lock_);

  auto it = state_map_.find(fh);
  if (it == state_map_.end()) {
    return nullptr;
  }

  return it->second;
}

void DirReader::DeleteState(uint64_t fh) {
  utils::WriteLockGuard lk(lock_);

  state_map_.erase(fh);
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
