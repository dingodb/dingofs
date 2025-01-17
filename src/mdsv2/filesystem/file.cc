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

#include "mdsv2/filesystem/file.h"

namespace dingofs {
namespace mdsv2 {

void OpenFiles::Open(uint64_t ino, InodePtr inode) {
  utils::WriteLockGuard lk(lock_);

  auto it = file_map_.find(ino);
  if (it != file_map_.end()) {
    ++it->second.ref_count;
    return;
  }

  State state;
  state.inode = inode;
  state.ref_count = 1;

  file_map_[ino] = state;
}

InodePtr OpenFiles::IsOpened(uint64_t ino) {
  utils::ReadLockGuard lk(lock_);

  auto it = file_map_.find(ino);
  return it != file_map_.end() ? it->second.inode : nullptr;
}

void OpenFiles::Close(uint64_t ino) {
  utils::WriteLockGuard lk(lock_);

  auto it = file_map_.find(ino);
  if (it == file_map_.end()) {
    return;
  }

  if (--it->second.ref_count == 0) {
    file_map_.erase(it);
  }
}

void OpenFiles::CloseAll() {
  utils::WriteLockGuard lk(lock_);

  for (auto it = file_map_.begin(); it != file_map_.end();) {
    auto& state = it->second;
    if (state.ref_count == 0) {
      it = file_map_.erase(it);
    } else {
      ++it;
    }
  }
}

std::vector<OpenFiles::State> OpenFiles::GetAllState() {
  utils::ReadLockGuard lk(lock_);

  std::vector<State> states;
  states.reserve(file_map_.size());
  for (const auto& [ino, state] : file_map_) {
    states.push_back(state);
  }

  return states;
}

}  // namespace mdsv2
}  // namespace dingofs