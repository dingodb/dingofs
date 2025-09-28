// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDS_COMMON_CONTEXT_H_
#define DINGOFS_MDS_COMMON_CONTEXT_H_

#include <cstdint>

#include "mds/common/tracing.h"
#include "mds/common/type.h"

namespace dingofs {
namespace mds {

class Context {
 public:
  Context() = default;
  Context(bool is_bypass_cache, uint64_t inode_version, const std::string& client_id = "")
      : is_bypass_cache_(is_bypass_cache), inode_version_(inode_version), client_id_(client_id) {};

  void SetBypassCache(bool is_bypass_cache) { is_bypass_cache_ = is_bypass_cache; }
  bool IsBypassCache() const { return is_bypass_cache_; }

  uint64_t GetInodeVersion() const { return inode_version_; }
  const std::string& ClientId() const { return client_id_; }
  Trace& GetTrace() { return trace_; }

  void SetAncestors(std::vector<Ino>&& ancestors) { ancestors_ = std::move(ancestors); }
  const std::vector<Ino>& GetAncestors() const { return ancestors_; }

 private:
  bool is_bypass_cache_{false};

  uint64_t inode_version_{0};

  std::string client_id_;

  std::vector<Ino> ancestors_;

  Trace trace_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_CONTEXT_H_
