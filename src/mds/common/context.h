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
#include <string>

#include "mds/common/tracing.h"
#include "mds/common/type.h"

namespace dingofs {
namespace mds {

class Context {
 public:
  Context() = default;
  Context(const std::string& request_id, const std::string method_name)
      : request_id_(request_id), method_name_(method_name) {};
  Context(const ContextEntry& ctx, const std::string& request_id, const std::string method_name)
      : is_bypass_cache_(ctx.is_bypass_cache()),
        use_base_version_(ctx.use_base_version()),
        inode_version_(ctx.inode_version()),
        client_id_(ctx.client_id()),
        request_id_(request_id),
        method_name_(method_name) {
    ancestors_ = {ctx.ancestors().begin(), ctx.ancestors().end()};
  };

  bool IsBypassCache() const { return is_bypass_cache_; }
  bool UseBaseVersion() const { return use_base_version_; }
  uint64_t GetInodeVersion() const { return inode_version_; }
  const std::string& ClientId() const { return client_id_; }
  const std::string& RequestId() const { return request_id_; }
  const std::string& MethodName() const { return method_name_; }
  Trace& GetTrace() { return trace_; }

  const std::vector<Ino>& GetAncestors() const { return ancestors_; }

 private:
  const bool is_bypass_cache_{false};

  const bool use_base_version_{false};

  const uint64_t inode_version_{0};

  const std::string client_id_;
  const std::string request_id_;
  const std::string method_name_;

  std::vector<Ino> ancestors_;

  Trace trace_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_CONTEXT_H_
