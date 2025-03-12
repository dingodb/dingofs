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

#ifndef DINGOFS_MDSV2_COMMON_CONTEXT_H_
#define DINGOFS_MDSV2_COMMON_CONTEXT_H_

#include <cstdint>

#include "mdsv2/common/tracing.h"

namespace dingofs {
namespace mdsv2 {

class Context {
 public:
  Context() = default;
  Context(bool is_bypass_cache) : is_bypass_cache_(is_bypass_cache) {};

  void SetBypassCache(bool is_bypass_cache) { is_bypass_cache_ = is_bypass_cache; }
  bool IsBypassCache() const { return is_bypass_cache_; }

  Trace& GetTrace() { return trace_; }

 private:
  bool is_bypass_cache_{false};
  Trace trace_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_COMMON_CONTEXT_H_
