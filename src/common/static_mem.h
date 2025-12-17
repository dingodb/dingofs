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

#ifndef DINGOFS_SRC_COMMON_STATIC_MEM_H_
#define DINGOFS_SRC_COMMON_STATIC_MEM_H_

#include <cstdint>

namespace dingofs {

static constexpr int64_t kStaticMemSize = 4 * 1024 * 1024;  // 4MB
extern const char kStaticMemory[kStaticMemSize];

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_STATIC_MEM_H_