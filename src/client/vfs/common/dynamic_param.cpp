/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "client/vfs/common/dynamic_param.h"

#include <cstdint>

namespace dingofs {
namespace client {
namespace vfs {

namespace {
bool PassDouble(const char*, double) { return true; }
bool PassUint64(const char*, uint64_t) { return true; }
bool PassInt32(const char*, int32_t) { return true; }
bool PassUint32(const char*, uint32_t) { return true; }
bool PassBool(const char*, bool) { return true; }
};  // namespace

// access log
DEFINE_bool(meta_logging, true, "enable meta system log");
DEFINE_validator(meta_logging, &PassBool);

}
}  // namespace client
}  // namespace dingofs