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

#include "options/client/client_dynamic_option.h"

#include "options/gflag_validator.h"

namespace dingofs {
namespace client {

// access log
DEFINE_bool(access_logging, true, "enable access log");
DEFINE_validator(access_logging, &PassBool);

DEFINE_int64(access_log_threshold_us, 0, "access log threshold");
DEFINE_validator(access_log_threshold_us, &PassInt64);

}  // namespace client
}  // namespace dingofs