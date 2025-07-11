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

#ifndef DINGOFS_OPTIONS_STUB_DYNAMIC_OPITON_H_
#define DINGOFS_OPTIONS_STUB_DYNAMIC_OPITON_H_

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

namespace dingofs {
namespace stub {

DECLARE_bool(mds_access_logging);
DECLARE_int64(mds_access_log_threshold_us);

DECLARE_bool(meta_access_logging);
DECLARE_int64(meta_access_log_threshold_us);

}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_OPTIONS_STUB_DYNAMIC_OPITON_H_
