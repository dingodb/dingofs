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

#ifndef DINGOFS_SRC_COMMON_DYNAMIC_CONFIG_H_
#define DINGOFS_SRC_COMMON_DYNAMIC_CONFIG_H_

#include <gflags/gflags.h>

namespace dingofs {
namespace common {

/**
 * use vlog_level to set vlog level on the fly
 * When vlog_level is set, CheckVLogLevel is called to check the validity of the
 * value. Dynamically modify the vlog level by setting FLAG_v in CheckVLogLevel.
 *
 * You can modify the vlog level to 0 using:
 * curl -s http://127.0.0.1:9000/flags/vlog_level?setvalue=0
 */
DECLARE_int32(vlog_level);

DECLARE_bool(mds_access_logging);
DECLARE_int64(mds_access_log_threshold_us);

DECLARE_bool(meta_access_logging);
DECLARE_int64(meta_access_log_threshold_us);

DECLARE_bool(s3_access_logging);
DECLARE_int64(s3_access_log_threshold_us);

}  // namespace common
}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_DYNAMIC_CONFIG_H_