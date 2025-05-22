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

#include "common/dynamic_config.h"

#include <glog/logging.h>

DECLARE_int32(v);

namespace dingofs {
namespace common {

namespace {
bool CheckVLogLevel(const char* /*name*/, int32_t value) {
  FLAGS_v = value;
  LOG(INFO) << "current verbose logging level is `" << FLAGS_v << "`";
  return true;
}

bool PassInt64(const char*, int64_t) { return true; }
bool PassBool(const char*, bool) { return true; }
};  // namespace

DEFINE_int32(vlog_level, 0, "set vlog level");
DEFINE_validator(vlog_level, CheckVLogLevel);

DEFINE_bool(mds_access_logging, true, "enable mds access log");
DEFINE_validator(mds_access_logging, &PassBool);
DEFINE_int64(mds_access_log_threshold_us, 10 * 1000,
             "mds access log threshold in us");
DEFINE_validator(mds_access_log_threshold_us, &PassInt64);

DEFINE_bool(meta_access_logging, true, "enable meta access log");
DEFINE_validator(meta_access_logging, &PassBool);
DEFINE_int64(meta_access_log_threshold_us, 0, "meta access log threshold");
DEFINE_validator(meta_access_log_threshold_us, &PassInt64);

DEFINE_bool(s3_access_logging, true, "enable s3 access log");
DEFINE_validator(s3_access_logging, &PassBool);

DEFINE_int64(s3_access_log_threshold_us, 0, "s3 access log threshold");
DEFINE_validator(s3_access_log_threshold_us, &PassInt64);

}  // namespace common
}  // namespace dingofs
