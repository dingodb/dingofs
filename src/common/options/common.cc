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

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>

namespace dingofs {

DEFINE_bool(daemonize, false, "run in background");
DEFINE_string(conf, "", "config file");

// log clean
DEFINE_bool(log_clean_enable, true, "enable log file clean");
DEFINE_validator(log_clean_enable, brpc::PassValidate);
DEFINE_int32(log_retention_seconds, 604800,
             "log file retention time in seconds");
DEFINE_validator(log_retention_seconds,
                 [](const char* /*name*/, int32_t value) { return value > 0; });
DEFINE_string(
    log_clean_filter_pattern, ".log.,.log",
    "only clean log files with this pattern in filename, separated by comma");
DEFINE_validator(log_clean_filter_pattern,
                 [](const char* /*name*/, const std::string& value) {
                   return !value.empty();
                 });

}  // namespace dingofs