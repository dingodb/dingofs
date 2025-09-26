/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * File Created: Tuesday, 19th March 2019 5:31:24 pm
 * Author: tongguangxun
 */

#ifndef SRC_COMMON_DINGO_DEFINE_H_
#define SRC_COMMON_DINGO_DEFINE_H_

#include <butil/endpoint.h>
#include <butil/status.h>
#include <unistd.h>

#ifndef DLOG_EVERY_SECOND
#define DLOG_EVERY_SECOND(severity) \
  BAIDU_LOG_IF_EVERY_SECOND_IMPL(DLOG_IF, severity, true)
#endif

namespace dingofs {
namespace utils {

const uint32_t kKB = 1024;
const uint32_t kMB = 1024 * kKB;
const uint32_t kGB = 1024 * kMB;

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_COMMON_DINGO_DEFINE_H_
