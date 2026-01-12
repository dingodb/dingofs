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
 * Created Date: Mon Dec 17 2018
 * Author: xuchaojie
 */

#ifndef SRC_COMMON_UUID_H_
#define SRC_COMMON_UUID_H_

#include <string>

namespace dingofs {
namespace utils {

/**
 *  @brief 生成uuid，优先采用的算法
 *  如果存在一个高质量的随机数生成器(/dev/urandom），
 *  UUID将基于其生成的随机数产生。
 *  备用算法：在高质量的随机数生成器不可用的情况下，如果可以获取到MAC地址，
 *  则将利用由随机数生成器产生的随机数、当前时间、MAC地址生成UUID。
 *  @param :
 *  @return 生成的uuid
 */
std::string GenerateUUID();

/**
 *  @brief 生成uuid
 *  使用全局时钟、MAC地址。有MAC地址泄露风险。为了保证唯一性还使用的时间同步机制，
 *  如果，时间同步机制不可用，多台机器上生成的uuid可能会重复。
 *  @param :
 *  @return 生成的uuid
 */
std::string GenerateUUIDTime();

/**
 *  @brief 生成uuid
 *  强制完全使用随机数，优先使用（/dev/urandom），备用（伪随机数生成器）。
 *  在使用伪随机数生成器的情况下，uuid有重复的风险。
 *  @return 生成的uuid
 */
std::string GenerateUUIDRandom();

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_COMMON_UUID_H_
