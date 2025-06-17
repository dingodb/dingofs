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
 * Created Date: 2020-01-06
 * Author: charisu
 */

#ifndef DINGOFS_SRC_UTILS_CHANNEL_POOL_H_
#define DINGOFS_SRC_UTILS_CHANNEL_POOL_H_

#include <brpc/channel.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "utils/concurrent/concurrent.h"

using ChannelPtr = std::shared_ptr<brpc::Channel>;

namespace dingofs {
namespace utils {

class ChannelPool {
 public:
  /**
   * @brief 从channelMap获取或创建并Init到指定地址的channel
   *
   * @param addr 对端的地址
   * @param[out] channelPtr 到指定地址的channel
   *
   * @return 成功返回0，失败返回-1
   */
  int GetOrInitChannel(const std::string& addr, ChannelPtr* channelPtr);

  /**
   * @brief 清空map
   */
  void Clear();

 private:
  Mutex mutex_;
  std::unordered_map<std::string, ChannelPtr> channelMap_;
};

}  // namespace utils
}  // namespace dingofs

#endif  // DINGOFS_SRC_UTILS_CHANNEL_POOL_H_
