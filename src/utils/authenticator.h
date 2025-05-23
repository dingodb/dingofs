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
 * File Created: Monday, 1st April 2019 5:15:20 pm
 * Author: tongguangxun
 */
#ifndef SRC_COMMON_AUTHENTICATOR_H_
#define SRC_COMMON_AUTHENTICATOR_H_

#include <cstdint>
#include <string>

namespace dingofs {
namespace utils {

class Authenticator {
 public:
  /**
   * bref: 获取要进行签名的字符串
   * @param: date, 当前的时间
   * @param: owner, 文件所有者
   * @return: 返回需要进行加密的字符串
   */
  static std::string GetString2Signature(uint64_t date,
                                         const std::string& owner);

  /**
   * bref: 为字符串计算签名
   * @param: String2Signature, 需要进行签名计算的字符串
   * @param: secretKey, 为计算的秘钥
   * @return: 返回需要进行签名过后的字符串
   */
  static std::string CalcString2Signature(const std::string& String2Signature,
                                          const std::string& secretKey);

 private:
  static int HMacSha256(const void* key, int key_size, const void* data,
                        int data_size, void* digest);

  static std::string Base64(const unsigned char* src, size_t sz);
};
}  // namespace utils
}  // namespace dingofs

#endif  // SRC_COMMON_AUTHENTICATOR_H_
