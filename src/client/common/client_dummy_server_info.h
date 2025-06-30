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

#ifndef DINGOFS_CLIENT_COMMON_CLIENT_DUMMY_SERVER_INFO_H_
#define DINGOFS_CLIENT_COMMON_CLIENT_DUMMY_SERVER_INFO_H_

#include <string>

namespace dingofs {
namespace client {

class ClientDummyServerInfo {
 public:
  static ClientDummyServerInfo& GetInstance() {
    static ClientDummyServerInfo clientInfo;
    return clientInfo;
  }

  void SetIP(const std::string& ip) { local_ip_ = ip; }

  std::string GetIP() const { return local_ip_; }

  void SetPort(uint32_t port) { local_port_ = port; }

  uint32_t GetPort() const { return local_port_; }

  void SetRegister(bool registerFlag) { register_ = registerFlag; }

  bool GetRegister() const { return register_; }

 private:
  ClientDummyServerInfo() = default;

 private:
  std::string local_ip_;
  uint32_t local_port_{0};
  bool register_ = false;
};

}  // namespace client
}  // namespace dingofs
#endif  // DINGOFS_CLIENT_COMMON_CLIENT_DUMMY_SERVER_INFO _H_