// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDSV2_SERVER_H_
#define DINGOFS_MDSV2_SERVER_H_

#include <memory>
#include <string>

#include "curvefs/src/mdsv2/common/crontab.h"
#include "curvefs/src/utils/configuration.h"

namespace dingofs {

namespace mdsv2 {

using ::curvefs::utils::Configuration;

class Server {
 public:
  static Server& GetInstance();

  bool InitConfig(const std::string& path);

  bool InitLog();

  bool InitCrontab();

  std::string GetListenAddr();

  void Run();

  void Stop();

 private:
  Configuration conf_;

  // This is manage crontab, like heartbeat.
  CrontabManager crontab_manager_;
  // Crontab config
  std::vector<CrontabConfig> crontab_configs_;
};

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVER_H_