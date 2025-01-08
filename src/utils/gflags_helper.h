/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Friday Oct 22 17:20:07 CST 2021
 * Author: wuhanqing
 */

#ifndef SRC_COMMON_GFLAGS_HELPER_H_
#define SRC_COMMON_GFLAGS_HELPER_H_

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>
#include <string>

#include "utils/configuration.h"

namespace dingofs {
namespace utils {

struct GflagsLoadValueFromConfIfCmdNotSet {
  template <typename T>
  bool Load(const std::shared_ptr<Configuration>& conf,
            const std::string& cmd_name, const std::string& conf_name, T* value,
            bool fatal_if_missing = true) {
    return Load(conf.get(), cmd_name, conf_name, value, fatal_if_missing);
  }

  template <typename ValueT>
  bool Load(Configuration* conf, const std::string& cmd_name,
            const std::string& conf_name, ValueT* value,
            bool fatal_if_missing = true) {
    using ::google::CommandLineFlagInfo;
    using ::google::GetCommandLineFlagInfo;

    CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo(cmd_name.c_str(), &info) && info.is_default) {
      bool succ = conf->GetValue(conf_name, value);
      if (!succ) {
        if (fatal_if_missing) {
          CHECK(false) << "Failed to get `" << conf_name
                       << "` from file: " << conf->GetConfigPath();
        } else {
          LOG(WARNING) << "Failed to get `" << conf_name
                       << "` from file: " << conf->GetConfigPath()
                       << ", current value: " << *value;
          return false;
        }
      }
    }

    return true;
  }
};

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_COMMON_GFLAGS_HELPER_H_
