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
 * Created Date: 2021-09-14
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_TOOLS_DINGOFS_TOOL_FACTORY_H_
#define DINGOFS_SRC_TOOLS_DINGOFS_TOOL_FACTORY_H_

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "tools/dingofs_tool.h"

namespace dingofs {
namespace tools {

class DingofsToolFactory {
 public:
  DingofsToolFactory();
  virtual ~DingofsToolFactory() {}

  std::shared_ptr<DingofsTool> GenerateDingofsTool(const std::string& command);

  /**
   * @brief add commands and function to generate objects
   *
   * @param command
   * @param function
   * @details
   * The same command will only take effect for the first one registered
   */
  virtual void RegisterDingofsTool(
      const std::string& command,
      const std::function<std::shared_ptr<DingofsTool>()>& function);

 private:
  // storage commands and function to generate objects
  std::unordered_map<std::string, std::function<std::shared_ptr<DingofsTool>()>>
      command2creator_;
};

}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_DINGOFS_TOOL_FACTORY_H_
