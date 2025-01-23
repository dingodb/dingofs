/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Project: Dingofs
 * Created Date: 2024-10-31
 * Author: Jingli Chen (Wine93)
 */

#include <array>
#include <memory>
#include <string>

#ifndef DINGOFS_TEST_METASERVER_SUPERPARTITION_BUILDER_SHELL_H_
#define DINGOFS_TEST_METASERVER_SUPERPARTITION_BUILDER_SHELL_H_

namespace dingofs {
namespace metaserver {
namespace superpartition {

using PcloseDeleter = int (*)(FILE*);

static bool RunShell(const std::string& cmd, std::string* ret) {
  std::array<char, 128> buffer;
  std::unique_ptr<FILE, PcloseDeleter> pipe(popen(cmd.c_str(), "r"), pclose);
  if (!pipe) {
    return false;
  } else if (ret == nullptr) {
    return true;
  }

  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    *ret += buffer.data();
  }
  return true;
}

}  // namespace superpartition
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_SUPERPARTITION_BUILDER_SHELL_H_
