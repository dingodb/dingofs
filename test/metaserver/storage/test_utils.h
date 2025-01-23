/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: 2022-04-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_METASERVER_STORAGE_UTILS_H_
#define DINGOFS_TEST_METASERVER_STORAGE_UTILS_H_

#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>

namespace dingofs {
namespace metaserver {
namespace storage {

static inline std::string RandomString() {
  uint64_t maxUint64 = std::numeric_limits<uint64_t>::max();
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> dist(0, maxUint64);

  std::ostringstream oss;
  oss << std::setw(std::to_string(maxUint64).size()) << std::setfill('0')
      << dist(rng);
  return oss.str();
}

static inline std::string RandomStoragePath(std::string basedir = "./storage") {
  return basedir + "_" + RandomString();
}

}  // namespace storage
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_STORAGE_UTILS_H_
