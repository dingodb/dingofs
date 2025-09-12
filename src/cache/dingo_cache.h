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

/*
 * Project: DingoFS
 * Created Date: 2025-06-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_DINGO_CACHE_H_
#define DINGOFS_SRC_CACHE_DINGO_CACHE_H_

#include "cache/common/flag.h"

namespace dingofs {
namespace cache {

class DingoCache {
 public:
  static int Run(int argc, char** argv);

 private:
  static int ParseFlags(int argc, char** argv);

  static void InitGlog();
  static void LogFlags();
  static void InitThreadPool();
  static void GlobalInitOrDie();

  static int StartServer();

  static FlagsInfo flags;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_DINGO_CACHE_H_
