/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-06-06
 * Author: Jingli Chen (Wine93)
 */

#include "cache/utils/bthread.h"

namespace dingofs {
namespace cache {

struct FuncArg {
  FuncArg(std::function<void()> func) : func(func) {}

  std::function<void()> func;
};

void RunInBthread(std::function<void()> func) {
  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  auto* arg = new FuncArg(func);
  int rc = bthread_start_background(  // It costs about 100~200 ns
      &tid, &attr,
      [](void* arg) -> void* {
        FuncArg* func_arg = reinterpret_cast<FuncArg*>(arg);
        func_arg->func();

        delete func_arg;
        return nullptr;
      },
      (void*)arg);

  if (rc != 0) {
    LOG(ERROR) << "Start bthread for block cache async task failed: rc = "
               << rc;
    func();
  }
}

}  // namespace cache
}  // namespace dingofs
