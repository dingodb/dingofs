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
 * Created Date: 2025-06-11
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_COMMON_LOGGING_H_
#define DINGOFS_SRC_COMMON_LOGGING_H_

#include <string>

#include "common/context.h"
#include "common/status.h"
#include "common/step_timer.h"

namespace dingofs {

bool InitTraceLog(const std::string& log_dir);

void ShutdownTraceLog();

void LogTrace(const std::string& message);

struct TraceLogGuard {
  template <typename... Args>
  TraceLogGuard(ContextSPtr ctx, Status& status, StepTimer& timer,
                const std::string module_name, const char* func_format,
                const Args&... func_params)
      : ctx(std::move(ctx)),
        status(status),
        timer(timer),
        module_name(module_name),
        func(absl::StrFormat(func_format, func_params...)) {}

  // [1920391111] [0.003361] [service] put(...): OK (...)
  ~TraceLogGuard() {
    auto message = absl::StrFormat(
        "[%s] [%.6lf] [%s] %s: %s (%s)", ctx->TraceId(), timer.UElapsed() / 1e6,
        module_name, func, status.ToString(), timer.ToString());
    LogTrace(message);
  }

  ContextSPtr ctx;
  Status& status;
  StepTimer& timer;
  std::string module_name;
  std::string func;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_LOGGING_H_
