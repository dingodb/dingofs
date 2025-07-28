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
 * Created Date: 2025-06-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_CONTEXT_H_
#define DINGOFS_SRC_CACHE_UTILS_CONTEXT_H_

#include <absl/strings/str_format.h>
#include <butil/fast_rand.h>
#include <butil/time.h>
#include <glog/logging.h>

#include <string>

#include "cache/utils/logging.h"
#include "cache/utils/step_timer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class Context {
 public:
  Context() : trace_id_(NewTraceId()) {}
  Context(const std::string& trace_id) : trace_id_(trace_id) {}

  std::string TraceId() const { return trace_id_; }

  std::string ToString() const { return absl::StrFormat("[%s]", trace_id_); }

  void SetCacheHit(bool cache_hit) { cache_hit_ = cache_hit; }
  bool GetCacheHit() const { return cache_hit_; }

 private:
  std::string NewTraceId() {
    return absl::StrFormat("%lld", butil::cpuwide_time_ns());
  }

  const std::string trace_id_;
  bool cache_hit_{false};
};

using ContextSPtr = std::shared_ptr<Context>;

inline ContextSPtr NewContext() { return std::make_shared<Context>(); }

inline ContextSPtr NewContext(const std::string& trace_id) {
  return std::make_shared<Context>(trace_id);
}

struct TraceLogGuard {
  template <typename... Args>
  TraceLogGuard(ContextSPtr ctx, Status& status, StepTimer& timer,
                const std::string module_name, const char* func_format,
                const Args&... func_params)
      : ctx(ctx),
        status(status),
        timer(timer),
        module_name(module_name),
        func(absl::StrFormat(func_format, func_params...)) {}

  // [1920391111] <0.003361> service::put(...): OK (...)
  ~TraceLogGuard() {
    auto message = absl::StrFormat(
        "[%s] <%.6lf> %s::%s: %s (%s)", ctx->TraceId(), timer.UElapsed() / 1e6,
        module_name, func, status.ToString(), timer.ToString());
    LogTrace(message);
  }

  ContextSPtr ctx;
  Status& status;
  StepTimer& timer;
  std::string module_name;
  std::string func;
};

#define NEXT_STEP(step_name) timer.NextStep(step_name);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_CONTEXT_H_
