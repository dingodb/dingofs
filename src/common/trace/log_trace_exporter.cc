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

#include "common/trace/log_trace_exporter.h"

#include <absl/strings/str_format.h>
#include <brpc/reloadable_flags.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>

#include <sstream>

namespace dingofs {

LogTraceExporter::LogTraceExporter(const std::string& name,
                                   const std::string& log_dir) {
  std::string filename =
      absl::StrFormat("%s/%s_trace_%d.log", log_dir, name, getpid());
  logger_ = spdlog::daily_logger_mt(name, filename, 0, 0);
  logger_->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%l] %v");
  logger_->set_level(spdlog::level::trace);
  spdlog::flush_every(std::chrono::seconds(1));
}

LogTraceExporter::~LogTraceExporter() { logger_->flush(); }

void LogTraceExporter::Export(const ITraceSpan& span) {
  std::ostringstream oss;
  bool first = true;
  for (const auto& [key, value] : span.GetAttributes()) {
    if (!first) {
      oss << ", ";
    }
    oss << key << ":" << value;
    first = false;
  }

  auto ctx = span.GetContext();
  auto message =
      absl::StrFormat("[%s] [%.6lf] [%s] (%s) %s [%s.%s] (%s)", ctx->trace_id,
                      span.UElapsed() / 1e6, ctx->module, span.GetName(),
                      span.GetStatus().ToString(), ctx->parent_span_id,
                      ctx->span_id, oss.str());

  logger_->trace(message);
}

}  // namespace dingofs