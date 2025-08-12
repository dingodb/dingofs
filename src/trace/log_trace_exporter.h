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

#ifndef DINGOFS_SRC_TRACE_LOG_TRACE_EXPORTER_H_
#define DINGOFS_SRC_TRACE_LOG_TRACE_EXPORTER_H_

#include <memory>

#include "trace/trace_exporter.h"
#include "trace/trace_span.h"

namespace dingofs {

class LogTraceExporter : public TraceExporter {
 public:
  explicit LogTraceExporter(const std::string& name,
                            const std::string& log_dir);

  ~LogTraceExporter() override;

  void Export(const TraceSpan& span) override;

 private:
  std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_TRACE_LOG_TRACE_EXPORTER_H_