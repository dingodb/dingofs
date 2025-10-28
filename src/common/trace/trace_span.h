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

#ifndef DINGOFS_COMMON_TRACE_TRACE_SPAN_H_
#define DINGOFS_COMMON_TRACE_TRACE_SPAN_H_

#include <atomic>
#include <memory>
#include <string>

#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/itrace_span.h"

namespace dingofs {

class Tracer;

class TraceSpan : public ITraceSpan {
 public:
  ~TraceSpan() override;

  TraceSpan(const TraceSpan&) = delete;
  TraceSpan& operator=(const TraceSpan&) = delete;

  const std::string& GetName() const override;

  std::shared_ptr<Context> GetContext() const override;

  void AddAttribute(const std::string& key, const std::string& value) override;

  const AttributeMap& GetAttributes() const override;

  // TODO :  to be implemented
  void AddEvent(const std::string& name) override {}

  void SetStatus(const Status& status) override;

  const Status& GetStatus() const override;

  void End() override;

  int64_t UElapsed() const override;

 private:
  friend class Tracer;

  TraceSpan(Tracer* tracer, std::string name, ContextSPtr context);

  Tracer* tracer_;
  std::string name_;
  butil::Timer timer_;
  ContextSPtr context_;
  Status status_;
  AttributeMap attributes_;
  std::atomic_bool ended_{false};
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_TRACE_TRACE_SPAN_H_