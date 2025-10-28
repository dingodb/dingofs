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

#ifndef DINGOFS_COMMON_TRACE_NOOP_TRACER_H_
#define DINGOFS_COMMON_TRACE_NOOP_TRACER_H_

#include <memory>

#include "common/trace/context.h"
#include "common/trace/itracer.h"
#include "common/trace/utils.h"

namespace dingofs {

class NoopTraceSpan : public ITraceSpan {
 public:
  NoopTraceSpan(const std::string& name, ContextSPtr ctx)
      : name_(name),
        context_(std::move(ctx)),
        start_time_(butil::gettimeofday_us()) {}

  const std::string& GetName() const override { return name_; }

  std::shared_ptr<Context> GetContext() const override { return context_; }

  void AddAttribute(const std::string& key, const std::string& value) override {

  }

  const AttributeMap& GetAttributes() const override { return attributes_; }

  void AddEvent(const std::string& name) override {}

  void SetStatus(const Status& status) override {}

  const Status& GetStatus() const override { return status_; }

  void End() override {}

  int64_t UElapsed() const override { return 0; }

 private:
  std::string name_;
  ContextSPtr context_;
  Status status_;
  AttributeMap attributes_;
  int64_t start_time_;
};

class NoopTracer : public ITracer {
 public:
  NoopTracer() = default;
  ~NoopTracer() override = default;

  std::unique_ptr<ITraceSpan> StartSpan(const std::string& module,
                                        const std::string& name) override {
    auto context =
        std::make_shared<Context>(module, GenerateTraceId(), GenerateSpanId());
    return std::make_unique<NoopTraceSpan>("NO_OP", context);
  }

  std::unique_ptr<ITraceSpan> StartSpanWithParent(
      const std::string& module, const std::string& name,
      const ITraceSpan& parent) override {
    const auto& parent_ctx = parent.GetContext();
    auto span_id = GenerateSpanId();
    auto context = std::make_shared<Context>(module, parent_ctx->trace_id,
                                             span_id, parent_ctx->span_id);
    return std::make_unique<NoopTraceSpan>("NO_OP", context);
  }

  std::unique_ptr<ITraceSpan> StartSpanWithContext(const std::string& module,
                                                   const std::string& name,
                                                   ContextSPtr ctx) override {
    auto span_id = GenerateSpanId();
    auto context =
        std::make_shared<Context>(module, ctx->trace_id, span_id, ctx->span_id);

    return std::make_unique<NoopTraceSpan>("NO_OP", context);
  }

  void EndSpan(const ITraceSpan& span) {}
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_TRACE_NOOP_TRACER_H_
