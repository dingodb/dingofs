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

#include "trace/tracer.h"

#include <butil/time.h>

#include <random>

#include "trace/context.h"

namespace dingofs {

constexpr size_t kTraceIdLength = 8;
constexpr size_t kSpanIdLength = 4;

std::unique_ptr<TraceSpan> Tracer::StartSpan(const std::string& module,
                                             const std::string& name) {
  auto trace_id = GenerateId(kTraceIdLength);
  auto span_id = GenerateId(kSpanIdLength);

  auto context = std::make_shared<Context>(module, trace_id, span_id);

  return std::unique_ptr<TraceSpan>(new TraceSpan(this, name, context));
}

std::unique_ptr<TraceSpan> Tracer::StartSpanWithParent(
    const std::string& module, const std::string& name,
    const TraceSpan& parent) {
  const auto& parent_ctx = parent.GetContext();
  auto span_id = GenerateId(kSpanIdLength);
  auto context = std::make_shared<Context>(module, parent_ctx->trace_id,
                                                span_id, parent_ctx->span_id);

  return std::unique_ptr<TraceSpan>(new TraceSpan(this, name, context));
}

std::unique_ptr<TraceSpan> Tracer::StartSpanWithContext(
    const std::string& module, const std::string& name, ContextSPtr ctx) {
  auto span_id = GenerateId(kSpanIdLength);
  auto context = std::make_shared<Context>(module, ctx->trace_id, span_id,
                                                ctx->span_id);

  return std::unique_ptr<TraceSpan>(new TraceSpan(this, name, context));
}

void Tracer::EndSpan(const TraceSpan& span) {
  if (sampler_->ShouldSample()) {
    exporter_->Export(span);
  }
}

void Tracer::SetSampler(std::unique_ptr<TraceSampler> sampler) {
  sampler_ = std::move(sampler);
}

std::string Tracer::GenerateId(size_t length) {
  static thread_local std::random_device rd;
  static thread_local std::mt19937_64 gen(rd());
  static thread_local std::uniform_int_distribution<uint64_t> dis;

  static constexpr char hex_lut[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                     '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  std::string id;
  id.reserve(length * 2);

  const size_t chunks = (length + 7) / 8;
  for (size_t i = 0; i < chunks; ++i) {
    uint64_t num = dis(gen);
    char buf[16];

    for (int j = 15; j >= 0; --j) {
      buf[j] = hex_lut[num & 0xF];
      num >>= 4;
    }

    id.append(buf, 16);
  }

  return id.substr(0, length * 2);
}

}  // namespace dingofs
