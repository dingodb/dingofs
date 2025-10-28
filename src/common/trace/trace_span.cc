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

#include "common/trace/trace_span.h"

#include <butil/time.h>
#include <glog/logging.h>

#include "common/trace/tracer.h"

namespace dingofs {

TraceSpan::TraceSpan(Tracer* tracer, std::string name, ContextSPtr context)
    : tracer_(tracer), name_(std::move(name)), context_(std::move(context)) {
  timer_.start();
}

TraceSpan::~TraceSpan() {
  if (!ended_) {
    End();
  }
}

const std::string& TraceSpan::GetName() const { return name_; }

std::shared_ptr<Context> TraceSpan::GetContext() const { return context_; }

void TraceSpan::AddAttribute(const std::string& key, const std::string& value) {
  attributes_[key] = value;
}

const AttributeMap& TraceSpan::GetAttributes() const { return attributes_; }

void TraceSpan::SetStatus(const Status& status) { status_ = status; }

const Status& TraceSpan::GetStatus() const { return status_; }

void TraceSpan::End() {
  if (ended_.load()) return;

  ended_.store(true);

  timer_.stop();
  tracer_->EndSpan(*this);
}

int64_t TraceSpan::UElapsed() const {
  CHECK(ended_.load()) << "Span must be ended before getting elapsed time.";
  return timer_.u_elapsed();
}

}  // namespace dingofs