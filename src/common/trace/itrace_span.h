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

#ifndef DINGOFS_COMMON_TRACE_ITRACE_SPAN_H_
#define DINGOFS_COMMON_TRACE_ITRACE_SPAN_H_

#include <map>
#include <memory>
#include <string>

#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {

using AttributeMap = std::map<std::string, std::string>;

class ITraceSpan {
 public:
  virtual ~ITraceSpan() = default;

  virtual const std::string& GetName() const = 0;

  virtual std::shared_ptr<Context> GetContext() const = 0;

  virtual void AddAttribute(const std::string& key,
                            const std::string& value) = 0;

  virtual const AttributeMap& GetAttributes() const = 0;

  virtual void AddEvent(const std::string& name) = 0;

  virtual void SetStatus(const Status& status) = 0;

  virtual const Status& GetStatus() const = 0;

  virtual void End() = 0;

  virtual int64_t UElapsed() const = 0;
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_TRACE_ITRACE_SPAN_H_