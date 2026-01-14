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

#include "common/trace/trace_manager.h"

#include <memory>

#include "common/opentrace/tracer.h"
#include "gflags/gflags.h"
#include "mds/common/version.h"

namespace dingofs {

DEFINE_string(trace_service_name, "dingofs", "service name");

DEFINE_string(otlp_export_endpoint, "172.30.14.125:4317", "otlp export addrs");

DEFINE_uint32(trace_export_thread_num, 2, "otlp export thread num");

DEFINE_bool(enable_trace, false, "Whether to enable trace");

bool TraceManager::Init() { return tracer_.Init(); }

void TraceManager::Stop() { tracer_.Stop(); };

TraceManager::TraceManager()
    : tracer_(OpenTeleMetryTracer(
          FLAGS_trace_service_name, FLAGS_otlp_export_endpoint,
          FLAGS_trace_export_thread_num, dingofs::mds::GetGitCommitHash(),
          dingofs::mds::GetGitVersion())) {}

}  // namespace dingofs