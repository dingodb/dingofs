/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include <glog/logging.h>

#include <iostream>

#include "cache/v2/tools/benchmark/benchmarker.h"
#include "cache/v2/tools/benchmark/cli.h"
#include "common/logging.h"

using dingofs::Logger;
using dingofs::cache::v2::Benchmarker;
using dingofs::cache::v2::FlagParser;
using dingofs::cache::v2::kUsage;

int main(int argc, char** argv) {
  if (!FlagParser::Parse(&argc, &argv, kUsage)) {
    return 0;
  }

  Logger::Init("cb");
  LOG(INFO) << FlagParser::GenCurrentFlags(FlagParser::Collect(kUsage));

  Benchmarker benchmarker;
  auto status = benchmarker.Start();
  if (!status.ok()) {
    std::cerr << "Failed to start benchmarker: " << status.ToString() << '\n';
    return -1;
  }

  benchmarker.RunUntilFinish();
  return 0;
}
