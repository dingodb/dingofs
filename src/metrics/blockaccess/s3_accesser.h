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

#ifndef DINGOFS_SRC_METRICS_BLOCKACCESS_S3_ACCESSER_H_
#define DINGOFS_SRC_METRICS_BLOCKACCESS_S3_ACCESSER_H_

#include <bvar/bvar.h>

#include <string>

#include "metrics/metric.h"

namespace dingofs {
namespace metrics {
namespace blockaccess {

struct S3Metric {
  inline static const std::string prefix = "dingofs_s3";

  InterfaceMetric write_s3;
  InterfaceMetric read_s3;

  explicit S3Metric()
      : write_s3(prefix, "_write_s3"), read_s3(prefix, "_read_s3") {}

 public:
  S3Metric(const S3Metric&) = delete;

  S3Metric& operator=(const S3Metric&) = delete;

 public:
  static S3Metric& GetInstance() {
    static S3Metric instance;
    return instance;
  }
};

}  // namespace blockaccess
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_BLOCKACCESS_S3_ACCESSER_H_
