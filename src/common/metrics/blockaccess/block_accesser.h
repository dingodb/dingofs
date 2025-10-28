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

#include "common/metrics/metric.h"

namespace dingofs {
namespace metrics {
namespace blockaccess {

struct BlockMetric {
  inline static const std::string prefix = "dingofs_block";

  InterfaceMetric write_block;
  InterfaceMetric read_block;

  explicit BlockMetric()
      : write_block(prefix, "_write_block"),
        read_block(prefix, "_read_block") {}

 public:
  BlockMetric(const BlockMetric&) = delete;

  BlockMetric& operator=(const BlockMetric&) = delete;

 public:
  static BlockMetric& GetInstance() {
    static BlockMetric instance;
    return instance;
  }
};

}  // namespace blockaccess
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_BLOCKACCESS_S3_ACCESSER_H_
