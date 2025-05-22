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

#ifndef DINGOFS_BLOCK_ACCESS_S3_COMMON_H_
#define DINGOFS_BLOCK_ACCESS_S3_COMMON_H_

#include <string>

#include "utils/configuration.h"

namespace dingofs {
namespace blockaccess {

struct S3Info {
  // should get from mds
  std::string ak;
  std::string sk;
  std::string endpoint;
  std::string bucket_name;
};

struct AwsSdkConfig {
  std::string region{"us-east-1"};
  int loglevel{4};
  std::string logPrefix;
  bool verifySsl{false};
  int maxConnections{32};
  int connectTimeout{60000};
  int requestTimeout{10000};
  bool use_crt_client{false};
  bool use_thread_pool{true};  // this only work when use_crt_client is false
  int asyncThreadNum{16};      // this only work when use_crt_client is false
  bool useVirtualAddressing{false};
  bool enableTelemetry{false};
};

struct S3Options {
  S3Info s3_info;
  AwsSdkConfig aws_sdk_config;
};

inline void InitAwsSdkConfig(utils::Configuration* conf,
                             AwsSdkConfig* aws_sdk_config) {
  LOG_IF(FATAL, !conf->GetIntValue("s3.logLevel", &aws_sdk_config->loglevel));
  LOG_IF(FATAL,
         !conf->GetStringValue("s3.logPrefix", &aws_sdk_config->logPrefix));
  LOG_IF(FATAL,
         !conf->GetBoolValue("s3.verify_SSL", &aws_sdk_config->verifySsl));
  LOG_IF(FATAL, !conf->GetIntValue("s3.maxConnections",
                                   &aws_sdk_config->maxConnections));
  LOG_IF(FATAL, !conf->GetIntValue("s3.connectTimeout",
                                   &aws_sdk_config->connectTimeout));
  LOG_IF(FATAL, !conf->GetIntValue("s3.requestTimeout",
                                   &aws_sdk_config->requestTimeout));

  if (!conf->GetBoolValue("s3.use_crt_client",
                          &aws_sdk_config->use_crt_client)) {
    aws_sdk_config->use_crt_client = false;
    LOG(INFO) << "Not found s3.use_crt_client in conf, use default "
              << (aws_sdk_config->use_crt_client ? "true" : "false");
  }

  if (!conf->GetBoolValue("s3.use_thread_pool",
                          &aws_sdk_config->use_thread_pool)) {
    LOG(INFO) << "Not found s3.use_thread_pool in conf, use default "
              << (aws_sdk_config->use_thread_pool ? "true" : "false");
  }

  if (!conf->GetIntValue("s3.async_thread_num_in_thread_pool",
                         &aws_sdk_config->asyncThreadNum)) {
    LOG(INFO)
        << "Not found s3.async_thread_num_in_thread_pool in conf, use default"
        << aws_sdk_config->asyncThreadNum;
  }

  LOG_IF(FATAL, !conf->GetBoolValue("s3.useVirtualAddressing",
                                    &aws_sdk_config->useVirtualAddressing));
  LOG_IF(FATAL, !conf->GetStringValue("s3.region", &aws_sdk_config->region));
  if (!conf->GetBoolValue("s3.enableTelemetry",
                          &aws_sdk_config->enableTelemetry)) {
    LOG(WARNING) << "Not found s3.enableTelemetry in conf,default to false";
    aws_sdk_config->enableTelemetry = false;
  }
}

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_BLOCK_ACCESS_S3_COMMON_H_