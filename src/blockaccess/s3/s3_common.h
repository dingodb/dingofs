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

#include "fmt/format.h"
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
  std::string log_prefix{"/tmp/aws_sdk_"};

  bool verify_ssl{false};
  int max_connections{32};
  int connect_timeout{60000};
  int request_timeout{10000};

  bool use_crt_client{true};

  bool use_thread_pool{true};  // this only work when use_crt_client is false
  int async_thread_num{16};    // this only work when use_crt_client is false

  bool use_virtual_addressing{false};
  bool enable_telemetry{false};
};

struct S3Options {
  S3Info s3_info;
  AwsSdkConfig aws_sdk_config;
};

inline void InitAwsSdkConfig(utils::Configuration* conf,
                             AwsSdkConfig* aws_sdk_config) {
  LOG_IF(FATAL, !conf->GetIntValue("s3.logLevel", &aws_sdk_config->loglevel));
  if (!conf->GetStringValue("client.common.logDir",
                            &aws_sdk_config->log_prefix)) {
    LOG(INFO) << fmt::format(
        "Not found client.common.logDir in conf, use default {}",
        aws_sdk_config->log_prefix);
  } else {
    aws_sdk_config->log_prefix =
        fmt::format("{}/aws_sdk_{}_", aws_sdk_config->log_prefix, getpid());
  }

  LOG_IF(FATAL,
         !conf->GetBoolValue("s3.verify_SSL", &aws_sdk_config->verify_ssl));
  LOG_IF(FATAL, !conf->GetIntValue("s3.maxConnections",
                                   &aws_sdk_config->max_connections));
  LOG_IF(FATAL, !conf->GetIntValue("s3.connectTimeout",
                                   &aws_sdk_config->connect_timeout));
  LOG_IF(FATAL, !conf->GetIntValue("s3.requestTimeout",
                                   &aws_sdk_config->request_timeout));

  if (!conf->GetBoolValue("s3.use_crt_client",
                          &aws_sdk_config->use_crt_client)) {
    aws_sdk_config->use_crt_client = true;
    LOG(INFO) << fmt::format(
        "Not found s3.use_crt_client in conf, use default {}",
        aws_sdk_config->use_crt_client);
  }

  if (!conf->GetBoolValue("s3.use_thread_pool",
                          &aws_sdk_config->use_thread_pool)) {
    aws_sdk_config->use_thread_pool = true;
    LOG(INFO) << fmt::format(
        "Not found s3.use_thread_pool in conf, use default {}",
        aws_sdk_config->use_thread_pool);
  }

  if (!conf->GetIntValue("s3.async_thread_num_in_thread_pool",
                         &aws_sdk_config->async_thread_num)) {
    aws_sdk_config->async_thread_num = 16;
    LOG(INFO) << fmt::format(
        "Not found s3.async_thread_num_in_thread_pool in conf, use default {}",
        aws_sdk_config->async_thread_num);
  }

  LOG_IF(FATAL, !conf->GetBoolValue("s3.useVirtualAddressing",
                                    &aws_sdk_config->use_virtual_addressing));
  LOG_IF(FATAL, !conf->GetStringValue("s3.region", &aws_sdk_config->region));

  if (!conf->GetBoolValue("s3.enableTelemetry",
                          &aws_sdk_config->enable_telemetry)) {
    aws_sdk_config->enable_telemetry = false;
    LOG(WARNING) << "Not found s3.enableTelemetry in conf, default false.";
  }
}

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_BLOCK_ACCESS_S3_COMMON_H_