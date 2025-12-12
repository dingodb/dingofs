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

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>

#include <string>

#include "common/options/blockaccess.h"
#include "common/options/client.h"
#include "fmt/format.h"
#include "glog/logging.h"

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
  std::string log_prefix;

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

inline void InitAwsSdkConfig(AwsSdkConfig* aws_sdk_config) {
  aws_sdk_config->region = FLAGS_s3_region;
  aws_sdk_config->loglevel = FLAGS_s3_loglevel;

  aws_sdk_config->log_prefix =
      fmt::format("{}/aws_sdk_{}_", client::FLAGS_client_log_dir, getpid());
  LOG(INFO) << fmt::format("s3_log_prefix: {}", aws_sdk_config->log_prefix);

  aws_sdk_config->verify_ssl = FLAGS_s3_verify_ssl;
  aws_sdk_config->max_connections = FLAGS_s3_max_connections;
  aws_sdk_config->connect_timeout = FLAGS_s3_connect_timeout;
  aws_sdk_config->request_timeout = FLAGS_s3_request_timeout;

  aws_sdk_config->use_crt_client = FLAGS_s3_use_crt_client;
  if (aws_sdk_config->use_crt_client) {
    LOG(INFO) << "s3 use crt client.";
  }
  aws_sdk_config->use_thread_pool = FLAGS_s3_use_thread_pool;
  if (aws_sdk_config->use_thread_pool) {
    LOG(INFO) << "s3 use thread pool.";
  }

  aws_sdk_config->async_thread_num = FLAGS_s3_async_thread_num;
  LOG(INFO) << fmt::format("s3 async thread num in thread pool: {}.",
                           aws_sdk_config->async_thread_num);

  aws_sdk_config->use_virtual_addressing = FLAGS_s3_use_virtual_address;
  LOG(INFO) << fmt::format("s3 use virtual addressing: {}.",
                           aws_sdk_config->use_virtual_addressing);

  aws_sdk_config->enable_telemetry = FLAGS_s3_enable_telemetry;
  LOG(INFO) << fmt::format("s3 enable telemetry: {}.",
                           aws_sdk_config->enable_telemetry);
}

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_BLOCK_ACCESS_S3_COMMON_H_