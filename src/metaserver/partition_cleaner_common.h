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

#ifndef METASERVER_PARTITION_CLEANER_COMMON_H_
#define METASERVER_PARTITION_CLEANER_COMMON_H_

#include <memory>

#include "dataaccess/accesser_common.h"
#include "dataaccess/block_accesser_factory.h"
#include "metaserver/s3/metaserver_s3_adaptor.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace metaserver {

struct PartitionCleanOption {
  uint32_t scanPeriodSec;
  uint32_t inodeDeletePeriodMs;
  dataaccess::BlockAccessOptions block_access_options;
  std::shared_ptr<dataaccess::BlockAccesserFactory> block_accesser_factory;
  S3ClientAdaptorOption s3_client_adaptor_option;
  std::shared_ptr<stub::rpcclient::MdsClient> mdsClient;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // METASERVER_PARTITION_CLEANER_COMMON_H_
