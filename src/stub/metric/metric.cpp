
/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: Fri Apr 21 2023
 * Author: Xinlong-Chen
 */

#include "stub/metric/metric.h"

namespace dingofs {
namespace stub {
namespace metric {

const std::string MDSClientMetric::prefix = "dingofs_mds_client";  // NOLINT
const std::string MetaServerClientMetric::prefix =
    "dingofs_metaserver_client";                            // NOLINT
const std::string ClientOpMetric::prefix = "dingofs_fuse";  // NOLINT
const std::string S3MultiManagerMetric::prefix =
    "dingofs_client_manager";                                         // NOLINT
const std::string FSMetric::prefix = "dingofs_filesystem";            // NOLINT
const std::string S3Metric::prefix = "dingofs_s3";                    // NOLINT
const std::string DiskCacheMetric::prefix = "dingofs_diskcache";      // NOLINT
const std::string KVClientMetric::prefix = "dingofs_kvclient";        // NOLINT
const std::string S3ChunkInfoMetric::prefix = "inode_s3_chunk_info";  // NOLINT
const std::string WarmupManagerS3Metric::prefix = "dingofs_warmup";   // NOLINT

}  // namespace metric
}  // namespace stub
}  // namespace dingofs
