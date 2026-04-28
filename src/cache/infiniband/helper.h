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

/*
 * Project: DingoFS
 * Created Date: 2026-05-28
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_HELPER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_HELPER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>

#include "cache/infiniband/infiniband.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

Status RegisterMemoryForRdma(const std::string& device_name, void* addr,
                             size_t size);

Status DeregisterMemoryForRdma(const std::string& device_name, void* addr);

// TODO:
// void FindMemoryRegion(const std::string& device_name, void* addr);

class Helper {
 public:
  static void SerializeToPb(const ConnManagmentMeta& cm_meta,
                            pb::infiniband::ConnManagementMeta* pb_cm_meta);

  static Status ParseFromPb(
      const pb::infiniband::ConnManagementMeta& pb_cm_meta,
      ConnManagmentMeta* cm_meta);
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_HELPER_H_
