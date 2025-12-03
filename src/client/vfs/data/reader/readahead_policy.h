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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_READAHEAD_POLICY_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_READAHEAD_POLICY_H_

#include <cstdint>
#include <string>

#include "client/vfs/data/common/common.h"

namespace dingofs {
namespace client {
namespace vfs {

// protected by file reader
struct ReadaheadPoclicy {
  const int64_t uuid;
  int8_t level{0};
  int32_t seqdata{0};
  int64_t last_offset{0};

  explicit ReadaheadPoclicy(int64_t p_uuid) : uuid(p_uuid) {}

  int64_t ReadaheadSize() const;
  void UpdateOnRead(const FileRange& frange, int64_t rbuffer_used,
                    int64_t rbuffer_total);
  std::string UUID() const;
  std::string ToString() const;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_READAHEAD_POLICY_H_
