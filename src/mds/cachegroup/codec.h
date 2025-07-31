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

/*
 * Project: DingoFS
 * Created Date: 2025-08-03
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_MDS_CACHEGROUP_CODEC_H_
#define DINGOFS_SRC_MDS_CACHEGROUP_CODEC_H_

#include <string>

#include "mds/cachegroup/common.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

class Codec {
 public:
  static std::string EncodeGroupName(const std::string& group_name);
  static std::string DecodeGroupName(const std::string& key);
  static std::string EncodeGroupBirthTime(uint64_t birth_time);
  static uint64_t DecodeGroupBirthTime(const std::string& value);

  static std::string EncodeMemberId(uint64_t member_id);
  static uint64_t DecodeMemberId(const std::string& key);
  static std::string EncodeMember(const PBCacheGroupMember& member);
  static PBCacheGroupMember DecodeMember(const std::string& value);
};

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CACHEGROUP_CODEC_H_
