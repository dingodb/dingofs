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

#include "mds/cachegroup/codec.h"

#include <glog/logging.h>

#include <string>

#include "mds/common/storage_key.h"
#include "utils/encode.h"
#include "utils/string.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

std::string Codec::EncodeGroupName(const std::string& group_name) {
  std::string key = CACHE_GROUP_GROUP_NAME_KEY_PREFIX;
  key.append(group_name);
  return key;
}

std::string Codec::DecodeGroupName(const std::string& key) {
  size_t prefix_len = CACHE_GROUP_PREFIX_LENGTH;
  return key.substr(prefix_len);
}

std::string Codec::EncodeGroupBirthTime(uint64_t birth_time) {
  return std::to_string(birth_time);
}

uint64_t Codec::DecodeGroupBirthTime(const std::string& value) {
  uint64_t birth_time = 0;
  CHECK(utils::Str2Int(value, &birth_time));
  return birth_time;
}

std::string Codec::EncodeMemberId(uint64_t member_id) {
  std::string key = CACHE_GROUP_MEMBER_ID_KEY_PREFIX;
  size_t prefix_len = CACHE_GROUP_PREFIX_LENGTH;
  key.resize(prefix_len + sizeof(uint64_t));
  utils::EncodeBigEndian(&(key[prefix_len]), member_id);
  return key;
}

uint64_t Codec::DecodeMemberId(const std::string& key) {
  size_t prefix_len = CACHE_GROUP_PREFIX_LENGTH;
  return utils::DecodeBigEndian(&(key[prefix_len]));
}

std::string Codec::EncodeMember(const PBCacheGroupMember& member) {
  std::string value;
  CHECK(member.SerializeToString(&value));
  return value;
}

PBCacheGroupMember Codec::DecodeMember(const std::string& value) {
  PBCacheGroupMember member;
  CHECK(member.ParseFromString(value));
  return member;
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
