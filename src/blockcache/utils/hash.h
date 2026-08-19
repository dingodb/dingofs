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

#ifndef DINGOFS_BLOCKCACHE_UTILS_HASH_H_
#define DINGOFS_BLOCKCACHE_UTILS_HASH_H_

#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

namespace dingofs {
namespace blockcache {

inline constexpr uint64_t kGolden64 = 0x9e3779b97f4a7c15ULL;

inline uint64_t Mix64(uint64_t x) {
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

inline uint64_t Fnv1a(std::string_view s) {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (const char c : s) {
    h = (h ^ static_cast<unsigned char>(c)) * 0x100000001b3ULL;
  }
  return h;
}

class ConsistentHash {
 public:
  void Add(uint32_t member, std::string_view id, uint32_t weight);
  void Finalize();
  uint32_t MemberOf(uint64_t key) const;

  size_t member_count() const { return member_count_; }
  bool empty() const { return ring_.empty(); }

 private:
  static constexpr uint32_t kPointsPerWeight = 160;

  size_t member_count_ = 0;
  bool final_ = false;
  std::vector<std::pair<uint32_t, uint32_t>> ring_;  // point -> member
};

std::vector<uint32_t> NormalizeGcd(const std::vector<uint64_t>& values);

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_HASH_H_
