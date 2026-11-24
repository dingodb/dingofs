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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dingofs {
namespace blockcache {

inline uint64_t HashInt(uint64_t x) {
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

inline uint64_t HashStr(std::string_view s) {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (const char c : s) {
    h = (h ^ static_cast<unsigned char>(c)) * 0x100000001b3ULL;
  }
  return HashInt(h);
}

class ConsistentHash {
 public:
  ConsistentHash() = default;

  ConsistentHash(const ConsistentHash&) = delete;
  ConsistentHash& operator=(const ConsistentHash&) = delete;

  void Add(std::string_view id, uint64_t weight);
  void Finalize();
  std::string_view GetMember(uint64_t key) const;

 private:
  friend class ConsistentHashPeer;

  static constexpr uint64_t kLightestPoints = 1280;  // of the lightest member
  static constexpr uint64_t kMaxPoints = uint64_t{1} << 20;  // of the ring

  std::vector<uint64_t> CalcPoints(const std::vector<uint64_t>& weights);

  bool final_ = false;
  std::vector<std::string> ids_;
  std::vector<uint64_t> weights_;
  std::vector<std::pair<uint32_t, uint32_t>> ring_;  // point -> index in ids_
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_HASH_H_
