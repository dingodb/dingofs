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

#include "utils/uuid.h"

#include <uuid/uuid.h>

#include <cstdint>
#include <random>

namespace dingofs {
namespace utils {

const uint32_t kBufSize = 36;

std::string GenerateUUID() {
  uuid_t out;
  char buf[kBufSize];
  uuid_generate(out);
  uuid_unparse_lower(out, buf);
  std::string str(&buf[0], kBufSize);
  return str;
}

std::string GenerateUUIDFastly() {
  // thread-local PRNG avoids the per-call getrandom()/open("/dev/urandom")
  // syscall inside libuuid's uuid_generate(), which dominates hot paths.
  thread_local std::mt19937_64 rng([] {
    std::random_device rd;
    return (static_cast<uint64_t>(rd()) << 32) ^ rd();
  }());

  uint64_t hi = rng();
  uint64_t lo = rng();
  // RFC 4122 version 4, variant 1
  hi = (hi & 0xffffffffffff0fffULL) | 0x0000000000004000ULL;
  lo = (lo & 0x3fffffffffffffffULL) | 0x8000000000000000ULL;

  // layout: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (hi:16 hex, lo:16 hex)
  static constexpr char kHex[] = "0123456789abcdef";
  char buf[kBufSize];
  char* p = buf;
  for (int i = 0; i < 32; i++) {
    if (i == 8 || i == 12 || i == 16 || i == 20) *p++ = '-';
    uint64_t v = (i < 16) ? hi : lo;
    *p++ = kHex[(v >> ((15 - (i & 15)) * 4)) & 0xf];
  }
  return std::string(buf, kBufSize);
}

std::string GenerateUUIDTime() {
  uuid_t out;
  char buf[kBufSize];
  uuid_generate_time(out);
  uuid_unparse_lower(out, buf);
  std::string str(&buf[0], kBufSize);
  return str;
}

std::string GenerateUUIDRandom() {
  uuid_t out;
  char buf[kBufSize];
  uuid_generate_random(out);
  uuid_unparse_lower(out, buf);
  std::string str(&buf[0], kBufSize);
  return str;
}
}  // namespace utils
}  // namespace dingofs
