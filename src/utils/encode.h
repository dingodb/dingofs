// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_UTILS_ENCODE_H_
#define SRC_UTILS_ENCODE_H_

#include <cstdint>

namespace dingofs {
namespace utils {

inline void EncodeBigEndian64(char* buf, uint64_t value) {
  buf[0] = (value >> 56) & 0xff;
  buf[1] = (value >> 48) & 0xff;
  buf[2] = (value >> 40) & 0xff;
  buf[3] = (value >> 32) & 0xff;
  buf[4] = (value >> 24) & 0xff;
  buf[5] = (value >> 16) & 0xff;
  buf[6] = (value >> 8) & 0xff;
  buf[7] = value & 0xff;
}

inline void EncodeBigEndian32(char* buf, uint32_t value) {
  buf[0] = (value >> 24) & 0xff;
  buf[1] = (value >> 16) & 0xff;
  buf[2] = (value >> 8) & 0xff;
  buf[3] = value & 0xff;
}

inline void EncodeLittleEndian64(char* buf, uint64_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  buf[4] = (value >> 32) & 0xff;
  buf[5] = (value >> 40) & 0xff;
  buf[6] = (value >> 48) & 0xff;
  buf[7] = (value >> 56) & 0xff;
}

inline void EncodeLittleEndian32(char* buf, uint32_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
}

inline uint64_t DecodeBigEndian64(const char* buf) {
  return (uint64_t(buf[0]) << 56) | (uint64_t(buf[1]) << 48) |
         (uint64_t(buf[2]) << 40) | (uint64_t(buf[3]) << 32) |
         (uint64_t(buf[4]) << 24) | (uint64_t(buf[5]) << 16) |
         (uint64_t(buf[6]) << 8) | uint64_t(buf[7]);
}

inline uint32_t DecodeBigEndian32(const char* buf) {
  return (uint32_t(buf[0]) << 24) | (uint32_t(buf[1]) << 16) |
         (uint32_t(buf[2]) << 8) | uint32_t(buf[3]);
}

inline uint64_t DecodeLittleEndian64(const char* buf) {
  return uint64_t(buf[0]) | (uint64_t(buf[1]) << 8) | (uint64_t(buf[2]) << 16) |
         (uint64_t(buf[3]) << 24) | (uint64_t(buf[4]) << 32) |
         (uint64_t(buf[5]) << 40) | (uint64_t(buf[6]) << 48) |
         (uint64_t(buf[7]) << 56);
}

inline uint32_t DecodeLittleEndian32(const char* buf) {
  return uint32_t(buf[0]) | (uint32_t(buf[1]) << 8) | (uint32_t(buf[2]) << 16) |
         (uint32_t(buf[3]) << 24);
}

inline bool IsLittleEndian() {
  static const uint16_t num = 0x0102;
  return reinterpret_cast<const uint8_t*>(&num)[0] == 0x02;
}

inline void EncodeNativeEndian64(char* buf, uint64_t value) {
  if (IsLittleEndian()) {
    EncodeLittleEndian64(buf, value);
  } else {
    EncodeBigEndian64(buf, value);
  }
}

inline void EncodeNativeEndian32(char* buf, uint32_t value) {
  if (IsLittleEndian()) {
    EncodeLittleEndian32(buf, value);
  } else {
    EncodeBigEndian32(buf, value);
  }
}

inline uint64_t DecodeNativeEndian64(const char* buf) {
  if (IsLittleEndian()) return DecodeLittleEndian64(buf);

  return DecodeBigEndian64(buf);
}

inline uint32_t DecodeNativeEndian32(const char* buf) {
  if (IsLittleEndian()) return DecodeLittleEndian32(buf);

  return DecodeBigEndian32(buf);
}

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_UTILS_ENCODE_H_
