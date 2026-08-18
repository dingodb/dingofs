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

#ifndef DINGOFS_CACHE_V2_CORE_SERVER_CODEC_H_
#define DINGOFS_CACHE_V2_CORE_SERVER_CODEC_H_

#include <google/protobuf/message_lite.h>

#include <concepts>
#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

namespace dingofs {
namespace cache {
namespace v2 {

template <typename T>
concept ProtoMessage = std::derived_from<T, google::protobuf::MessageLite>;

// Wire codec, picked by type: protobuf message or trivially-copyable POD.
template <typename T, typename Enable = void>
struct Codec;

template <typename T>
struct Codec<
    T, std::enable_if_t<std::is_trivially_copyable_v<T> &&
                        !std::derived_from<T, google::protobuf::MessageLite>>> {
  static bool Decode(std::string_view in, T* out) {
    if (in.size() != sizeof(T)) {
      return false;
    }
    std::memcpy(out, in.data(), sizeof(T));
    return true;
  }
  static size_t EncodedSize(const T& /*value*/) { return sizeof(T); }
  static void Encode(const T& value, char* dst) {
    std::memcpy(dst, &value, sizeof(T));
  }
};

template <ProtoMessage T>
struct Codec<T> {
  static bool Decode(std::string_view in, T* out) {
    return out->ParseFromArray(in.data(), static_cast<int>(in.size()));
  }
  static size_t EncodedSize(const T& value) { return value.ByteSizeLong(); }
  static void Encode(const T& value, char* dst) {
    value.SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t*>(dst));
  }
};

// In-place encoded message; view() points into it -- must outlive the send.
template <typename T>
class Encoded {
 public:
  explicit Encoded(const T& value) {
    const size_t n = Codec<T>::EncodedSize(value);
    char* dst = inlined_;
    if (n > sizeof(inlined_)) {
      spilled_.resize(n);
      dst = spilled_.data();
    }
    Codec<T>::Encode(value, dst);
    view_ = {dst, n};
  }

  Encoded(const Encoded&) = delete;
  Encoded& operator=(const Encoded&) = delete;

  std::string_view view() const { return view_; }

 private:
  char inlined_[256];
  std::string spilled_;
  std::string_view view_;
};

// Empty request/response for verbs whose whole answer is the reply code.
struct Empty {};

template <>
struct Codec<Empty> {
  static bool Decode(std::string_view /*in*/, Empty* /*out*/) { return true; }
  static size_t EncodedSize(const Empty& /*value*/) { return 0; }
  static void Encode(const Empty& /*value*/, char* /*dst*/) {}
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_SERVER_CODEC_H_
