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

#ifndef DINGOFS_COMMON_SLICE_H_
#define DINGOFS_COMMON_SLICE_H_

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>  // RocksDB now requires C++17 support

namespace dingofs {

class StringSlice {
 public:
  // Create an empty slice.
  StringSlice() : data_(""), size_(0) {}

  // Create a slice that refers to d[0,n-1].
  StringSlice(const char* d, size_t n) : data_(d), size_(n) {}

  // Create a slice that refers to the contents of "s"
  /* implicit */
  StringSlice(const std::string& s) : data_(s.data()), size_(s.size()) {}

  // Create a slice that refers to the same contents as "sv"
  /* implicit */
  StringSlice(const std::string_view& sv)
      : data_(sv.data()), size_(sv.size()) {}

  // Create a slice that refers to s[0,strlen(s)-1]
  /* implicit */
  StringSlice(const char* s) : data_(s) {
    size_ = (s == nullptr) ? 0 : strlen(s);
  }

  // Create a single slice from StringSliceParts using buf as storage.
  // buf must exist as long as the returned Slice exists.
  StringSlice(const struct StringSliceParts& parts, std::string* buf);

  // Return a pointer to the beginning of the referenced data
  const char* data() const { return data_; }  // NOLINT

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }  // NOLINT

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }  // NOLINT

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() {  // NOLINT
    data_ = "";
    size_ = 0;
  }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {  // NOLINT
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  void remove_suffix(size_t n) {  // NOLINT
    assert(n <= size());
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  // when hex is true, returns a string of twice the length hex encoded (0-9A-F)
  std::string ToString(bool hex = false) const;

  // Return a string_view that references the same data as this slice.
  std::string_view ToStringView() const {
    return std::string_view(data_, size_);
  }

  // Decodes the current slice interpreted as an hexadecimal string into result,
  // if successful returns true, if this isn't a valid hex string
  // (e.g not coming from StringSlice::ToString(true)) DecodeHex returns false.
  // This slice is expected to have an even number of 0-9A-F characters
  // also accepts lowercase (a-f)
  bool DecodeHex(std::string* result) const;

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const StringSlice& b) const;  // NOLINT

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const StringSlice& x) const  // NOLINT
  {
    return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
  }

  bool ends_with(const StringSlice& x) const  // NOLINT
  {
    return ((size_ >= x.size_) &&
            (memcmp(data_ + size_ - x.size_, x.data_, x.size_) == 0));
  }

  // Compare two slices and returns the first byte where they differ
  size_t difference_offset(const StringSlice& b) const;  // NOLINT

  // private: make these public for rocksdbjni access
  const char* data_;  // NOLINT
  size_t size_;       // NOLINT

  // Intentionally copyable
};

// A set of Slices that are virtually concatenated together.  'parts' points
// to an array of Slices.  The number of elements in the array is 'num_parts'.
struct StringSliceParts {
  StringSliceParts(const StringSlice* _parts, int _num_parts)
      : parts(_parts), num_parts(_num_parts) {}  // NOLINT
  StringSliceParts() : parts(nullptr), num_parts(0) {}

  const StringSlice* parts;
  int num_parts;
};

inline bool operator==(const StringSlice& x, const StringSlice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const StringSlice& x, const StringSlice& y) {
  return !(x == y);
}

inline int StringSlice::compare(const StringSlice& b) const {
  assert(data_ != nullptr && b.data_ != nullptr);
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_)
      r = -1;
    else if (size_ > b.size_)
      r = +1;
  }
  return r;
}

inline size_t StringSlice::difference_offset(const StringSlice& b) const {
  size_t off = 0;
  const size_t len = (size_ < b.size_) ? size_ : b.size_;
  for (; off < len; off++) {
    if (data_[off] != b.data_[off]) break;
  }
  return off;
}

}  // namespace dingofs

#endif  // DINGOFS_COMMON_SLICE_H_