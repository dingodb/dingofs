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

#ifndef DINGOFS_SRC_COMMON_READMEMPOOL_READ_BUF_H_
#define DINGOFS_SRC_COMMON_READMEMPOOL_READ_BUF_H_

#include <cstddef>
#include <cstdint>

namespace dingofs {

class ReadMemPool;

// A move-only RAII handle for a single slot. The dtor returns the slot to its
// pool; Disown() transfers ownership elsewhere (e.g. an IOBuf deleter). An empty
// handle (operator bool == false) means allocation failed (pool exhausted / oversize).
//
// Produced by ReadMemPool::Allocate (private ctor, friend). Reset() needs the
// complete ReadMemPool type, so it is defined in read_buf.cc.
class ReadBuf {
 public:
  ReadBuf() = default;
  ~ReadBuf() { Reset(); }

  ReadBuf(ReadBuf&& o) noexcept { MoveFrom(o); }
  ReadBuf& operator=(ReadBuf&& o) noexcept {
    if (this != &o) {
      Reset();
      MoveFrom(o);
    }
    return *this;
  }

  ReadBuf(const ReadBuf&) = delete;
  ReadBuf& operator=(const ReadBuf&) = delete;

  uint8_t* data() const { return data_; }   // contiguous base (slot start)
  size_t capacity() const { return cap_; }    // actual slot capacity (>= requested len)
  explicit operator bool() const { return pool_ != nullptr; }

  // Hand off ownership: return the raw address; the dtor will not return it
  // afterward (avoids double free).
  uint8_t* Disown() {
    uint8_t* p = data_;
    pool_ = nullptr;
    data_ = nullptr;
    return p;
  }

 private:
  friend class ReadMemPool;
  ReadBuf(ReadMemPool* pool, uint8_t* data, uint64_t off, size_t cap,
            uint32_t meta)
      : pool_(pool), data_(data), off_(off), cap_(cap), meta_(meta) {}

  void Reset();  // defined in read_buf.cc (needs the complete ReadMemPool type)
  void MoveFrom(ReadBuf& o) {
    pool_ = o.pool_;
    data_ = o.data_;
    off_ = o.off_;
    cap_ = o.cap_;
    meta_ = o.meta_;
    o.pool_ = nullptr;
    o.data_ = nullptr;
  }

  ReadMemPool* pool_ = nullptr;
  uint8_t* data_ = nullptr;
  uint64_t off_ = 0;
  size_t cap_ = 0;
  uint32_t meta_ = 0;  // {source:1 (0=buddy,1=slab), order: low 7 bits}
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_READMEMPOOL_READ_BUF_H_
