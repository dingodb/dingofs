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

#include "client/vfs/data/reader/readahead_policy.h"

#include <fmt/format.h>
#include <glog/logging.h>

namespace dingofs {
namespace client {
namespace vfs {

static const uint8_t kReadaheadMaxLevel = 4;
static const int64_t kReadAheadBaseSize = 1 * 1024 * 1024;  // 1MB
static const int64_t kSeqAccessWindowSize = 2 * 1024 * 1024;

std::string ReadAheadStats::ToString() const {
  return fmt::format("(read_count={}, sequential_count={}, random_count={} ",
                     read_count, sequentail_read_count, random_read_count);
}

std::string ReadaheadPoclicy::UUID() const {
  return fmt::format("policy-{}", uuid);
}

std::string ReadaheadPoclicy::ToString() const {
  return fmt::format(
      "(uuid: {}, level: {}, seqdata: {}, last_offset: {}, readahead_size: {})",
      UUID(), level, seqdata, last_offset, ReadaheadSize());
}

// if kReadAheadBaseSize is 1MB
// level 1: (1 * kReadAheadBaseSize) 1MB
// level 2: (4 * kReadAheadBaseSize) 4MB
// level 3: (16 * kReadAheadBaseSize) 16MB
// level 4: (64 * kReadAheadBaseSize) 64MB
int64_t ReadaheadPoclicy::ReadaheadSize() const {
  return level > 0 ? kReadAheadBaseSize * (1 << ((level - 1) * 2)) : 0;
}

// NOTE: This function adjusts the readahead policy based on access pattern
// and buffer pressure. Rules:
// 1. Same offset and offset == 0 => initialize level to 1.
// 2. Sequential growth: if still at last_offset, and accumulated seqdata
// exceeds current level size -> promote level.
// 3. Random / far jump outside sequential window => degrade level and reset
// seqdata.
// 4. Memory pressure: if buffer usage high while level > 1 => degrade.
// 5. Always reset seqdata on level changes
void ReadaheadPoclicy::UpdateOnRead(const FileRange& frange,
                                    int64_t rbuffer_used,
                                    int64_t rbuffer_total) {
  VLOG(12) << fmt::format(
      "{} UpdateOnRead frange: {}, policy before: {}, rbuffer_used: {}, "
      "rbuffer_total: {}",
      UUID(), frange.ToString(), ToString(), rbuffer_used, rbuffer_total);

  bool within_seq_window =
      std::abs(frange.offset - last_offset) <= kSeqAccessWindowSize;

  readahead_stats.read_count++;

  if (within_seq_window) {
    seqdata += frange.len;
    readahead_stats.sequentail_read_count++;
  } else {
    readahead_stats.random_read_count++;
  }

  if (frange.offset == last_offset) {
    if (frange.offset == 0) {
      level = 1;
      seqdata = 0;
      VLOG(9) << fmt::format("{} ReadaheadPoclicy init level, policy: {}",
                             UUID(), ToString());
    } else if (level < kReadaheadMaxLevel) {
      if (seqdata >= ReadaheadSize()) {
        level++;
        seqdata = 0;
        VLOG(9) << fmt::format(
            "{} ReadaheadPoclicy upgrade (seqdata enough) policy: {}", UUID(),
            ToString());
      }
    }
  } else {
    if (!within_seq_window) {
      // random access
      if (level > 0) {
        Degrade();
      }
      seqdata = 0;
      VLOG(2) << fmt::format(
          "{} ReadaheadPoclicy degrade (random access) policy: {}", UUID(),
          ToString());
    }
  }

  if (level > 1) {
    int64_t mem_pressure_threshold =
        (rbuffer_total / 2) + ((rbuffer_total * 1) / (level * 2));
    if (rbuffer_used > mem_pressure_threshold) {
      Degrade();
      VLOG(1) << fmt::format(
          "{} CheckReadahead degrade (memory pressure) policy: {}, "
          "used: {}, total: {}, threshold: {}",
          UUID(), ToString(), rbuffer_used, rbuffer_total,
          mem_pressure_threshold);
    }
  }
}

void ReadaheadPoclicy::Degrade() {
  if (level > 0) {
    level--;
    seqdata = 0;
    if (level == 0) {
      last_offset = 0;
      VLOG(1) << fmt::format(
          "{} ReadaheadPoclicy degraded to level 0 (no readahead), reset "
          "last_offset",
          UUID());
    }
  }
}
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
