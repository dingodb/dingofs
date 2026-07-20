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

/*
 * Parser for the legacy `fuse_access` spdlog access-log text produced by
 * client/vfs/access_log.h + vfs_wrapper.cc's AccessLogGuard.
 *
 * Line shape (spdlog default "%+" pattern):
 *   [YYYY-MM-DD HH:MM:SS.mmm] [fuse_access] [level] [pid:uid:gid] opname
 *   (args...): <Status type>[ (errno:N)][: msg][ extra-result-fields]
 *   <duration_seconds>
 *
 * The parser deliberately avoids std::regex: the grammar is simple enough
 * that hand-written scanning is both faster and easier to reason about for
 * the delimiter-ambiguity cases called out below.
 *
 * Only the fields required to reconstruct and replay an operation are
 * extracted; free-form text (Status messages, xattr values, symlink
 * targets used only for display, ...) is intentionally left unparsed
 * because nothing downstream depends on it.
 */

#ifndef DINGOFS_TOOLS_REPLAY_REPLAY_PARSER_H_
#define DINGOFS_TOOLS_REPLAY_REPLAY_PARSER_H_

#include <cstdint>
#include <ostream>
#include <string>

namespace dingofs {
namespace tools {
namespace replay {

enum class OpKind {
  kLookup,
  kGetAttr,
  kSetAttr,
  kFallocate,
  kCopyFileRange,
  kReadLink,
  kMkNod,
  kUnlink,
  kSymlink,
  kRename,
  kLink,
  kOpen,
  kCreate,
  kRead,
  kWrite,
  kFlush,
  kRelease,
  kFsync,
  kGetXattr,
  kRemoveXattr,
  kListXattr,
  kMkDir,
  kOpenDir,
  kReadDir,
  kReleaseDir,
  kRmDir,
  kStatFs,
  // Recognized but never replayed: critical input (xattr value / ioctl
  // buffers) is not captured by the access log.
  kUnsupported,
};

const char* OpKindName(OpKind op);

// One successfully parsed access-log line. Only fields relevant to `op` are
// populated; the rest keep their zero-value default.
struct ParsedRecord {
  uint64_t seq = 0;  // assigned by the caller for stable ordering

  double end_time_sec = 0;    // completion timestamp, from log line
  double duration_sec = 0;    // recorded operation duration
  double start_time_sec = 0;  // end_time_sec - duration_sec

  OpKind op = OpKind::kUnsupported;
  int32_t pid = 0;
  uint32_t uid = 0;
  uint32_t gid = 0;

  bool ok = false;          // source Status::ok()
  std::string status_type;  // e.g. "OK", "NotExist"

  // Input arguments. Meaning depends on `op`:
  //  ino1: parent / ino / old_parent / src_ino
  //  ino2: new_parent / dst_ino
  uint64_t ino1 = 0;
  uint64_t ino2 = 0;
  std::string name1;  // name / old_name / xattr name
  std::string name2;  // new_name / symlink target
  uint32_t mode = 0;
  int flags = 0;  // open flags / fallocate mode
  int set = 0;    // setattr set-mask
  int datasync = 0;
  uint64_t offset = 0;
  uint64_t size = 0;
  uint64_t length = 0;
  uint32_t cp_flags = 0;
  uint64_t src_off = 0;
  uint64_t src_fh = 0;
  uint64_t dst_off = 0;
  uint64_t dst_fh = 0;
  bool with_attr = false;  // readdir
  uint64_t fh = 0;  // fh being operated on (read/write/flush/release/fsync/
                    // readdir/releasedir input) -- distinct from result_fh
                    // below, which is a *newly produced* fh.

  // Result fields, only meaningful when the corresponding has_* is set.
  // Only lookup/mknod/symlink/link/create/mkdir (has_result_ino) and
  // open/create/opendir (has_result_fh) ever set these, and only when ok.
  bool has_result_ino = false;
  uint64_t result_ino = 0;
  bool has_result_fh = false;
  uint64_t result_fh = 0;

  // setattr's in_attr is not logged; it is reconstructed from the logged
  // *result* attr, which only holds valid values when ok == true.
  bool has_result_attr = false;
  uint32_t r_mode = 0;
  uint32_t r_uid = 0;
  uint32_t r_gid = 0;
  uint64_t r_atime = 0;
  uint64_t r_mtime = 0;
  uint64_t r_length = 0;
};

enum class LineParseStatus {
  kOk,             // successfully parsed into `record`
  kIgnoredOther,   // not a fuse_access line (different logger/blank/etc.)
  kControlRecord,  // start:/stop: record, intentionally ignored
  kMalformed,      // looked like an access record but failed to parse
};

struct LineParseResult {
  LineParseStatus status = LineParseStatus::kIgnoredOther;
  std::string reason;   // set when status == kMalformed
  ParsedRecord record;  // valid iff status == kOk
};

LineParseResult ParseAccessLogLine(const std::string& line);

// Runs a handful of representative + malformed lines through
// ParseAccessLogLine and reports PASS/FAIL to `out`. Returns true iff every
// case matched its expectation. Requires no mounted filesystem or network.
bool RunParserSelfCheck(std::ostream& out);

}  // namespace replay
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_TOOLS_REPLAY_REPLAY_PARSER_H_
