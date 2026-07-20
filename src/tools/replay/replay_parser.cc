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

#include "tools/replay/replay_parser.h"

#include <cctype>
#include <cstdlib>
#include <ctime>
#include <string_view>
#include <vector>

namespace dingofs {
namespace tools {
namespace replay {

namespace {

using std::string_view;

bool ParseUint(string_view sv, uint64_t* out) {
  if (sv.empty()) return false;
  uint64_t v = 0;
  for (char c : sv) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + static_cast<uint64_t>(c - '0');
  }
  *out = v;
  return true;
}

bool ParseInt(string_view sv, int* out) {
  if (sv.empty()) return false;
  bool neg = false;
  size_t i = 0;
  if (sv[0] == '-') {
    neg = true;
    i = 1;
  }
  if (i >= sv.size()) return false;
  int64_t v = 0;
  for (; i < sv.size(); i++) {
    char c = sv[i];
    if (c < '0' || c > '9') return false;
    v = v * 10 + (c - '0');
  }
  *out = static_cast<int>(neg ? -v : v);
  return true;
}

bool ParseHex(string_view sv, uint32_t* out) {
  if (sv.empty()) return false;
  uint32_t v = 0;
  for (char c : sv) {
    v <<= 4;
    if (c >= '0' && c <= '9')
      v |= static_cast<uint32_t>(c - '0');
    else if (c >= 'a' && c <= 'f')
      v |= static_cast<uint32_t>(c - 'a' + 10);
    else if (c >= 'A' && c <= 'F')
      v |= static_cast<uint32_t>(c - 'A' + 10);
    else
      return false;
  }
  *out = v;
  return true;
}

// "0x%X" formatted value.
bool ParseHexPrefixed(string_view sv, uint32_t* out) {
  if (sv.size() < 3 || sv[0] != '0' || (sv[1] != 'x' && sv[1] != 'X'))
    return false;
  return ParseHex(sv.substr(2), out);
}

// "0%o" formatted octal value (a leading literal '0' followed by octal
// digits representing the actual mode value).
bool ParseOctalPrefixed(string_view sv, uint32_t* out) {
  if (sv.size() < 2 || sv[0] != '0') return false;
  uint32_t v = 0;
  for (size_t i = 1; i < sv.size(); i++) {
    char c = sv[i];
    if (c < '0' || c > '7') return false;
    v = (v << 3) | static_cast<uint32_t>(c - '0');
  }
  *out = v;
  return true;
}

string_view TrimLeft(string_view sv) {
  size_t i = 0;
  while (i < sv.size() && sv[i] == ' ') i++;
  return sv.substr(i);
}

bool StartsWith(string_view sv, string_view prefix) {
  return sv.size() >= prefix.size() && sv.substr(0, prefix.size()) == prefix;
}

bool EndsWith(string_view sv, string_view suffix) {
  return sv.size() >= suffix.size() &&
         sv.substr(sv.size() - suffix.size()) == suffix;
}

// Splits `sv` on top-level commas (no nesting expected in this grammar).
std::vector<string_view> SplitComma(string_view sv) {
  std::vector<string_view> out;
  size_t start = 0;
  for (size_t i = 0; i <= sv.size(); i++) {
    if (i == sv.size() || sv[i] == ',') {
      out.push_back(sv.substr(start, i - start));
      start = i + 1;
    }
  }
  return out;
}

// Finds the '(' matching the *last* ')' of `sv` (sv must end with ')'),
// scanning backward and tracking paren depth. This correctly isolates a
// trailing "(...)" block appended after free-form text, even when that
// free-form text itself contains unrelated parentheses, because scanning
// starts at the true end of the string.
bool FindTrailingParenBlock(string_view sv, size_t* open_idx) {
  if (sv.empty() || sv.back() != ')') return false;
  int depth = 0;
  for (size_t i = sv.size(); i-- > 0;) {
    if (sv[i] == ')') {
      depth++;
    } else if (sv[i] == '(') {
      depth--;
      if (depth == 0) {
        *open_idx = i;
        return true;
      }
    }
  }
  return false;
}

// Parses the "YYYY-MM-DD HH:MM:SS.mmm" spdlog timestamp into epoch seconds
// (UTC; only relative differences are used by the caller, so timezone does
// not matter as long as it is applied consistently).
bool ParseTimestamp(string_view sv, double* out) {
  int year, mon, day, hour, min, sec, ms;
  // "YYYY-MM-DD HH:MM:SS.mmm" is exactly 23 chars.
  if (sv.size() != 23) return false;
  std::string s(sv);
  if (std::sscanf(s.c_str(), "%4d-%2d-%2d %2d:%2d:%2d.%3d", &year, &mon, &day,
                  &hour, &min, &sec, &ms) != 7) {
    return false;
  }
  struct tm tm{};
  tm.tm_year = year - 1900;
  tm.tm_mon = mon - 1;
  tm.tm_mday = day;
  tm.tm_hour = hour;
  tm.tm_min = min;
  tm.tm_sec = sec;
  time_t t = timegm(&tm);
  if (t == static_cast<time_t>(-1)) return false;
  *out = static_cast<double>(t) + ms / 1000.0;
  return true;
}

// Strips the trailing " <duration>" suffix appended by AccessLogGuard.
// Requires it to be the literal end of the string (nothing after '>').
bool StripDuration(string_view sv, string_view* content, double* duration) {
  if (sv.empty() || sv.back() != '>') return false;
  size_t lt = sv.rfind('<');
  if (lt == string_view::npos || lt == 0 || sv[lt - 1] != ' ') return false;
  string_view num = sv.substr(lt + 1, sv.size() - lt - 2);
  if (num.empty()) return false;
  char* end = nullptr;
  std::string tmp(num);
  double v = std::strtod(tmp.c_str(), &end);
  if (end != tmp.c_str() + tmp.size()) return false;
  *duration = v;
  *content = sv.substr(0, lt - 1);
  return true;
}

bool ParseCtx(string_view sv, int32_t* pid, uint32_t* uid, uint32_t* gid) {
  size_t c1 = sv.find(':');
  if (c1 == string_view::npos) return false;
  size_t c2 = sv.find(':', c1 + 1);
  if (c2 == string_view::npos) return false;
  int pid_i;
  if (!ParseInt(sv.substr(0, c1), &pid_i)) return false;
  uint64_t uid_u, gid_u;
  if (!ParseUint(sv.substr(c1 + 1, c2 - c1 - 1), &uid_u)) return false;
  if (!ParseUint(sv.substr(c2 + 1), &gid_u)) return false;
  *pid = pid_i;
  *uid = static_cast<uint32_t>(uid_u);
  *gid = static_cast<uint32_t>(gid_u);
  return true;
}

const char* kKnownStatusTypes[] = {
    "OK",           "Internal",        "Unknown",
    "Exist",        "NotExist",        "NoSpace",
    "BadFd",        "InvalidParam",    "NoPermission",
    "NotEmpty",     "NoFlush",         "NotSupport",
    "NameTooLong",  "MountPointExist", "MountFailed",
    "OutOfRange",   "NoData",          "IoError",
    "Stale",        "NoSys",           "NoPermitted",
    "NetError",     "NotFound",        "NotDirectory",
    "FileTooLarge", "EndOfFile",       "Abort",
    "CacheDown",    "CacheUnhealthy",  "CacheFull",
    "Stop",         "NotFit",          "Timeout",
    "OutOfMemory",  "Deleted",
};

bool IsKnownStatusType(string_view sv) {
  for (const char* t : kKnownStatusTypes) {
    if (sv == t) return true;
  }
  return false;
}

// Extracts the leading alphabetic run of `tail` as the Status type token.
bool ExtractStatusType(string_view tail, string_view* type, string_view* rest) {
  size_t i = 0;
  while (i < tail.size() && std::isalpha(static_cast<unsigned char>(tail[i])))
    i++;
  string_view t = tail.substr(0, i);
  if (!IsKnownStatusType(t)) return false;
  *type = t;
  *rest = tail.substr(i);
  return true;
}

// Extracts a trailing "[fh:<digits>]" marker anchored at the true end of
// `sv` (or immediately before a previously-stripped suffix). Returns the fh
// value and the portion of `sv` preceding the marker (trailing space kept
// trimmed).
bool ExtractTrailingFh(string_view sv, uint64_t* fh, string_view* before) {
  if (!EndsWith(sv, "]")) return false;
  size_t start = sv.rfind("[fh:");
  if (start == string_view::npos) return false;
  string_view num = sv.substr(start + 4, sv.size() - start - 4 - 1);
  if (!ParseUint(num, fh)) return false;
  string_view rest = sv.substr(0, start);
  while (!rest.empty() && rest.back() == ' ') rest.remove_suffix(1);
  *before = rest;
  return true;
}

// Extracts the leading `ino` field of a trailing StrAttr block
// " (<ino>,[...])" anchored at the end of `sv`.
bool ExtractTrailingStrAttrIno(string_view sv, uint64_t* ino) {
  size_t open_idx;
  if (!FindTrailingParenBlock(sv, &open_idx)) return false;
  string_view inner = sv.substr(open_idx + 1, sv.size() - open_idx - 2);
  size_t comma = inner.find(',');
  if (comma == string_view::npos) return false;
  return ParseUint(inner.substr(0, comma), ino);
}

// Extracts every numeric field of a trailing StrAttr block:
// " (<ino>,[<modestr>:0<mode>,<nlink>,<uid>,<gid>,<atime>,<mtime>,<ctime>,
// <length>])".
bool ExtractTrailingStrAttrFields(string_view sv, uint64_t* ino, uint32_t* mode,
                                  uint32_t* uid, uint32_t* gid, uint64_t* atime,
                                  uint64_t* mtime, uint64_t* length) {
  size_t open_idx;
  if (!FindTrailingParenBlock(sv, &open_idx)) return false;
  string_view inner = sv.substr(open_idx + 1, sv.size() - open_idx - 2);
  size_t comma = inner.find(',');
  if (comma == string_view::npos) return false;
  if (!ParseUint(inner.substr(0, comma), ino)) return false;
  string_view rest = inner.substr(comma + 1);
  if (rest.empty() || rest.front() != '[' || rest.back() != ']') return false;
  rest = rest.substr(1, rest.size() - 2);
  std::vector<string_view> fields = SplitComma(rest);
  if (fields.size() != 8) return false;
  size_t colon = fields[0].rfind(':');
  if (colon == string_view::npos) return false;
  if (!ParseOctalPrefixed(fields[0].substr(colon + 1), mode)) return false;
  uint64_t uid64, gid64;
  if (!ParseUint(fields[2], &uid64)) return false;
  if (!ParseUint(fields[3], &gid64)) return false;
  *uid = static_cast<uint32_t>(uid64);
  *gid = static_cast<uint32_t>(gid64);
  if (!ParseUint(fields[4], atime)) return false;
  if (!ParseUint(fields[5], mtime)) return false;
  if (!ParseUint(fields[7], length)) return false;
  return true;
}

// Extracts the `ino` field of a trailing Attr2Str block (used by symlink):
// "(ino: <ino>, mode: ..., ...)".
bool ExtractTrailingAttr2StrIno(string_view sv, uint64_t* ino) {
  size_t open_idx;
  if (!FindTrailingParenBlock(sv, &open_idx)) return false;
  string_view inner = sv.substr(open_idx + 1, sv.size() - open_idx - 2);
  const string_view kPrefix = "ino: ";
  if (!StartsWith(inner, kPrefix)) return false;
  inner = inner.substr(kPrefix.size());
  size_t comma = inner.find(',');
  if (comma == string_view::npos) return false;
  return ParseUint(inner.substr(0, comma), ino);
}

struct OpEntry {
  const char* name;
  OpKind kind;
};

const OpEntry kOpTable[] = {
    {"lookup", OpKind::kLookup},
    {"getattr", OpKind::kGetAttr},
    {"setattr", OpKind::kSetAttr},
    {"fallocate", OpKind::kFallocate},
    {"copyfilerange", OpKind::kCopyFileRange},
    {"readlink", OpKind::kReadLink},
    {"mknod", OpKind::kMkNod},
    {"unlink", OpKind::kUnlink},
    {"symlink", OpKind::kSymlink},
    {"rename", OpKind::kRename},
    {"link", OpKind::kLink},
    {"open", OpKind::kOpen},
    {"create", OpKind::kCreate},
    {"read", OpKind::kRead},
    {"write", OpKind::kWrite},
    {"flush", OpKind::kFlush},
    {"release", OpKind::kRelease},
    {"fsync", OpKind::kFsync},
    {"setxattr", OpKind::kUnsupported},
    {"getxattr", OpKind::kGetXattr},
    {"removexattr", OpKind::kRemoveXattr},
    {"listxattr", OpKind::kListXattr},
    {"mkdir", OpKind::kMkDir},
    {"opendir", OpKind::kOpenDir},
    {"readdir", OpKind::kReadDir},
    {"releasedir", OpKind::kReleaseDir},
    {"rmdir", OpKind::kRmDir},
    {"statfs", OpKind::kStatFs},
    {"ioctl", OpKind::kUnsupported},
};

bool LookupOp(string_view name, OpKind* kind) {
  for (const auto& e : kOpTable) {
    if (name == e.name) {
      *kind = e.kind;
      return true;
    }
  }
  return false;
}

// Parses the args block and result tail for a single operation. `ok` is the
// already-extracted Status::ok() flag (front-anchored, always safe).
bool ParseArgsAndTail(OpKind op, string_view args, string_view tail, bool ok,
                      ParsedRecord* rec, std::string* reason) {
  auto bad = [&](const char* why) {
    *reason = why;
    return false;
  };

  switch (op) {
    case OpKind::kLookup: {
      // "(%llu/%s)" — parent and name are slash-separated, not comma.
      size_t slash = args.find('/');
      if (slash == string_view::npos) return bad("lookup: missing '/'");
      if (!ParseUint(args.substr(0, slash), &rec->ino1))
        return bad("lookup: bad parent");
      rec->name1 = std::string(args.substr(slash + 1));
      if (ok) {
        if (!ExtractTrailingStrAttrIno(tail, &rec->result_ino))
          return bad("lookup: bad result attr");
        rec->has_result_ino = true;
      }
      return true;
    }
    case OpKind::kGetAttr: {
      if (!ParseUint(args, &rec->ino1)) return bad("getattr: bad ino");
      return true;  // result attr unused downstream
    }
    case OpKind::kSetAttr: {
      auto f = SplitComma(args);
      if (f.size() != 2) return bad("setattr: expected 2 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("setattr: bad ino");
      uint32_t set_hex;
      if (!ParseHexPrefixed(f[1], &set_hex)) return bad("setattr: bad set");
      rec->set = static_cast<int>(set_hex);
      if (ok) {
        uint64_t ino;
        if (!ExtractTrailingStrAttrFields(tail, &ino, &rec->r_mode, &rec->r_uid,
                                          &rec->r_gid, &rec->r_atime,
                                          &rec->r_mtime, &rec->r_length))
          return bad("setattr: bad result attr");
        rec->has_result_attr = true;
      }
      return true;
    }
    case OpKind::kFallocate: {
      auto f = SplitComma(args);
      if (f.size() != 4) return bad("fallocate: expected 4 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("fallocate: bad ino");
      string_view mode_f = TrimLeft(f[1]);
      if (!StartsWith(mode_f, "mode=")) return bad("fallocate: bad mode field");
      uint32_t mode_hex;
      if (!ParseHexPrefixed(mode_f.substr(5), &mode_hex))
        return bad("fallocate: bad mode value");
      rec->flags = static_cast<int>(mode_hex);
      string_view off_f = TrimLeft(f[2]);
      if (!StartsWith(off_f, "off=")) return bad("fallocate: bad off field");
      if (!ParseUint(off_f.substr(4), &rec->offset))
        return bad("fallocate: bad off value");
      string_view len_f = TrimLeft(f[3]);
      if (!StartsWith(len_f, "len=")) return bad("fallocate: bad len field");
      if (!ParseUint(len_f.substr(4), &rec->length))
        return bad("fallocate: bad len value");
      return true;
    }
    case OpKind::kCopyFileRange: {
      auto f = SplitComma(args);
      if (f.size() != 3) return bad("copyfilerange: expected 3 groups");
      // Each group is "src_ino src_off fh:src_fh" / "len 0xflags" (space
      // separated, not comma separated).
      auto split_space = [](string_view sv) {
        std::vector<string_view> out;
        size_t start = 0;
        for (size_t i = 0; i <= sv.size(); i++) {
          if (i == sv.size() || sv[i] == ' ') {
            if (i > start) out.push_back(sv.substr(start, i - start));
            start = i + 1;
          }
        }
        return out;
      };
      auto t0 = split_space(TrimLeft(f[0]));
      auto t1 = split_space(TrimLeft(f[1]));
      auto t2 = split_space(TrimLeft(f[2]));
      if (t0.size() != 3 || t1.size() != 3 || t2.size() != 2)
        return bad("copyfilerange: bad group field count");
      if (!ParseUint(t0[0], &rec->ino1)) return bad("copyfilerange: src_ino");
      if (!ParseUint(t0[1], &rec->src_off))
        return bad("copyfilerange: src_off");
      if (!StartsWith(t0[2], "fh:") ||
          !ParseUint(t0[2].substr(3), &rec->src_fh))
        return bad("copyfilerange: src_fh");
      if (!ParseUint(t1[0], &rec->ino2)) return bad("copyfilerange: dst_ino");
      if (!ParseUint(t1[1], &rec->dst_off))
        return bad("copyfilerange: dst_off");
      if (!StartsWith(t1[2], "fh:") ||
          !ParseUint(t1[2].substr(3), &rec->dst_fh))
        return bad("copyfilerange: dst_fh");
      if (!ParseUint(t2[0], &rec->length)) return bad("copyfilerange: len");
      if (!ParseHexPrefixed(t2[1], &rec->cp_flags))
        return bad("copyfilerange: flags");
      return true;
    }
    case OpKind::kReadLink: {
      if (!ParseUint(args, &rec->ino1)) return bad("readlink: bad ino");
      return true;  // link text unused downstream
    }
    case OpKind::kMkNod: {
      auto f = SplitComma(args);
      if (f.size() != 3) return bad("mknod: expected 3 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("mknod: bad parent");
      rec->name1 = std::string(f[1]);
      size_t colon = f[2].rfind(':');
      if (colon == string_view::npos) return bad("mknod: bad mode field");
      if (!ParseOctalPrefixed(f[2].substr(colon + 1), &rec->mode))
        return bad("mknod: bad mode value");
      if (ok) {
        if (!ExtractTrailingStrAttrIno(tail, &rec->result_ino))
          return bad("mknod: bad result attr");
        rec->has_result_ino = true;
      }
      return true;
    }
    case OpKind::kUnlink: {
      auto f = SplitComma(args);
      if (f.size() != 2) return bad("unlink: expected 2 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("unlink: bad parent");
      rec->name1 = std::string(f[1]);
      return true;
    }
    case OpKind::kSymlink: {
      auto f = SplitComma(args);
      if (f.size() != 3) return bad("symlink: expected 3 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("symlink: bad parent");
      rec->name1 = std::string(f[1]);
      rec->name2 = std::string(f[2]);  // link target
      if (ok) {
        if (!ExtractTrailingAttr2StrIno(tail, &rec->result_ino))
          return bad("symlink: bad result attr");
        rec->has_result_ino = true;
      }
      return true;
    }
    case OpKind::kRename: {
      auto f = SplitComma(args);
      if (f.size() != 4) return bad("rename: expected 4 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("rename: bad old_parent");
      rec->name1 = std::string(f[1]);
      if (!ParseUint(f[2], &rec->ino2)) return bad("rename: bad new_parent");
      rec->name2 = std::string(f[3]);
      return true;
    }
    case OpKind::kLink: {
      auto f = SplitComma(args);
      if (f.size() != 3) return bad("link: expected 3 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("link: bad ino");
      if (!ParseUint(f[1], &rec->ino2)) return bad("link: bad new_parent");
      rec->name1 = std::string(f[2]);
      if (ok) {
        if (!ExtractTrailingStrAttrIno(tail, &rec->result_ino))
          return bad("link: bad result attr");
        rec->has_result_ino = true;
      }
      return true;
    }
    case OpKind::kOpen: {
      if (!ParseUint(args, &rec->ino1)) return bad("open: bad ino");
      // tail: "<flags> <descflags> <Status...> [fh:N]"
      uint64_t fh = 0;
      string_view before_fh;
      bool has_fh = ExtractTrailingFh(tail, &fh, &before_fh);
      if (!has_fh) return bad("open: missing [fh:N]");
      size_t sp1 = before_fh.find(' ');
      if (sp1 == string_view::npos) return bad("open: missing flags field");
      int flags_val;
      if (!ParseInt(before_fh.substr(0, sp1), &flags_val))
        return bad("open: bad flags");
      rec->flags = flags_val;
      string_view rest = before_fh.substr(sp1 + 1);
      size_t sp2 = rest.find(' ');
      if (sp2 == string_view::npos) return bad("open: missing descflags field");
      // rest.substr(0, sp2) is the descflags token (unused for replay).
      string_view status_part = rest.substr(sp2 + 1);
      string_view type, remainder;
      if (!ExtractStatusType(status_part, &type, &remainder))
        return bad("open: unknown status type");
      rec->status_type = std::string(type);
      rec->ok = (type == "OK");
      if (rec->ok) {
        rec->result_fh = fh;
        rec->has_result_fh = true;
      }
      return true;
    }
    case OpKind::kCreate: {
      auto f = SplitComma(args);
      if (f.size() != 2) return bad("create: expected 2 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("create: bad parent");
      rec->name1 = std::string(f[1]);
      uint64_t fh = 0;
      string_view before_fh;
      if (!ExtractTrailingFh(tail, &fh, &before_fh))
        return bad("create: missing [fh:N]");
      if (ok) {
        uint64_t ino;
        if (!ExtractTrailingStrAttrFields(
                before_fh, &ino, &rec->mode, &rec->r_uid, &rec->r_gid,
                &rec->r_atime, &rec->r_mtime, &rec->r_length))
          return bad("create: bad result attr");
        rec->result_ino = ino;
        rec->has_result_ino = true;
        rec->result_fh = fh;
        rec->has_result_fh = true;
      }
      return true;
    }
    case OpKind::kRead: {
      auto f = SplitComma(args);
      if (f.size() != 3) return bad("read: expected 3 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("read: bad ino");
      if (!ParseUint(f[1], &rec->size)) return bad("read: bad size");
      if (!ParseUint(f[2], &rec->offset)) return bad("read: bad offset");
      uint64_t fh;
      string_view before;
      if (!ExtractTrailingFh(tail, &fh, &before))
        return bad("read: missing [fh:N]");
      rec->fh = fh;
      return true;
    }
    case OpKind::kWrite: {
      auto f = SplitComma(args);
      if (f.size() != 3) return bad("write: expected 3 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("write: bad ino");
      if (!ParseUint(f[1], &rec->size)) return bad("write: bad size");
      if (!ParseUint(f[2], &rec->offset)) return bad("write: bad offset");
      uint64_t fh;
      string_view before;
      if (!ExtractTrailingFh(tail, &fh, &before))
        return bad("write: missing [fh:N]");
      rec->fh = fh;
      return true;
    }
    case OpKind::kFlush: {
      if (!ParseUint(args, &rec->ino1)) return bad("flush: bad ino");
      uint64_t fh;
      string_view before;
      if (!ExtractTrailingFh(tail, &fh, &before))
        return bad("flush: missing [fh:N]");
      rec->fh = fh;
      return true;
    }
    case OpKind::kRelease: {
      if (!ParseUint(args, &rec->ino1)) return bad("release: bad ino");
      uint64_t fh;
      string_view before;
      if (!ExtractTrailingFh(tail, &fh, &before))
        return bad("release: missing [fh:N]");
      rec->fh = fh;
      return true;
    }
    case OpKind::kFsync: {
      auto f = SplitComma(args);
      if (f.size() != 2) return bad("fsync: expected 2 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("fsync: bad ino");
      int ds;
      if (!ParseInt(f[1], &ds)) return bad("fsync: bad datasync");
      rec->datasync = ds;
      uint64_t fh;
      string_view before;
      if (!ExtractTrailingFh(tail, &fh, &before))
        return bad("fsync: missing [fh:N]");
      rec->fh = fh;
      return true;
    }
    case OpKind::kGetXattr: {
      auto f = SplitComma(args);
      if (f.size() != 2) return bad("getxattr: expected 2 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("getxattr: bad ino");
      rec->name1 = std::string(f[1]);
      return true;
    }
    case OpKind::kRemoveXattr: {
      auto f = SplitComma(args);
      if (f.size() != 2) return bad("removexattr: expected 2 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("removexattr: bad ino");
      rec->name1 = std::string(f[1]);
      return true;
    }
    case OpKind::kListXattr: {
      if (!ParseUint(args, &rec->ino1)) return bad("listxattr: bad ino");
      return true;
    }
    case OpKind::kMkDir: {
      auto f = SplitComma(args);
      if (f.size() != 5) return bad("mkdir: expected 5 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("mkdir: bad parent");
      rec->name1 = std::string(f[1]);
      size_t colon = f[2].rfind(':');
      if (colon == string_view::npos) return bad("mkdir: bad mode field");
      if (!ParseOctalPrefixed(f[2].substr(colon + 1), &rec->mode))
        return bad("mkdir: bad mode value");
      if (ok) {
        if (!ExtractTrailingStrAttrIno(tail, &rec->result_ino))
          return bad("mkdir: bad result attr");
        rec->has_result_ino = true;
      }
      return true;
    }
    case OpKind::kOpenDir: {
      if (!ParseUint(args, &rec->ino1)) return bad("opendir: bad ino");
      uint64_t fh;
      string_view before;
      if (!ExtractTrailingFh(tail, &fh, &before))
        return bad("opendir: missing [fh:N]");
      if (ok) {
        rec->result_fh = fh;
        rec->has_result_fh = true;
      }
      return true;
    }
    case OpKind::kReadDir: {
      if (!ParseUint(args, &rec->ino1)) return bad("readdir: bad ino");
      string_view t = tail;
      if (EndsWith(t, " true")) {
        rec->with_attr = true;
        t.remove_suffix(5);
      } else if (EndsWith(t, " false")) {
        rec->with_attr = false;
        t.remove_suffix(6);
      } else {
        return bad("readdir: missing with_attr suffix");
      }
      uint64_t fh;
      string_view before;
      if (!ExtractTrailingFh(t, &fh, &before))
        return bad("readdir: missing [fh:N]");
      rec->fh = fh;
      // `before` ends with "(<offset> <count>)"; extract offset only.
      size_t open_idx;
      if (!FindTrailingParenBlock(before, &open_idx))
        return bad("readdir: missing (offset count)");
      string_view inner =
          before.substr(open_idx + 1, before.size() - open_idx - 2);
      size_t sp = inner.find(' ');
      if (sp == string_view::npos) return bad("readdir: bad offset/count");
      if (!ParseUint(inner.substr(0, sp), &rec->offset))
        return bad("readdir: bad offset value");
      return true;
    }
    case OpKind::kReleaseDir: {
      if (!ParseUint(args, &rec->ino1)) return bad("releasedir: bad ino");
      uint64_t fh;
      string_view before;
      if (!ExtractTrailingFh(tail, &fh, &before))
        return bad("releasedir: missing [fh:N]");
      rec->fh = fh;
      return true;
    }
    case OpKind::kRmDir: {
      auto f = SplitComma(args);
      if (f.size() != 2) return bad("rmdir: expected 2 args");
      if (!ParseUint(f[0], &rec->ino1)) return bad("rmdir: bad parent");
      rec->name1 = std::string(f[1]);
      return true;
    }
    case OpKind::kStatFs: {
      if (!ParseUint(args, &rec->ino1)) return bad("statfs: bad ino");
      return true;
    }
    default:
      return bad("internal: unhandled op");
  }
}

}  // namespace

const char* OpKindName(OpKind op) {
  for (const auto& e : kOpTable) {
    if (e.kind == op) return e.name;
  }
  return "unknown";
}

LineParseResult ParseAccessLogLine(const std::string& line) {
  LineParseResult res;
  res.status = LineParseStatus::kMalformed;

  string_view sv(line);
  while (!sv.empty() && (sv.back() == '\n' || sv.back() == '\r'))
    sv.remove_suffix(1);
  if (sv.empty()) {
    res.status = LineParseStatus::kIgnoredOther;
    return res;
  }

  if (sv[0] != '[') {
    res.reason = "line does not start with '['";
    return res;
  }
  size_t ts_end = sv.find(']');
  if (ts_end == string_view::npos) {
    res.reason = "unterminated timestamp bracket";
    return res;
  }
  double end_time;
  if (!ParseTimestamp(sv.substr(1, ts_end - 1), &end_time)) {
    res.reason = "malformed timestamp";
    return res;
  }
  size_t pos = ts_end + 1;
  if (pos >= sv.size() || sv[pos] != ' ') {
    res.reason = "malformed spacing after timestamp";
    return res;
  }
  pos++;

  if (pos >= sv.size() || sv[pos] != '[') {
    res.status = LineParseStatus::kIgnoredOther;
    return res;
  }
  size_t logger_end = sv.find(']', pos);
  if (logger_end == string_view::npos) {
    res.reason = "unterminated logger bracket";
    return res;
  }
  if (sv.substr(pos + 1, logger_end - pos - 1) != "fuse_access") {
    res.status = LineParseStatus::kIgnoredOther;
    return res;
  }
  pos = logger_end + 1;
  if (pos >= sv.size() || sv[pos] != ' ') {
    res.reason = "malformed spacing after logger name";
    return res;
  }
  pos++;

  if (pos >= sv.size() || sv[pos] != '[') {
    res.reason = "missing level bracket";
    return res;
  }
  size_t level_end = sv.find(']', pos);
  if (level_end == string_view::npos) {
    res.reason = "unterminated level bracket";
    return res;
  }
  pos = level_end + 1;
  if (pos >= sv.size() || sv[pos] != ' ') {
    res.reason = "malformed spacing after level";
    return res;
  }
  pos++;

  string_view msg = sv.substr(pos);

  string_view content;
  double duration;
  if (!StripDuration(msg, &content, &duration)) {
    res.reason = "missing or malformed duration suffix";
    return res;
  }

  if (StartsWith(content, "start:") || StartsWith(content, "stop:")) {
    res.status = LineParseStatus::kControlRecord;
    return res;
  }

  if (content.empty() || content[0] != '[') {
    res.reason = "missing [pid:uid:gid] prefix";
    return res;
  }
  size_t ctx_end = content.find(']');
  if (ctx_end == string_view::npos) {
    res.reason = "unterminated [pid:uid:gid] bracket";
    return res;
  }
  int32_t pid;
  uint32_t uid, gid;
  if (!ParseCtx(content.substr(1, ctx_end - 1), &pid, &uid, &gid)) {
    res.reason = "malformed pid:uid:gid";
    return res;
  }
  size_t p2 = ctx_end + 1;
  if (p2 >= content.size() || content[p2] != ' ') {
    res.reason = "malformed spacing after pid:uid:gid";
    return res;
  }
  p2++;

  size_t name_end = p2;
  while (name_end < content.size() &&
         std::islower(static_cast<unsigned char>(content[name_end])))
    name_end++;
  string_view opname = content.substr(p2, name_end - p2);
  OpKind op;
  if (!LookupOp(opname, &op)) {
    res.reason = "unknown opname";
    return res;
  }

  size_t p3 = name_end;
  if (p3 >= content.size() || content[p3] != ' ') {
    res.reason = "missing space before args";
    return res;
  }
  p3++;
  if (p3 >= content.size() || content[p3] != '(') {
    res.reason = "missing '(' for args";
    return res;
  }
  int depth = 1;
  size_t p4 = p3 + 1;
  while (p4 < content.size() && depth > 0) {
    if (content[p4] == '(')
      depth++;
    else if (content[p4] == ')')
      depth--;
    if (depth > 0) p4++;
  }
  if (depth != 0) {
    res.reason = "unterminated args parens";
    return res;
  }
  string_view args = content.substr(p3 + 1, p4 - (p3 + 1));

  size_t p5 = p4 + 1;
  if (p5 + 1 >= content.size() || content[p5] != ':' ||
      content[p5 + 1] != ' ') {
    res.reason = "missing '): ' result separator";
    return res;
  }
  p5 += 2;
  string_view tail = content.substr(p5);

  ParsedRecord rec;
  rec.op = op;
  rec.pid = pid;
  rec.uid = uid;
  rec.gid = gid;
  rec.end_time_sec = end_time;
  rec.duration_sec = duration;
  rec.start_time_sec = end_time - duration;

  if (op == OpKind::kUnsupported) {
    res.status = LineParseStatus::kOk;
    res.record = rec;
    return res;
  }

  // `open` embeds its status type after echoing flags; every other op has
  // the status type as the very first token of `tail`.
  if (op != OpKind::kOpen) {
    string_view type, remainder;
    if (!ExtractStatusType(tail, &type, &remainder)) {
      res.reason = "unknown status type";
      return res;
    }
    rec.status_type = std::string(type);
    rec.ok = (type == "OK");
  }

  std::string reason;
  if (!ParseArgsAndTail(op, args, tail, rec.ok, &rec, &reason)) {
    res.reason = reason;
    return res;
  }

  res.status = LineParseStatus::kOk;
  res.record = rec;
  return res;
}

bool RunParserSelfCheck(std::ostream& out) {
  struct Case {
    const char* line;
    LineParseStatus expect_status;
    OpKind expect_op;
    bool expect_ok;
  };

  const std::vector<Case> cases = {
      {"[2026-07-17 10:20:39.666] [fuse_access] [info] [1234:0:0] lookup "
       "(1/foo.txt): OK (2,[-rw-r--r--:0100644,1,0,0,1700000000,1700000000,"
       "1700000000,0]) <0.000123>",
       LineParseStatus::kOk, OpKind::kLookup, true},
      {"[2026-07-17 10:20:39.667] [fuse_access] [info] [1234:0:0] create "
       "(1,bar.txt): OK (3,[-rw-r--r--:0100644,1,0,0,1700000000,1700000000,"
       "1700000000,0]) [fh:5] <0.000321>",
       LineParseStatus::kOk, OpKind::kCreate, true},
      {"[2026-07-17 10:20:39.668] [fuse_access] [info] [1234:0:0] read "
       "(3,4096,0): OK (4096) [fh:5] <0.000045>",
       LineParseStatus::kOk, OpKind::kRead, true},
      {"[2026-07-17 10:20:39.669] [fuse_access] [info] [1234:0:0] write "
       "(3,4096,0): OK (4096) [fh:5] <0.000210>",
       LineParseStatus::kOk, OpKind::kWrite, true},
      {"[2026-07-17 10:20:39.670] [fuse_access] [info] [1234:0:0] open "
       "(3): 2 RDWR OK [fh:5] <0.000012>",
       LineParseStatus::kOk, OpKind::kOpen, true},
      {"[2026-07-17 10:20:39.671] [fuse_access] [info] [1234:0:0] mknod "
       "(1,dev.node,crw-r--r--:0100644): NameTooLong: name(3) too long "
       "(0,[?:000000,0,0,0,0,0,0,0]) <0.000001>",
       LineParseStatus::kOk, OpKind::kMkNod, false},
      {"[2026-07-17 10:20:39.672] [fuse_access] [info] start: OK "
       "<0.000500>",
       LineParseStatus::kControlRecord, OpKind::kUnsupported, false},
      {"[2026-07-17 10:20:39.673] [fuse_access] [info] [1234:0:0] setxattr "
       "(3,user.foo): OK <0.000030>",
       LineParseStatus::kOk, OpKind::kUnsupported, false},
      // Malformed: missing duration suffix entirely.
      {"[2026-07-17 10:20:39.674] [fuse_access] [info] [1234:0:0] statfs "
       "(1): OK",
       LineParseStatus::kMalformed, OpKind::kUnsupported, false},
      // Malformed: unterminated args parens.
      {"[2026-07-17 10:20:39.675] [fuse_access] [info] [1234:0:0] getattr "
       "(1: OK <0.000010>",
       LineParseStatus::kMalformed, OpKind::kUnsupported, false},
      // Not a fuse_access line: should be ignored, not reported malformed.
      {"[2026-07-17 10:20:39.676] [other_logger] [info] unrelated line",
       LineParseStatus::kIgnoredOther, OpKind::kUnsupported, false},
  };

  bool all_ok = true;
  int idx = 0;
  for (const auto& c : cases) {
    idx++;
    LineParseResult r = ParseAccessLogLine(c.line);
    bool pass = (r.status == c.expect_status);
    if (pass && c.expect_status == LineParseStatus::kOk) {
      pass = (r.record.op == c.expect_op) && (r.record.ok == c.expect_ok);
    }
    out << "[self-check] case " << idx << ": " << (pass ? "PASS" : "FAIL");
    if (!pass) {
      out << " (line=\"" << c.line << "\" reason=\"" << r.reason << "\")";
      all_ok = false;
    }
    out << "\n";
  }
  out << "[self-check] " << (all_ok ? "ALL PASSED" : "FAILURES DETECTED")
      << " (" << cases.size() << " cases)\n";
  return all_ok;
}

}  // namespace replay
}  // namespace tools
}  // namespace dingofs
