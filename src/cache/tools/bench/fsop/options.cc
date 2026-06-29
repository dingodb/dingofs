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

#include "cache/tools/bench/fsop/options.h"

#include <sstream>
#include <stdexcept>

namespace dingofs {
namespace cache {
namespace bench {
namespace fsop {

namespace {
std::vector<std::string> Split(const std::string& text) {
  std::vector<std::string> out;
  std::string item;
  std::istringstream is(text);
  while (std::getline(is, item, ',')) {
    if (!item.empty()) out.push_back(item);
  }
  return out;
}
}  // namespace

void RegisterFlags(FlagSet* fs, Options* o) {
  fs->Str("dir", &o->dir, "test directory (required, real disk for O_DIRECT)");
  fs->Str("sizes", &o->sizes_str, "comma-separated block sizes to test");
  fs->Str("threads", &o->threads_str, "comma-separated thread counts to test");
  fs->U32("iters", &o->iters, "write+read cycles per thread in each cell");
  fs->Switch("direct", &o->direct, "use O_DIRECT (sizes 4KiB-aligned)");
  fs->Switch("fallocate", &o->fallocate, "measure fallocate");
  fs->Switch("fsync", &o->fsync, "measure fdatasync after the write");
  fs->Switch("shared_dir", &o->shared_dir,
             "all threads share one dir tree (isolates directory-lock cost)");
  fs->Switch("keep", &o->keep, "keep the test directory after the run");
}

std::string Validate(Options* o) {
  if (o->dir.empty()) return "--dir is required";
  if (o->iters == 0) return "iters must be > 0";

  o->sizes.clear();
  for (const auto& part : Split(o->sizes_str)) {
    uint64_t size = 0;
    if (!ParseSize(part, &size) || size == 0) {
      return "invalid size in --sizes: " + part;
    }
    if (o->direct && size % 4096 != 0) {
      return "with --direct, every size must be 4KiB-aligned: " + part;
    }
    o->sizes.push_back(size);
  }
  if (o->sizes.empty()) return "--sizes is empty";

  o->threads.clear();
  for (const auto& part : Split(o->threads_str)) {
    try {
      const unsigned long n = std::stoul(part);
      if (n == 0 || n > 4096) return "thread count must be in [1, 4096]: " + part;
      o->threads.push_back(static_cast<uint32_t>(n));
    } catch (const std::exception&) {
      return "invalid thread count in --threads: " + part;
    }
  }
  if (o->threads.empty()) return "--threads is empty";
  return "";
}

}  // namespace fsop
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
