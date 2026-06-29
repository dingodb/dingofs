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

#include "cache/tools/bench/aio/options.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace aio {

void RegisterFlags(FlagSet* fs, Options* o) {
  fs->Str("dir", &o->dir, "test directory (required, real disk for O_DIRECT)");
  fs->Str("rw", &o->rw_str, "read|write|randread|randwrite|randrw");
  fs->Size("bs", &o->bs, "i/o block size");
  fs->Size("filesize", &o->filesize, "size of each test file");
  fs->U32("nrfiles", &o->nrfiles, "number of test files");
  fs->U32("iodepth", &o->iodepth, "outstanding i/o (concurrency), <= 4096");
  fs->U32("rwmixread", &o->rwmixread, "read percent for randrw");
  fs->U32("runtime", &o->runtime_s, "run seconds; 0 = use io_size");
  fs->Size("io_size", &o->io_size, "total bytes; 0 = use runtime");
  fs->U32("warmup", &o->warmup_s, "warmup seconds excluded from summary");
  fs->Switch("direct", &o->direct, "use O_DIRECT (bs 4KiB-aligned)");
  fs->Switch("fixed", &o->fixed, "use registered (fixed) io_uring buffers");
  fs->Switch("prep", &o->prep, "pre-fill files before read tests");
  fs->Switch("keep", &o->keep, "keep test files after the run");
  fs->U32("report_interval", &o->report_interval_s, "progress interval (s)");
}

std::string RwName(Rw rw) {
  switch (rw) {
    case Rw::kRead: return "read";
    case Rw::kWrite: return "write";
    case Rw::kRandRead: return "randread";
    case Rw::kRandWrite: return "randwrite";
    case Rw::kRandRw: return "randrw";
  }
  return "unknown";
}

std::string Validate(Options* o) {
  if (o->rw_str == "read") {
    o->rw = Rw::kRead;
  } else if (o->rw_str == "write") {
    o->rw = Rw::kWrite;
  } else if (o->rw_str == "randread") {
    o->rw = Rw::kRandRead;
  } else if (o->rw_str == "randwrite") {
    o->rw = Rw::kRandWrite;
  } else if (o->rw_str == "randrw") {
    o->rw = Rw::kRandRw;
  } else {
    return "rw must be read|write|randread|randwrite|randrw: " + o->rw_str;
  }

  if (o->dir.empty()) return "--dir is required";
  if (o->bs == 0) return "bs must be > 0";
  if (o->filesize < o->bs) return "filesize must be >= bs";
  if (o->nrfiles == 0) return "nrfiles must be > 0";
  if (o->iodepth == 0 || o->iodepth > 4096) return "iodepth must be in [1, 4096]";
  if (o->rwmixread > 100) return "rwmixread must be in [0, 100]";
  if (o->runtime_s == 0 && o->io_size == 0) {
    return "set --runtime or --io_size";
  }
  if (o->direct && (o->bs % 4096 != 0 || o->filesize % 4096 != 0)) {
    return "with --direct, bs and filesize must be 4KiB-aligned";
  }
  if (o->report_interval_s == 0) return "report_interval must be > 0";
  return "";
}

}  // namespace aio
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
