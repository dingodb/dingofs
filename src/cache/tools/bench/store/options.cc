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

#include "cache/tools/bench/store/options.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace store {

void RegisterFlags(FlagSet* fs, Options* o) {
  fs->Str("layer", &o->layer_str, "diskcache | mem | blockcache");
  fs->Str("dir", &o->dir, "cache directory (required for diskcache/blockcache)");
  fs->Str("rw", &o->rw_str, "read|write|randread|randwrite|randrw");
  fs->Size("bs", &o->bs, "block size, <= 4MiB");
  fs->U64("nrfiles", &o->nrfiles, "distinct blocks (key space)");
  fs->U32("jobs", &o->jobs, "concurrent caller threads; 0 = same as iodepth");
  fs->U32("iodepth", &o->iodepth, "in-flight AIO cap; also sizes slab pool");
  fs->U32("rwmixread", &o->rwmixread, "read percent for randrw");
  fs->U32("store_size_mb", &o->store_size_mb, "cache capacity in MiB");
  fs->U32("runtime", &o->runtime_s, "run seconds; 0 = use io_size");
  fs->Size("io_size", &o->io_size, "total bytes; 0 = use runtime");
  fs->U32("warmup", &o->warmup_s, "warmup seconds excluded from summary");
  fs->Switch("prep", &o->prep, "pre-fill blocks before read tests");
  fs->Switch("keep", &o->keep, "keep the cache dir after the run");
  fs->U32("report_interval", &o->report_interval_s, "progress interval (s)");
}

std::string LayerName(Layer layer) {
  switch (layer) {
    case Layer::kDiskCache: return "diskcache";
    case Layer::kMem: return "mem";
    case Layer::kBlockCache: return "blockcache";
  }
  return "unknown";
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
  if (o->layer_str == "diskcache" || o->layer_str == "disk") {
    o->layer = Layer::kDiskCache;
  } else if (o->layer_str == "mem" || o->layer_str == "memory") {
    o->layer = Layer::kMem;
  } else if (o->layer_str == "blockcache" || o->layer_str == "block") {
    o->layer = Layer::kBlockCache;
  } else {
    return "layer must be diskcache|mem|blockcache: " + o->layer_str;
  }

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

  if (o->NeedsDisk() && o->dir.empty()) {
    return "--dir is required for diskcache/blockcache";
  }
  if (o->bs == 0 || o->bs > 4ULL * 1024 * 1024) return "bs must be in (0, 4MiB]";
  if (o->nrfiles == 0) return "nrfiles must be > 0";
  if (o->iodepth == 0 || o->iodepth > 4096) return "iodepth must be in [1, 4096]";
  if (o->jobs == 0) o->jobs = o->iodepth;
  if (o->rwmixread > 100) return "rwmixread must be in [0, 100]";
  if (o->store_size_mb == 0) return "store_size_mb must be > 0";
  if (o->runtime_s == 0 && o->io_size == 0) return "set --runtime or --io_size";
  if (o->report_interval_s == 0) return "report_interval must be > 0";
  return "";
}

}  // namespace store
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
