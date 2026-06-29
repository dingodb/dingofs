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

#include "cache/tools/bench/store/command.h"

#include <gflags/gflags.h>

#include <iostream>

#include "cache/tools/bench/common/flags.h"
#include "cache/tools/bench/store/options.h"
#include "cache/tools/bench/store/runner.h"

namespace dingofs {
namespace cache {

// FLAGS_iodepth is DEFINEd inside namespace dingofs::cache (aio_queue.cc), so
// the matching DECLARE must live in the same namespace to resolve at link time.
DECLARE_uint32(iodepth);  // InflightTracker / AioQueue / slab pool sizing

namespace bench {
namespace store {

int Run(const std::string& program, int argc, char** argv) {
  Options o;
  FlagSet fs;
  RegisterFlags(&fs, &o);

  std::string err;
  if (!fs.Parse(argc, argv, &err)) {
    std::cerr << "error: " << err << "  (run `" << program << " --help`)\n";
    return 1;
  }
  if (fs.HelpRequested()) {
    fs.PrintHelp(program, program + " --layer=<diskcache|mem|blockcache> [OPTIONS]",
                 "  $ cb store --layer=diskcache --dir=/data/t --rw=randread --bs=1MiB --iodepth=64\n"
                 "  $ cb store --layer=blockcache --dir=/data/t --rw=randread --nrfiles=4096\n"
                 "  $ cb store --layer=mem --rw=randread --bs=64KiB --nrfiles=8192\n");
    return 0;
  }
  err = Validate(&o);
  if (!err.empty()) {
    std::cerr << "error: " << err << "  (run `" << program << " --help`)\n";
    return 1;
  }

  FLAGS_iodepth = o.iodepth;  // slab pool / AioQueue / InflightTracker sizing

  Runner runner(o);
  return runner.Run().ok() ? 0 : 1;
}

}  // namespace store
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
