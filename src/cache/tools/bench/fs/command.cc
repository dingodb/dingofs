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

#include "cache/tools/bench/fs/command.h"

#include <gflags/gflags.h>

#include <iostream>

#include "cache/tools/bench/common/flags.h"
#include "cache/tools/bench/fs/options.h"
#include "cache/tools/bench/fs/runner.h"

namespace dingofs {
namespace cache {

// FLAGS_iodepth is DEFINEd inside namespace dingofs::cache (aio_queue.cc), so
// the matching DECLARE must live in the same namespace to resolve at link time.
DECLARE_uint32(iodepth);  // InflightTracker / AioQueue / slab pool sizing

namespace bench {
namespace fs {

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
    fs.PrintHelp(program, program + " --dir=DIR [OPTIONS]",
                 "  $ cb fs --dir=/data/t --rw=randread --bs=1MiB --nrfiles=4096 --iodepth=64\n"
                 "  $ cb fs --dir=/data/t --rw=write --bs=4MiB --jobs=32 --iodepth=32\n");
    return 0;
  }
  err = Validate(&o);
  if (!err.empty()) {
    std::cerr << "error: " << err << "  (run `" << program << " --help`)\n";
    return 1;
  }

  // LocalFileSystem reads FLAGS_iodepth for InflightTracker / AioQueue / slab.
  FLAGS_iodepth = o.iodepth;

  Runner runner(o);
  return runner.Run().ok() ? 0 : 1;
}

}  // namespace fs
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
