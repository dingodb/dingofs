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

#include "cache/tools/bench/aio/command.h"

#include <gflags/gflags.h>

#include <iostream>

#include "cache/tools/bench/aio/options.h"
#include "cache/tools/bench/aio/runner.h"
#include "cache/tools/bench/common/flags.h"

namespace dingofs {
namespace cache {

// FLAGS_iodepth is DEFINEd inside namespace dingofs::cache (aio_queue.cc), so
// the matching DECLARE must live in the same namespace to resolve at link time.
DECLARE_uint32(iodepth);  // AioQueue completion array / slab pool sizing

namespace bench {
namespace aio {

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
                 "  $ cb aio --dir=/data/t --rw=randread --bs=4KiB --iodepth=128 --runtime=30\n"
                 "  $ cb aio --dir=/data/t --rw=randread --bs=4KiB --iodepth=128 --threads=4 --runtime=30\n"
                 "  $ cb aio --dir=/data/t --rw=write --bs=4MiB --nrfiles=4 --filesize=4GiB --iodepth=16\n");
    return 0;
  }
  err = Validate(&o);
  if (!err.empty()) {
    std::cerr << "error: " << err << "  (run `" << program << " --help`)\n";
    return 1;
  }

  // AioQueue reads FLAGS_iodepth for its completion array and slab pool sizing.
  FLAGS_iodepth = o.iodepth;

  Runner runner(o);
  return runner.Run().ok() ? 0 : 1;
}

}  // namespace aio
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
