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

#include "cache/tools/bench/client/command.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>

#include "cache/tools/bench/client/options.h"
#include "cache/tools/bench/client/runner.h"
#include "cache/tools/bench/common/flags.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace client {

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
    fs.PrintHelp(program, program + " --mds_addrs=HOST:PORT --fsid=ID --op=<put|range> [OPTIONS]",
                 "  $ cb client --mds_addrs=127.0.0.1:7400 --fsid=1 --op=put --key_dist=seq --keyspace=100000 --threads=8\n"
                 "  $ cb client --mds_addrs=127.0.0.1:7400 --fsid=1 --op=range --key_dist=zipf --keyspace=100000 --qps=20000 --duration=60 --warmup=10\n");
    return 0;
  }
  err = Validate(&o);
  if (!err.empty()) {
    std::cerr << "error: " << err << "  (run `" << program << " --help`)\n";
    return 1;
  }

  // The MDS client and block accesser read FLAGS_mds_addrs.
  google::SetCommandLineOption("mds_addrs", o.mds_addrs.c_str());

  Runner runner(o);
  return runner.Run().ok() ? 0 : 1;
}

}  // namespace client
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
