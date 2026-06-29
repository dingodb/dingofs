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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iomanip>
#include <iostream>
#include <string>

#include "cache/tools/bench/aio/command.h"
#include "cache/tools/bench/client/command.h"
#include "cache/tools/bench/common/command.h"
#include "cache/tools/bench/fs/command.h"
#include "cache/tools/bench/fsop/command.h"
#include "cache/tools/bench/store/command.h"

namespace {

using dingofs::cache::bench::SubCommand;

const SubCommand kCommands[] = {
    {"aio", "io_uring/AioQueue raw async disk i/o",
     dingofs::cache::bench::aio::Run},
    {"fs", "LocalFileSystem whole-block WriteFile/ReadFile",
     dingofs::cache::bench::fs::Run},
    {"store", "DiskCache / MemCache / LocalBlockCache store layer",
     dingofs::cache::bench::store::Run},
    {"client", "TierBlockCache full client stack (needs mds)",
     dingofs::cache::bench::client::Run},
    {"fsop", "per-syscall latency breakdown (open/rename/fallocate/...)",
     dingofs::cache::bench::fsop::Run},
};

void PrintTopHelp() {
  std::cout << "cb - DingoFS cache-layer benchmark\n\n";
  std::cout << "Usage:\n";
  std::cout << "  cb <command> [options]\n";
  std::cout << "  cb <command> --help\n\n";
  std::cout << "Commands:\n";
  for (const auto& c : kCommands) {
    std::cout << "  " << std::left << std::setw(8) << c.name << c.desc << '\n';
  }
  std::cout << "\nRun `cb <command> --help` for command-specific options.\n";
}

const SubCommand* Find(const std::string& name) {
  for (const auto& c : kCommands) {
    if (name == c.name) {
      return &c;
    }
  }
  return nullptr;
}

}  // namespace

int main(int argc, char** argv) {
  // Keep the report clean: hide the engine's INFO chatter.
  FLAGS_minloglevel = 1;
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);

  if (argc < 2) {
    PrintTopHelp();
    return 0;
  }

  const std::string sub = argv[1];
  if (sub == "-h" || sub == "--help" || sub == "help") {
    if (argc >= 3) {
      const SubCommand* c = Find(argv[2]);
      if (c != nullptr) {
        char* help_argv[] = {argv[2], const_cast<char*>("--help")};
        return c->run(std::string("cb ") + c->name, 2, help_argv);
      }
    }
    PrintTopHelp();
    return 0;
  }

  const SubCommand* c = Find(sub);
  if (c == nullptr) {
    std::cerr << "unknown command: " << sub << "\n\n";
    PrintTopHelp();
    return 1;
  }
  return c->run(std::string("cb ") + c->name, argc - 1, argv + 1);
}
