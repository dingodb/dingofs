#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>

#include "cache/tools/cache_bench/options.h"
#include "cache/tools/cache_bench/runner.h"
#include "common/logging.h"

int main(int argc, char** argv) {
  google::SetUsageMessage(dingofs::cache::Usage(argv[0]));
  google::ParseCommandLineFlags(&argc, &argv, true);

  dingofs::cache::Options options;
  auto status = dingofs::cache::LoadOptions(&options);
  if (!status.ok()) {
    std::cerr << "Invalid cache bench options: " << status.ToString() << "\n\n"
              << dingofs::cache::Usage(argv[0]) << '\n';
    return 1;
  }

  dingofs::Logger::Init("cache-bench");

  dingofs::cache::Runner runner(options);
  status = runner.Run();
  if (!status.ok()) {
    std::cerr << "Cache bench failed: " << status.ToString() << '\n';
    return 1;
  }

  return 0;
}
