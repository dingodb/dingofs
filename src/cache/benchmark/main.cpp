
#include <glog/logging.h>

#include <thread>

#include "cache/benchmark/benchmarker.h"
#include "cache/utils/logging.h"

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, false);
  dingofs::cache::InitLogging(argv[0]);

  dingofs::cache::Benchmarker benchmarker;
  auto status = benchmarker.Run();
  if (!status.ok()) {
    std::cerr << "Run benchmarker failed: " << status.ToString() << std::endl;
    return -1;
  }

  benchmarker.Shutdown();

  return 0;
}
