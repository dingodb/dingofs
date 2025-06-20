
#include <glog/logging.h>

#include <iostream>
#include <thread>

#include "cache/benchmark/benchmarker.h"
#include "cache/utils/logging.h"

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, false);

  dingofs::cache::InitLogging(argv[0]);

  // Init benchmarker
  dingofs::cache::Benchmarker benchmarker;
  auto status = benchmarker.Init();
  if (!status.ok()) {
    std::cerr << "Failed to initialize benchmarker: " << status.ToString()
              << std::endl;
    return -1;
  }

  // Run until finish
  benchmarker.RunUntilFinish();

  return 0;
}
