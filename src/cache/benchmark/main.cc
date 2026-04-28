
#include <glog/logging.h>

#include <iostream>

#include "cache/benchmark/benchmarker.h"
#include "common/logging.h"

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, false);

  dingofs::Logger::Init("cache-bench");

  // Init benchmarker
  dingofs::cache::Benchmarker benchmarker;
  auto status = benchmarker.Start();
  if (!status.ok()) {
    std::cerr << "Failed to initialize benchmarker: " << status.ToString()
              << '\n';
    return -1;
  }

  // Run until finish
  benchmarker.RunUntilFinish();

  return 0;
}
