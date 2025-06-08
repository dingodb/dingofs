#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <thread>

#include "base/time/time.h"
#include "cache/benchmark/benchmarker.h"
#include "cache/common/const.h"
#include "cache/config/benchmark.h"
#include "cache/config/block_cache.h"
#include "cache/config/config.h"
#include "cache/utils/access_log.h"

namespace dingofs {
namespace cache {

BenchmarkOption* g_option;

}  // namespace cache
}  // namespace dingofs

using dingofs::cache::Benchmarker;
using dingofs::cache::BenchmarkOption;
using dingofs::cache::FLAGS_logdir;
using dingofs::cache::g_option;
using dingofs::cache::InitCacheAccessLog;

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, false);
  g_option = new BenchmarkOption();

  FLAGS_log_dir = FLAGS_logdir;
  static const std::string program_name = "cache-bench";
  google::InitGoogleLogging(program_name.c_str());

  bool yes = InitCacheAccessLog(FLAGS_logdir);
  if (!yes) {
    return -1;
  }

  Benchmarker c;
  auto status = c.Run();
  if (!status.ok()) {
    return -1;
  }

  c.Stop();
}
