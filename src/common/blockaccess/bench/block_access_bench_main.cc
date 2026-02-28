/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include <iostream>

#include "common/blockaccess/bench/block_access_bench.h"

DECLARE_bool(s3_use_crt_client);
DECLARE_bool(s3_use_thread_pool);
DECLARE_int32(s3_async_thread_num);

static void PrintUsage(const char* prog) {
  std::cout
      << R"(
Block Access Benchmark Tool

Usage:
  )" << prog
      << R"( [command] [options]

Command:
  put             Run PUT benchmark
  async_put       Run ASYNC PUT benchmark
  get             Run GET benchmark
  async_get       Run ASYNC GET benchmark
  range           Run RANGE benchmark

Options:
  --access_type        Access type: file, s3, rados (default: file)
  --data_path          Local file path (default: /tmp/block_bench)
  --s3_bucket          S3 bucket name
  --s3_endpoint        S3 endpoint
  --s3_ak              S3 access key
  --s3_sk              S3 secret key
  --s3_use_crt_client  Use AWS CRT S3 client (default: true)
  --s3_use_thread_pool Use thread pool for async (default: true)
  --s3_async_thread_num Number of async threads (default: 16)
  --rados_pool_name    Rados pool name
  --rados_mon_host     Rados mon host
  --rados_cluster_name Rados cluster name (default: ceph)
  --rados_user_name    Rados user name
  --rados_key          Rados key

  --threads            Number of threads (default: 1)
  --num_ops            Number of operations per thread (default: 10000)
  --block_size         Block size in bytes (default: 4194304)

Performance Options:
  --bind_to_cpu        Bind threads to specific CPU cores for better cache locality (default: false)
  --skip_prefill       Skip data prefill for GET benchmarks (use when data already exists)
  --log_dir            Log directory (default: /tmp)

Example:
  # 1. Write data first
  )" << prog
      << R"( put --access_type=file --num_ops=10000

  # 2. Read existing data (avoid re-writing)
  )" << prog
      << R"( get --access_type=file --num_ops=10000 --skip_prefill

  # Or combine in single run (no prefill needed)
  )" << prog
      << R"( put get --access_type=file --num_ops=10000

Other Examples:
  # File PUT benchmark
  )" << prog
      << R"( put --access_type=file --data_path=/tmp/bench_test --num_ops=10000

  # S3 GET benchmark
  )" << prog
      << R"( get --access_type=s3 --s3_bucket=test --s3_endpoint=http://localhost:9000

  # S3 async benchmark with thread pool
  )" << prog
      << R"( async_put --access_type=s3 --s3_bucket=test --s3_endpoint=http://localhost:9000 --s3_use_thread_pool --s3_async_thread_num=32
)";
}

DEFINE_string(access_type, "file", "Access type: file, s3, rados, fake");
DEFINE_string(data_path, "/tmp/block_bench", "Local file path or rados pool");
DEFINE_string(s3_bucket, "", "S3 bucket name");
DEFINE_string(s3_endpoint, "", "S3 endpoint");
DEFINE_string(s3_ak, "", "S3 access key");
DEFINE_string(s3_sk, "", "S3 secret key");

DEFINE_string(rados_mon_host, "", "Rados mon host");
DEFINE_string(rados_pool_name, "", "Rados pool name");
DEFINE_string(rados_cluster_name, "ceph", "Rados cluster name");
DEFINE_string(rados_user_name, "", "Rados user name");
DEFINE_string(rados_key, "", "Rados key");

DEFINE_uint32(threads, 1, "Number of threads");
DEFINE_uint32(num_ops, 10000, "Number of operations per thread");
DEFINE_uint32(block_size, 4194304, "Block size in bytes (default: 4MB)");

// Performance flags
DEFINE_bool(bind_to_cpu, false,
            "Bind threads to specific CPU cores for better cache locality");
DEFINE_bool(
    skip_prefill, false,
    "Skip data prefill for GET benchmarks (use when data already exists)");
DEFINE_uint32(prefill_delay_sec, 0,
              "Delay seconds after prefill before benchmark (for storage "
              "backend to settle)");

namespace dingofs {
namespace blockaccess {

class BlockAccessBenchTool {
 public:
  BlockAccessBenchTool() = default;

  int Run(int argc, char* argv[]) {
    if (argc < 2) {
      PrintUsage(argv[0]);
      return 0;
    }

    for (int i = 1; i < argc; ++i) {
      if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
        PrintUsage(argv[0]);
        return 0;
      }
    }

    std::string command = argv[1];
    bool valid_command = false;
    std::vector<std::string> valid_commands = {"put", "async_put", "get",
                                               "async_get", "range"};
    for (const auto& cmd : valid_commands) {
      if (command == cmd) {
        valid_command = true;
        break;
      }
    }
    if (!valid_command) {
      std::cerr << "Invalid command: " << command << '\n';
      PrintUsage(argv[0]);
      return -1;
    }

    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_log_dir.empty()) {
      ::FLAGS_log_dir = "/tmp";
    }
    google::InitGoogleLogging(argv[0]);

    BenchOptions opts;

    if (FLAGS_access_type == "file") {
      opts.accesser_type = AccesserType::kLocalFile;
      opts.data_path = FLAGS_data_path;
    } else if (FLAGS_access_type == "s3") {
      opts.accesser_type = AccesserType::kS3;
      opts.s3_bucket = FLAGS_s3_bucket;
      opts.s3_endpoint = FLAGS_s3_endpoint;
      opts.s3_ak = FLAGS_s3_ak;
      opts.s3_sk = FLAGS_s3_sk;
      opts.s3_use_crt_client = FLAGS_s3_use_crt_client;
      opts.s3_use_thread_pool = FLAGS_s3_use_thread_pool;
      opts.s3_async_thread_num = FLAGS_s3_async_thread_num;
    } else if (FLAGS_access_type == "rados") {
      opts.accesser_type = AccesserType::kRados;
      opts.rados_pool = FLAGS_rados_pool_name;
      opts.rados_mon_host = FLAGS_rados_mon_host;
      opts.rados_cluster_name = FLAGS_rados_cluster_name;
      opts.rados_user_name = FLAGS_rados_user_name;
      opts.rados_key = FLAGS_rados_key;
    } else if (FLAGS_access_type == "fake") {
      opts.accesser_type = AccesserType::kLocalFile;
    } else {
      std::cerr << "Unknown access type: " << FLAGS_access_type << '\n';
      return -1;
    }

    opts.num_ops = FLAGS_num_ops * FLAGS_threads;
    opts.num_ops_per_thread = FLAGS_num_ops;
    opts.threads = FLAGS_threads;
    opts.block_size = FLAGS_block_size;

    // Performance options
    opts.bind_to_cpu = FLAGS_bind_to_cpu;
    opts.skip_prefill = FLAGS_skip_prefill;
    opts.prefill_delay_sec = FLAGS_prefill_delay_sec;

    opts.run_put = (command == "put");
    opts.run_async_put = (command == "async_put");
    opts.run_get = (command == "get");
    opts.run_async_get = (command == "async_get");
    opts.run_range = (command == "range");

    std::cout << "=== Benchmark Options ===" << '\n';
    std::cout << "  command: " << command << '\n';
    std::cout << "  access_type: " << FLAGS_access_type << '\n';
    std::cout << "  threads: " << FLAGS_threads << '\n';
    std::cout << "  num_ops_per_thread: " << FLAGS_num_ops << '\n';
    std::cout << "  total_ops: " << opts.num_ops << '\n';
    std::cout << "  block_size: " << opts.block_size << '\n';
    std::cout << "  bind_to_cpu: " << (opts.bind_to_cpu ? "true" : "false")
              << '\n';
    std::cout << "=========================" << '\n';

    BlockAccessBench bench(opts);
    auto status = bench.Init();
    if (!status.ok()) {
      std::cerr << "Init failed: " << status.ToString() << '\n';
      return -1;
    }

    bench.Run();

    google::ShutdownGoogleLogging();
    return 0;
  }
};

}  // namespace blockaccess
}  // namespace dingofs

int main(int argc, char* argv[]) {
  return dingofs::blockaccess::BlockAccessBenchTool().Run(argc, argv);
}
