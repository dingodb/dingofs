// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mds/common/helper.h"

void InitLog(const std::string& log_dir) {
  if (!dingofs::mds::Helper::IsExistPath(log_dir)) {
    dingofs::mds::Helper::CreateDirectories(log_dir);
  }

  FLAGS_logbufsecs = 0;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;

  std::string program_name = "mds_unit_test";

  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(
      google::GLOG_INFO,
      fmt::format("{}/{}.info.log.", log_dir, program_name).c_str());
  google::SetLogDestination(
      google::GLOG_WARNING,
      fmt::format("{}/{}.warn.log.", log_dir, program_name).c_str());
  google::SetLogDestination(
      google::GLOG_ERROR,
      fmt::format("{}/{}.error.log.", log_dir, program_name).c_str());
  google::SetLogDestination(
      google::GLOG_FATAL,
      fmt::format("{}/{}.fatal.log.", log_dir, program_name).c_str());
  google::SetStderrLogging(google::GLOG_FATAL);
}

int main(int argc, char** argv) {
  InitLog("./log");

  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (testing::FLAGS_gtest_filter == "*") {
    std::string default_run_case;
    // common

    default_run_case += "MetaDataCodecTest.*";
    default_run_case += ":HelperTest.*";
    default_run_case += ":AutoIncrementIdGeneratorTest.*";
    default_run_case += ":FileSystemSetTest.*";
    default_run_case += ":InodeCacheTest.*";
    default_run_case += ":DentryCacheTest.*";
    default_run_case += ":FileSystemTest.*";

    default_run_case += ":CoorDistributionLockTest.*";
    default_run_case += ":MutationMergerTest.*";
    default_run_case += ":MutationProcessorTest.*";
    default_run_case += ":HashPartitionHelperTest.*";

    testing::GTEST_FLAG(filter) = default_run_case;
  }

  return RUN_ALL_TESTS();
}
