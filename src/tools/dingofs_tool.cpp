/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: 2021-11-02
 * Author: chengyi01
 */

#include "tools/dingofs_tool.h"

namespace dingofs {
namespace tools {

void DingofsTool::PrintHelp() {
  std::cout << "Example :" << std::endl;
  std::cout << programe_ << " " << command_ << " " << kConfPathHelp;
}

int DingofsTool::Run() {
  if (Init() < 0) {
    return -1;
  }
  int ret = RunCommand();
  if (ret != 0) {
    PrintError();
  }
  return ret;
}

void DingofsTool::PrintError() {
  if (errorOutput_.tellp() > 0) {
    // only show if errorOutput is not empty
    std::cerr << "[error]" << std::endl;
    std::cerr << errorOutput_.str();
  }
}

int DingofsToolMetric::Init(const std::shared_ptr<MetricClient>& metricClient) {
  metricClient_ = metricClient;
  return 0;
}

void DingofsToolMetric::PrintHelp() {
  DingofsTool::PrintHelp();
  std::cout << " [-rpcTimeoutMs=" << FLAGS_rpcTimeoutMs << "]"
            << " [-rpcRetryTimes=" << FLAGS_rpcRetryTimes << "]";
}

void DingofsToolMetric::AddUpdateFlagsFunc(
    const std::function<void(dingofs::utils::Configuration*,
                             google::CommandLineFlagInfo*)>& func) {
  updateFlagsFunc_.push_back(func);
}

int DingofsToolMetric::RunCommand() {
  int ret = 0;
  for (auto const& i : addr2SubUri) {
    std::string value;
    MetricStatusCode statusCode =
        metricClient_->GetMetric(i.first, i.second, &value);
    AfterGetMetric(i.first, i.second, value, statusCode);
  }

  if (ProcessMetrics() != 0) {
    ret = -1;
  }
  return ret;
}

void DingofsToolMetric::AddUpdateFlags() {
  // rpcTimeout and rpcRetrytimes is default
  AddUpdateFlagsFunc(SetRpcTimeoutMs);
  AddUpdateFlagsFunc(SetRpcRetryTimes);
}

void DingofsToolMetric::UpdateFlags() {
  dingofs::utils::Configuration conf;
  conf.SetConfigPath(FLAGS_confPath);
  if (!conf.LoadConfig()) {
    std::cerr << "load configure file " << FLAGS_confPath << " failed!"
              << std::endl;
  }
  google::CommandLineFlagInfo info;

  for (auto& i : updateFlagsFunc_) {
    i(&conf, &info);
  }
}

void DingofsToolMetric::AddAddr2Suburi(
    const std::pair<std::string, std::string>& addrSubUri) {
  addr2SubUri.push_back(addrSubUri);
}

int DingofsToolMetric::Init() {
  // add need update flags
  AddUpdateFlags();
  UpdateFlags();
  InitHostsAddr();
  return 0;
}

}  // namespace tools
}  // namespace dingofs
