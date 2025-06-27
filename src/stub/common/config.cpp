/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include "stub/common/config.h"

#include "utils/string_util.h"

namespace dingofs {
namespace stub {
namespace common {

using ::dingofs::utils::Configuration;

void InitMdsOption(Configuration* conf, MdsOption* mds_opt) {
  conf->GetValueFatalIfFail("mdsOpt.mdsMaxRetryMS", &mds_opt->mdsMaxRetryMS);
  conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.maxRPCTimeoutMS",
                            &mds_opt->rpcRetryOpt.maxRPCTimeoutMS);
  conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.rpcTimeoutMs",
                            &mds_opt->rpcRetryOpt.rpcTimeoutMs);
  conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.rpcRetryIntervalUS",
                            &mds_opt->rpcRetryOpt.rpcRetryIntervalUS);
  conf->GetValueFatalIfFail(
      "mdsOpt.rpcRetryOpt.maxFailedTimesBeforeChangeAddr",
      &mds_opt->rpcRetryOpt.maxFailedTimesBeforeChangeAddr);
  conf->GetValueFatalIfFail(
      "mdsOpt.rpcRetryOpt.normalRetryTimesBeforeTriggerWait",
      &mds_opt->rpcRetryOpt.normalRetryTimesBeforeTriggerWait);
  conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.waitSleepMs",
                            &mds_opt->rpcRetryOpt.waitSleepMs);
  std::string adds;
  conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.addrs", &adds);

  std::vector<std::string> mds_addr;
  dingofs::utils::SplitString(adds, ",", &mds_addr);
  mds_opt->rpcRetryOpt.addrs.assign(mds_addr.begin(), mds_addr.end());
}

}  // namespace common
}  // namespace stub
}  // namespace dingofs