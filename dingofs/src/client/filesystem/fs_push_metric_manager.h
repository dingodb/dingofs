// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_PUSH_METRIC_MANAGER_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_PUSH_METRIC_MANAGER_H_

#include <atomic>
#include <memory>

#include "base/timer/timer.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace client {
namespace filesystem {

using base::timer::Timer;
using ::dingofs::pb::mds::FsStatsData;
using ::dingofs::stub::rpcclient::MdsClient;

class FsPushMetricManager {
 public:
  FsPushMetricManager(const std::string fsname,
                      std::shared_ptr<MdsClient> mds_client,
                      std::shared_ptr<Timer> timer)
      : fsname_(fsname), mds_client_(mds_client), timer_(std::move(timer)) {}

  virtual ~FsPushMetricManager() = default;

  void Start();

  void Stop();

  bool IsRunning() const;

 private:
  void PushClientMetrics();

  void DoPushClientMetrics();

  static FsStatsData GetClientMetrics();

  const std::string fsname_;
  std::shared_ptr<MdsClient> mds_client_;
  std::shared_ptr<Timer> timer_;
  std::atomic<bool> running_{false};
  // store last client push metrics
  FsStatsData last_client_metrics_;
};

inline FsStatsData operator-(const FsStatsData& current,
                             const FsStatsData& last) {
  FsStatsData delta;
  // filesystem delta statistics
  delta.set_readbytes(current.readbytes() - last.readbytes());
  delta.set_readqps(current.readqps() - last.readqps());
  delta.set_writebytes(current.writebytes() - last.writebytes());
  delta.set_writeqps(current.writeqps() - last.writeqps());
  // s3 delta statistics
  delta.set_s3readbytes(current.s3readbytes() - last.s3readbytes());
  delta.set_s3readqps(current.s3readqps() - last.s3readqps());
  delta.set_s3writebytes(current.s3writebytes() - last.s3writebytes());
  delta.set_s3writeqps(current.s3writeqps() - last.s3writeqps());

  return delta;
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_PUSH_METRIC_MANAGER_H_