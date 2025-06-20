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

#include "client/vfs/statistics/fs_stats_manager.h"

#include "common/status.h"
#include "glog/logging.h"
#include "stub/metric/metric.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::dingofs::pb::mdsv2::FsStatsData;
using ::dingofs::stub::metric::FSMetric;
using ::dingofs::stub::metric::ObjectMetric;

void FsStatsManager::PushFsStats() {
  FsStatsData current_fs_stats = GetCurrentFsStatsData();
  FsStatsData delta_fs_stats = current_fs_stats - last_fs_stats_;
  if (IsEmpty(delta_fs_stats)) return;

  Status s = mds_client_->PushFsStatsToMDS(fsname_, delta_fs_stats);
  if (BAIDU_LIKELY(s.ok())) {
    VLOG(12) << "PushFsStats success, fs_name: " << fsname_
             << ",delta fsstats data:[" << FsStatsDataToStr(delta_fs_stats)
             << "]";
    last_fs_stats_ = current_fs_stats;

  } else {
    LOG(ERROR) << "PushFsStats failed, fs_name: " << fsname_
               << ", status: " << s.ToString() << ", delta metrics data:["
               << FsStatsDataToStr(delta_fs_stats) << "]";
  }
}

FsStatsData FsStatsManager::GetCurrentFsStatsData() {
  FsStatsData fs_stats_data;

  auto& fs = FSMetric::GetInstance();
  auto& object = ObjectMetric::GetInstance();

  // filesystem metrics
  fs_stats_data.set_read_bytes(fs.user_read.bps.count.get_value());
  fs_stats_data.set_read_qps(fs.user_read.qps.count.get_value());
  fs_stats_data.set_write_bytes(fs.user_write.bps.count.get_value());
  fs_stats_data.set_write_qps(fs.user_write.qps.count.get_value());

  // s3 or rados metrics
  fs_stats_data.set_s3_read_bytes(object.read_object.bps.count.get_value());
  fs_stats_data.set_s3_read_qps(object.read_object.qps.count.get_value());
  fs_stats_data.set_s3_write_bytes(object.write_object.bps.count.get_value());
  fs_stats_data.set_s3_write_qps(object.write_object.qps.count.get_value());

  return fs_stats_data;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs