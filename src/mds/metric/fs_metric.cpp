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
 * Created Date: Tue Jul 27 16:41:09 CST 2021
 * Author: wuhanqing
 */

#include "mds/metric/fs_metric.h"

#include "dingofs/mds.pb.h"
#include "mds/metric/metric.h"

namespace dingofs {
namespace mds {

using dingofs::pb::mds::FsStatsData;
using dingofs::pb::mds::FSStatusCode;

void FsMetric::OnMount(const std::string& fsname, const Mountpoint& mp) {
  std::lock_guard<Mutex> lock(mtx_);

  auto iter = metrics_.find(fsname);
  if (iter == metrics_.end()) {
    auto r = metrics_.emplace(fsname, new FsMountMetric(fsname));
    iter = r.first;
  }

  iter->second->OnMount(mp);
}

void FsMetric::OnUnMount(const std::string& fsname, const Mountpoint& mp) {
  std::lock_guard<Mutex> lock(mtx_);

  auto iter = metrics_.find(fsname);
  if (iter == metrics_.end()) {
    return;
  }

  iter->second->OnUnMount(mp);
}

void FsMetric::SetFsStats(const std::string& fsname,
                          const FsStatsData& fs_stats_data) {
  std::lock_guard<Mutex> lock(mtx_);

  auto iter = fsStatsMetrics_.find(fsname);
  if (iter == fsStatsMetrics_.end()) {
    auto r = fsStatsMetrics_.emplace(fsname, new FSStatsMetric(fsname));
    iter = r.first;
  }

  iter->second->SetFsStats(fs_stats_data);
}

FSStatusCode FsMetric::GetFsStats(const std::string& fsname,
                                  FsStatsData* fs_stats_data) {
  std::lock_guard<Mutex> lock(mtx_);

  auto iter = fsStatsMetrics_.find(fsname);
  if (iter == fsStatsMetrics_.end()) {
    return FSStatusCode::UNKNOWN_ERROR;
  }

  iter->second->GetFsStats(fs_stats_data);
  return FSStatusCode::OK;
}

FSStatusCode FsMetric::GetFsPerSecondStats(const std::string& fsname,
                                           FsStatsData* fs_stats_data) {
  std::lock_guard<Mutex> lock(mtx_);

  auto iter = fsStatsMetrics_.find(fsname);
  if (iter == fsStatsMetrics_.end()) {
    return FSStatusCode::UNKNOWN_ERROR;
  }

  iter->second->GetFsPerSecondStats(fs_stats_data);
  return FSStatusCode::OK;
}

}  // namespace mds
}  // namespace dingofs
