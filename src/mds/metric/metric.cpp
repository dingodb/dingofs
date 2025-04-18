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
 * Created Date: Mon Jul 26 18:03:27 CST 2021
 * Author: wuhanqing
 */

#include "mds/metric/metric.h"

#include <glog/logging.h>

#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "utils/string_util.h"

namespace dingofs {
namespace mds {

void FsMountMetric::OnMount(const Mountpoint& mp) {
  std::string key = Key(mp);

  {
    std::lock_guard<Mutex> lock(mtx_);

    auto* metric = new bvar::Status<std::string>();
    metric->expose(key);

    // value format is: {"host": "1.2.3.4", "port": "1234", "dir": "/tmp"}
    metric->set_value("{\"host\": \"%s\", \"port\": \"%d\", \"dir\": \"%s\"}",
                      mp.hostname().c_str(), mp.port(), mp.path().c_str());

    mps_.emplace(std::move(key), metric);
  }
}

void FsMountMetric::OnUnMount(const Mountpoint& mp) {
  {
    std::lock_guard<Mutex> lock(mtx_);
    mps_.erase(Key(mp));
  }
}

void FsMountMetric::OnUpdateMountCount(const uint32_t& mount_count) {
  count_.set_value(mount_count);
}

std::string FsMountMetric::Key(const Mountpoint& mp) {
  return "fs_mount_" + fsname_ + "_" + mp.hostname() + "_" +
         std::to_string(mp.port()) + "_" + mp.path();
}

// set fs cluster statistics, fsStatsData contains delta statistics since last
// read
void FSStatsMetric::SetFsStats(const FsStatsData& fs_stats_data) {
  readBytes_.count << fs_stats_data.readbytes();
  readQps_.count << fs_stats_data.readqps();
  writeBytes_.count << fs_stats_data.writebytes();
  writeQps_.count << fs_stats_data.writeqps();
  s3ReadBytes_.count << fs_stats_data.s3readbytes();
  s3ReadQps_.count << fs_stats_data.s3readqps();
  s3WriteBytes_.count << fs_stats_data.s3writebytes();
  s3WriteQps_.count << fs_stats_data.s3writeqps();
}

// get fs cluster statistics, fsStatsData contains the sum of all the client
// total amount statistics
void FSStatsMetric::GetFsStats(FsStatsData* fs_stats_data) const {
  fs_stats_data->set_readbytes(readBytes_.count.get_value());
  fs_stats_data->set_readqps(readQps_.count.get_value());
  fs_stats_data->set_writebytes(writeBytes_.count.get_value());
  fs_stats_data->set_writeqps(writeQps_.count.get_value());
  fs_stats_data->set_s3readbytes(s3ReadBytes_.count.get_value());
  fs_stats_data->set_s3readqps(s3ReadQps_.count.get_value());
  fs_stats_data->set_s3writebytes(s3WriteBytes_.count.get_value());
  fs_stats_data->set_s3writeqps(s3WriteQps_.count.get_value());
}

// get fs cluster per second statistics, fsStatsData contains recently
// statistics for per second
void FSStatsMetric::GetFsPerSecondStats(FsStatsData* fs_stats_data) const {
  fs_stats_data->set_readbytes(readBytes_.value.get_value());
  fs_stats_data->set_readqps(readQps_.value.get_value());
  fs_stats_data->set_writebytes(writeBytes_.value.get_value());
  fs_stats_data->set_writeqps(writeQps_.value.get_value());
  fs_stats_data->set_s3readbytes(s3ReadBytes_.value.get_value());
  fs_stats_data->set_s3readqps(s3ReadQps_.value.get_value());
  fs_stats_data->set_s3writebytes(s3WriteBytes_.value.get_value());
  fs_stats_data->set_s3writeqps(s3WriteQps_.value.get_value());
}

}  // namespace mds
}  // namespace dingofs
