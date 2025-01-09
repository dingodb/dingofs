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

#ifndef DINGOFS_SRC_MDS_METRIC_METRIC_H_
#define DINGOFS_SRC_MDS_METRIC_METRIC_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "dingofs/mds.pb.h"
#include "mds/common/types.h"

namespace dingofs {
namespace mds {

using dingofs::pb::mds::FsStatsData;
using dingofs::pb::mds::Mountpoint;

// Metric for a filesystem
// includes filesystem mount number and filesystem mountpoint lists
class FsMountMetric {
 public:
  explicit FsMountMetric(const std::string& fsname)
      : fsname_(fsname),
        count_("fs_" + fsname + "_mount_count"),
        mtx_(),
        mps_() {}

  void OnMount(const Mountpoint& mp);
  void OnUnMount(const Mountpoint& mp);

 private:
  // mountpoint metric key
  // format is fs_mount_${fsname}_${host}_${port}_${mountdir}
  std::string Key(const Mountpoint& mp);

 private:
  const std::string fsname_;

  // current number of fs mountpoints
  bvar::Adder<int64_t> count_;

  using MountPointMetric =
      std::unordered_map<std::string,
                         std::unique_ptr<bvar::Status<std::string>>>;
  // protect mps_
  Mutex mtx_;

  MountPointMetric mps_;
};

// metric stats per second
struct PerSecondMetric {
  bvar::Adder<uint64_t> count;                   // total count
  bvar::PerSecond<bvar::Adder<uint64_t>> value;  // average count persecond
  PerSecondMetric(const std::string& prefix, const std::string& name)
      : count(prefix, name + "_total_count"), value(prefix, name, &count, 1) {}
};

class FSStatsMetric {
 public:
  explicit FSStatsMetric(const std::string& fsname)
      : fsname_(fsname),
        readBytes_("fs_", fsname + "_read_bytes"),
        readQps_("fs_", fsname + "_read_qps"),
        writeBytes_("fs_", fsname + "_write_bytes"),
        writeQps_("fs_", fsname + "_write_qps"),
        s3ReadBytes_("fs_", fsname + "_s3_read_bytes"),
        s3ReadQps_("fs_", fsname + "_s3_read_qps"),
        s3WriteBytes_("fs_", fsname + "_s3_write_bytes"),
        s3WriteQps_("fs_", fsname + "_s3_write_qps") {}

  void SetFsStats(const FsStatsData& fs_stats_data);

  void GetFsStats(FsStatsData* fs_stats_data) const;

  void GetFsPerSecondStats(FsStatsData* fs_stats_data) const;

 private:
  std::string fsname_;
  PerSecondMetric readBytes_;
  PerSecondMetric readQps_;
  PerSecondMetric writeBytes_;
  PerSecondMetric writeQps_;
  PerSecondMetric s3ReadBytes_;
  PerSecondMetric s3ReadQps_;
  PerSecondMetric s3WriteBytes_;
  PerSecondMetric s3WriteQps_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_METRIC_METRIC_H_
