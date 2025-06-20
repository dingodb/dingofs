// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CLIENT_VFS_STATISTICS_FS_STATS_MANAGER_H_
#define DINGOFS_CLIENT_VFS_STATISTICS_FS_STATS_MANAGER_H_

#include <memory>
#include <ostream>
#include <sstream>

#include "client/vfs/meta/v2/mds_client.h"
#include "dingofs/mdsv2.pb.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::dingofs::client::vfs::v2::MDSClient;
using ::dingofs::pb::mdsv2::FsStatsData;
class FsStatsManager;
using FsStatsManagerUPtr = std::unique_ptr<FsStatsManager>;

// TODO: Proto(mdsv2) FsStatsData member name s3xxxx change to objectxxxx

inline std::string FsStatsDataToStr(const FsStatsData& fs_stat_data) {
  std::ostringstream oss;
  oss << "read_bytes: " << fs_stat_data.read_bytes() << ", "
      << "write_bytes: " << fs_stat_data.write_bytes() << ", "
      << "read_qps: " << fs_stat_data.read_qps() << ", "
      << "write_qps: " << fs_stat_data.write_qps() << ", "
      << "object_read_bytes: " << fs_stat_data.s3_read_bytes() << ", "
      << "object_write_bytes: " << fs_stat_data.s3_write_bytes() << ", "
      << "object_read_qps: " << fs_stat_data.s3_read_qps() << ", "
      << "object_write_qps: " << fs_stat_data.s3_write_qps();

  return oss.str();
}

inline bool IsEmpty(const FsStatsData& fs_stat_data) {
  return fs_stat_data.read_bytes() == 0 && fs_stat_data.write_bytes() == 0 &&
         fs_stat_data.read_qps() == 0 && fs_stat_data.write_qps() == 0 &&
         fs_stat_data.s3_read_bytes() == 0 &&
         fs_stat_data.s3_write_bytes() == 0 &&
         fs_stat_data.s3_read_qps() == 0 && fs_stat_data.s3_write_qps() == 0;
}

inline FsStatsData operator-(const FsStatsData& current,
                             const FsStatsData& last) {
  FsStatsData delta;

  // filesystem delta statistics
  delta.set_read_bytes(current.read_bytes() - last.read_bytes());
  delta.set_read_qps(current.read_qps() - last.read_qps());
  delta.set_write_bytes(current.write_bytes() - last.write_bytes());
  delta.set_write_qps(current.write_qps() - last.write_qps());

  // s3、rados delta statistics
  delta.set_s3_read_bytes(current.s3_read_bytes() - last.s3_read_bytes());
  delta.set_s3_read_qps(current.s3_read_qps() - last.s3_read_qps());
  delta.set_s3_write_bytes(current.s3_write_bytes() - last.s3_write_bytes());
  delta.set_s3_write_qps(current.s3_write_qps() - last.s3_write_qps());

  return delta;
}

class FsStatsManager {
 public:
  FsStatsManager(const std::string& fs_name,
                 std::shared_ptr<MDSClient> mds_client)
      : fsname_(fs_name), mds_client_(mds_client) {}

  virtual ~FsStatsManager() = default;

  void PushFsStats();

  static FsStatsManagerUPtr New(const std::string& fs_name,
                                std::shared_ptr<MDSClient> mds_client) {
    return std::make_unique<FsStatsManager>(fs_name, mds_client);
  }

 private:
  FsStatsData GetCurrentFsStatsData();

  const std::string fsname_;
  std::shared_ptr<MDSClient> mds_client_;
  FsStatsData last_fs_stats_;  // store last fs stats
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_STATISTICS_FS_STATS_MANAGER_H_