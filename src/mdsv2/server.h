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

#ifndef DINGOFS_MDSV2_SERVER_H_
#define DINGOFS_MDSV2_SERVER_H_

#include <atomic>
#include <string>

#include "brpc/server.h"
#include "mdsv2/background/fsinfo_sync.h"
#include "mdsv2/background/gc.h"
#include "mdsv2/background/heartbeat.h"
#include "mdsv2/background/monitor.h"
#include "mdsv2/background/quota_sync.h"
#include "mdsv2/common/crontab.h"
#include "mdsv2/coordinator/coordinator_client.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/filesystem/notify_buddy.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/service/debug_service.h"
#include "mdsv2/service/fsstat_service.h"
#include "mdsv2/service/mds_service.h"
#include "mdsv2/storage/storage.h"
#include "options/mdsv2/app.h"
#include "utils/configuration.h"

namespace dingofs {
namespace mdsv2 {

using ::dingofs::utils::Configuration;

class Server {
 public:
  static Server& GetInstance();

  bool InitConfig(const std::string& path);

  bool InitLog();

  bool InitMDSMeta();

  bool InitCoordinatorClient(const std::string& coor_url);

  bool InitStorage(const std::string& store_url);

  bool InitOperationProcessor();

  bool InitNotifyBuddy();

  bool InitFileSystem();

  bool InitHeartbeat();

  bool InitFsInfoSync();

  bool InitMonitor();

  bool InitQuotaSynchronizer();

  bool InitGcProcessor();

  bool InitCrontab();

  bool InitService();

  std::string GetPidFilePath();
  std::string GetListenAddr();
  MDSMeta& GetMDSMeta();
  MDSMetaMapSPtr GetMDSMetaMap();
  KVStorageSPtr GetKVStorage();
  HeartbeatSPtr GetHeartbeat();
  FsInfoSync& GetFsInfoSync();
  CoordinatorClientSPtr GetCoordinatorClient();
  notify::NotifyBuddySPtr GetNotifyBuddy();
  FileSystemSetSPtr GetFileSystemSet();
  MonitorSPtr GetMonitor();
  OperationProcessorSPtr GetOperationProcessor();
  QuotaSynchronizerSPtr GetQuotaSynchronizer();
  GcProcessorSPtr GetGcProcessor();

  void Run();

  void Stop();

 private:
  explicit Server() = default;
  ~Server();

  std::atomic<bool> stop_{false};

  options::mdsv2::AppOption app_option_;

  // mds self info
  MDSMeta self_mds_meta_;
  // all cluster mds
  MDSMetaMapSPtr mds_meta_map_;

  // This is manage crontab, like heartbeat.
  CrontabManager crontab_manager_;
  // Crontab config
  std::vector<CrontabConfig> crontab_configs_;

  // coordinator client
  CoordinatorClientSPtr coordinator_client_;

  // backend kv storage
  KVStorageSPtr kv_storage_;

  // mutation merger
  OperationProcessorSPtr operation_processor_;

  // filesystem
  FileSystemSetSPtr file_system_set_;

  // heartbeat to coordinator
  HeartbeatSPtr heartbeat_;

  // fs info sync
  FsInfoSync fs_info_sync_;

  // notify buddy
  notify::NotifyBuddySPtr notify_buddy_;

  // mds monitor
  MonitorSPtr monitor_;

  // quota synchronizer
  QuotaSynchronizerSPtr quota_synchronizer_;

  // gc
  GcProcessorSPtr gc_processor_;

  // service
  MDSServiceImplUPtr mds_service_;
  DebugServiceImplUPtr debug_service_;
  FsStatServiceImplUPtr fs_stat_service_;

  brpc::Server brpc_server_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVER_H_