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

#ifndef DINGOFS_MDS_SERVER_H_
#define DINGOFS_MDS_SERVER_H_

#include <atomic>
#include <string>

#include "brpc/server.h"
#include "json/value.h"
#include "mds/background/cache_member_sync.h"
#include "mds/background/fsinfo_sync.h"
#include "mds/background/gc.h"
#include "mds/background/heartbeat.h"
#include "mds/background/monitor.h"
#include "mds/background/quota_sync.h"
#include "mds/cachegroup/member_manager.h"
#include "mds/common/crontab.h"
#include "mds/coordinator/coordinator_client.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/notify_buddy.h"
#include "mds/mds/mds_meta.h"
#include "mds/service/debug_service.h"
#include "mds/service/fsstat_service.h"
#include "mds/service/mds_service.h"
#include "mds/storage/storage.h"
#include "utils/configuration.h"

namespace dingofs {
namespace mds {

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

  bool InitCacheGroupMemberManager();

  bool InitNotifyBuddy();

  bool InitFileSystem();

  bool InitHeartbeat();

  bool InitFsInfoSync();

  bool InitCacheMemberSynchronizer();

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
  FsInfoSyncSPtr GetFsInfoSync();
  CoordinatorClientSPtr GetCoordinatorClient();
  notify::NotifyBuddySPtr GetNotifyBuddy();
  FileSystemSetSPtr GetFileSystemSet();
  MonitorSPtr GetMonitor();
  OperationProcessorSPtr GetOperationProcessor();
  QuotaSynchronizerSPtr GetQuotaSynchronizer();
  GcProcessorSPtr GetGcProcessor();
  CacheGroupMemberManagerSPtr GetCacheGroupMemberManager();
  CacheMemberSynchronizerSPtr& GetCacheMemberSynchronizer();

  MDSServiceImplUPtr& GetMDSService();
  DebugServiceImplUPtr& GetDebugService();
  FsStatServiceImplUPtr& GetFsStatService();

  void Run();

  void Stop();

  void DescribeByJson(Json::Value& value);

 private:
  explicit Server() = default;
  ~Server();

  std::atomic<bool> stop_{false};

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

  // worker set for quota
  WorkerSetSPtr quota_worker_set_;

  // filesystem
  FileSystemSetSPtr file_system_set_;

  // heartbeat to coordinator
  HeartbeatSPtr heartbeat_;

  // fs info sync
  FsInfoSyncSPtr fs_info_sync_;

  // cache group manager
  CacheGroupMemberManagerSPtr cache_group_member_manager_;

  // cache group member sync
  CacheMemberSynchronizerSPtr cache_member_synchronizer_;

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

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_SERVER_H_