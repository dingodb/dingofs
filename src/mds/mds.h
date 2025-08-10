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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef DINGOFS_SRC_MDS_MDS_H_
#define DINGOFS_SRC_MDS_MDS_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>

#include "mds/cachegroup/member_manager.h"
#include "mds/dlock/dlock.h"
#include "mds/fs_manager.h"
#include "mds/heartbeat/copyset_conf_generator.h"
#include "mds/heartbeat/heartbeat_manager.h"
#include "mds/heartbeat/metaserver_healthy_checker.h"
#include "mds/idgenerator/chunkid_allocator.h"
#include "mds/leader_election/leader_election.h"
#include "mds/schedule/schedule_define.h"
#include "mds/topology/topology.h"
#include "mds/topology/topology_config.h"
#include "mds/topology/topology_metric.h"
#include "utils/configuration.h"

namespace dingofs {
namespace mds {

// TODO(split InitEtcdConf): split this InitEtcdConf to a single module

struct MDSOptions {
  int dummyPort;
  std::string mdsListenAddr;
  MetaserverOptions metaserverOptions;
  // TODO(add EtcdConf): add etcd configure

  topology::TopologyOption topologyOptions;
  heartbeat::HeartbeatOption heartbeatOption;
  schedule::ScheduleOption scheduleOption;

  dlock::DLockOptions dLockOptions;
};

class MDS {
 public:
  MDS();
  ~MDS();

  MDS(const MDS&) = delete;
  MDS& operator=(const MDS&) = delete;

  void InitOptions(std::shared_ptr<utils::Configuration> conf);
  void Init();
  void Run();
  void Stop();

  // Start dummy server for metric
  void StartDummyServer();

  // Start leader election
  void StartCompaginLeader();

 private:
  void InitEtcdClient();
  void InitEtcdConf(EtcdConf* etcd_conf);
  bool CheckEtcd();

  void InitLeaderElectionOption(election::LeaderElectionOptions* option);
  void InitLeaderElection(const election::LeaderElectionOptions& option);

  void InitHeartbeatOption(heartbeat::HeartbeatOption* heartbeat_option);
  void InitScheduleOption(schedule::ScheduleOption* schedule_option);

  void InitDLockOptions(dlock::DLockOptions* d_lock_options);

  void InitMetaServerOption(MetaserverOptions* metaserver_option);
  void InitTopologyOption(topology::TopologyOption* topology_option);

  void InitTopology(const topology::TopologyOption& option);

  void InitTopologyManager(const topology::TopologyOption& option);

  void InitTopologyMetricService(const topology::TopologyOption& option);

  void InitHeartbeatManager();

  void InitCoordinator();

  void InitFsManagerOptions(FsManagerOption* fs_manager_option);

  void InitCacheGroupOption();
  void InitCacheGroup();

  // mds configuration items
  std::shared_ptr<utils::Configuration> conf_;
  // initialized or not
  bool inited_;
  // running as the main MDS or not
  bool running_;
  std::shared_ptr<FsManager> fsManager_;
  std::shared_ptr<FsStorage> fsStorage_;
  std::shared_ptr<MetaserverClient> metaserverClient_;
  std::shared_ptr<ChunkIdAllocator> chunkIdAllocator_;
  std::shared_ptr<topology::TopologyImpl> topology_;
  std::shared_ptr<schedule::TopologyManager> topologyManager_;
  std::shared_ptr<heartbeat::Coordinator> coordinator_;
  std::shared_ptr<heartbeat::HeartbeatManager> heartbeatManager_;
  std::shared_ptr<topology::TopologyMetricService> topologyMetricService_;
  MDSOptions options_;

  bool etcdClientInited_;
  std::shared_ptr<dingofs::kvstorage::EtcdClientImp> etcdClient_;

  std::shared_ptr<dingofs::election::LeaderElection> leaderElection_;

  bvar::Status<std::string> status_;

  std::string etcdEndpoint_;

  std::shared_ptr<cachegroup::CacheGroupMemberManager>
      cache_group_member_manager_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_MDS_H_
