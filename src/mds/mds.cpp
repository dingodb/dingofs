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

#include "mds/mds.h"

#include <brpc/channel.h>
#include <brpc/server.h>
#include <glog/logging.h>

#include <utility>

#include "blockaccess/block_accesser_factory.h"
#include "common/version.h"
#include "mds/cachegroup/cache_group_member_manager.h"
#include "mds/cachegroup/cache_group_member_service.h"
#include "mds/cachegroup/config.h"
#include "mds/heartbeat/heartbeat_service.h"
#include "mds/mds_service.h"
#include "mds/topology/topology_service.h"
#include "mds/topology/topology_storge_etcd.h"

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace dingofs {
namespace mds {

using election::LeaderElection;
using election::LeaderElectionOptions;
using kvstorage::EtcdClientImp;
using mds::dlock::DLock;
using mds::dlock::DLockOptions;
using mds::heartbeat::HeartbeatOption;
using mds::heartbeat::HeartbeatServiceImpl;
using mds::schedule::Coordinator;
using mds::schedule::ScheduleMetrics;
using mds::schedule::ScheduleOption;
using mds::schedule::TopoAdapterImpl;
using mds::topology::DefaultIdGenerator;
using mds::topology::DefaultTokenGenerator;
using mds::topology::TopologyImpl;
using mds::topology::TopologyManager;
using mds::topology::TopologyMetricService;
using mds::topology::TopologyServiceImpl;
using mds::topology::TopologyStorageCodec;
using mds::topology::TopologyStorageEtcd;
using topology::TopologyOption;
using utils::Configuration;

MDS::MDS()
    : conf_(),
      inited_(false),
      running_(false),
      fsManager_(),
      fsStorage_(),
      metaserverClient_(),
      topology_(),
      options_(),
      etcdClientInited_(false),
      etcdClient_(),
      leaderElection_(),
      status_(),
      etcdEndpoint_() {}

MDS::~MDS() = default;

void MDS::InitOptions(std::shared_ptr<utils::Configuration> conf) {
  conf_ = std::move(conf);
  conf_->GetValueFatalIfFail("mds.listen.addr", &options_.mdsListenAddr);
  conf_->GetValueFatalIfFail("mds.dummy.port", &options_.dummyPort);

  InitMetaServerOption(&options_.metaserverOptions);
  InitTopologyOption(&options_.topologyOptions);
  InitScheduleOption(&options_.scheduleOption);
  InitDLockOptions(&options_.dLockOptions);
}

void MDS::InitMetaServerOption(MetaserverOptions* metaserver_option) {
  conf_->GetValueFatalIfFail("metaserver.addr",
                             &metaserver_option->metaserverAddr);
  conf_->GetValueFatalIfFail("metaserver.rpcTimeoutMs",
                             &metaserver_option->rpcTimeoutMs);
  conf_->GetValueFatalIfFail("metaserver.rpcRertyTimes",
                             &metaserver_option->rpcRetryTimes);
  conf_->GetValueFatalIfFail("metaserver.rpcRetryIntervalUs",
                             &metaserver_option->rpcRetryIntervalUs);
}

void MDS::InitTopologyOption(TopologyOption* topology_option) {
  conf_->GetValueFatalIfFail("mds.topology.TopologyUpdateToRepoSec",
                             &topology_option->topologyUpdateToRepoSec);
  conf_->GetValueFatalIfFail("mds.topology.MaxPartitionNumberInCopyset",
                             &topology_option->maxPartitionNumberInCopyset);
  conf_->GetValueFatalIfFail("mds.topology.IdNumberInPartition",
                             &topology_option->idNumberInPartition);
  conf_->GetValueFatalIfFail("mds.topology.CreatePartitionNumber",
                             &topology_option->createPartitionNumber);
  conf_->GetValueFatalIfFail("mds.topology.MaxCopysetNumInMetaserver",
                             &topology_option->maxCopysetNumInMetaserver);
  conf_->GetValueFatalIfFail("mds.topology.UpdateMetricIntervalSec",
                             &topology_option->UpdateMetricIntervalSec);
}

void MDS::InitScheduleOption(ScheduleOption* schedule_option) {
  conf_->GetValueFatalIfFail("mds.enable.recover.scheduler",
                             &schedule_option->enableRecoverScheduler);
  conf_->GetValueFatalIfFail("mds.enable.copyset.scheduler",
                             &schedule_option->enableCopysetScheduler);
  conf_->GetValueFatalIfFail("mds.enable.leader.scheduler",
                             &schedule_option->enableLeaderScheduler);
  conf_->GetValueFatalIfFail("mds.copyset.scheduler.balanceRatioPercent",
                             &schedule_option->balanceRatioPercent);

  conf_->GetValueFatalIfFail("mds.recover.scheduler.intervalSec",
                             &schedule_option->recoverSchedulerIntervalSec);
  conf_->GetValueFatalIfFail("mds.copyset.scheduler.intervalSec",
                             &schedule_option->copysetSchedulerIntervalSec);
  conf_->GetValueFatalIfFail("mds.leader.scheduler.intervalSec",
                             &schedule_option->leaderSchedulerIntervalSec);

  conf_->GetValueFatalIfFail("mds.schduler.operator.concurrent",
                             &schedule_option->operatorConcurrent);
  conf_->GetValueFatalIfFail("mds.schduler.transfer.limitSec",
                             &schedule_option->transferLeaderTimeLimitSec);
  conf_->GetValueFatalIfFail("mds.scheduler.add.limitSec",
                             &schedule_option->addPeerTimeLimitSec);
  conf_->GetValueFatalIfFail("mds.scheduler.remove.limitSec",
                             &schedule_option->removePeerTimeLimitSec);
  conf_->GetValueFatalIfFail("mds.scheduler.change.limitSec",
                             &schedule_option->changePeerTimeLimitSec);
  conf_->GetValueFatalIfFail("mds.scheduler.metaserver.cooling.timeSec",
                             &schedule_option->metaserverCoolingTimeSec);
}

void MDS::InitDLockOptions(DLockOptions* d_lock_options) {
  conf_->GetValueFatalIfFail("dlock.ttl_ms", &d_lock_options->ttlMs);
  conf_->GetValueFatalIfFail("dlock.try_timeout_ms",
                             &d_lock_options->tryTimeoutMs);
  conf_->GetValueFatalIfFail("dlock.try_interval_ms",
                             &d_lock_options->tryIntervalMs);
}

void MDS::InitFsManagerOptions(FsManagerOption* fs_manager_option) {
  conf_->GetValueFatalIfFail("mds.fsmanager.backEndThreadRunInterSec",
                             &fs_manager_option->backEndThreadRunInterSec);

  LOG_IF(ERROR, !conf_->GetUInt32Value("mds.fsmanager.client.timeoutSec",
                                       &fs_manager_option->clientTimeoutSec))
      << "Get `mds.fsmanager.client.timeoutSec` from conf error, use "
         "default value: "
      << fs_manager_option->clientTimeoutSec;

  LOG_IF(ERROR,
         !conf_->GetUInt32Value("mds.fsmanager.reloadSpaceConcurrency",
                                &fs_manager_option->spaceReloadConcurrency))
      << "Get `mds.fsmanager.reloadSpaceConcurrency` from conf error, use "
         "default value: "
      << fs_manager_option->spaceReloadConcurrency;

  blockaccess::InitAwsSdkConfig(
      conf_.get(),
      &fs_manager_option->block_access_option.s3_options.aws_sdk_config);
  blockaccess::InitBlockAccesserThrottleOptions(
      conf_.get(), &fs_manager_option->block_access_option.throttle_options);
}

void MDS::Init() {
  LOG(INFO) << "Init MDS start";
  InitEtcdClient();

  fsStorage_ = std::make_shared<PersisKVStorage>(etcdClient_);
  metaserverClient_ =
      std::make_shared<MetaserverClient>(options_.metaserverOptions);
  auto dlock = std::make_shared<DLock>(options_.dLockOptions, etcdClient_);

  block_accesser_factory_ =
      std::make_shared<blockaccess::BlockAccesserFactory>();

  // init topology
  InitTopology(options_.topologyOptions);
  InitTopologyMetricService(options_.topologyOptions);
  InitTopologyManager(options_.topologyOptions);
  InitCoordinator();
  InitHeartbeatManager();
  InitCacheGroup();

  FsManagerOption fs_manager_option;
  InitFsManagerOptions(&fs_manager_option);
  fs_manager_option.block_accesser_factory = block_accesser_factory_;

  fsManager_ =
      std::make_shared<FsManager>(fsStorage_, metaserverClient_,
                                  topologyManager_, dlock, fs_manager_option);
  LOG_IF(FATAL, !fsManager_->Init()) << "fsManager Init fail";

  chunkIdAllocator_ = std::make_shared<ChunkIdAllocator>(etcdClient_);
  CHECK(chunkIdAllocator_->Init()) << "init chunk id allocator fail";

  inited_ = true;

  LOG(INFO) << "Init MDS success";
}

void MDS::InitTopology(const TopologyOption& option) {
  auto topology_id_generator = std::make_shared<DefaultIdGenerator>();
  auto topology_token_generator = std::make_shared<DefaultTokenGenerator>();

  auto codec = std::make_shared<TopologyStorageCodec>();
  auto topology_storage =
      std::make_shared<TopologyStorageEtcd>(etcdClient_, codec);
  LOG(INFO) << "init topologyStorage success.";

  topology_ = std::make_shared<TopologyImpl>(
      topology_id_generator, topology_token_generator, topology_storage);
  LOG_IF(FATAL, topology_->Init(option) < 0) << "init topology fail.";
  LOG(INFO) << "init topology success.";
}

void MDS::InitTopologyManager(const TopologyOption& option) {
  topologyManager_ =
      std::make_shared<TopologyManager>(topology_, metaserverClient_);
  topologyManager_->Init(option);
  LOG(INFO) << "init topologyManager success.";
}

void MDS::InitTopologyMetricService(const TopologyOption& option) {
  topologyMetricService_ = std::make_shared<TopologyMetricService>(topology_);
  topologyMetricService_->Init(option);
  LOG(INFO) << "init topologyMetricService success.";
}

void MDS::InitCoordinator() {
  auto schedule_metrics = std::make_shared<ScheduleMetrics>(topology_);
  auto topo_adapter =
      std::make_shared<TopoAdapterImpl>(topology_, topologyManager_);
  coordinator_ = std::make_shared<Coordinator>(topo_adapter);
  coordinator_->InitScheduler(options_.scheduleOption, schedule_metrics);
}

void MDS::Run() {
  LOG(INFO) << "Run MDS";
  if (!inited_) {
    LOG(ERROR) << "MDS not inited yet!";
    return;
  }

  // set mds version in metric
  dingofs::ExposeDingoVersion();

  LOG_IF(FATAL, topology_->Run()) << "run topology module fail";
  topologyMetricService_->Run();
  coordinator_->Run();
  heartbeatManager_->Run();
  fsManager_->Run();

  brpc::Server server;
  // add heartbeat service
  HeartbeatServiceImpl heartbeat_service(heartbeatManager_);
  LOG_IF(FATAL, server.AddService(&heartbeat_service,
                                  brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
      << "add heartbeatService error";

  // add mds service
  MdsServiceImpl mds_service(fsManager_, chunkIdAllocator_);
  LOG_IF(FATAL,
         server.AddService(&mds_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
      << "add mdsService error";

  // add topology service
  TopologyServiceImpl topology_service(topologyManager_);
  LOG_IF(FATAL, server.AddService(&topology_service,
                                  brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
      << "add topologyService error";

  // add cachegroup member service
  cachegroup::CacheGroupMemberServiceImpl cache_group_member_service(
      cache_group_member_manager_);
  auto rc = server.AddService(&cache_group_member_service,
                              brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(FATAL) << "Add cache group member service failed, rc = " << rc;
  }

  // start rpc server
  brpc::ServerOptions option;
  LOG_IF(FATAL, server.Start(options_.mdsListenAddr.c_str(), &option) != 0)
      << "start brpc server error";
  running_ = true;

  brpc::FLAGS_graceful_quit_on_sigterm = true;
  server.RunUntilAskedToQuit();
}

void MDS::Stop() {
  LOG(INFO) << "Stop MDS";
  if (!running_) {
    LOG(WARNING) << "Stop MDS, but MDS is not running, return OK";
    return;
  }
  brpc::AskToQuit();
  heartbeatManager_->Stop();
  coordinator_->Stop();
  topologyMetricService_->Stop();
  fsManager_->Uninit();
  topology_->Stop();
  cache_group_member_manager_->Shutdown();
}

void MDS::StartDummyServer() {
  conf_->ExposeMetric("dingofs_mds_config");
  status_.expose("dingofs_mds_status");
  status_.set_value("follower");

  // set mds version in metric
  dingofs::ExposeDingoVersion();

  LOG_IF(FATAL, 0 != brpc::StartDummyServerAt(options_.dummyPort))
      << "Start dummy server failed";
}

void MDS::StartCompaginLeader() {
  InitEtcdClient();

  LeaderElectionOptions election_option;
  InitLeaderElectionOption(&election_option);
  election_option.etcdCli = etcdClient_;
  election_option.campaginPrefix = "";

  InitLeaderElection(election_option);

  while (0 != leaderElection_->CampaignLeader()) {
    LOG(INFO) << leaderElection_->GetLeaderName()
              << " compagin for leader again";
  }

  LOG(INFO) << "Compagin leader success, I am leader now";
  status_.set_value("leader");
  leaderElection_->StartObserverLeader();
}

void MDS::InitEtcdClient() {
  if (etcdClientInited_) {
    return;
  }

  EtcdConf etcd_conf;
  InitEtcdConf(&etcd_conf);

  int etcd_timeout = 0;
  int etcd_retry_times = 0;
  conf_->GetValueFatalIfFail("etcd.operation.timeoutMs", &etcd_timeout);
  conf_->GetValueFatalIfFail("etcd.retry.times", &etcd_retry_times);

  etcdClient_ = std::make_shared<EtcdClientImp>();

  int r = etcdClient_->Init(etcd_conf, etcd_timeout, etcd_retry_times);
  LOG_IF(FATAL, r != EtcdErrCode::EtcdOK)
      << "Init etcd client error: " << r
      << ", etcd address: " << std::string(etcd_conf.Endpoints, etcd_conf.len)
      << ", etcdtimeout: " << etcd_conf.DialTimeout
      << ", operation timeout: " << etcd_timeout
      << ", etcd retrytimes: " << etcd_retry_times;

  LOG_IF(FATAL, !CheckEtcd()) << "Check etcd failed";

  LOG(INFO) << "Init etcd client succeeded, etcd address: "
            << std::string(etcd_conf.Endpoints, etcd_conf.len)
            << ", etcdtimeout: " << etcd_conf.DialTimeout
            << ", operation timeout: " << etcd_timeout
            << ", etcd retrytimes: " << etcd_retry_times;

  etcdClientInited_ = true;
}

void MDS::InitEtcdConf(EtcdConf* etcd_conf) {
  conf_->GetValueFatalIfFail("etcd.endpoint", &etcdEndpoint_);
  conf_->GetValueFatalIfFail("etcd.dailtimeoutMs", &etcd_conf->DialTimeout);

  LOG(INFO) << "etcd.endpoint: " << etcdEndpoint_;
  LOG(INFO) << "etcd.dailtimeoutMs: " << etcd_conf->DialTimeout;

  etcd_conf->len = etcdEndpoint_.size();
  etcd_conf->Endpoints = etcdEndpoint_.data();
}

bool MDS::CheckEtcd() {
  std::string out;
  int r = etcdClient_->Get("test", &out);

  if (r != EtcdErrCode::EtcdOK && r != EtcdErrCode::EtcdKeyNotExist) {
    LOG(ERROR) << "Check etcd error: " << r;
    return false;
  } else {
    LOG(INFO) << "Check etcd ok";
    return true;
  }
}

void MDS::InitLeaderElectionOption(LeaderElectionOptions* option) {
  conf_->GetValueFatalIfFail("mds.listen.addr", &option->leaderUniqueName);
  conf_->GetValueFatalIfFail("leader.sessionInterSec",
                             &option->sessionInterSec);
  conf_->GetValueFatalIfFail("leader.electionTimeoutMs",
                             &option->electionTimeoutMs);
}

void MDS::InitLeaderElection(const LeaderElectionOptions& option) {
  leaderElection_ = std::make_shared<LeaderElection>(option);
}

void MDS::InitHeartbeatOption(HeartbeatOption* heartbeat_option) {
  conf_->GetValueFatalIfFail("mds.heartbeat.intervalMs",
                             &heartbeat_option->heartbeatIntervalMs);
  conf_->GetValueFatalIfFail("mds.heartbeat.misstimeoutMs",
                             &heartbeat_option->heartbeatMissTimeOutMs);
  conf_->GetValueFatalIfFail("mds.heartbeat.offlinetimeoutMs",
                             &heartbeat_option->offLineTimeOutMs);
  conf_->GetValueFatalIfFail("mds.heartbeat.clean_follower_afterMs",
                             &heartbeat_option->cleanFollowerAfterMs);
}

void MDS::InitHeartbeatManager() {
  // init option
  HeartbeatOption heartbeat_option;
  InitHeartbeatOption(&heartbeat_option);

  heartbeat_option.mdsStartTime = heartbeat::steady_clock::now();
  heartbeatManager_ = std::make_shared<heartbeat::HeartbeatManager>(
      heartbeat_option, topology_, coordinator_);
  heartbeatManager_->Init();
}

void MDS::InitCacheGroupOption() {
  conf_->GetValueFatalIfFail("cachegroup.heartbeat.interval_s",
                             &cachegroup::FLAGS_heartbeat_interval_s);
  conf_->GetValueFatalIfFail("cachegroup.heartbeat.miss_timeout_s",
                             &cachegroup::FLAGS_heartbeat_miss_timeout_s);
  conf_->GetValueFatalIfFail("cachegroup.heartbeat.offline_timeout_s",
                             &cachegroup::FLAGS_heartbeat_offline_timeout_s);
}

void MDS::InitCacheGroup() {
  InitCacheGroupOption();

  cache_group_member_manager_ =
      std::make_shared<cachegroup::CacheGroupMemberManagerImpl>(etcdClient_);
  LOG_IF(FATAL, !cache_group_member_manager_->Start().ok())
      << "Start cache group member manager failed.";
}

}  // namespace mds
}  // namespace dingofs
