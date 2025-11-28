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

#include "mds/server.h"

#include <string>
#include <utility>
#include <vector>

#include "common/options/mds.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"
#include "json/value.h"
#include "mds/background/cache_member_sync.h"
#include "mds/background/fsinfo_sync.h"
#include "mds/background/heartbeat.h"
#include "mds/cachegroup/member_manager.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/version.h"
#include "mds/coordinator/dingo_coordinator_client.h"
#include "mds/service/debug_service.h"
#include "mds/service/fsstat_service.h"
#include "mds/service/mds_service.h"
#include "mds/statistics/fs_stat.h"
#include "mds/storage/dingodb_storage.h"
#include "mds/storage/dummy_storage.h"

#ifdef USE_TCMALLOC
#include "gperftools/malloc_extension.h"
#endif

namespace dingofs {
namespace mds {

DEFINE_string(mds_monitor_lock_name, "/lock/mds/monitor", "mds monitor lock name");
DEFINE_string(mds_gc_lock_name, "/lock/mds/gc", "gc lock name");

DEFINE_string(mds_pid_file_name, "pid", "pid file name");

DECLARE_string(mds_service_worker_set_type);

const std::string kQuotaWorkerSetName = "QUOTA_WORKER_SET";
DEFINE_uint32(mds_quota_worker_num, 24, "quota worker number");
DEFINE_uint32(mds_quota_worker_max_pending_num, 4096, "quota worker max pending number");

// crontab config
DEFINE_uint32(mds_crontab_heartbeat_interval_s, 5, "heartbeat interval seconds");
DEFINE_uint32(mds_crontab_fsinfosync_interval_s, 10, "fs info sync interval seconds");
DEFINE_uint32(mds_crontab_mdsmonitor_interval_s, 5, "mds monitor interval seconds");
DEFINE_uint32(mds_crontab_quota_sync_interval_s, 3, "quota sync interval seconds");
DEFINE_uint32(mds_crontab_gc_interval_s, 60, "gc interval seconds");
DEFINE_uint32(mds_crontab_cache_member_sync_interval_s, 3, "cache member sync interval seconds");
DEFINE_uint32(mds_crontab_clean_expired_cache_interval_s, 600, "clean expired cache interval seconds");

// log config
DEFINE_string(mds_log_level, "INFO", "log level, DEBUG, INFO, WARNING, ERROR, FATAL");
DEFINE_string(mds_log_path, "./log", "log path, if empty, use default log path");

// service config
DEFINE_uint32(mds_server_id, 1001, "server id, must be unique in the cluster");
DEFINE_string(mds_server_host, "127.0.0.1", "server host");
DEFINE_string(mds_server_listen_host, "0.0.0.0", "server listen host");
DEFINE_uint32(mds_server_port, 7801, "server port");
DEFINE_bool(mds_cache_member_enable_cache, true, "cache member enable cache, default:true");

DECLARE_string(mds_storage_engine);
DECLARE_string(mds_id_generator_type);

Server::~Server() {}  // NOLINT

Server& Server::GetInstance() {
  static Server instance;
  return instance;
}

static LogLevel GetDingoLogLevel(const std::string& log_level) {
  if (Helper::IsEqualIgnoreCase(log_level, "DEBUG")) {
    return LogLevel::kDEBUG;
  } else if (Helper::IsEqualIgnoreCase(log_level, "INFO")) {
    return LogLevel::kINFO;
  } else if (Helper::IsEqualIgnoreCase(log_level, "WARNING")) {
    return LogLevel::kWARNING;
  } else if (Helper::IsEqualIgnoreCase(log_level, "ERROR")) {
    return LogLevel::kERROR;
  } else if (Helper::IsEqualIgnoreCase(log_level, "FATAL")) {
    return LogLevel::kFATAL;
  } else {
    return LogLevel::kINFO;
  }
}

bool Server::InitConfig(const std::string& path) {
  DINGO_LOG(INFO) << fmt::format("config path: {}", path);

  DINGO_LOG(INFO) << fmt::format("mds_server_id: {}", FLAGS_mds_server_id);
  DINGO_LOG(INFO) << fmt::format("mds_server_host: {}:{}", FLAGS_mds_server_host, FLAGS_mds_server_port);
  DINGO_LOG(INFO) << fmt::format("mds_storage_engine: {}", FLAGS_mds_storage_engine);
  DINGO_LOG(INFO) << fmt::format("mds_id_generator_type: {}", FLAGS_mds_id_generator_type);
  DINGO_LOG(INFO) << fmt::format("mds_log_path: {}", FLAGS_mds_log_path);

  if (FLAGS_mds_server_id == 0) {
    DINGO_LOG(ERROR) << "mds server id is 0, please set a valid id.";
    return false;
  }
  if (FLAGS_mds_server_host.empty()) {
    DINGO_LOG(ERROR) << "mds server host is empty, please set a valid host.";
    return false;
  }
  if (FLAGS_mds_server_host == "0.0.0.0") {
    DINGO_LOG(ERROR) << "mds server host is 0.0.0.0, can't set it.";
    return false;
  }
  if (FLAGS_mds_server_port == 0) {
    DINGO_LOG(ERROR) << "mds server port is 0, please set a valid port.";
    return false;
  }

  if (FLAGS_mds_log_level.empty()) {
    DINGO_LOG(ERROR) << "mds log level is empty, please set a valid log level.";
    return false;
  }
  if (FLAGS_mds_log_path.empty()) {
    DINGO_LOG(ERROR) << "mds log path is empty, please set a valid log path.";
    return false;
  }
  if (FLAGS_mds_storage_engine != "dingo-store" && FLAGS_mds_storage_engine != "dummy") {
    DINGO_LOG(ERROR) << fmt::format("unsupported mds storage engine({}).", FLAGS_mds_storage_engine);
    return false;
  }
  if (FLAGS_mds_id_generator_type != "coor" && FLAGS_mds_id_generator_type != "store") {
    DINGO_LOG(ERROR) << fmt::format("unsupported mds id generator type({}).", FLAGS_mds_id_generator_type);
    return false;
  }

  need_coordinator_ = (FLAGS_mds_storage_engine == "dingo-store");

  return true;
}

bool Server::InitLog() {
  DINGO_LOG(INFO) << fmt::format("Init log: {} {}", FLAGS_mds_log_level, FLAGS_mds_log_path);

  DingoLogger::InitLogger(FLAGS_mds_log_path, "mds", GetDingoLogLevel(FLAGS_mds_log_level));

  DingoLogVersion();
  return true;
}

bool Server::InitMDSMeta() {
  self_mds_meta_.SetID(FLAGS_mds_server_id);

  self_mds_meta_.SetHost(FLAGS_mds_server_host);
  self_mds_meta_.SetPort(FLAGS_mds_server_port);
  self_mds_meta_.SetState(MDSMeta::State::kNormal);

  DINGO_LOG(INFO) << fmt::format("init mds meta, self: {}.", self_mds_meta_.ToString());

  mds_meta_map_ = MDSMetaMap::New();
  CHECK(mds_meta_map_ != nullptr) << "new MDSMetaMap fail.";

  mds_meta_map_->UpsertMDSMeta(self_mds_meta_);

  return true;
}

bool Server::InitCoordinatorClient(const std::string& coor_url) {
  if (FLAGS_mds_storage_engine != "dingo-store") {
    return true;
  }

  DINGO_LOG(INFO) << fmt::format("init coordinator client, addr({}).", coor_url);

  std::string coor_addrs = Helper::ParseCoorAddr(coor_url);
  if (coor_addrs.empty()) {
    return false;
  }

  coordinator_client_ = DingoCoordinatorClient::New();
  CHECK(coordinator_client_ != nullptr) << "new DingoCoordinatorClient fail.";

  return coordinator_client_->Init(coor_addrs);
}

bool Server::InitStorage(const std::string& store_url) {
  DINGO_LOG(INFO) << fmt::format("init storage, engine({}) url({}).", FLAGS_mds_storage_engine, store_url);
  CHECK(!store_url.empty()) << "store url is empty.";

  if (FLAGS_mds_storage_engine == "dingo-store") {
    kv_storage_ = DingodbStorage::New();

  } else if (FLAGS_mds_storage_engine == "dummy") {
    kv_storage_ = DummyStorage::New();

  } else {
    DINGO_LOG(ERROR) << fmt::format("unsupported mds storage engine({}).", FLAGS_mds_storage_engine);
    return false;
  }
  CHECK(kv_storage_ != nullptr) << "new dingodb storage fail.";

  std::string store_addrs = Helper::ParseCoorAddr(store_url);
  if (store_addrs.empty()) {
    return false;
  }

  return kv_storage_->Init(store_addrs);
}

bool Server::InitOperationProcessor() {
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";

  operation_processor_ = OperationProcessor::New(kv_storage_);
  CHECK(operation_processor_ != nullptr) << "new OperationProcessor fail.";

  return operation_processor_->Init();
}

bool Server::InitCacheGroupMemberManager() {
  DINGO_LOG(INFO) << "init cache group member manager.";
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";
  cache_group_member_manager_ = CacheGroupMemberManager::New(operation_processor_);
  CHECK(cache_group_member_manager_ != nullptr) << "cache_group_member_manager is nullptr.";
  cache_group_member_manager_->LoadCacheMembers();

  return true;
}

bool Server::InitFileSystem() {
  DINGO_LOG(INFO) << "init filesystem.";

  CHECK((!need_coordinator_ || coordinator_client_ != nullptr)) << "coordinator client is nullptr.";
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";
  CHECK(mds_meta_map_ != nullptr) << "mds_meta_map is nullptr.";
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";
  CHECK(notify_buddy_ != nullptr) << "notify_buddy is nullptr.";

  IdGeneratorUPtr fs_id_generator;
  IdGeneratorSPtr slice_id_generator;
  if (FLAGS_mds_storage_engine == "dingo-store") {
    fs_id_generator =
        (FLAGS_mds_id_generator_type == "coor") ? NewFsIdGenerator(coordinator_client_) : NewFsIdGenerator(kv_storage_);

    slice_id_generator = (FLAGS_mds_id_generator_type == "coor") ? NewSliceIdGenerator(coordinator_client_)
                                                                 : NewSliceIdGenerator(kv_storage_);

  } else if (FLAGS_mds_storage_engine == "dummy") {
    fs_id_generator = NewFsIdGenerator(kv_storage_);
    slice_id_generator = NewSliceIdGenerator(kv_storage_);
  }

  CHECK(fs_id_generator != nullptr) << "new fs AutoIncrementIdGenerator fail.";
  CHECK(fs_id_generator->Init()) << "init fs AutoIncrementIdGenerator fail.";

  CHECK(slice_id_generator != nullptr) << "new slice AutoIncrementIdGenerator fail.";
  CHECK(slice_id_generator->Init()) << "init slice AutoIncrementIdGenerator fail.";

  quota_worker_set_ = ExecqWorkerSet::NewUnique(kQuotaWorkerSetName, FLAGS_mds_quota_worker_num,
                                                FLAGS_mds_quota_worker_max_pending_num);
  CHECK(quota_worker_set_ != nullptr) << "new quota worker set fail.";
  CHECK(quota_worker_set_->Init()) << "init service read worker set fail.";

  file_system_set_ =
      FileSystemSet::New(coordinator_client_, std::move(fs_id_generator), slice_id_generator, kv_storage_,
                         self_mds_meta_, mds_meta_map_, operation_processor_, quota_worker_set_, notify_buddy_);
  CHECK(file_system_set_ != nullptr) << "new FileSystem fail.";

  return file_system_set_->Init();
}

bool Server::InitHeartbeat() {
  DINGO_LOG(INFO) << "init heartbeat.";
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";
  CHECK(cache_group_member_manager_ != nullptr) << "cache_group_member_manager is nullptr.";

  heartbeat_ = Heartbeat::New(operation_processor_, cache_group_member_manager_);
  return heartbeat_->Init();
}

bool Server::InitFsInfoSync() {
  DINGO_LOG(INFO) << "init fs info sync.";
  CHECK(file_system_set_ != nullptr) << "file_system_set is nullptr.";

  fs_info_sync_ = FsInfoSync::New(file_system_set_);
  CHECK(fs_info_sync_ != nullptr) << "new FsInfoSync fail.";

  return true;
}

bool Server::InitCacheMemberSynchronizer() {
  CHECK(cache_group_member_manager_ != nullptr) << "cache_group_member_manager is nullptr.";

  cache_member_synchronizer_ = CacheMemberSynchronizer::New(cache_group_member_manager_);
  CHECK(cache_member_synchronizer_ != nullptr) << "new CacheMemberSynchronizer fail.";

  return true;
}

bool Server::InitNotifyBuddy() {
  CHECK(mds_meta_map_ != nullptr) << "mds meta map is nullptr.";
  notify_buddy_ = notify::NotifyBuddy::New(mds_meta_map_, self_mds_meta_.ID());

  return notify_buddy_->Init();
}

bool Server::InitMonitor() {
  CHECK((!need_coordinator_ || coordinator_client_ != nullptr)) << "coordinator client is nullptr.";
  CHECK(self_mds_meta_.ID() > 0) << "mds id is invalid.";
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";
  CHECK(notify_buddy_ != nullptr) << "notify_buddy is nullptr.";

  auto dist_lock = StoreDistributionLock::New(kv_storage_, FLAGS_mds_monitor_lock_name, self_mds_meta_.ID());
  CHECK(dist_lock != nullptr) << "gc dist lock is nullptr.";

  monitor_ = Monitor::New(file_system_set_, dist_lock, notify_buddy_);
  CHECK(monitor_ != nullptr) << "new MDSMonitor fail.";

  CHECK(monitor_->Init()) << "init MDSMonitor fail.";

  return true;
}

bool Server::InitQuotaSynchronizer() {
  CHECK(file_system_set_ != nullptr) << "file_system_set is nullptr.";

  quota_synchronizer_ = QuotaSynchronizer::New(file_system_set_);
  CHECK(quota_synchronizer_ != nullptr) << "new QuotaSynchronizer fail.";

  return true;
}

bool Server::InitGcProcessor() {
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";
  CHECK(file_system_set_ != nullptr) << "file system set is nullptr.";

  auto dist_lock = StoreDistributionLock::New(kv_storage_, FLAGS_mds_gc_lock_name, self_mds_meta_.ID());
  CHECK(dist_lock != nullptr) << "gc dist lock is nullptr.";

  gc_processor_ = GcProcessor::New(file_system_set_, operation_processor_, dist_lock);

  CHECK(gc_processor_->Init()) << "init GcProcessor fail.";

  return true;
}

bool Server::InitCrontab() {
  DINGO_LOG(INFO) << "init crontab.";

  // Add heartbeat crontab
  crontab_configs_.push_back({
      "HEARTBEAT",
      FLAGS_mds_crontab_heartbeat_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetHeartbeat()->Run(); },
  });

  // Add fs info sync crontab
  crontab_configs_.push_back({
      "FSINFO_SYNC",
      FLAGS_mds_crontab_fsinfosync_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetFsInfoSync()->Run(); },
  });

  // Add fs info sync crontab
  crontab_configs_.push_back({
      "MDS_MONITOR",
      FLAGS_mds_crontab_mdsmonitor_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetMonitor()->Run(); },
  });

  // Add quota sync crontab
  crontab_configs_.push_back({
      "QUOTA_SYNC",
      FLAGS_mds_crontab_quota_sync_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetQuotaSynchronizer()->Run(); },
  });

  // Add fs info sync crontab
  crontab_configs_.push_back({
      "GC",
      FLAGS_mds_crontab_gc_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetGcProcessor()->Run(); },
  });

  // Add filesystem cache crontab
  crontab_configs_.push_back({
      "CLEAN_EXPIRED_CACHE",
      FLAGS_mds_crontab_clean_expired_cache_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetFileSystemSet()->CleanExpiredCache(); },
  });

  // Add cache member sync crontab
  crontab_configs_.push_back({
      "CACHE_MEMBER_SYNC",
      FLAGS_mds_crontab_cache_member_sync_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetCacheMemberSynchronizer()->Run(); },
  });

  crontab_manager_.AddCrontab(crontab_configs_);

  return true;
}

bool Server::InitService() {
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";

  // mds service
  auto fs_stats = FsStats::New(operation_processor_);
  CHECK(fs_stats != nullptr) << "fsstats is nullptr.";

  CHECK(cache_group_member_manager_ != nullptr) << "cache_group_member_manager is nullptr.";

  mds_service_ = MDSServiceImpl::New(file_system_set_, gc_processor_, std::move(fs_stats), cache_group_member_manager_);
  CHECK(mds_service_ != nullptr) << "new MDSServiceImpl fail.";

  if (!mds_service_->Init()) {
    DINGO_LOG(ERROR) << "init MDSServiceImpl fail.";
    return false;
  }

  // debug service
  debug_service_ = DebugServiceImpl::New(file_system_set_);
  CHECK(debug_service_ != nullptr) << "new DebugServiceImpl fail.";

  // fs stat service
  fs_stat_service_ = FsStatServiceImpl::New();
  CHECK(fs_stat_service_ != nullptr) << "new FsStatServiceImpl fail.";

  return true;
}

std::string Server::GetPidFilePath() { return FLAGS_mds_log_path + "/" + FLAGS_mds_pid_file_name; }

std::string Server::GetListenAddr() {
  std::string host = FLAGS_mds_server_listen_host.empty() ? FLAGS_mds_server_host : FLAGS_mds_server_listen_host;

  return fmt::format("{}:{}", host, FLAGS_mds_server_port);
}

MDSMeta& Server::GetMDSMeta() { return self_mds_meta_; }

MDSMetaMapSPtr Server::GetMDSMetaMap() {
  CHECK(mds_meta_map_ != nullptr) << "mds meta map is nullptr.";
  return mds_meta_map_;
}

KVStorageSPtr Server::GetKVStorage() {
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";
  return kv_storage_;
}

HeartbeatSPtr Server::GetHeartbeat() {
  CHECK(heartbeat_ != nullptr) << "heartbeat is nullptr.";

  return heartbeat_;
}

FsInfoSyncSPtr Server::GetFsInfoSync() { return fs_info_sync_; }

CacheMemberSynchronizerSPtr& Server::GetCacheMemberSynchronizer() { return cache_member_synchronizer_; }

CoordinatorClientSPtr Server::GetCoordinatorClient() {
  CHECK((!need_coordinator_ || coordinator_client_ != nullptr)) << "coordinator client is nullptr.";

  return coordinator_client_;
}

FileSystemSetSPtr Server::GetFileSystemSet() { return file_system_set_; }

notify::NotifyBuddySPtr Server::GetNotifyBuddy() {
  CHECK(notify_buddy_ != nullptr) << "notify_buddy is nullptr.";

  return notify_buddy_;
}

MonitorSPtr Server::GetMonitor() {
  CHECK(monitor_ != nullptr) << "mds_monitor is nullptr.";

  return monitor_;
}

OperationProcessorSPtr Server::GetOperationProcessor() {
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";
  return operation_processor_;
}

QuotaSynchronizerSPtr Server::GetQuotaSynchronizer() {
  CHECK(quota_synchronizer_ != nullptr) << "quota_synchronizer is nullptr.";

  return quota_synchronizer_;
}

GcProcessorSPtr Server::GetGcProcessor() {
  CHECK(gc_processor_ != nullptr) << "gc_processor is nullptr.";

  return gc_processor_;
}

CacheGroupMemberManagerSPtr Server::GetCacheGroupMemberManager() {
  CHECK(cache_group_member_manager_ != nullptr) << "cache_group_member_manager_ is nullptr.";

  return cache_group_member_manager_;
}

MDSServiceImplUPtr& Server::GetMDSService() {
  CHECK(mds_service_ != nullptr) << "mds_service is nullptr.";
  return mds_service_;
}

DebugServiceImplUPtr& Server::GetDebugService() {
  CHECK(debug_service_ != nullptr) << "debug_service is nullptr.";
  return debug_service_;
}

FsStatServiceImplUPtr& Server::GetFsStatService() {
  CHECK(fs_stat_service_ != nullptr) << "fs_stat_service is nullptr.";
  return fs_stat_service_;
}

void Server::Run() {
  CHECK(brpc_server_.AddService(mds_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) == 0) << "add mds service error.";

  CHECK(brpc_server_.AddService(debug_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) == 0)
      << "add debug service error.";

  CHECK(brpc_server_.AddService(fs_stat_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) == 0)
      << "add fsstat service error.";

  brpc::ServerOptions option;
  CHECK(brpc_server_.Start(GetListenAddr().c_str(), &option) == 0) << "start brpc server error.";

  while (!brpc::IsAskedToQuit() && !stop_.load()) {
    bthread_usleep(1000000L);
  }
}

void Server::Stop() {
  // Only stop once
  bool expected = false;
  if (!stop_.compare_exchange_strong(expected, true)) {
    return;
  }

  mds_service_->Destroy();

  brpc_server_.Stop(0);
  brpc_server_.Join();
  operation_processor_->Destroy();
  heartbeat_->Destroy();
  crontab_manager_.Destroy();
  monitor_->Destroy();
}

static void DescribeTcmallocByJson(Json::Value& value) {
#ifdef USE_TCMALLOC
  auto* tcmalloc = MallocExtension::instance();
  if (tcmalloc != nullptr) {
    std::string stat_buf(4096, '\0');
    tcmalloc->GetStats(stat_buf.data(), stat_buf.size());

    std::vector<std::string> lines;
    Helper::SplitString(stat_buf, '\n', lines);
    for (size_t i = 0; i < lines.size() - 1; ++i) {
      value[fmt::format("stats-{:0>2}", i)] = lines[i];
    }
  }
#endif
}

void Server::DescribeByJson(Json::Value& value) {
  // tcmalloc
  Json::Value tcmalloc_value(Json::objectValue);
  DescribeTcmallocByJson(tcmalloc_value);
  value["a-tcmalloc"] = tcmalloc_value;

  // self mds meta
  Json::Value self_mds_value;
  self_mds_meta_.DescribeByJson(self_mds_value);
  value["b-self_mds_meta"] = self_mds_value;

  // mds meta map
  Json::Value mds_map_value(Json::arrayValue);
  mds_meta_map_->DescribeByJson(mds_map_value);
  value["c-mds_meta_map"] = mds_map_value;

  // file_system_set
  Json::Value fsset_value;
  file_system_set_->DescribeByJson(fsset_value);
  value["d-file_system_set"] = fsset_value;

  // mds service
  Json::Value mds_service_value;
  mds_service_->DescribeByJson(mds_service_value);
  value["e-mds_service"] = mds_service_value;

  // crontab
  Json::Value crontab_value(Json::arrayValue);
  crontab_manager_.DescribeByJson(crontab_value);
  value["f-crontab"] = crontab_value;

  // gc

  // monitor

  // heartbeat

  // quota

  // operation_processor
}

}  // namespace mds
}  // namespace dingofs
