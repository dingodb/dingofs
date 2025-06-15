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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "client/common/config.h"

#include <string>

#include "base/filepath/filepath.h"
#include "base/math/math.h"
#include "base/string/string.h"
#include "blockaccess/accesser_common.h"
#include "blockaccess/s3/s3_common.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/config/block_cache.h"
#include "cache/config/remote_cache.h"
#include "client/common/dynamic_config.h"
#include "gflags/gflags.h"
#include "utils/gflags_helper.h"

namespace brpc {
DECLARE_int32(defer_close_second);
DECLARE_int32(health_check_interval);
}  // namespace brpc

namespace dingofs {
namespace client {
namespace common {

using ::dingofs::base::filepath::PathJoin;
using ::dingofs::base::math::kMiB;
using ::dingofs::base::string::Str2Int;
using ::dingofs::utils::Configuration;

using ::dingofs::base::string::StrSplit;
using ::dingofs::stub::common::ExcutorOpt;
using ::dingofs::stub::common::MetaCacheOpt;

using dingofs::client::common::FLAGS_in_time_warmup;

static void InitMetaCacheOption(Configuration* conf, MetaCacheOpt* opts) {
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRetry",
                            &opts->metacacheGetLeaderRetry);
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheRPCRetryIntervalUS",
                            &opts->metacacheRPCRetryIntervalUS);
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRPCTimeOutMS",
                            &opts->metacacheGetLeaderRPCTimeOutMS);
}

static void InitExcutorOption(Configuration* conf, ExcutorOpt* opts,
                              bool internal) {
  if (internal) {
    conf->GetValueFatalIfFail("executorOpt.maxInternalRetry", &opts->maxRetry);
  } else {
    conf->GetValueFatalIfFail("executorOpt.maxRetry", &opts->maxRetry);
  }

  conf->GetValueFatalIfFail("executorOpt.retryIntervalUS",
                            &opts->retryIntervalUS);
  conf->GetValueFatalIfFail("executorOpt.rpcTimeoutMS", &opts->rpcTimeoutMS);
  conf->GetValueFatalIfFail("executorOpt.rpcStreamIdleTimeoutMS",
                            &opts->rpcStreamIdleTimeoutMS);
  conf->GetValueFatalIfFail("executorOpt.maxRPCTimeoutMS",
                            &opts->maxRPCTimeoutMS);
  conf->GetValueFatalIfFail("executorOpt.maxRetrySleepIntervalUS",
                            &opts->maxRetrySleepIntervalUS);
  conf->GetValueFatalIfFail("executorOpt.minRetryTimesForceTimeoutBackoff",
                            &opts->minRetryTimesForceTimeoutBackoff);
  conf->GetValueFatalIfFail("executorOpt.maxRetryTimesBeforeConsiderSuspend",
                            &opts->maxRetryTimesBeforeConsiderSuspend);
  conf->GetValueFatalIfFail("executorOpt.batchInodeAttrLimit",
                            &opts->batchInodeAttrLimit);
  conf->GetValueFatalIfFail("fuseClient.enableMultiMountPointRename",
                            &opts->enableRenameParallel);
}

static void InitS3ClientAdaptorOption(
    Configuration* conf, S3ClientAdaptorOption* s3_client_adaptor_opt) {
  conf->GetValueFatalIfFail("s3.fakeS3", &FLAGS_useFakeS3);
  conf->GetValueFatalIfFail("data_stream.page.size",
                            &s3_client_adaptor_opt->pageSize);
  conf->GetValueFatalIfFail("s3.prefetchBlocks",
                            &s3_client_adaptor_opt->prefetchBlocks);
  conf->GetValueFatalIfFail("s3.prefetchExecQueueNum",
                            &s3_client_adaptor_opt->prefetchExecQueueNum);
  conf->GetValueFatalIfFail("data_stream.background_flush.interval_ms",
                            &s3_client_adaptor_opt->intervalMs);
  conf->GetValueFatalIfFail("data_stream.slice.stay_in_memory_max_second",
                            &s3_client_adaptor_opt->flushIntervalSec);
  conf->GetValueFatalIfFail("s3.writeCacheMaxByte",
                            &s3_client_adaptor_opt->writeCacheMaxByte);
  conf->GetValueFatalIfFail("s3.readCacheMaxByte",
                            &s3_client_adaptor_opt->readCacheMaxByte);
  conf->GetValueFatalIfFail("s3.readCacheThreads",
                            &s3_client_adaptor_opt->readCacheThreads);
  conf->GetValueFatalIfFail("s3.nearfullRatio",
                            &s3_client_adaptor_opt->nearfullRatio);
  conf->GetValueFatalIfFail("s3.baseSleepUs",
                            &s3_client_adaptor_opt->baseSleepUs);
  conf->GetValueFatalIfFail("s3.maxReadRetryIntervalMs",
                            &s3_client_adaptor_opt->maxReadRetryIntervalMs);
  conf->GetValueFatalIfFail("s3.readRetryIntervalMs",
                            &s3_client_adaptor_opt->readRetryIntervalMs);
}

static void InitLeaseOpt(Configuration* conf, LeaseOpt* lease_opt) {
  conf->GetValueFatalIfFail("mds.leaseTimesUs", &lease_opt->leaseTimeUs);
  conf->GetValueFatalIfFail("mds.refreshTimesPerLease",
                            &lease_opt->refreshTimesPerLease);
}

void InitUdsOption(Configuration* conf, UdsOption* uds_opt) {
  if (!conf->GetValue("uds.fdCommPath", &uds_opt->fd_comm_path)) {
    uds_opt->fd_comm_path = "/var/run";
  }
}

static void InitRefreshDataOpt(Configuration* conf, RefreshDataOption* opt) {
  conf->GetValueFatalIfFail("fuseClient.maxDataSize", &opt->maxDataSize);
  conf->GetValueFatalIfFail("fuseClient.refreshDataIntervalSec",
                            &opt->refreshDataIntervalSec);
}

static void InitKVClientManagerOpt(Configuration* conf,
                                   KVClientManagerOpt* config) {
  conf->GetValueFatalIfFail("fuseClient.supportKVcache", &FLAGS_supportKVcache);
  conf->GetValueFatalIfFail("fuseClient.setThreadPool",
                            &config->setThreadPooln);
  conf->GetValueFatalIfFail("fuseClient.getThreadPool",
                            &config->getThreadPooln);
}

static void InitFileSystemOption(Configuration* c, FileSystemOption* option) {
  c->GetValueFatalIfFail("fs.cto", &option->cto);
  c->GetValueFatalIfFail("fs.cto", &FLAGS_enableCto);
  c->GetValueFatalIfFail("fs.nocto_suffix", &option->nocto_suffix);
  c->GetValueFatalIfFail("fs.disableXAttr", &option->disableXAttr);
  c->GetValueFatalIfFail("fs.maxNameLength", &option->maxNameLength);
  c->GetValueFatalIfFail("fs.accessLogging", &FLAGS_access_logging);
  {  // kernel cache option
    auto* o = &option->kernelCacheOption;
    c->GetValueFatalIfFail("fs.kernelCache.attrTimeoutSec", &o->attrTimeoutSec);
    c->GetValueFatalIfFail("fs.kernelCache.dirAttrTimeoutSec",
                           &o->dirAttrTimeoutSec);
    c->GetValueFatalIfFail("fs.kernelCache.entryTimeoutSec",
                           &o->entryTimeoutSec);
    c->GetValueFatalIfFail("fs.kernelCache.dirEntryTimeoutSec",
                           &o->dirEntryTimeoutSec);
  }
  {  // lookup cache option
    auto* o = &option->lookupCacheOption;
    c->GetValueFatalIfFail("fs.lookupCache.lruSize", &o->lruSize);
    c->GetValueFatalIfFail("fs.lookupCache.negativeTimeoutSec",
                           &o->negativeTimeoutSec);
    c->GetValueFatalIfFail("fs.lookupCache.minUses", &o->minUses);
  }
  {  // dir cache option
    auto* o = &option->dirCacheOption;
    c->GetValueFatalIfFail("fs.dirCache.lruSize", &o->lruSize);
  }
  {  // attr watcher option
    auto* o = &option->attrWatcherOption;
    c->GetValueFatalIfFail("fs.attrWatcher.lruSize", &o->lruSize);
  }
  {  // rpc option
    auto* o = &option->rpcOption;
    c->GetValueFatalIfFail("fs.rpc.listDentryLimit", &o->listDentryLimit);
  }
  {  // defer sync option
    auto* o = &option->deferSyncOption;
    c->GetValueFatalIfFail("fs.deferSync.delay", &o->delay);
    c->GetValueFatalIfFail("fs.deferSync.deferDirMtime", &o->deferDirMtime);
  }
}

static void InitDataStreamOption(Configuration* c, DataStreamOption* option) {
  {  // background flush option
    auto* o = &option->background_flush_option;
    c->GetValueFatalIfFail(
        "data_stream.background_flush.trigger_force_memory_ratio",
        &o->trigger_force_memory_ratio);
  }
  {  // file option
    auto* o = &option->file_option;
    c->GetValueFatalIfFail("data_stream.file.flush_workers", &o->flush_workers);
    c->GetValueFatalIfFail("data_stream.file.flush_queue_size",
                           &o->flush_queue_size);
  }
  {  // chunk option
    auto* o = &option->chunk_option;
    c->GetValueFatalIfFail("data_stream.chunk.flush_workers",
                           &o->flush_workers);
    c->GetValueFatalIfFail("data_stream.chunk.flush_queue_size",
                           &o->flush_queue_size);
  }
  {  // slice option
    auto* o = &option->slice_option;
    c->GetValueFatalIfFail("data_stream.slice.flush_workers",
                           &o->flush_workers);
    c->GetValueFatalIfFail("data_stream.slice.flush_queue_size",
                           &o->flush_queue_size);
  }
  {  // page option
    auto* o = &option->page_option;
    c->GetValueFatalIfFail("data_stream.page.size", &o->page_size);
    c->GetValueFatalIfFail("data_stream.page.total_size_mb", &o->total_size);
    c->GetValueFatalIfFail("data_stream.page.use_pool", &o->use_pool);

    if (o->page_size == 0) {
      CHECK(false) << "Page size must greater than 0.";
    }

    o->total_size = o->total_size * kMiB;
    if (o->total_size < 64 * kMiB) {
      CHECK(false) << "Page total size must greater than 64MiB.";
    }

    double trigger_force_flush_memory_ratio =
        option->background_flush_option.trigger_force_memory_ratio;
    if (o->total_size * (1.0 - trigger_force_flush_memory_ratio) < 32 * kMiB) {
      CHECK(false) << "Please gurantee the free memory size greater than 32MiB "
                      "before force flush.";
    }
  }
}

static void InitFuseOption(Configuration* c, FuseOption* option) {
  {  // fuse conn info
    auto* o = &option->conn_info;
    c->GetValueFatalIfFail("fuse.conn_info.want_splice_move",
                           &o->want_splice_move);
    c->GetValueFatalIfFail("fuse.conn_info.want_splice_read",
                           &o->want_splice_read);
    c->GetValueFatalIfFail("fuse.conn_info.want_splice_write",
                           &o->want_splice_write);
    c->GetValueFatalIfFail("fuse.conn_info.want_auto_inval_data",
                           &o->want_auto_inval_data);
  }

  {  // fuse file info
    c->GetValueFatalIfFail("fuse.file_info.direct_io",
                           &FLAGS_fuse_file_info_direct_io);
    c->GetValueFatalIfFail("fuse.file_info.keep_cache",
                           &FLAGS_fuse_file_info_keep_cache);
  }
}

static void InitBlockCacheOption(Configuration* c,
                                 cache::BlockCacheOption* option) {
  {  // block cache option
    c->GetValue("block_cache.cache_store", &cache::FLAGS_cache_store);
    c->GetValue("block_cache.enable_stage", &cache::FLAGS_enable_stage);
    c->GetValue("block_cache.enable_cache", &cache::FLAGS_enable_cache);
    c->GetValue("block_cache.access_logging",
                &cache::FLAGS_cache_access_logging);
    c->GetValue("block_cache.upload_stage_throttle_enable",
                &cache::FLAGS_upload_stage_throttle_enable);
    c->GetValue("block_cache.upload_stage_throttle_bandwidth_mb",
                &cache::FLAGS_upload_stage_throttle_bandwidth_mb);
    c->GetValue("block_cache.upload_stage_throttle_iops",
                &cache::FLAGS_upload_stage_throttle_iops);
    c->GetValue("block_cache.prefecth_max_inflights",
                &cache::FLAGS_prefetch_max_inflights);
  }

  {  // disk cache option
    c->GetValue("disk_cache.cache_dir", &cache::FLAGS_cache_dir);
    c->GetValue("disk_cache.cache_size_mb", &cache::FLAGS_cache_size_mb);
    c->GetValue("disk_cache.free_space_ratio", &cache::FLAGS_free_space_ratio);
    c->GetValue("disk_cache.cache_expire_second", &cache::FLAGS_cache_expire_s);
    c->GetValue("disk_cache.cleanup_expire_interval_millsecond",
                &cache::FLAGS_cleanup_expire_interval_ms);
    c->GetValue("disk_cache.ioring_iodepth", &cache::FLAGS_ioring_iodepth);
    c->GetValue("disk_cache.ioring_blksize", &cache::FLAGS_ioring_blksize);
    c->GetValue("disk_cache.ioring_prefetch", &cache::FLAGS_ioring_prefetch);
    c->GetValue("disk_cache.drop_page_cache", &cache::FLAGS_drop_page_cache);
  }

  {  // disk state option

    c->GetValue("disk_state.tick_duration_second",
                &cache::FLAGS_state_tick_duration_s);
    c->GetValue("disk_state.normal2unstable_io_error_num",
                &cache::FLAGS_state_normal2unstable_error_num);
    c->GetValue("disk_state.unstable2normal_io_succ_num",
                &cache::FLAGS_state_unstable2normal_succ_num);
    c->GetValue("disk_state.unstable2down_second",
                &cache::FLAGS_state_unstable2down_s);
    c->GetValue("disk_state.disk_check_duration_millsecond",
                &cache::FLAGS_check_disk_state_duration_ms);
  }

  *option = cache::BlockCacheOption();
}

static void InitRemoteBlockCacheOption(Configuration* c,
                                       cache::RemoteBlockCacheOption* option) {
  c->GetValue("remote_cache.cache_group", &cache::FLAGS_cache_group);
  c->GetValue("remote_cache.load_members_interval_ms",
              &cache::FLAGS_load_members_interval_ms);
  c->GetValue("remote_cache.mds_rpc_addrs", &cache::FLAGS_mds_rpc_addrs);
  c->GetValue("remote_cache.mds_rpc_retry_total_ms",
              &cache::FLAGS_mds_rpc_retry_total_ms);
  c->GetValue("remote_cache.mds_rpc_max_timeout_ms",
              &cache::FLAGS_mds_rpc_max_timeout_ms);
  c->GetValue("remote_cache.mds_rpc_timeout_ms",
              &cache::FLAGS_mds_rpc_timeout_ms);
  c->GetValue("remote_cache.mds_rpc_retry_interval_us",
              &cache::FLAGS_mds_rpc_retry_interval_us);
  c->GetValue("remote_cache.mds_rpc_max_failed_times_before_change_addr",
              &cache::FLAGS_mds_rpc_max_failed_times_before_change_addr);
  c->GetValue("remote_cache.mds_rpc_normal_retry_times_before_trigger_wait",
              &cache::FLAGS_mds_rpc_normal_retry_times_before_trigger_wait);
  c->GetValue("remote_cache.mds_rpc_wait_sleep_ms",
              &cache::FLAGS_mds_rpc_wait_sleep_ms);
  c->GetValue("remote_cache.remote_put_rpc_timeout_ms",
              &cache::FLAGS_remote_put_rpc_timeout_ms);
  c->GetValue("remote_cache.remote_range_rpc_timeout_ms",
              &cache::FLAGS_remote_range_rpc_timeout_ms);
  c->GetValue("remote_cache.remote_cache_rpc_timeout_ms",
              &cache::FLAGS_remote_cache_rpc_timeout_ms);
  c->GetValue("remote_cache.remote_prefetch_rpc_timeout_ms",
              &cache::FLAGS_remote_prefetch_rpc_timeout_ms);
  *option = cache::RemoteBlockCacheOption();
}

static void SetBrpcOpt(Configuration* conf) {
  dingofs::utils::GflagsLoadValueFromConfIfCmdNotSet dummy;
  dummy.Load(conf, "defer_close_second", "rpc.defer.close.second",
             &brpc::FLAGS_defer_close_second);
  dummy.Load(conf, "health_check_interval", "rpc.healthCheckIntervalSec",
             &brpc::FLAGS_health_check_interval);
}

void InitClientOption(Configuration* conf, ClientOption* client_option) {
  InitMdsOption(conf, &client_option->mdsOpt);
  InitMetaCacheOption(conf, &client_option->metaCacheOpt);
  InitExcutorOption(conf, &client_option->excutorOpt, false);
  InitExcutorOption(conf, &client_option->excutorInternalOpt, true);
  InitS3ClientAdaptorOption(conf, &client_option->s3_client_adaptor_opt);
  blockaccess::InitAwsSdkConfig(
      conf, &client_option->block_access_opt.s3_options.aws_sdk_config);
  blockaccess::InitBlockAccesserThrottleOptions(
      conf, &client_option->block_access_opt.throttle_options);
  InitLeaseOpt(conf, &client_option->leaseOpt);
  InitRefreshDataOpt(conf, &client_option->refreshDataOption);
  InitKVClientManagerOpt(conf, &client_option->kvClientManagerOpt);
  InitFileSystemOption(conf, &client_option->fileSystemOption);
  InitDataStreamOption(conf, &client_option->data_stream_option);
  InitBlockCacheOption(conf, &client_option->block_cache_option);
  InitRemoteBlockCacheOption(conf, &client_option->remote_block_cache_option);
  InitFuseOption(conf, &client_option->fuse_option);

  conf->GetValueFatalIfFail("fuseClient.listDentryLimit",
                            &client_option->listDentryLimit);
  conf->GetValueFatalIfFail("fuseClient.listDentryThreads",
                            &client_option->listDentryThreads);
  conf->GetValueFatalIfFail("client.dummyServer.startPort",
                            &client_option->dummyServerStartPort);
  conf->GetValueFatalIfFail("fuseClient.enableMultiMountPointRename",
                            &client_option->enableMultiMountPointRename);
  conf->GetValueFatalIfFail("fuseClient.downloadMaxRetryTimes",
                            &client_option->downloadMaxRetryTimes);
  conf->GetValueFatalIfFail("fuseClient.warmupThreadsNum",
                            &client_option->warmupThreadsNum);
  if (!conf->GetBoolValue("fuseClient.in_time_warmup", &FLAGS_in_time_warmup)) {
    LOG(INFO) << "Not found `fuseClient.in_time_warmup` in conf, default to "
                 "false";
    FLAGS_in_time_warmup = false;
  }

  if (!conf->GetIntValue("fuseClient.bthread_worker_num",
                         &FLAGS_bthread_worker_num)) {
    FLAGS_bthread_worker_num = 0;
    LOG(INFO) << "Not found `fuseClient.bthread_worker_num` in conf, "
                 "default to 0";
  }

  conf->GetValueFatalIfFail("fuseClient.throttle.avgWriteBytes",
                            &FLAGS_fuseClientAvgWriteBytes);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteBytes",
                            &FLAGS_fuseClientBurstWriteBytes);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteBytesSecs",
                            &FLAGS_fuseClientBurstWriteBytesSecs);

  conf->GetValueFatalIfFail("fuseClient.throttle.avgWriteIops",
                            &FLAGS_fuseClientAvgWriteIops);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteIops",
                            &FLAGS_fuseClientBurstWriteIops);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteIopsSecs",
                            &FLAGS_fuseClientBurstWriteIopsSecs);

  conf->GetValueFatalIfFail("fuseClient.throttle.avgReadBytes",
                            &FLAGS_fuseClientAvgReadBytes);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstReadBytes",
                            &FLAGS_fuseClientBurstReadBytes);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstReadBytesSecs",
                            &FLAGS_fuseClientBurstReadBytesSecs);

  conf->GetValueFatalIfFail("fuseClient.throttle.avgReadIops",
                            &FLAGS_fuseClientAvgReadIops);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstReadIops",
                            &FLAGS_fuseClientBurstReadIops);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstReadIopsSecs",
                            &FLAGS_fuseClientBurstReadIopsSecs);
  SetBrpcOpt(conf);
}

void RewriteCacheDir(cache::BlockCacheOption* option, std::string uuid) {
  auto& disk_cache_options = option->disk_cache_options;
  for (auto& disk_cache_option : disk_cache_options) {
    std::string cache_dir = disk_cache_option.cache_dir;
    disk_cache_option.cache_dir = cache::RealCacheDir(cache_dir, uuid);
  }
}

}  // namespace common
}  // namespace client
}  // namespace dingofs
