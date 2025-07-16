/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "options/client/vfs_legacy/vfs_legacy_option.h"

#include <glog/logging.h>

#include <string>

#include "blockaccess/s3/s3_common.h"
#include "options/client/client_dynamic_option.h"
#include "options/client/common_option.h"
#include "options/client/vfs_legacy/vfs_legacy_dynamic_config.h"
#include "options/stub/dynamic_option.h"
#include "stub/common/config.h"

namespace dingofs {
namespace client {

using ::dingofs::client::FLAGS_in_time_warmup;

static void InitMetaCacheOption(utils::Configuration* conf,
                                stub::common::MetaCacheOpt* opts) {
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRetry",
                            &opts->metacacheGetLeaderRetry);
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheRPCRetryIntervalUS",
                            &opts->metacacheRPCRetryIntervalUS);
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRPCTimeOutMS",
                            &opts->metacacheGetLeaderRPCTimeOutMS);
}

static void InitExcutorOption(utils::Configuration* conf,
                              stub::common::ExcutorOpt* opts, bool internal) {
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
    utils::Configuration* conf, S3ClientAdaptorOption* s3_client_adaptor_opt) {
  conf->GetValueFatalIfFail("data_stream.page.size",
                            &s3_client_adaptor_opt->pageSize);

  if (!conf->GetBoolValue("s3.prefetch", &FLAGS_s3_prefetch)) {
    LOG(INFO) << "Not found `s3.prefetch` in conf, default: "
              << (FLAGS_s3_prefetch ? "true" : "false");
  }

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

static void InitLeaseOpt(utils::Configuration* conf, LeaseOpt* lease_opt) {
  conf->GetValueFatalIfFail("mds.leaseTimesUs", &lease_opt->leaseTimeUs);
  conf->GetValueFatalIfFail("mds.refreshTimesPerLease",
                            &lease_opt->refreshTimesPerLease);
}

static void InitRefreshDataOpt(utils::Configuration* conf,
                               RefreshDataOption* opt) {
  conf->GetValueFatalIfFail("fuseClient.maxDataSize", &opt->maxDataSize);
  conf->GetValueFatalIfFail("fuseClient.refreshDataIntervalSec",
                            &opt->refreshDataIntervalSec);
}

static void InitKVClientManagerOpt(utils::Configuration* conf,
                                   KVClientManagerOpt* config) {
  conf->GetValueFatalIfFail("fuseClient.setThreadPool",
                            &config->setThreadPooln);
  conf->GetValueFatalIfFail("fuseClient.getThreadPool",
                            &config->getThreadPooln);
}

static void InitFileSystemOption(utils::Configuration* c,
                                 FileSystemOption* option) {
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

void InitVFSLegacyOption(utils::Configuration* conf, VFSLegacyOption* option) {
  stub::common::InitMdsOption(conf, &option->mdsOpt);
  InitMetaCacheOption(conf, &option->metaCacheOpt);
  InitExcutorOption(conf, &option->excutorOpt, false);
  InitExcutorOption(conf, &option->excutorInternalOpt, true);
  InitS3ClientAdaptorOption(conf, &option->s3_client_adaptor_opt);
  blockaccess::InitAwsSdkConfig(
      conf, &option->block_access_opt.s3_options.aws_sdk_config);
  blockaccess::InitBlockAccesserThrottleOptions(
      conf, &option->block_access_opt.throttle_options);
  InitLeaseOpt(conf, &option->leaseOpt);
  InitRefreshDataOpt(conf, &option->refreshDataOption);
  InitKVClientManagerOpt(conf, &option->kvClientManagerOpt);
  InitFileSystemOption(conf, &option->fileSystemOption);
  InitDataStreamOption(conf, &option->data_stream_option);
  InitBlockCacheOption(conf, &option->block_cache_option);
  InitRemoteBlockCacheOption(conf, &option->remote_block_cache_option);
  InitFuseOption(conf, &option->fuse_option);

  if (!conf->GetUInt32Value("client.fs_usage_flush_interval_second",
                            &FLAGS_fs_usage_flush_interval_second)) {
    LOG(INFO) << "Not found `client.fs_usage_flush_interval_second` in conf, "
                 "default: "
              << FLAGS_fs_usage_flush_interval_second;
  }

  if (!conf->GetUInt32Value("client.flush_quota_interval_second",
                            &FLAGS_flush_quota_interval_second)) {
    LOG(INFO) << "Not found `client.flush_quota_interval_second` in conf, "
                 "default: "
              << FLAGS_flush_quota_interval_second;
  }

  if (!conf->GetUInt32Value("client.load_quota_interval_second",
                            &FLAGS_load_quota_interval_second)) {
    LOG(INFO) << "Not found `client.load_quota_interval_second` in conf, "
                 "default: "
              << FLAGS_load_quota_interval_second;
  }

  conf->GetValueFatalIfFail("fuseClient.listDentryLimit",
                            &option->listDentryLimit);
  conf->GetValueFatalIfFail("fuseClient.listDentryThreads",
                            &option->listDentryThreads);
  conf->GetValueFatalIfFail("client.dummyServer.startPort",
                            &option->dummyServerStartPort);
  conf->GetValueFatalIfFail("fuseClient.enableMultiMountPointRename",
                            &option->enableMultiMountPointRename);
  conf->GetValueFatalIfFail("fuseClient.downloadMaxRetryTimes",
                            &option->downloadMaxRetryTimes);
  conf->GetValueFatalIfFail("fuseClient.warmupThreadsNum",
                            &option->warmupThreadsNum);
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

  // logging
  if (!conf->GetBoolValue("access_logging", &FLAGS_access_logging)) {
    LOG(INFO) << "Not found `access_logging` in conf, default: "
              << FLAGS_access_logging;
  }
  if (!conf->GetInt64Value("access_log_threshold_us",
                           &FLAGS_access_log_threshold_us)) {
    LOG(INFO) << "Not found `access_log_threshold_us` in conf, "
                 "default: "
              << FLAGS_access_log_threshold_us;
  }

  if (!conf->GetBoolValue("mds_access_logging",
                          &stub::FLAGS_mds_access_logging)) {
    LOG(INFO) << "Not found `mds_access_logging` in conf, default: "
              << stub::FLAGS_mds_access_logging;
  }
  if (!conf->GetInt64Value("mds_access_log_threshold_us",
                           &stub::FLAGS_mds_access_log_threshold_us)) {
    LOG(INFO) << "Not found `mds_access_log_threshold_us` in conf, "
                 "default: "
              << stub::FLAGS_mds_access_log_threshold_us;
  }

  if (!conf->GetBoolValue("meta_access_logging",
                          &stub::FLAGS_meta_access_logging)) {
    LOG(INFO) << "Not found `meta_access_logging` in conf, default: "
              << stub::FLAGS_meta_access_logging;
  }
  if (!conf->GetInt64Value("meta_access_log_threshold_us",
                           &stub::FLAGS_meta_access_log_threshold_us)) {
    LOG(INFO) << "Not found `meta_access_log_threshold_us` in conf, "
                 "default: "
              << stub::FLAGS_meta_access_log_threshold_us;
  }

  SetBrpcOpt(conf);
}

}  // namespace client
}  // namespace dingofs