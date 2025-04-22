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
#include "client/common/dynamic_config.h"
#include "gflags/gflags.h"
#include "options/cache/blockcache.h"
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
using ::dingofs::dataaccess::aws::S3InfoOption;
using ::dingofs::utils::Configuration;

using ::dingofs::base::string::StrSplit;
using ::dingofs::stub::common::ExcutorOpt;
using ::dingofs::stub::common::MetaCacheOpt;

using dingofs::client::common::FLAGS_in_time_warmup;

using options::cache::DiskCacheOption;
using options::cache::DiskStateOption;

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

static void InitS3Option(Configuration* conf, S3Option* s3_opt) {
  conf->GetValueFatalIfFail("s3.fakeS3", &FLAGS_useFakeS3);
  conf->GetValueFatalIfFail("data_stream.page.size",
                            &s3_opt->s3ClientAdaptorOpt.pageSize);
  conf->GetValueFatalIfFail("s3.prefetchBlocks",
                            &s3_opt->s3ClientAdaptorOpt.prefetchBlocks);
  conf->GetValueFatalIfFail("s3.prefetchExecQueueNum",
                            &s3_opt->s3ClientAdaptorOpt.prefetchExecQueueNum);
  conf->GetValueFatalIfFail("data_stream.background_flush.interval_ms",
                            &s3_opt->s3ClientAdaptorOpt.intervalMs);
  conf->GetValueFatalIfFail("data_stream.slice.stay_in_memory_max_second",
                            &s3_opt->s3ClientAdaptorOpt.flushIntervalSec);
  conf->GetValueFatalIfFail("s3.writeCacheMaxByte",
                            &s3_opt->s3ClientAdaptorOpt.writeCacheMaxByte);
  conf->GetValueFatalIfFail("s3.readCacheMaxByte",
                            &s3_opt->s3ClientAdaptorOpt.readCacheMaxByte);
  conf->GetValueFatalIfFail("s3.readCacheThreads",
                            &s3_opt->s3ClientAdaptorOpt.readCacheThreads);
  conf->GetValueFatalIfFail("s3.nearfullRatio",
                            &s3_opt->s3ClientAdaptorOpt.nearfullRatio);
  conf->GetValueFatalIfFail("s3.baseSleepUs",
                            &s3_opt->s3ClientAdaptorOpt.baseSleepUs);
  conf->GetValueFatalIfFail("s3.maxReadRetryIntervalMs",
                            &s3_opt->s3ClientAdaptorOpt.maxReadRetryIntervalMs);
  conf->GetValueFatalIfFail("s3.readRetryIntervalMs",
                            &s3_opt->s3ClientAdaptorOpt.readRetryIntervalMs);
  dataaccess::aws::InitS3AdaptorOptionExceptS3InfoOption(conf,
                                                         &s3_opt->s3AdaptrOpt);
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

namespace {

void SplitDiskCacheOption(DiskCacheOption option,
                          std::vector<DiskCacheOption>* options) {
  std::vector<std::string> dirs = StrSplit(option.cache_dir(), ";");
  for (size_t i = 0; i < dirs.size(); i++) {
    uint64_t cache_size_mb = option.cache_size_mb();
    std::vector<std::string> items = StrSplit(dirs[i], ":");
    if (items.size() > 2 ||
        (items.size() == 2 && !Str2Int(items[1], &cache_size_mb))) {
      CHECK(false) << "Invalid cache dir: " << dirs[i];
    } else if (cache_size_mb == 0) {
      CHECK(false) << "Cache size must greater than 0.";
    }

    DiskCacheOption o = option;
    o.cache_index() = i;
    o.cache_dir() = items[0];
    o.cache_size_mb() = cache_size_mb;
    options->emplace_back(o);
  }
}

}  // namespace

static void InitBlockCacheOption(Configuration* c, BlockCacheOption* option) {
  {  // block cache option
    c->GetValueFatalIfFail("block_cache.logging", &option->logging());
    c->GetValueFatalIfFail("block_cache.cache_store", &option->cache_store());
    c->GetValueFatalIfFail("block_cache.stage", &option->stage());
    c->GetValueFatalIfFail("block_cache.stage_bandwidth_throttle_enable",
                           &option->stage_bandwidth_throttle_enable());
    c->GetValueFatalIfFail("block_cache.stage_bandwidth_throttle_mb",
                           &option->stage_bandwidth_throttle_mb());
    c->GetValueFatalIfFail("block_cache.upload_stage_workers",
                           &option->upload_stage_workers());
    c->GetValueFatalIfFail("block_cache.upload_stage_queue_size",
                           &option->upload_stage_queue_size());
    c->GetValueFatalIfFail("block_cache.prefetch_workers",
                           &option->prefetch_workers());
    c->GetValueFatalIfFail("block_cache.prefetch_queue_size",
                           &option->prefetch_queue_size());

    std::string cache_store = option->cache_store();
    if (cache_store != "none" && cache_store != "disk" &&
        cache_store != "3fs") {
      CHECK(false) << "Only support none, disk or 3fs cache store.";
    }
  }

  {  // disk cache option
    DiskCacheOption& o = option->disk_cache_option();
    c->GetValueFatalIfFail("disk_cache.cache_dir", &o.cache_dir());
    c->GetValueFatalIfFail("disk_cache.cache_size_mb", &o.cache_size_mb());
    c->GetValueFatalIfFail("disk_cache.free_space_ratio",
                           &o.free_space_ratio());
    c->GetValueFatalIfFail("disk_cache.cache_expire_second",
                           &o.cache_expire_s());
    c->GetValueFatalIfFail("disk_cache.cleanup_expire_interval_millsecond",
                           &o.cleanup_expire_interval_ms());
    c->GetValueFatalIfFail("disk_cache.drop_page_cache", &o.drop_page_cache());
    if (!c->GetUInt32Value("disk_cache.ioring_iodepth", &o.ioring_iodepth())) {
      o.ioring_iodepth() = 128;
    }
    if (!c->GetUInt32Value("disk_cache.ioring_blksize", &o.ioring_blksize())) {
      o.ioring_blksize() = 1048576;
    }
    o.ioring_prefetch() = c->GetBoolValue("disk_cache.ioring_prefetch", true);
  }

  {  // disk state option

    DiskStateOption& o = option->disk_cache_option().disk_state_option();
    c->GetValueFatalIfFail("disk_state.tick_duration_second",
                           &o.tick_duration_s());
    c->GetValueFatalIfFail("disk_state.normal2unstable_io_error_num",
                           &o.normal2unstable_io_error_num());
    c->GetValueFatalIfFail("disk_state.unstable2normal_io_succ_num",
                           &o.unstable2normal_io_succ_num());
    c->GetValueFatalIfFail("disk_state.unstable2down_second",
                           &o.unstable2down_s());
    c->GetValueFatalIfFail("disk_state.disk_check_duration_millsecond",
                           &o.disk_check_duration_ms());
  }

  // rewrite disk cache options
  {
    DiskCacheOption& o = option->disk_cache_option();
    if (option->cache_store() == "disk" || option->cache_store() == "3fs") {
      o.filesystem_type() = (option->cache_store() == "3fs") ? "3fs" : "local";
      SplitDiskCacheOption(o, &option->disk_cache_options());
    }
  }
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
  InitS3Option(conf, &client_option->s3Opt);
  InitLeaseOpt(conf, &client_option->leaseOpt);
  InitRefreshDataOpt(conf, &client_option->refreshDataOption);
  InitKVClientManagerOpt(conf, &client_option->kvClientManagerOpt);
  InitFileSystemOption(conf, &client_option->fileSystemOption);
  InitDataStreamOption(conf, &client_option->data_stream_option);
  InitBlockCacheOption(conf, &client_option->block_cache_option);
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
    LOG(INFO)
        << "Not found `fuseClient.bthread_worker_num` in conf, default to 0";
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

void SetClientS3Option(ClientOption* client_option,
                       const S3InfoOption& fs_s3_opt) {
  client_option->s3Opt.s3ClientAdaptorOpt.blockSize = fs_s3_opt.blockSize;
  client_option->s3Opt.s3ClientAdaptorOpt.chunkSize = fs_s3_opt.chunkSize;
  client_option->s3Opt.s3ClientAdaptorOpt.objectPrefix = fs_s3_opt.objectPrefix;
  client_option->s3Opt.s3AdaptrOpt.s3Address = fs_s3_opt.s3Address;
  client_option->s3Opt.s3AdaptrOpt.ak = fs_s3_opt.ak;
  client_option->s3Opt.s3AdaptrOpt.sk = fs_s3_opt.sk;
  client_option->s3Opt.s3AdaptrOpt.bucketName = fs_s3_opt.bucketName;
}

void S3Info2FsS3Option(const pb::common::S3Info& s3, S3InfoOption* fs_s3_opt) {
  fs_s3_opt->ak = s3.ak();
  fs_s3_opt->sk = s3.sk();
  fs_s3_opt->s3Address = s3.endpoint();
  fs_s3_opt->bucketName = s3.bucketname();
  fs_s3_opt->blockSize = s3.blocksize();
  fs_s3_opt->chunkSize = s3.chunksize();
  fs_s3_opt->objectPrefix = s3.has_objectprefix() ? s3.objectprefix() : 0;
}

void RewriteCacheDir(BlockCacheOption* option, std::string uuid) {
  auto& disk_cache_options = option->disk_cache_options();
  for (auto& disk_cache_option : disk_cache_options) {
    std::string cache_dir = disk_cache_option.cache_dir();
    disk_cache_option.cache_dir() = PathJoin({cache_dir, uuid});
  }
}

}  // namespace common
}  // namespace client
}  // namespace dingofs
