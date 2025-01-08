/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"){}
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
 * Created Date: Thur Oct Jun 28 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_SRC_CLIENT_METRIC_CLIENT_METRIC_H_
#define DINGOFS_SRC_CLIENT_METRIC_CLIENT_METRIC_H_

#include <bvar/bvar.h>

#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>

#include "utils/string_util.h"
#include "utils/timeutility.h"

namespace dingofs {
namespace stub {
namespace metric {

using ::dingofs::utils::TimeUtility;

// metric stats per second
struct PerSecondMetric {
  bvar::Adder<uint64_t> count;                   // total count
  bvar::PerSecond<bvar::Adder<uint64_t>> value;  // average count persecond
  PerSecondMetric(const std::string& prefix, const std::string& name)
      : count(prefix, name + "_total_count"), value(prefix, name, &count, 1) {}
};

// interface metric statistics
struct InterfaceMetric {
  PerSecondMetric qps;             // processed per second
  PerSecondMetric eps;             // error request per second
  PerSecondMetric bps;             // throughput with byte per second
  bvar::LatencyRecorder latency;   // latency
  bvar::Adder<uint64_t> latTotal;  // latency total value

  InterfaceMetric(const std::string& prefix, const std::string& name)
      : qps(prefix, name + "_qps"),
        eps(prefix, name + "_eps"),
        bps(prefix, name + "_bps"),
        latency(prefix, name + "_lat", 1),
        latTotal(prefix, name + "_lat_total_value") {}
};

struct MDSClientMetric {
  static const std::string prefix;

  InterfaceMetric mountFs;
  InterfaceMetric umountFs;
  InterfaceMetric getFsInfo;
  InterfaceMetric getMetaServerInfo;
  InterfaceMetric getMetaServerListInCopysets;
  InterfaceMetric createPartition;
  InterfaceMetric getCopysetOfPartitions;
  InterfaceMetric listPartition;
  InterfaceMetric allocS3ChunkId;
  InterfaceMetric refreshSession;
  InterfaceMetric getLatestTxId;
  InterfaceMetric commitTx;
  InterfaceMetric allocOrGetMemcacheCluster;
  // all
  InterfaceMetric getAllOperation;

  MDSClientMetric()
      : mountFs(prefix, "mountFs"),
        umountFs(prefix, "umountFs"),
        getFsInfo(prefix, "getFsInfo"),
        getMetaServerInfo(prefix, "getMetaServerInfo"),
        getMetaServerListInCopysets(prefix, "getMetaServerListInCopysets"),
        createPartition(prefix, "createPartition"),
        getCopysetOfPartitions(prefix, "getCopysetOfPartitions"),
        listPartition(prefix, "listPartition"),
        allocS3ChunkId(prefix, "allocS3ChunkId"),
        refreshSession(prefix, "refreshSession"),
        getLatestTxId(prefix, "getLatestTxId"),
        commitTx(prefix, "commitTx"),
        allocOrGetMemcacheCluster(prefix, "allocOrGetMemcacheCluster"),
        getAllOperation(prefix, "getAllopt") {}
};

struct MetaServerClientMetric {
  static const std::string prefix;

  // dentry
  InterfaceMetric getDentry;
  InterfaceMetric listDentry;
  InterfaceMetric createDentry;
  InterfaceMetric deleteDentry;

  // inode
  InterfaceMetric getInode;
  InterfaceMetric batchGetInodeAttr;
  InterfaceMetric batchGetXattr;
  InterfaceMetric createInode;
  InterfaceMetric updateInode;
  InterfaceMetric deleteInode;
  InterfaceMetric appendS3ChunkInfo;

  // txn
  InterfaceMetric prepareRenameTx;

  // volume extent
  InterfaceMetric updateVolumeExtent;
  InterfaceMetric getVolumeExtent;

  // write operation
  InterfaceMetric getTxnOperation;

  // qutoa related
  InterfaceMetric get_fs_quota;
  InterfaceMetric flush_fs_usage;
  InterfaceMetric load_dir_quotas;
  InterfaceMetric flush_dir_usages;

  // all
  InterfaceMetric getAllOperation;

  MetaServerClientMetric()
      : getDentry(prefix, "getDentry"),
        listDentry(prefix, "listDentry"),
        createDentry(prefix, "createDentry"),
        deleteDentry(prefix, "deleteDentry"),
        getInode(prefix, "getInode"),
        batchGetInodeAttr(prefix, "batchGetInodeAttr"),
        batchGetXattr(prefix, "batchGetXattr"),
        createInode(prefix, "createInode"),
        updateInode(prefix, "updateInode"),
        deleteInode(prefix, "deleteInode"),
        appendS3ChunkInfo(prefix, "appendS3ChunkInfo"),
        prepareRenameTx(prefix, "prepareRenameTx"),
        updateVolumeExtent(prefix, "updateVolumeExtent"),
        getVolumeExtent(prefix, "getVolumeExtent"),
        getTxnOperation(prefix, "getTxnopt"),
        get_fs_quota(prefix, "getFsQuota"),
        flush_fs_usage(prefix, "flushFsUsage"),
        load_dir_quotas(prefix, "loadDirQuotas"),
        flush_dir_usages(prefix, "flushDirUsages"),
        getAllOperation(prefix, "getAllopt") {}
};

struct OpMetric {
  bvar::LatencyRecorder latency;
  bvar::Adder<int64_t> inflightOpNum;
  bvar::Adder<uint64_t> ecount;
  bvar::Adder<uint64_t> qpsTotal;  // qps total count
  bvar::Adder<uint64_t> latTotal;  // latency total count

  explicit OpMetric(const std::string& prefix, const std::string& name)
      : latency(prefix, name + "_lat", 1),
        inflightOpNum(prefix, name + "_inflight_num"),
        ecount(prefix, name + "_error_num"),
        qpsTotal(prefix, name + "_qps_total_count"),
        latTotal(prefix, name + "_lat_total_value") {}
};

struct ClientOpMetric {
  static const std::string prefix;

  OpMetric opLookup;
  OpMetric opOpen;
  OpMetric opCreate;
  OpMetric opMkNod;
  OpMetric opMkDir;
  OpMetric opLink;
  OpMetric opUnlink;
  OpMetric opRmDir;
  OpMetric opOpenDir;
  OpMetric opReleaseDir;
  OpMetric opReadDir;
  OpMetric opRename;
  OpMetric opGetAttr;
  OpMetric opSetAttr;
  OpMetric opGetXattr;
  OpMetric opListXattr;
  OpMetric opSymlink;
  OpMetric opReadLink;
  OpMetric opRelease;
  OpMetric opFsync;
  OpMetric opFlush;
  OpMetric opRead;
  OpMetric opWrite;
  OpMetric opAll;

  ClientOpMetric()
      : opLookup(prefix, "opLookup"),
        opOpen(prefix, "opOpen"),
        opCreate(prefix, "opCreate"),
        opMkNod(prefix, "opMknod"),
        opMkDir(prefix, "opMkdir"),
        opLink(prefix, "opLink"),
        opUnlink(prefix, "opUnlink"),
        opRmDir(prefix, "opRmdir"),
        opOpenDir(prefix, "opOpendir"),
        opReleaseDir(prefix, "opReleasedir"),
        opReadDir(prefix, "opReaddir"),
        opRename(prefix, "opRename"),
        opGetAttr(prefix, "opGetattr"),
        opSetAttr(prefix, "opSetattr"),
        opGetXattr(prefix, "opGetxattr"),
        opListXattr(prefix, "opListxattr"),
        opSymlink(prefix, "opSymlink"),
        opReadLink(prefix, "opReadlink"),
        opRelease(prefix, "opRelease"),
        opFsync(prefix, "opFsync"),
        opFlush(prefix, "opFlush"),
        opRead(prefix, "opRead"),
        opWrite(prefix, "opWrite"),
        opAll(prefix, "opAll") {}
};

struct S3MultiManagerMetric {
  static const std::string prefix;

  bvar::Adder<int64_t> fileManagerNum;
  bvar::Adder<int64_t> chunkManagerNum;
  bvar::Adder<int64_t> writeDataCacheNum;
  bvar::Adder<int64_t> writeDataCacheByte;
  bvar::Adder<int64_t> readDataCacheNum;
  bvar::Adder<int64_t> readDataCacheByte;

  S3MultiManagerMetric() {
    fileManagerNum.expose_as(prefix, "file_manager_num");
    chunkManagerNum.expose_as(prefix, "chunk_manager_num");
    writeDataCacheNum.expose_as(prefix, "write_data_cache_num");
    writeDataCacheByte.expose_as(prefix, "write_data_cache_byte");
    readDataCacheNum.expose_as(prefix, "read_data_cache_num");
    readDataCacheByte.expose_as(prefix, "read_data_cache_byte");
  }
};

struct FSMetric {
  static const std::string prefix;

  std::string fsName;

  InterfaceMetric userWrite;
  InterfaceMetric userRead;
  bvar::Status<uint32_t> userWriteIoSize;  // last write io size
  bvar::Status<uint32_t> userReadIoSize;   // last read io size

  explicit FSMetric(const std::string& name = "")
      : userWrite(prefix, "_userWrite"),
        userRead(prefix, "_userRead"),
        userWriteIoSize(prefix, "_userWriteIoSizeLast", 0),
        userReadIoSize(prefix, "_userReadIoSizeLast", 0) {
    (void)name;
  }
};

struct S3Metric {
  static const std::string prefix;

  InterfaceMetric write_s3;
  InterfaceMetric read_s3;

 private:
  explicit S3Metric()
      : write_s3(prefix, "_write_s3"), read_s3(prefix, "_read_s3") {}
  S3Metric(const S3Metric&) = delete;
  S3Metric& operator=(const S3Metric&) = delete;

 public:
  static S3Metric& GetInstance() {
    static S3Metric instance_;
    return instance_;
  }
};

struct DiskCacheMetric {
  static const std::string prefix;

  InterfaceMetric write_disk;
  InterfaceMetric read_disk;

 private:
  explicit DiskCacheMetric()
      : write_disk(prefix, "_write_disk"), read_disk(prefix, "_read_disk") {}
  DiskCacheMetric(const DiskCacheMetric&) = delete;
  DiskCacheMetric& operator=(const DiskCacheMetric&) = delete;

 public:
  static DiskCacheMetric& GetInstance() {
    static DiskCacheMetric instance_;
    return instance_;
  }
};

struct KVClientMetric {
  static const std::string prefix;
  InterfaceMetric kvClientGet;
  InterfaceMetric kvClientSet;

  KVClientMetric() : kvClientGet(prefix, "get"), kvClientSet(prefix, "set") {}
};

struct S3ChunkInfoMetric {
  static const std::string prefix;

  bvar::Adder<int64_t> s3ChunkInfoSize;

  S3ChunkInfoMetric() : s3ChunkInfoSize(prefix, "size") {}
};

struct WarmupManagerS3Metric {
  static const std::string prefix;

  InterfaceMetric warmupS3Cached;
  bvar::Adder<uint64_t> warmupS3CacheSize;

  WarmupManagerS3Metric()
      : warmupS3Cached(prefix, "s3_cached"),
        warmupS3CacheSize(prefix, "s3_cache_size") {}
};

struct MetricGuard {
  explicit MetricGuard(int* rc, InterfaceMetric* metric, size_t count,
                       uint64_t start)
      : rc_(rc), metric_(metric), count_(count), start_(start) {}
  ~MetricGuard() {
    if (*rc_ == 0) {
      metric_->bps.count << count_;
      metric_->qps.count << 1;
      auto duration = butil::cpuwide_time_us() - start_;
      metric_->latency << duration;
      metric_->latTotal << duration;
    } else {
      metric_->eps.count << 1;
    }
  }
  int* rc_;
  InterfaceMetric* metric_;
  size_t count_;
  uint64_t start_;
};
// metric guard for one or more metrics collection
struct MetricListGuard {
  explicit MetricListGuard(bool* rc, std::list<InterfaceMetric*> metricList,
                           uint64_t start)
      : rc_(rc), metricList_(metricList), start_(start) {}
  ~MetricListGuard() {
    for (auto& metric_ : metricList_) {
      auto duration = butil::cpuwide_time_us() - start_;
      if (*rc_) {
        metric_->qps.count << 1;
        metric_->latency << duration;
        metric_->latTotal << duration;
      } else {
        metric_->eps.count << 1;
      }
    }
  }
  bool* rc_;
  std::list<InterfaceMetric*> metricList_;
  uint64_t start_;
};

struct LatencyGuard {
  bvar::LatencyRecorder* latencyRec;
  uint64_t startTimeUs;

  explicit LatencyGuard(bvar::LatencyRecorder* latency) {
    latencyRec = latency;
    startTimeUs = TimeUtility::GetTimeofDayUs();
  }

  ~LatencyGuard() {
    *latencyRec << (TimeUtility::GetTimeofDayUs() - startTimeUs);
  }
};

}  // namespace metric
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_METRIC_CLIENT_METRIC_H_
