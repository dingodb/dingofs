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

#ifndef DINGOFS_SRC_METRICS_CLIENT_VFS_LEGACY_MDS_CIENT_H_
#define DINGOFS_SRC_METRICS_CLIENT_VFS_LEGACY_MDS_CIENT_H_

#include <bvar/bvar.h>

#include <string>

#include "metrics/metric.h"

namespace dingofs {
namespace metrics {
namespace client {
namespace vfs_legacy {

struct MDSClientMetric {
  inline static const std::string prefix = "dingofs_mds_client";

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

  explicit MDSClientMetric()
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

 public:
  MDSClientMetric(const MDSClientMetric&) = delete;

  MDSClientMetric& operator=(const MDSClientMetric&) = delete;

  static MDSClientMetric& GetInstance() {
    static MDSClientMetric instance;
    return instance;
  }
};
}  // namespace vfs_legacy
}  // namespace client
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_CLIENT_VFS_LEGACY_MDS_CIENT_H_
