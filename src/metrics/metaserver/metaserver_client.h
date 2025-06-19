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

#ifndef DINGOFS_SRC_METRICS_METASERVER_METASERVER_CIENT_H_
#define DINGOFS_SRC_METRICS_METASERVER_METASERVER_CIENT_H_

#include <bvar/bvar.h>

#include <string>

#include "metrics/metric.h"

namespace dingofs {
namespace metrics {
namespace metaserver {

struct MetaServerClientMetric {
  inline static const std::string prefix = "dingofs_metaserver_client";

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

}  // namespace metaserver
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_METASERVER_METASERVER_CIENT_H_
