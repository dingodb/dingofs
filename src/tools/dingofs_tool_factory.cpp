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
 * Created Date: 2021-09-14
 * Author: chengyi01
 */

#include "tools/dingofs_tool_factory.h"

#include "tools/check/dingofs_copyset_check.h"
#include "tools/copyset/dingofs_copyset_status.h"
#include "tools/create/dingofs_create_fs.h"
#include "tools/create/dingofs_create_topology_tool.h"
#include "tools/delete/dingofs_delete_fs_tool.h"
#include "tools/delete/dingofs_delete_partition_tool.h"
#include "tools/dingofs_tool_abstract_creator.h"
#include "tools/dingofs_tool_define.h"
#include "tools/list/dingofs_copysetinfo_list.h"
#include "tools/list/dingofs_fsinfo_list.h"
#include "tools/list/dingofs_partition_list.h"
#include "tools/list/dingofs_topology_list.h"
#include "tools/query/dingofs_copyset_query.h"
#include "tools/query/dingofs_fs_query.h"
#include "tools/query/dingofs_inode_query.h"
#include "tools/query/dingofs_metaserver_query.h"
#include "tools/query/dingofs_partition_query.h"
#include "tools/status/dingofs_copyset_status.h"
#include "tools/status/dingofs_etcd_status.h"
#include "tools/status/dingofs_mds_status.h"
#include "tools/status/dingofs_metaserver_status.h"
#include "tools/status/dingofs_status.h"
#include "tools/umount/dingofs_umount_fs_tool.h"
#include "tools/usage/dingofs_metadata_usage_tool.h"
#include "tools/version/dingofs_version_tool.h"

namespace dingofs {
namespace tools {

DingofsToolFactory::DingofsToolFactory() {
  // version
  RegisterDingofsTool(std::string(kVersionCmd),
                      DingofsToolCreator<version::VersionTool>::Create);

  // umount-fs
  RegisterDingofsTool(std::string(kUmountFsCmd),
                      DingofsToolCreator<umount::UmountFsTool>::Create);

  // create-topology
  RegisterDingofsTool(
      std::string(kCreateTopologyCmd),
      DingofsToolCreator<topology::DingofsBuildTopologyTool>::Create);

  // create-fs
  RegisterDingofsTool(std::string(kCreateFsCmd),
                      DingofsToolCreator<create::CreateFsTool>::Create);

  // usage-metadata
  RegisterDingofsTool(std::string(kMetedataUsageCmd),
                      DingofsToolCreator<usage::MatedataUsageTool>::Create);
  // status
  RegisterDingofsTool(std::string(kStatusCmd),
                      DingofsToolCreator<status::StatusTool>::Create);
  // status-mds
  RegisterDingofsTool(std::string(kMdsStatusCmd),
                      DingofsToolCreator<status::MdsStatusTool>::Create);

  // status-metaserver
  RegisterDingofsTool(std::string(kMetaserverStatusCmd),
                      DingofsToolCreator<status::MetaserverStatusTool>::Create);

  // status-etcd
  RegisterDingofsTool(std::string(kEtcdStatusCmd),
                      DingofsToolCreator<status::EtcdStatusTool>::Create);

  // status-copyset
  RegisterDingofsTool(std::string(kCopysetStatusCmd),
                      DingofsToolCreator<status::CopysetStatusTool>::Create);

  // list-fs
  RegisterDingofsTool(std::string(kFsInfoListCmd),
                      DingofsToolCreator<list::FsInfoListTool>::Create);
  // list-copysetInfo
  RegisterDingofsTool(std::string(kCopysetInfoListCmd),
                      DingofsToolCreator<list::CopysetInfoListTool>::Create);

  // list-topology
  RegisterDingofsTool(std::string(kTopologyListCmd),
                      DingofsToolCreator<list::TopologyListTool>::Create);

  // list-partition
  RegisterDingofsTool(std::string(kPartitionListCmd),
                      DingofsToolCreator<list::PartitionListTool>::Create);

  // query-copyset
  RegisterDingofsTool(std::string(kCopysetQueryCmd),
                      DingofsToolCreator<query::CopysetQueryTool>::Create);

  // query-partion
  RegisterDingofsTool(std::string(kPartitionQueryCmd),
                      DingofsToolCreator<query::PartitionQueryTool>::Create);

  // query-metaserver
  RegisterDingofsTool(std::string(kMetaserverQueryCmd),
                      DingofsToolCreator<query::MetaserverQueryTool>::Create);

  // query-fs
  RegisterDingofsTool(std::string(kFsQueryCmd),
                      DingofsToolCreator<query::FsQueryTool>::Create);

  // query-inode
  RegisterDingofsTool(std::string(kInodeQueryCmd),
                      DingofsToolCreator<query::InodeQueryTool>::Create);

  // delete-fs
  RegisterDingofsTool(std::string(kDeleteFsCmd),
                      DingofsToolCreator<delete_::DeleteFsTool>::Create);

  // delete-partition
  RegisterDingofsTool(std::string(kPartitionDeleteCmd),
                      DingofsToolCreator<delete_::DeletePartitionTool>::Create);

  // check-copyset
  RegisterDingofsTool(std::string(kCopysetCheckCmd),
                      DingofsToolCreator<check::CopysetCheckTool>::Create);
}

std::shared_ptr<DingofsTool> DingofsToolFactory::GenerateDingofsTool(
    const std::string& command) {
  auto search = command2creator_.find(command);
  if (search != command2creator_.end()) {
    return search->second();
  }
  return nullptr;
}

void DingofsToolFactory::RegisterDingofsTool(
    const std::string& command,
    const std::function<std::shared_ptr<DingofsTool>()>& function) {
  command2creator_.insert({command, function});
}

}  // namespace tools
}  // namespace dingofs
