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
 * @Project: curve
 * @Date: 2021-11-11 15:28:57
 * @Author: chenwei
 */

#include "curvefs/src/mds/heartbeat/copyset_conf_generator.h"

#include <glog/logging.h>

#include <string>

#include "curvefs/proto/heartbeat.pb.h"
#include "curvefs/src/mds/topology/deal_peerid.h"

using ::curvefs::mds::topology::TopoStatusCode;
using std::chrono::milliseconds;

namespace curvefs {
namespace mds {
namespace heartbeat {
bool CopysetConfGenerator::GenCopysetConf(
    MetaServerIdType report_id,
    const ::curvefs::mds::topology::CopySetInfo& report_copy_set_info,
    const ::curvefs::mds::heartbeat::ConfigChangeInfo& config_ch_info,
    ::curvefs::mds::heartbeat::CopySetConf* copyset_conf) {
  // if copyset is creating return false directly
  if (topo_->IsCopysetCreating(report_copy_set_info.GetCopySetKey())) {
    return false;
  }

  // reported copyset not exist in topology
  // in this case an empty configuration will be sent to metaserver
  // to delete it
  ::curvefs::mds::topology::CopySetInfo record_copy_set_info;
  if (!topo_->GetCopySet(report_copy_set_info.GetCopySetKey(),
                         &record_copy_set_info)) {
    LOG(ERROR) << "heartbeatManager receive copyset("
               << report_copy_set_info.GetPoolId() << ","
               << report_copy_set_info.GetId()
               << ") information, but can not get info from topology";
    copyset_conf->set_poolid(report_copy_set_info.GetPoolId());
    copyset_conf->set_copysetid(report_copy_set_info.GetId());
    copyset_conf->set_epoch(0);
    return true;
  }

  if (report_copy_set_info.GetLeader() == report_id) {
    MetaServerIdType candidate = LeaderGenCopysetConf(
        report_copy_set_info, config_ch_info, copyset_conf);
    // new config to dispatch available, update candidate to topology
    if (candidate != ::curvefs::mds::topology::UNINITIALIZE_ID) {
      auto new_copy_set_info = report_copy_set_info;
      new_copy_set_info.SetCandidate(candidate);
      TopoStatusCode update_code = topo_->UpdateCopySetTopo(new_copy_set_info);
      if (TopoStatusCode::TOPO_OK != update_code) {
        // error occurs when update to memory of topology
        LOG(WARNING) << "topoUpdater update copyset("
                     << report_copy_set_info.GetPoolId() << ","
                     << report_copy_set_info.GetId()
                     << ") got error code: " << update_code;
        return false;
      } else {
        // update to memory successfully
        return true;
      }
    } else {
      // no config to dispatch
      return false;
    }
  } else {
    return FollowerGenCopysetConf(report_id, report_copy_set_info,
                                  record_copy_set_info, copyset_conf);
  }
}

MetaServerIdType CopysetConfGenerator::LeaderGenCopysetConf(
    const ::curvefs::mds::topology::CopySetInfo& copy_set_info,
    const ::curvefs::mds::heartbeat::ConfigChangeInfo& config_ch_info,
    ::curvefs::mds::heartbeat::CopySetConf* copyset_conf) {
  // pass data to scheduler
  return coordinator_->CopySetHeartbeat(copy_set_info, config_ch_info,
                                        copyset_conf);
}

bool CopysetConfGenerator::FollowerGenCopysetConf(
    MetaServerIdType report_id,
    const ::curvefs::mds::topology::CopySetInfo& report_copy_set_info,
    const ::curvefs::mds::topology::CopySetInfo& record_copy_set_info,
    ::curvefs::mds::heartbeat::CopySetConf* copyset_conf) {
  // if there's no candidate on a copyset, and epoch on MDS is larger or equal
  // to what non-leader copy report,
  // copy(ies) can be deleted according to the configuration of MDS

  // epoch that MDS recorded >= epoch reported
  if (record_copy_set_info.GetEpoch() >= report_copy_set_info.GetEpoch()) {
    steady_clock::duration time_pass = steady_clock::now() - mdsStartTime_;
    if (time_pass < milliseconds(cleanFollowerAfterMs_)) {
      LOG_FIRST_N(INFO, 1) << "begin to clean follower copyset after "
                           << cleanFollowerAfterMs_ / 1000
                           << " seconds of mds start";
      return false;
    }
    // judge whether the reporting metaserver is in the copyset it reported,
    // and whether this metaserver is a candidate or going to be added into
    // the copyset
    bool exist = record_copy_set_info.HasMember(report_id);
    if (exist || report_id == record_copy_set_info.GetCandidate() ||
        coordinator_->MetaserverGoingToAdd(
            report_id, report_copy_set_info.GetCopySetKey())) {
      return false;
    } else {
      LOG(WARNING) << "report metaserver: " << report_id
                   << " is not a replica or candidate of copyset("
                   << record_copy_set_info.GetPoolId() << ","
                   << record_copy_set_info.GetId() << ")";
    }

    // if the metaserver doesn't belong to any of the copyset, MDS will
    // dispatch the configuration
    copyset_conf->set_poolid(record_copy_set_info.GetPoolId());
    copyset_conf->set_copysetid(record_copy_set_info.GetId());
    copyset_conf->set_epoch(record_copy_set_info.GetEpoch());
    if (record_copy_set_info.HasCandidate()) {
      std::string candidate_addr =
          BuildPeerByMetaserverId(record_copy_set_info.GetCandidate());
      if (candidate_addr.empty()) {
        return false;
      }
      auto* replica = new ::curvefs::common::Peer();
      replica->set_id(record_copy_set_info.GetCandidate());
      replica->set_address(candidate_addr);
      // memory of replica will be free by proto
      copyset_conf->set_allocated_configchangeitem(replica);
    }

    for (const auto& peer : record_copy_set_info.GetCopySetMembers()) {
      std::string add_peer = BuildPeerByMetaserverId(peer);
      if (add_peer.empty()) {
        return false;
      }
      auto* replica = copyset_conf->add_peers();
      replica->set_id(peer);
      replica->set_address(add_peer);
    }
    return true;
  }
  return false;
}

std::string CopysetConfGenerator::BuildPeerByMetaserverId(
    MetaServerIdType ms_id) {
  MetaServer meta_server;
  if (!topo_->GetMetaServer(ms_id, &meta_server)) {
    return "";
  }

  return topology::BuildPeerIdWithIpPort(meta_server.GetInternalIp(),
                                         meta_server.GetInternalPort(), 0);
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs
