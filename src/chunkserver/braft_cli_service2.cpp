/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 */

#include "src/chunkserver/braft_cli_service2.h"

#include <braft/closure_helper.h>  // NewCallback
#include <braft/node_manager.h>    // NodeManager
#include <brpc/controller.h>       // brpc::Controller
#include <glog/logging.h>

#include <cerrno>
#include <string>
#include <vector>

namespace curve {
namespace chunkserver {

static void add_peer_returned(brpc::Controller* cntl,
                              const AddPeerRequest2* request,
                              AddPeerResponse2* response,
                              std::vector<braft::PeerId> old_peers,
                              scoped_refptr<braft::NodeImpl> /*node*/,
                              Closure* done, const butil::Status& st) {
  brpc::ClosureGuard done_guard(done);
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  bool already_exists = false;
  for (size_t i = 0; i < old_peers.size(); ++i) {
    response->add_oldpeers()->set_address(old_peers[i].to_string());
    response->add_newpeers()->set_address(old_peers[i].to_string());
    if (old_peers[i] == request->addpeer().address()) {
      already_exists = true;
    }
  }
  if (!already_exists) {
    response->add_newpeers()->set_address(request->addpeer().address());
  }
}

void BRaftCliServiceImpl2::AddPeer(RpcController* controller,
                                   const AddPeerRequest2* request,
                                   AddPeerResponse2* response, Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  scoped_refptr<braft::NodeImpl> node;
  LogicPoolID logicPoolId = request->logicpoolid();
  CopysetID copysetId = request->copysetid();
  butil::Status st =
      get_node(&node, logicPoolId, copysetId, request->leader().address());
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  std::vector<braft::PeerId> peers;
  st = node->list_peers(&peers);
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  braft::PeerId adding_peer;
  if (adding_peer.parse(request->addpeer().address()) != 0) {
    cntl->SetFailed(EINVAL, "Fail to parse peer_id %s",
                    request->addpeer().address().c_str());
    return;
  }
  LOG(WARNING) << "Receive AddPeerRequest to " << node->node_id() << " from "
               << cntl->remote_side() << ", adding "
               << request->addpeer().address();
  braft::Closure* add_peer_done =
      NewCallback(add_peer_returned, cntl, request, response, peers, node,
                  done_guard.release());
  return node->add_peer(adding_peer, add_peer_done);
}

static void remove_peer_returned(brpc::Controller* cntl,
                                 const RemovePeerRequest2* request,
                                 RemovePeerResponse2* response,
                                 std::vector<braft::PeerId> old_peers,
                                 scoped_refptr<braft::NodeImpl> /*node*/,
                                 Closure* done, const butil::Status& st) {
  brpc::ClosureGuard done_guard(done);
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  for (size_t i = 0; i < old_peers.size(); ++i) {
    response->add_oldpeers()->set_address(old_peers[i].to_string());
    if (old_peers[i] != request->removepeer().address()) {
      response->add_newpeers()->set_address(old_peers[i].to_string());
    }
  }
}

void BRaftCliServiceImpl2::RemovePeer(RpcController* controller,
                                      const RemovePeerRequest2* request,
                                      RemovePeerResponse2* response,
                                      Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  scoped_refptr<braft::NodeImpl> node;
  LogicPoolID logicPoolId = request->logicpoolid();
  CopysetID copysetId = request->copysetid();
  butil::Status st =
      get_node(&node, logicPoolId, copysetId, request->leader().address());
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  std::vector<braft::PeerId> peers;
  st = node->list_peers(&peers);
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  braft::PeerId removing_peer;
  if (removing_peer.parse(request->removepeer().address()) != 0) {
    cntl->SetFailed(EINVAL, "Fail to parse peer_id %s",
                    request->removepeer().address().c_str());
    return;
  }
  LOG(WARNING) << "Receive RemovePeerRequest to " << node->node_id() << " from "
               << cntl->remote_side() << ", removing "
               << request->removepeer().address();
  braft::Closure* remove_peer_done =
      NewCallback(remove_peer_returned, cntl, request, response, peers, node,
                  done_guard.release());
  return node->remove_peer(removing_peer, remove_peer_done);
}

static void change_peers_returned(
    brpc::Controller* cntl, const ChangePeersRequest2* request,
    ChangePeersResponse2* response, std::vector<braft::PeerId> old_peers,
    Configuration new_peers, scoped_refptr<braft::NodeImpl> /*node*/,
    ::google::protobuf::Closure* done, const butil::Status& st) {
  (void)request;
  brpc::ClosureGuard done_guard(done);
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  for (size_t i = 0; i < old_peers.size(); ++i) {
    response->add_oldpeers()->set_address(old_peers[i].to_string());
  }
  for (Configuration::const_iterator iter = new_peers.begin();
       iter != new_peers.end(); ++iter) {
    response->add_newpeers()->set_address(iter->to_string());
  }
}

void BRaftCliServiceImpl2::ChangePeers(RpcController* controller,
                                       const ChangePeersRequest2* request,
                                       ChangePeersResponse2* response,
                                       Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  scoped_refptr<braft::NodeImpl> node;
  LogicPoolID logicPoolId = request->logicpoolid();
  CopysetID copysetId = request->copysetid();
  butil::Status st =
      get_node(&node, logicPoolId, copysetId, request->leader().address());
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  std::vector<braft::PeerId> peers;
  st = node->list_peers(&peers);
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  Configuration conf;
  for (int i = 0; i < request->newpeers_size(); ++i) {
    PeerId peer;
    if (peer.parse(request->newpeers(i).address()) != 0) {
      cntl->SetFailed(EINVAL, "Fail to p2arse %s",
                      request->newpeers(i).address().c_str());
      return;
    }
    conf.add_peer(peer);
  }
  LOG(WARNING) << "Receive ChangePeersRequest to " << node->node_id()
               << " from " << cntl->remote_side() << ", new conf: " << conf;
  braft::Closure* change_peers_done =
      NewCallback(change_peers_returned, cntl, request, response, peers, conf,
                  node, done_guard.release());
  return node->change_peers(conf, change_peers_done);
}

void BRaftCliServiceImpl2::GetLeader(RpcController* controller,
                                     const GetLeaderRequest2* request,
                                     GetLeaderResponse2* response,
                                     Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  std::vector<scoped_refptr<braft::NodeImpl>> nodes;
  braft::NodeManager* const nm = braft::NodeManager::GetInstance();
  LogicPoolID logicPoolId = request->logicpoolid();
  CopysetID copysetId = request->copysetid();
  braft::GroupId group_id = ToGroupId(logicPoolId, copysetId);
  nm->get_nodes_by_group_id(group_id, &nodes);
  if (nodes.empty()) {
    cntl->SetFailed(ENOENT, "No nodes in group %s", group_id.c_str());
    return;
  }
  for (size_t i = 0; i < nodes.size(); ++i) {
    braft::PeerId leader_id = nodes[i]->leader_id();
    if (!leader_id.is_empty()) {
      Peer* peer = new Peer();
      response->set_allocated_leader(peer);
      peer->set_address(leader_id.to_string());
      return;
    }
  }
  cntl->SetFailed(EAGAIN, "Unknown leader");
}

butil::Status BRaftCliServiceImpl2::get_node(
    scoped_refptr<braft::NodeImpl>* node, const LogicPoolID& logicPoolId,
    const CopysetID& copysetId, const std::string& peer_id) {
  braft::GroupId group_id = ToGroupId(logicPoolId, copysetId);
  braft::NodeManager* const nm = braft::NodeManager::GetInstance();
  /* peer id is required have been guaranteed in proto */
  *node = nm->get(group_id, peer_id);
  if (!(*node)) {
    return butil::Status(ENOENT, "Fail to find node %s in group %s",
                         peer_id.c_str(), group_id.c_str());
  }

  if ((*node)->disable_cli()) {
    return butil::Status(EACCES,
                         "CliService is not allowed to access node "
                         "%s",
                         (*node)->node_id().to_string().c_str());
  }

  return butil::Status::OK();
}

void BRaftCliServiceImpl2::TransferLeader(RpcController* controller,
                                          const TransferLeaderRequest2* request,
                                          TransferLeaderResponse2* response,
                                          ::google::protobuf::Closure* done) {
  (void)response;
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  scoped_refptr<braft::NodeImpl> node;
  LogicPoolID logicPoolId = request->logicpoolid();
  CopysetID copysetId = request->copysetid();
  butil::Status st =
      get_node(&node, logicPoolId, copysetId, request->leader().address());
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }
  braft::PeerId peer = braft::ANY_PEER;
  if (peer.parse(request->transferee().address()) != 0) {
    cntl->SetFailed(EINVAL, "Fail to parse %s",
                    request->transferee().address().c_str());
    return;
  }
  const int rc = node->transfer_leadership_to(peer);
  if (rc != 0) {
    cntl->SetFailed(rc, "Fail to invoke transfer_leadership_to : %s",
                    berror(rc));
    return;
  }
}

void BRaftCliServiceImpl2::ResetPeer(RpcController* controller,
                                     const ResetPeerRequest2* request,
                                     ResetPeerResponse2* response,
                                     Closure* done) {
  (void)response;
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  scoped_refptr<braft::NodeImpl> node;
  LogicPoolID logicPoolId = request->logicpoolid();
  CopysetID copysetId = request->copysetid();
  butil::Status st =
      get_node(&node, logicPoolId, copysetId, request->requestpeer().address());
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }

  Configuration conf;
  for (int i = 0; i < request->newpeers_size(); ++i) {
    PeerId peer;
    if (peer.parse(request->newpeers(i).address()) != 0) {
      cntl->SetFailed(EINVAL, "Fail to p2arse %s",
                      request->newpeers(i).address().c_str());
      return;
    }
    conf.add_peer(peer);
  }
  LOG(WARNING) << "Receive ResetPeerRequest to " << node->node_id() << " from "
               << cntl->remote_side() << ", new conf: " << conf;

  st = node->reset_peers(conf);
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
  }
}

static void snapshot_returned(brpc::Controller* cntl,
                              scoped_refptr<braft::NodeImpl> node,
                              ::google::protobuf::Closure* done,
                              const butil::Status& st) {
  (void)node;
  brpc::ClosureGuard done_guard(done);
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
  }
}

void BRaftCliServiceImpl2::Snapshot(RpcController* controller,
                                    const SnapshotRequest2* request,
                                    SnapshotResponse2* response,
                                    Closure* done) {
  (void)response;
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  scoped_refptr<braft::NodeImpl> node;
  LogicPoolID logicPoolId = request->logicpoolid();
  CopysetID copysetId = request->copysetid();
  butil::Status st =
      get_node(&node, logicPoolId, copysetId, request->peer().address());
  if (!st.ok()) {
    cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    return;
  }

  braft::Closure* snapshot_done =
      NewCallback(snapshot_returned, cntl, node, done_guard.release());
  return node->snapshot(snapshot_done);
}

void BRaftCliServiceImpl2::SnapshotAll(RpcController* controller,
                                       const SnapshotAllRequest* request,
                                       SnapshotAllResponse* response,
                                       Closure* done) {
  (void)request;
  (void)response;
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  braft::NodeManager* const nm = braft::NodeManager::GetInstance();
  std::vector<scoped_refptr<braft::NodeImpl>> nodes;
  nm->get_all_nodes(&nodes);
  for (const auto& node : nodes) {
    braft::Closure* snapshot_done =
        NewCallback(snapshot_returned, cntl, node, done_guard.release());
    node->snapshot(snapshot_done);
  }
}

}  // namespace chunkserver
}  // namespace curve
