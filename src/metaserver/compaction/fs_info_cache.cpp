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

#include "metaserver/compaction/fs_info_cache.h"

namespace dingofs {
namespace metaserver {

using pb::mds::FsInfo;

void FsInfoCache::UpdateRecentUnlocked(uint64_t fsid) {
  if (pos_.find(fsid) != pos_.end()) {
    recent_.erase(pos_[fsid]);
  } else if (recent_.size() >= capacity_) {
    auto old = recent_.back();
    recent_.pop_back();
    cache_.erase(old);
    pos_.erase(old);
  }
  recent_.push_front(fsid);
  pos_[fsid] = recent_.begin();
}

FsInfo FsInfoCache::GetUnlocked(uint64_t fsid) {
  UpdateRecentUnlocked(fsid);
  return cache_[fsid];
}

void FsInfoCache::PutUnlocked(uint64_t fsid, const FsInfo& fs_info) {
  UpdateRecentUnlocked(fsid);
  cache_[fsid] = fs_info;
  ;
}

Status FsInfoCache::RequestFsInfo(uint64_t fsid, FsInfo* fs_info) {
  // send GetFsInfoRequest to mds
  pb::mds::GetFsInfoRequest request;
  pb::mds::GetFsInfoResponse response;
  request.set_fsid(fsid);

  brpc::Channel channel;

  std::string current_mds_addr = mds_addrs_[mds_index_];
  if (channel.Init(current_mds_addr.c_str(), nullptr) != 0) {
    LOG(WARNING) << "Fail to init channle to Mds: " << current_mds_addr
                 << " in metaserver: " << metaserver_addr_.ip << ":"
                 << metaserver_addr_.port;
    return Status::InvalidParam("Fail to init channel to Mds");
  }

  pb::mds::MdsService_Stub stub(&channel);
  brpc::Controller cntl;
  stub.GetFsInfo(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail get fs info from Mds: " << current_mds_addr
                 << ",in metaserver: " << metaserver_addr_.ip << ":"
                 << metaserver_addr_.port
                 << " cntl errorCode: " << cntl.ErrorCode()
                 << " cntl error: " << cntl.ErrorText();

    if (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == ETIMEDOUT ||
        cntl.ErrorCode() == brpc::ELOGOFF ||
        cntl.ErrorCode() == brpc::ERPCTIMEDOUT) {
      mds_index_ = (mds_index_ + 1) % mds_addrs_.size();
      LOG(INFO) << "Next will try next Mds: " << mds_addrs_[mds_index_];
    }

    return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
  } else {
    auto ret = response.statuscode();
    if (ret == pb::mds::FSStatusCode::OK) {
      VLOG(6) << "Succes get fs info, fs_id: " << fsid
              << " from Mds: " << current_mds_addr
              << ",in metaserver: " << metaserver_addr_.ip << ":"
              << metaserver_addr_.port
              << " fs_info: " << response.ShortDebugString();

      *fs_info = response.fsinfo();
      return Status::OK();
    } else {
      LOG(INFO) << "Fail get fs info, fs_id: " << fsid
                << " from Mds: " << current_mds_addr
                << ",in metaserver: " << metaserver_addr_.ip << ":"
                << metaserver_addr_.port << " ret: " << ret;
      return Status::Internal(ret, "Fail get fs info");
    }
  }
}

Status FsInfoCache::GetFsInfo(uint64_t fsid, FsInfo* fs_info) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (cache_.find(fsid) != cache_.end()) {
    *fs_info = GetUnlocked(fsid);
    return Status::OK();
  } else {
    // TODO: out of lock
    FsInfo info;
    Status s = RequestFsInfo(fsid, &info);
    if (s.ok()) {
      PutUnlocked(fsid, info);
      *fs_info = info;
    }
    return s;
  }
}

void FsInfoCache::InvalidateFsInfo(uint64_t fsid) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (pos_.find(fsid) != pos_.end()) {
    auto iter = pos_[fsid];
    recent_.erase(iter);
    pos_.erase(fsid);
    cache_.erase(fsid);
  }
}

}  // namespace metaserver
}  // namespace dingofs
