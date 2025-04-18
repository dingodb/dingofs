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
 * @Project: dingo
 * @Date: 2021-09-07
 * @Author: majie1
 */

#include "metaserver/s3compact_inode.h"

#include <algorithm>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "common/s3util.h"
#include "dingofs/common.pb.h"
#include "dingofs/metaserver.pb.h"
#include "metaserver/copyset/meta_operator.h"
#include "metaserver/s3compact_manager.h"

namespace dingofs {
namespace metaserver {

using copyset::GetOrModifyS3ChunkInfoOperator;
using dataaccess::aws::S3Adapter;

using pb::common::S3Info;
using pb::metaserver::Inode;
using pb::metaserver::MetaStatusCode;

std::vector<uint64_t> CompactInodeJob::GetNeedCompact(
    const ::google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3chunkinfoMap,
    uint64_t inodeLen, uint64_t chunkSize) {
  std::vector<uint64_t> needCompact;
  for (const auto& item : s3chunkinfoMap) {
    if (needCompact.size() >= opts_->maxChunksPerCompact) {
      VLOG(9) << "s3compact: reach max chunks to compact per time";
      break;
    }
    if (item.first * chunkSize > inodeLen - 1) {
      // we need delete this chunk
      needCompact.push_back(item.first);
      continue;
    }
    if (static_cast<uint64_t>(item.second.s3chunks_size()) >
        opts_->fragmentThreshold) {
      needCompact.push_back(item.first);
    } else {
      const auto& l = item.second;
      for (int i = 0; i < l.s3chunks_size(); i++) {
        if (l.s3chunks(i).offset() + l.s3chunks(i).len() > inodeLen) {
          // part of chunk is useless, we need to delete them
          needCompact.push_back(item.first);
          break;
        }
      }
    }
  }
  return needCompact;
}

void CompactInodeJob::DeleteObjs(const std::vector<std::string>& objs,
                                 S3Adapter* s3adapter) {
  for (const auto& obj : objs) {
    VLOG(9) << "s3compact: delete " << obj;
    const Aws::String aws_key(obj.c_str(), obj.size());
    int ret = s3adapter->DeleteObject(aws_key);  // don't care success or not
    if (ret != 0) {
      VLOG(9) << "s3compact: delete " << obj << " failed";
    }
  }
}

std::list<struct CompactInodeJob::Node> CompactInodeJob::BuildValidList(
    const S3ChunkInfoList& s3chunkinfolist, uint64_t inodeLen, uint64_t index,
    uint64_t chunkSize) {
  std::list<Node> validList;
  if (chunkSize * index > inodeLen - 1) {
    // inode may be truncated smaller
    return validList;
  }
  std::list<std::pair<uint64_t, uint64_t>> freeList;  // [begin, end]
  freeList.emplace_back(chunkSize * index,
                        std::min(chunkSize * (index + 1) - 1,
                                 inodeLen - 1));  // start with full chunk
  std::map<uint64_t, std::pair<uint64_t, uint64_t>>
      used;  // begin -> pair(end, i)
  auto fill = [&](uint64_t i) {
    const auto& info = s3chunkinfolist.s3chunks(i);
    VLOG(9) << "chunkid: " << info.chunkid() << ", offset:" << info.offset()
            << ", len:" << info.len() << ", compaction:" << info.compaction()
            << ", zero: " << info.zero();
    const uint64_t begin = info.offset();
    const uint64_t end = info.offset() + info.len() - 1;
    for (auto it = freeList.begin(); it != freeList.end();) {
      auto n = std::next(it);
      // overlap means we can take this free
      auto b = it->first;
      auto e = it->second;
      if (begin <= b) {
        if (end < b) {
          return;
        } else if (end >= b && end < e) {
          // free [it->begin, it->end] -> [end+1, it->end]
          // used [it->begin, end]
          *it = std::make_pair(end + 1, e);
          used[b] = std::make_pair(end, i);
        } else {
          // free [it->begin, it->end] -> erase
          // used [it->begin, it->end]
          freeList.erase(it);
          used[b] = std::make_pair(e, i);
        }
      } else if (begin > b && begin <= e) {
        if (end < e) {
          // free [it-begin, it->end]
          // -> [it->begin, begin-1], [end+1, it->end]
          // used [begin, end]
          *it = std::make_pair(end + 1, e);
          freeList.insert(it, std::make_pair(b, begin - 1));
          used[begin] = std::make_pair(end, i);
        } else {
          // free [it->begin, it->end] -> [it->begin, begin-1]
          // used [begin, it->end]
          *it = std::make_pair(b, begin - 1);
          used[begin] = std::make_pair(e, i);
        }
      } else {
        // begin > it->end
        // do nothing
      }
      it = n;
    }
  };

  VLOG(9) << "s3compact: list s3chunkinfo list";
  for (auto i = s3chunkinfolist.s3chunks_size() - 1; i >= 0; i--) {
    if (freeList.empty()) break;
    fill(i);
  }

  for (const auto& v : used) {
    const auto& info = s3chunkinfolist.s3chunks(v.second.second);
    validList.emplace_back(v.first, v.second.first, info.chunkid(),
                           info.compaction(), info.offset(), info.len(),
                           info.zero());
  }

  return validList;
}

void CompactInodeJob::GenS3ReadRequests(const struct S3CompactCtx& ctx,
                                        const std::list<struct Node>& validList,
                                        std::vector<struct S3Request>* reqs,
                                        struct S3NewChunkInfo* newChunkInfo) {
  int reqIndex = 0;
  uint64_t newChunkId = 0;
  uint64_t newCompaction = 0;
  for (auto curr = validList.begin(); curr != validList.end();) {
    auto next = std::next(curr);
    if (curr->zero) {
      reqs->emplace_back(reqIndex++, true, "", 0, curr->end - curr->begin + 1);
      curr = next;
      continue;
    }

    const auto& blockSize = ctx.blockSize;
    const auto& chunkSize = ctx.chunkSize;
    uint64_t beginRoundDown = curr->begin / chunkSize * chunkSize;
    uint64_t startIndex = (curr->begin - beginRoundDown) / blockSize;
    for (uint64_t index = startIndex;
         beginRoundDown + index * blockSize <= curr->end; index++) {
      // read the block obj
      std::string objName =
          common::s3util::GenObjName(curr->chunkid, index, curr->compaction,
                                     ctx.fsId, ctx.inodeId, ctx.objectPrefix);
      uint64_t s3objBegin =
          std::max(curr->chunkoff, beginRoundDown + index * blockSize);
      uint64_t s3objEnd =
          std::min(curr->chunkoff + curr->chunklen - 1,
                   beginRoundDown + (index + 1) * blockSize - 1);
      if (curr->begin >= s3objBegin && curr->end <= s3objEnd) {
        // all what we need is only part of block
        reqs->emplace_back(reqIndex++, false, std::move(objName),
                           curr->begin - s3objBegin,
                           curr->end - curr->begin + 1);
      } else if (curr->begin >= s3objBegin && curr->end > s3objEnd) {
        // not last block, what we need is part of block
        reqs->emplace_back(reqIndex++, false, std::move(objName),
                           curr->begin - s3objBegin,
                           s3objEnd - curr->begin + 1);
      } else if (curr->begin < s3objBegin && curr->end > s3objEnd) {
        // what we need is full block
        reqs->emplace_back(reqIndex++, false, std::move(objName), 0, blockSize);
      } else if (curr->begin < s3objBegin && curr->end <= s3objEnd) {
        // last block, what we need is part of block
        reqs->emplace_back(reqIndex++, false, std::move(objName), 0,
                           curr->end - s3objBegin + 1);
        break;
      }
    }

    if (curr->chunkid >= newChunkId) {
      newChunkId = curr->chunkid;
      newCompaction = curr->compaction;
    }
    if (next != validList.end() && curr->end + 1 < next->begin) {
      // hole, append 0
      reqs->emplace_back(reqIndex++, true, "", 0, next->begin - curr->end - 1);
    }
    curr = next;
  }
  // inc compaction
  newCompaction += 1;
  newChunkInfo->newChunkId = newChunkId;
  newChunkInfo->newOff = validList.front().chunkoff;
  newChunkInfo->newCompaction = newCompaction;
}

int CompactInodeJob::ReadFullChunk(const struct S3CompactCtx& ctx,
                                   const std::list<struct Node>& validList,
                                   std::string* fullChunk,
                                   struct S3NewChunkInfo* newChunkInfo) {
  std::vector<struct S3Request> s3reqs;
  std::vector<std::string> readContent;
  // generate s3request first
  GenS3ReadRequests(ctx, validList, &s3reqs, newChunkInfo);
  VLOG(9) << "s3compact: s3 request generated";
  for (const auto& s3req : s3reqs) {
    VLOG(9) << "index:" << s3req.reqIndex << ", zero:" << s3req.zero
            << ", s3objname:" << s3req.objName << ", off:" << s3req.off
            << ", len:" << s3req.len;
  }
  readContent.resize(s3reqs.size());
  std::unordered_map<std::string, std::vector<struct S3Request*>> objReqs;
  for (auto& req : s3reqs) {
    if (req.zero) {
      objReqs["zero"].emplace_back(&req);
    } else {
      objReqs[req.objName].emplace_back(&req);
    }
  }
  // process zero first and will not fail
  if (objReqs.find("zero") != objReqs.end()) {
    for (const auto& req : objReqs["zero"]) {
      readContent[req->reqIndex] = std::string(req->len, '\0');
    }
    objReqs.erase("zero");
  }
  // read and process objs one by one
  uint64_t retry = 0;
  for (auto it = objReqs.begin(); it != objReqs.end(); it++) {
    std::string buf;
    const std::string& objName = it->first;
    const auto& reqs = it->second;
    const Aws::String aws_key(objName.c_str(), objName.size());
    const auto maxRetry = opts_->s3ReadMaxRetry;
    const auto retryInterval = opts_->s3ReadRetryInterval;
    while (retry <= maxRetry) {
      // why we need retry
      // if you enable client's diskcache,
      // metadata may be newer than data in s3
      // which means you cannot read data from s3
      // we have to wait data to be flushed to s3
      int ret = ctx.s3adapter->GetObject(aws_key, &buf);
      if (ret != 0) {
        LOG(WARNING) << "s3compact: get s3 obj " << objName << " failed";
        if (retry == maxRetry) return -1;  // no chance
        retry++;
        LOG(WARNING) << "s3compact: will retry after " << retryInterval
                     << " seconds, current retry time:" << retry;
        std::this_thread::sleep_for(std::chrono::seconds(retryInterval));
        continue;
      }
      for (const auto& req : reqs) {
        readContent[req->reqIndex] = buf.substr(req->off, req->len);
      }
      break;
    }
  }

  // merge all read content
  for (auto content : readContent) {
    (*fullChunk) += std::move(content);
  }

  return 0;
}

MetaStatusCode CompactInodeJob::UpdateInode(
    copyset::CopysetNode* copysetNode, const pb::common::PartitionInfo& pinfo,
    uint64_t inodeId,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3ChunkInfoAdd,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3ChunkInfoRemove) {
  pb::metaserver::GetOrModifyS3ChunkInfoRequest request;
  request.set_poolid(pinfo.poolid());
  request.set_copysetid(pinfo.copysetid());
  request.set_partitionid(pinfo.partitionid());
  request.set_fsid(pinfo.fsid());
  request.set_inodeid(inodeId);
  *request.mutable_s3chunkinfoadd() = std::move(s3ChunkInfoAdd);
  *request.mutable_s3chunkinforemove() = std::move(s3ChunkInfoRemove);
  request.set_returns3chunkinfomap(false);
  request.set_froms3compaction(true);
  pb::metaserver::GetOrModifyS3ChunkInfoResponse response;
  GetOrModifyS3ChunkInfoClosure done;
  // if copysetnode change to nullptr, maybe crash
  auto GetOrModifyS3ChunkInfoOp = new GetOrModifyS3ChunkInfoOperator(
      copysetNode, nullptr, &request, &response, &done);
  GetOrModifyS3ChunkInfoOp->Propose();
  done.WaitRunned();
  return response.statuscode();
}

int CompactInodeJob::WriteFullChunk(const struct S3CompactCtx& ctx,
                                    const struct S3NewChunkInfo& newChunkInfo,
                                    const std::string& fullChunk,
                                    std::vector<std::string>* objsAdded) {
  uint64_t chunkLen = fullChunk.length();
  const auto& blockSize = ctx.blockSize;
  const auto& chunkSize = ctx.chunkSize;
  const auto& newOff = newChunkInfo.newOff;
  uint64_t offRoundDown = newOff / chunkSize * chunkSize;
  uint64_t startIndex = (newOff - newOff / chunkSize * chunkSize) / blockSize;
  for (uint64_t index = startIndex;
       index * blockSize + offRoundDown < newOff + chunkLen; index += 1) {
    std::string objName = common::s3util::GenObjName(
        newChunkInfo.newChunkId, index, newChunkInfo.newCompaction, ctx.fsId,
        ctx.inodeId, ctx.objectPrefix);
    const Aws::String aws_key(objName.c_str(), objName.size());
    int ret;
    uint64_t s3objBegin = std::max(newOff, offRoundDown + index * blockSize);
    uint64_t s3objEnd = std::min(newOff + chunkLen - 1,
                                 offRoundDown + (index + 1) * blockSize - 1);
    VLOG(9) << "s3compact: put " << objName << ", [" << s3objBegin << "-"
            << s3objEnd << "]";
    ret = ctx.s3adapter->PutObject(
        aws_key,
        fullChunk.substr(s3objBegin - newOff, s3objEnd - s3objBegin + 1));
    if (ret != 0) {
      LOG(WARNING) << "s3compact: put s3 object " << objName << " failed";
      return ret;
    } else {
      objsAdded->emplace_back(std::move(objName));
    }
  }
  return 0;
}

bool CompactInodeJob::CompactPrecheck(const struct S3CompactTask& task,
                                      Inode* inode) {
  // am i copysetnode leader?
  if (!task.copysetNodeWrapper->IsLeaderTerm()) {
    VLOG(6) << "s3compact: i am not the leader, finish";
    return false;
  }

  // inode exist?
  MetaStatusCode ret = task.inodeManager->GetInode(
      task.inodeKey.fsId, task.inodeKey.inodeId, inode, true);
  if (ret != MetaStatusCode::OK) {
    LOG(WARNING) << "s3compact: GetInode fail, inodeKey = "
                 << task.inodeKey.fsId << "," << task.inodeKey.inodeId
                 << ", ret = " << MetaStatusCode_Name(ret);
    return false;
  }

  // deleted?
  if (inode->nlink() == 0) {
    VLOG(6) << "s3compact: inode is already deleted";
    return false;
  }

  if (inode->s3chunkinfomap().empty()) {
    VLOG(6) << "Inode s3chunkinfo is empty";
    return false;
  }

  // pass
  return true;
}

S3Adapter* CompactInodeJob::SetupS3Adapter(uint64_t fsId,
                                           uint64_t* s3adapterIndex,
                                           uint64_t* blockSize,
                                           uint64_t* chunkSize,
                                           uint32_t* objectPrefix) {
  auto pairResult = opts_->s3adapterManager->GetS3Adapter();
  *s3adapterIndex = pairResult.first;
  auto s3adapter = pairResult.second;
  if (s3adapter == nullptr) {
    LOG(WARNING) << "s3compact: fail to get s3adapter";
    return nullptr;
  }

  S3Info s3info;
  int status = opts_->s3infoCache->GetS3Info(fsId, &s3info);
  if (status == 0) {
    *blockSize = s3info.blocksize();
    *chunkSize = s3info.chunksize();
    *objectPrefix = s3info.objectprefix();
    if (s3adapter->GetS3Ak() != s3info.ak() ||
        s3adapter->GetS3Sk() != s3info.sk() ||
        s3adapter->GetS3Endpoint() != s3info.endpoint()) {
      auto option = opts_->s3adapterManager->GetBasicS3AdapterOption();
      option.ak = s3info.ak();
      option.sk = s3info.sk();
      option.s3Address = s3info.endpoint();
      option.bucketName = s3info.bucketname();
      s3adapter->Reinit(option);
    }
    Aws::String bucketName(s3info.bucketname().c_str(),
                           s3info.bucketname().size());
    if (s3adapter->GetBucketName() != bucketName) {
      s3adapter->SetBucketName(bucketName);
    }
    VLOG(6) << "s3compact: set s3info success, ak: " << s3info.ak()
            << ", sk: " << s3info.sk() << ", endpoint: " << s3info.endpoint()
            << ", bucket: " << s3info.bucketname();
  } else {
    LOG(WARNING) << "s3compact: fail to get s3info of " << fsId;
    return nullptr;
  }
  VLOG(6) << "s3compact: set s3adapter " << s3info.ak() << ", " << s3info.sk()
          << ", " << s3info.endpoint() << ", " << s3info.bucketname();
  return s3adapter;
}

void CompactInodeJob::CompactChunk(
    const struct S3CompactCtx& compactCtx, uint64_t index, const Inode& inode,
    std::unordered_map<uint64_t, std::vector<std::string>>* objsAddedMap,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3ChunkInfoAdd,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3ChunkInfoRemove) {
  auto cleanup = absl::MakeCleanup(
      [&]() { VLOG(6) << "s3compact: exit index " << index; });
  VLOG(6) << "s3compact: begin to compact index " << index;
  const auto& s3chunkinfolist = inode.s3chunkinfomap().at(index);
  // 1.1 build valid list
  std::list<Node> validList(BuildValidList(s3chunkinfolist, inode.length(),
                                           index, compactCtx.chunkSize));
  VLOG(6) << "s3compact: finish build valid list";
  VLOG(9) << "s3compact: show valid list";
  for (const auto& node : validList) {
    VLOG(9) << "[" << node.begin << "-" << node.end
            << "], chunkid:" << node.chunkid << ", chunkoff:" << node.chunkoff
            << ", chunklen:" << node.chunklen << ", zero:" << node.zero;
  }
  if (validList.empty()) {
    // chunk is not valid, just delete this chunk
    s3ChunkInfoRemove->insert({index, s3chunkinfolist});
    return;
  }
  // 1.2  first read full chunk
  struct S3NewChunkInfo newChunkInfo;
  std::string fullChunk;
  int ret = ReadFullChunk(compactCtx, validList, &fullChunk, &newChunkInfo);
  if (ret != 0) {
    LOG(WARNING) << "s3compact: ReadFullChunk failed, index " << index;
    opts_->s3infoCache->InvalidateS3Info(
        compactCtx.fsId);  // maybe s3info changed?
    return;
  }
  VLOG(6) << "s3compact: finish read full chunk, size: " << fullChunk.size();
  VLOG(6) << "s3compact: new s3chunk info will be id:"
          << newChunkInfo.newChunkId << ", off:" << newChunkInfo.newOff
          << ", compaction:" << newChunkInfo.newCompaction;
  // 1.3 then write objs with newChunkid and newCompaction
  std::vector<std::string> objsAdded;
  ret = WriteFullChunk(compactCtx, newChunkInfo, fullChunk, &objsAdded);
  if (ret != 0) {
    LOG(WARNING) << "s3compact: WriteFullChunk failed, index " << index;
    opts_->s3infoCache->InvalidateS3Info(
        compactCtx.fsId);  // maybe s3info changed?
    DeleteObjs(objsAdded, compactCtx.s3adapter);
    return;
  }
  VLOG(6) << "s3compact: finish write full chunk";
  // 1.4 record add/delete
  objsAddedMap->emplace(index, std::move(objsAdded));
  // to add
  S3ChunkInfoList toAddList;
  S3ChunkInfo toAdd;
  toAdd.set_chunkid(newChunkInfo.newChunkId);
  toAdd.set_compaction(newChunkInfo.newCompaction);
  toAdd.set_offset(newChunkInfo.newOff);
  toAdd.set_len(fullChunk.length());
  toAdd.set_size(fullChunk.length());
  toAdd.set_zero(false);
  *toAddList.add_s3chunks() = std::move(toAdd);
  s3ChunkInfoAdd->insert({index, std::move(toAddList)});
  // to remove
  s3ChunkInfoRemove->insert({index, s3chunkinfolist});
}

void CompactInodeJob::DeleteObjsOfS3ChunkInfoList(
    const struct S3CompactCtx& ctx, const S3ChunkInfoList& s3chunkinfolist) {
  for (auto i = 0; i < s3chunkinfolist.s3chunks_size(); i++) {
    const auto& chunkinfo = s3chunkinfolist.s3chunks(i);
    uint64_t off = chunkinfo.offset();
    uint64_t len = chunkinfo.len();
    uint64_t offRoundDown = off / ctx.chunkSize * ctx.chunkSize;
    uint64_t startIndex = (off - offRoundDown) / ctx.blockSize;
    for (uint64_t index = startIndex;
         offRoundDown + index * ctx.blockSize < off + len; index++) {
      std::string objName = common::s3util::GenObjName(
          chunkinfo.chunkid(), index, chunkinfo.compaction(), ctx.fsId,
          ctx.inodeId, ctx.objectPrefix);
      VLOG(6) << "s3compact: delete " << objName;
      const Aws::String aws_key(objName.c_str(), objName.size());
      int r =
          ctx.s3adapter->DeleteObject(aws_key);  // don't care success or not
      if (r != 0) VLOG(6) << "s3compact: delete obj " << objName << "failed.";
    }
  }
}

void CompactInodeJob::CompactChunks(const S3CompactTask& task) {
  VLOG(6) << "s3compact: try to compact, fsId: " << task.inodeKey.fsId
          << " , inodeId: " << task.inodeKey.inodeId;

  // full inode including s3 info
  Inode inode;
  if (!CompactPrecheck(task, &inode)) return;
  uint64_t fsId = inode.fsid();
  uint64_t inodeId = inode.inodeid();

  // let's compact
  // 0. get s3adapter&s3info, set s3adapter
  // include ak, sk, addr, bucket, blocksize, chunksize
  uint64_t blockSize;
  uint64_t chunkSize;
  uint64_t s3adapterIndex;
  uint32_t objectPrefix;
  S3Adapter* s3adapter = SetupS3Adapter(task.inodeKey.fsId, &s3adapterIndex,
                                        &blockSize, &chunkSize, &objectPrefix);
  if (s3adapter == nullptr) return;
  // need compact?
  std::vector<uint64_t> needCompact =
      GetNeedCompact(inode.s3chunkinfomap(), inode.length(), chunkSize);
  if (needCompact.empty()) {
    VLOG(6) << "s3compact: no need to compact " << inode.inodeid();
    opts_->s3adapterManager->ReleaseS3Adapter(s3adapterIndex);
    return;
  }

  // 1. read full chunk & write new objs, each chunk one by one
  struct S3CompactCtx compactCtx {
    task.inodeKey.inodeId, task.inodeKey.fsId, task.pinfo, blockSize, chunkSize,
        s3adapterIndex, objectPrefix, s3adapter
  };
  std::unordered_map<uint64_t, std::vector<std::string>> objsAddedMap;
  ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3ChunkInfoAdd;
  ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3ChunkInfoRemove;
  std::vector<uint64_t> indexToDelete;
  VLOG(6) << "s3compact: begin to compact fsId:" << fsId
          << ", inodeId:" << inodeId;
  for (const auto& index : needCompact) {
    // s3chunklist order: from small chunkid to big chunkid
    CompactChunk(compactCtx, index, inode, &objsAddedMap, &s3ChunkInfoAdd,
                 &s3ChunkInfoRemove);
  }
  if (s3ChunkInfoAdd.empty() && s3ChunkInfoRemove.empty()) {
    VLOG(6) << "s3compact: do nothing to metadata";
    opts_->s3adapterManager->ReleaseS3Adapter(s3adapterIndex);
    return;
  }

  // 2. update inode
  VLOG(6) << "s3compact: start update inode";
  if (!task.copysetNodeWrapper->IsValid()) {
    VLOG(6) << "s3compact: invalid copysetNode";
    opts_->s3adapterManager->ReleaseS3Adapter(s3adapterIndex);
    return;
  }
  std::vector<int> s3ChunkInfoRemoveIndex;
  s3ChunkInfoRemoveIndex.reserve(s3ChunkInfoRemove.size());
  for (const auto& element : s3ChunkInfoRemove) {
    s3ChunkInfoRemoveIndex.push_back(element.first);
  }
  auto ret =
      UpdateInode(task.copysetNodeWrapper->Get(), compactCtx.pinfo, inodeId,
                  std::move(s3ChunkInfoAdd), std::move(s3ChunkInfoRemove));
  if (ret != MetaStatusCode::OK) {
    LOG(WARNING) << "s3compact: UpdateInode failed, inodeKey = "
                 << compactCtx.fsId << "," << compactCtx.inodeId
                 << ", ret = " << MetaStatusCode_Name(ret);
    for (const auto& item : objsAddedMap) {
      DeleteObjs(item.second, s3adapter);
    }
    opts_->s3adapterManager->ReleaseS3Adapter(s3adapterIndex);
    return;
  }
  VLOG(6) << "s3compact: finish update inode";

  // 3. delete old objs
  VLOG(6) << "s3compact: start delete old objs";
  for (const auto& index : s3ChunkInfoRemoveIndex) {
    const auto& l = inode.s3chunkinfomap().at(index);
    DeleteObjsOfS3ChunkInfoList(compactCtx, l);
  }
  VLOG(6) << "s3compact: finish delete objs";
  opts_->s3adapterManager->ReleaseS3Adapter(s3adapterIndex);
  VLOG(6) << "s3compact: compact successfully";
}

}  // namespace metaserver
}  // namespace dingofs
