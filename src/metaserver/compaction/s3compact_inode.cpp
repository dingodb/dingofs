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

#include "metaserver/compaction/s3compact_inode.h"

#include <glog/logging.h>
#include <google/protobuf/descriptor.h>

#include <algorithm>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "blockaccess/block_accesser.h"
#include "common/config_mapper.h"
#include "common/s3util.h"
#include "dingofs/common.pb.h"
#include "dingofs/mds.pb.h"
#include "dingofs/metaserver.pb.h"
#include "metaserver/compaction/fs_info_cache.h"
#include "metaserver/copyset/meta_operator.h"

namespace dingofs {
namespace metaserver {

using copyset::GetOrModifyS3ChunkInfoOperator;

using pb::mds::FsInfo;
using pb::metaserver::Inode;
using pb::metaserver::MetaStatusCode;

namespace {

std::unique_ptr<blockaccess::BlockAccesser> SetupS3Adapter(
    const S3CompactWorkerOptions* opts, uint64_t fs_id, uint64_t* block_size,
    uint64_t* chunk_size) {
  FsInfo fs_info;
  Status status = opts->fs_info_cache->GetFsInfo(fs_id, &fs_info);

  if (!status.ok()) {
    LOG(WARNING) << "Failed to get fs_info, fs_id: " << fs_id
                 << ", status: " << status.ToString();
    return nullptr;
  }

  *block_size = fs_info.block_size();
  *chunk_size = fs_info.chunk_size();

  blockaccess::BlockAccessOptions block_access_opts = opts->block_access_opts;
  FillBlockAccessOption(fs_info.storage_info(), &block_access_opts);

  auto block_accesser =
      opts->block_accesser_factory->NewBlockAccesser(block_access_opts);

  status = block_accesser->Init();
  if (!status.ok()) {
    LOG(WARNING) << "Failed to initialize block accesser, storage_info: "
                 << fs_info.storage_info().ShortDebugString()
                 << ", status: " << status.ToString();
    return nullptr;
  }

  VLOG(6) << "Successfully initialized block accesser, storage_info: "
          << fs_info.storage_info().ShortDebugString();
  return block_accesser;
}

void DeleteObjs(const std::vector<std::string>& objs,
                blockaccess::BlockAccesser* block_accesser) {
  for (const auto& obj : objs) {
    VLOG(9) << "s3compact: delete block: " << obj;
    Status s = block_accesser->Delete(obj);
    if (!s.ok()) {
      LOG(INFO) << "Fail delete block " << obj
                << " failed, status: " << s.ToString();
    }
  }
}
}  // namespace

std::vector<uint64_t> CompactInodeJob::GetNeedCompact(
    const ::google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3_chunk_info_map,
    uint64_t inode_len, uint64_t chunk_size) {
  std::vector<uint64_t> need_compact;
  for (const auto& item : s3_chunk_info_map) {
    if (need_compact.size() >= opts_->maxChunksPerCompact) {
      VLOG(9) << "s3compact: reach max chunks to compact per time";
      break;
    }
    if (item.first * chunk_size > inode_len - 1) {
      // we need delete this chunk
      need_compact.push_back(item.first);
      continue;
    }
    if (static_cast<uint64_t>(item.second.s3chunks_size()) >
        opts_->fragmentThreshold) {
      need_compact.push_back(item.first);
    } else {
      const auto& l = item.second;
      for (int i = 0; i < l.s3chunks_size(); i++) {
        if (l.s3chunks(i).offset() + l.s3chunks(i).len() > inode_len) {
          // part of chunk is useless, we need to delete them
          need_compact.push_back(item.first);
          break;
        }
      }
    }
  }
  return need_compact;
}

std::list<struct CompactInodeJob::Node> CompactInodeJob::BuildValidList(
    const S3ChunkInfoList& s3chunkinfolist, uint64_t inode_len, uint64_t index,
    uint64_t chunk_size) {
  std::list<Node> valid_list;
  if (chunk_size * index > inode_len - 1) {
    // inode may be truncated smaller
    return valid_list;
  }
  std::list<std::pair<uint64_t, uint64_t>> free_list;  // [begin, end]
  free_list.emplace_back(chunk_size * index,
                         std::min((chunk_size * (index + 1)) - 1,
                                  inode_len - 1));  // start with full chunk
  std::map<uint64_t, std::pair<uint64_t, uint64_t>>
      used;  // begin -> pair(end, i)
  auto fill = [&](uint64_t i) {
    const auto& info = s3chunkinfolist.s3chunks(i);
    VLOG(9) << "chunkid: " << info.chunkid() << ", offset:" << info.offset()
            << ", len:" << info.len() << ", compaction:" << info.compaction()
            << ", zero: " << info.zero();
    const uint64_t begin = info.offset();
    const uint64_t end = info.offset() + info.len() - 1;
    for (auto it = free_list.begin(); it != free_list.end();) {
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
          free_list.erase(it);
          used[b] = std::make_pair(e, i);
        }
      } else if (begin > b && begin <= e) {
        if (end < e) {
          // free [it-begin, it->end]
          // -> [it->begin, begin-1], [end+1, it->end]
          // used [begin, end]
          *it = std::make_pair(end + 1, e);
          free_list.insert(it, std::make_pair(b, begin - 1));
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
    if (free_list.empty()) break;
    fill(i);
  }

  for (const auto& v : used) {
    const auto& info = s3chunkinfolist.s3chunks(v.second.second);
    valid_list.emplace_back(v.first, v.second.first, info.chunkid(),
                            info.compaction(), info.offset(), info.len(),
                            info.zero());
  }

  return valid_list;
}

void CompactInodeJob::GenS3ReadRequests(
    const struct S3CompactCtx& ctx, const std::list<struct Node>& valid_list,
    std::vector<struct S3Request>* reqs,
    struct S3NewChunkInfo* new_chunk_info) {
  int req_index = 0;
  uint64_t new_chunk_id = 0;
  uint64_t new_compaction = 0;
  for (auto curr = valid_list.begin(); curr != valid_list.end();) {
    auto next = std::next(curr);
    if (curr->zero) {
      reqs->emplace_back(req_index++, true, "", 0, curr->end - curr->begin + 1,
                         curr->begin);
      curr = next;
      continue;
    }

    const auto& block_size = ctx.blockSize;
    const auto& chunk_size = ctx.chunkSize;
    uint64_t begin_round_down = (curr->begin / chunk_size) * chunk_size;
    uint64_t start_index = (curr->begin - begin_round_down) / block_size;

    // this chunk data may be from more than one block
    uint64_t curr_file_offset = curr->begin;
    for (uint64_t index = start_index;
         begin_round_down + index * block_size <= curr->end; index++) {
      // read the block obj
      std::string obj_name = common::s3util::GenObjName(
          curr->chunkid, index, curr->compaction, ctx.fsId, ctx.inodeId);

      uint64_t obj_begin =
          std::max(curr->chunkoff, begin_round_down + (index * block_size));
      uint64_t obj_end =
          std::min(curr->chunkoff + curr->chunklen - 1,
                   begin_round_down + ((index + 1) * block_size) - 1);

      uint64_t req_len = 0;

      if (curr->begin >= obj_begin && curr->end <= obj_end) {
        // all what we need is only part of block
        req_len = curr->end - curr->begin + 1;
        reqs->emplace_back(req_index++, false, std::move(obj_name),
                           curr->begin - obj_begin, req_len, curr_file_offset);
      } else if (curr->begin >= obj_begin && curr->end > obj_end) {
        // not last block, what we need is part of block
        req_len = obj_end - curr->begin + 1;
        reqs->emplace_back(req_index++, false, std::move(obj_name),
                           curr->begin - obj_begin, req_len, curr_file_offset);
      } else if (curr->begin < obj_begin && curr->end > obj_end) {
        // what we need is full block
        req_len = block_size;
        reqs->emplace_back(req_index++, false, std::move(obj_name), 0, req_len,
                           curr_file_offset);
      } else if (curr->begin < obj_begin && curr->end <= obj_end) {
        // last block, what we need is part of block
        req_len = curr->end - obj_begin + 1;
        reqs->emplace_back(req_index++, false, std::move(obj_name), 0, req_len,
                           curr_file_offset);
        break;
      }

      curr_file_offset += req_len;
    }

    if (curr->chunkid >= new_chunk_id) {
      new_chunk_id = curr->chunkid;
      new_compaction = curr->compaction;
    }

    if (next != valid_list.end() && curr->end + 1 < next->begin) {
      // hole, append 0
      reqs->emplace_back(req_index++, true, "", 0, next->begin - curr->end - 1,
                         curr->end);
    }
    curr = next;
  }

  // inc compaction
  new_compaction += 1;
  new_chunk_info->new_chunk_id = new_chunk_id;
  new_chunk_info->new_off = valid_list.front().chunkoff;
  new_chunk_info->new_compaction = new_compaction;
}

int CompactInodeJob::ReadFullChunk(const struct S3CompactCtx& ctx,
                                   const std::list<struct Node>& valid_list,
                                   std::string* full_chunk,
                                   struct S3NewChunkInfo* new_chunk_info) {
  std::vector<struct S3Request> s3reqs;
  std::vector<std::string> read_content;

  // generate s3request first
  GenS3ReadRequests(ctx, valid_list, &s3reqs, new_chunk_info);
  VLOG(9) << "s3compact: s3 request generated";
  for (const auto& s3req : s3reqs) {
    VLOG(9) << "index:" << s3req.req_index << ", zero:" << s3req.zero
            << ", s3objname:" << s3req.obj_name << ", off:" << s3req.off
            << ", len:" << s3req.len << ", file_range[" << s3req.in_file_offset
            << "-" << s3req.in_file_offset + s3req.len - 1 << "]";

    // check if s3 request is out of range
    CHECK((s3req.off + s3req.len) <= ctx.blockSize)
        << "s3compact: s3 request out of range, index: " << s3req.req_index
        << ", s3objname:" << s3req.obj_name << ", off: " << s3req.off
        << ", len: " << s3req.len << ", blockSize: " << ctx.blockSize
        << ", file_range[" << s3req.in_file_offset << "-"
        << s3req.in_file_offset + s3req.len - 1 << "]";
  }

  read_content.resize(s3reqs.size());

  std::unordered_map<std::string, std::vector<struct S3Request*>> obj_reqs;
  for (auto& req : s3reqs) {
    if (req.zero) {
      obj_reqs["zero"].emplace_back(&req);
    } else {
      obj_reqs[req.obj_name].emplace_back(&req);
    }
  }

  // process zero first and will not fail
  if (obj_reqs.find("zero") != obj_reqs.end()) {
    for (const auto& req : obj_reqs["zero"]) {
      read_content[req->req_index] = std::string(req->len, '\0');
    }
    obj_reqs.erase("zero");
  }

  // read and process objs one by one
  uint64_t retry = 0;
  for (const auto& [obj_name, reqs] : obj_reqs) {
    std::string buf;
    const auto max_retry = opts_->s3ReadMaxRetry;
    const auto retry_interval = opts_->s3ReadRetryInterval;

    while (retry <= max_retry) {
      // why we need retry
      // if you enable client's diskcache,
      // metadata may be newer than data in s3
      // which means you cannot read data from s3
      // we have to wait data to be flushed to s3
      Status s = ctx.block_accesser->Get(obj_name, &buf);
      if (!s.ok()) {
        LOG(WARNING) << "s3compact: Fail get block: " << obj_name
                     << ", status: " << s.ToString();

        if (retry == max_retry) return -1;  // no chance

        retry++;
        LOG(WARNING) << "s3compact: Will retry after " << retry_interval
                     << " seconds, current retry time:" << retry;
        std::this_thread::sleep_for(std::chrono::seconds(retry_interval));
        continue;
      }
      for (const auto& req : reqs) {
        read_content[req->req_index] = buf.substr(req->off, req->len);
      }
      break;
    }
  }

  // merge all read content
  for (const auto& content : read_content) {
    (*full_chunk) += content;
  }

  return 0;
}

MetaStatusCode CompactInodeJob::UpdateInode(
    copyset::CopysetNode* copyset_node, const pb::common::PartitionInfo& pinfo,
    uint64_t inode_id,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3_chunk_info_add,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3_chunk_info_remove) {
  pb::metaserver::GetOrModifyS3ChunkInfoRequest request;
  request.set_poolid(pinfo.poolid());
  request.set_copysetid(pinfo.copysetid());
  request.set_partitionid(pinfo.partitionid());
  request.set_fsid(pinfo.fsid());
  request.set_inodeid(inode_id);
  *request.mutable_s3chunkinfoadd() = std::move(s3_chunk_info_add);
  *request.mutable_s3chunkinforemove() = std::move(s3_chunk_info_remove);
  request.set_returns3chunkinfomap(false);
  request.set_froms3compaction(true);
  pb::metaserver::GetOrModifyS3ChunkInfoResponse response;
  GetOrModifyS3ChunkInfoClosure done;
  // if copysetnode change to nullptr, maybe crash
  auto* get_or_modify_s3_chunk_info_op = new GetOrModifyS3ChunkInfoOperator(
      copyset_node, nullptr, &request, &response, &done);
  get_or_modify_s3_chunk_info_op->Propose();
  done.WaitRunned();
  return response.statuscode();
}

Status CompactInodeJob::WriteFullChunk(
    const struct S3CompactCtx& ctx, const struct S3NewChunkInfo& new_chunk_info,
    const std::string& full_chunk, std::vector<std::string>* block_added) {
  uint64_t chunk_len = full_chunk.length();
  const auto& block_size = ctx.blockSize;
  const auto& chunk_size = ctx.chunkSize;
  const auto& new_off = new_chunk_info.new_off;

  uint64_t off_round_down = (new_off / chunk_size) * chunk_size;
  uint64_t start_index =
      (new_off - ((new_off / chunk_size) * chunk_size)) / block_size;

  for (uint64_t index = start_index;
       index * block_size + off_round_down < new_off + chunk_len; index += 1) {
    std::string block_name = common::s3util::GenObjName(
        new_chunk_info.new_chunk_id, index, new_chunk_info.new_compaction,
        ctx.fsId, ctx.inodeId);

    uint64_t obj_begin =
        std::max(new_off, off_round_down + (index * block_size));
    uint64_t obj_end =
        std::min(new_off + chunk_len - 1,
                 off_round_down + ((index + 1) * block_size) - 1);

    VLOG(9) << "s3compact: put " << block_name << ", [" << obj_begin << "-"
            << obj_end << "]";

    Status s = ctx.block_accesser->Put(
        block_name,
        full_chunk.substr(obj_begin - new_off, obj_end - obj_begin + 1));
    if (!s.ok()) {
      LOG(WARNING) << "s3compact: put block: " << block_name << " failed";
      return s;
    } else {
      block_added->emplace_back(std::move(block_name));
    }
  }
  return Status::OK();
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

void CompactInodeJob::CompactChunk(
    const struct S3CompactCtx& compact_ctx, uint64_t index, const Inode& inode,
    std::unordered_map<uint64_t, std::vector<std::string>>* objs_added_map,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3_chunk_info_add,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3_chunk_info_remove) {
  auto cleanup = absl::MakeCleanup(
      [&]() { VLOG(6) << "s3compact: exit index " << index; });
  VLOG(6) << "s3compact: begin to compact index " << index;

  const auto& s3chunkinfolist = inode.s3chunkinfomap().at(index);

  // 1.1 build valid list
  std::list<Node> valid_list(BuildValidList(s3chunkinfolist, inode.length(),
                                            index, compact_ctx.chunkSize));
  VLOG(6) << "s3compact: finish build valid list";
  VLOG(9) << "s3compact: show valid list";
  for (const auto& node : valid_list) {
    VLOG(9) << "[" << node.begin << "-" << node.end
            << "], chunkid:" << node.chunkid << ", chunkoff:" << node.chunkoff
            << ", chunklen:" << node.chunklen << ", zero:" << node.zero;
  }

  if (valid_list.empty()) {
    // chunk is not valid, just delete this chunk
    s3_chunk_info_remove->insert({index, s3chunkinfolist});
    return;
  }

  // 1.2  first read full chunk
  struct S3NewChunkInfo new_chunk_info;
  std::string full_chunk;
  int ret =
      ReadFullChunk(compact_ctx, valid_list, &full_chunk, &new_chunk_info);
  if (ret != 0) {
    LOG(WARNING) << "s3compact: ReadFullChunk failed, index " << index;
    opts_->fs_info_cache->InvalidateFsInfo(
        compact_ctx.fsId);  // maybe s3info changed?
    return;
  }
  // check chunk size
  uint64_t expectChunkSize =
      valid_list.back().end - valid_list.front().begin + 1;
  CHECK(full_chunk.size() == expectChunkSize)
      << "s3compact: ReadFullChunk size mismatch, expect: " << expectChunkSize
      << ", actual: " << full_chunk.size();

  VLOG(6) << "s3compact: finish read full chunk, size: " << full_chunk.size();
  VLOG(6) << "s3compact: new s3chunk info will be id:"
          << new_chunk_info.new_chunk_id << ", off:" << new_chunk_info.new_off
          << ", compaction:" << new_chunk_info.new_compaction;

  // 1.3 then write objs with newChunkid and newCompaction
  std::vector<std::string> objs_added;
  Status s =
      WriteFullChunk(compact_ctx, new_chunk_info, full_chunk, &objs_added);
  if (!s.ok()) {
    LOG(WARNING) << "s3compact: WriteFullChunk failed, index " << index
                 << ", status: " << s.ToString();
    opts_->fs_info_cache->InvalidateFsInfo(compact_ctx.fsId);
    // maybe s3info changed?
    DeleteObjs(objs_added, compact_ctx.block_accesser);
    return;
  }
  VLOG(6) << "s3compact: finish write full chunk";

  // 1.4 record add/delete
  objs_added_map->emplace(index, std::move(objs_added));
  // to add
  S3ChunkInfo to_add;
  to_add.set_chunkid(new_chunk_info.new_chunk_id);
  to_add.set_compaction(new_chunk_info.new_compaction);
  to_add.set_offset(new_chunk_info.new_off);
  to_add.set_len(full_chunk.length());
  to_add.set_size(full_chunk.length());
  to_add.set_zero(false);

  S3ChunkInfoList to_add_list;
  *to_add_list.add_s3chunks() = std::move(to_add);

  s3_chunk_info_add->insert({index, std::move(to_add_list)});
  // to remove
  s3_chunk_info_remove->insert({index, s3chunkinfolist});
}

void CompactInodeJob::DeleteObjsOfS3ChunkInfoList(
    const struct S3CompactCtx& ctx, const S3ChunkInfoList& s3chunkinfolist) {
  for (auto i = 0; i < s3chunkinfolist.s3chunks_size(); i++) {
    const auto& chunkinfo = s3chunkinfolist.s3chunks(i);
    uint64_t off = chunkinfo.offset();
    uint64_t len = chunkinfo.len();
    uint64_t off_round_down = off / ctx.chunkSize * ctx.chunkSize;
    uint64_t start_index = (off - off_round_down) / ctx.blockSize;
    for (uint64_t index = start_index;
         off_round_down + index * ctx.blockSize < off + len; index++) {
      std::string obj_name = common::s3util::GenObjName(
          chunkinfo.chunkid(), index, chunkinfo.compaction(), ctx.fsId,
          ctx.inodeId);
      if (opts_->deleteOldObjs) {
        VLOG(6) << "s3compact: delete " << obj_name;
        Status s =
            ctx.block_accesser->Delete(obj_name);  // don't care success or not
        if (!s.ok()) {
          LOG(INFO) << "s3compact: Fail delete block " << obj_name
                    << " failed, status: " << s.ToString();
        }
      } else {
        VLOG(6) << "s3compact: skip delete " << obj_name << ", chunkinfo: ["
                << chunkinfo.ShortDebugString() << "]";
      }
    }
  }
}

void CompactInodeJob::CompactChunks(const S3CompactTask& task) {
  VLOG(6) << "s3compact: try to compact, fsId: " << task.inodeKey.fsId
          << " , inodeId: " << task.inodeKey.inodeId;

  // full inode including s3 info
  Inode inode;
  if (!CompactPrecheck(task, &inode)) return;
  uint64_t fs_id = inode.fsid();
  uint64_t inode_id = inode.inodeid();

  uint64_t block_size;
  uint64_t chunk_size;
  std::unique_ptr<blockaccess::BlockAccesser> block_accesser =
      SetupS3Adapter(opts_, fs_id, &block_size, &chunk_size);

  if (block_accesser == nullptr) return;

  // need compact?
  std::vector<uint64_t> need_compact =
      GetNeedCompact(inode.s3chunkinfomap(), inode.length(), chunk_size);
  if (need_compact.empty()) {
    VLOG(6) << "s3compact: no need to compact " << inode.inodeid();
    return;
  }

  // 1. read full chunk & write new objs, each chunk one by one
  struct S3CompactCtx compact_ctx{.inodeId = task.inodeKey.inodeId,
                                  .fsId = task.inodeKey.fsId,
                                  .pinfo = task.pinfo,
                                  .blockSize = block_size,
                                  .chunkSize = chunk_size,
                                  .block_accesser = block_accesser.get()};

  std::unordered_map<uint64_t, std::vector<std::string>> objs_added_map;
  ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3_chunk_info_add;
  ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3_chunk_info_remove;
  VLOG(6) << "s3compact: begin to compact fsId:" << fs_id
          << ", inodeId:" << inode_id;

  for (const auto& index : need_compact) {
    // s3chunklist order: from small chunkid to big chunkid
    CompactChunk(compact_ctx, index, inode, &objs_added_map, &s3_chunk_info_add,
                 &s3_chunk_info_remove);
  }

  if (s3_chunk_info_add.empty() && s3_chunk_info_remove.empty()) {
    VLOG(6) << "s3compact: do nothing to metadata";
    return;
  }

  // print add,remove chunk info
  VLOG(9) << "s3compact: s3chunkinfo add:";
  for (const auto& element : s3_chunk_info_add) {
    auto chunkIndex = element.first;
    const auto& S3ChunkInfoList = element.second;

    for (int i = 0; i < S3ChunkInfoList.s3chunks_size(); i++) {
      const auto& chunkInfo = S3ChunkInfoList.s3chunks(i);
      VLOG(9) << "Add chunk, chunkindex: " << chunkIndex << ", chunkinfo: ["
              << chunkInfo.ShortDebugString() << "]";
    }
  }
  VLOG(9) << "s3compact: s3chunkinfo remove:";
  for (const auto& element : s3_chunk_info_remove) {
    auto chunkIndex = element.first;
    const auto& S3ChunkInfoList = element.second;

    for (int i = 0; i < S3ChunkInfoList.s3chunks_size(); i++) {
      const auto& chunkInfo = S3ChunkInfoList.s3chunks(i);
      VLOG(9) << "remove chunk, chunkindex: " << chunkIndex << ", chunkinfo: ["
              << chunkInfo.ShortDebugString() << "]";
    }
  }

  // 2. update inode
  VLOG(6) << "s3compact: start update inode";
  if (!task.copysetNodeWrapper->IsValid()) {
    VLOG(6) << "s3compact: invalid copysetNode";
    return;
  }

  std::vector<int> s3_chunk_info_remove_index;
  s3_chunk_info_remove_index.reserve(s3_chunk_info_remove.size());
  for (const auto& element : s3_chunk_info_remove) {
    s3_chunk_info_remove_index.push_back(element.first);
  }

  auto ret = UpdateInode(task.copysetNodeWrapper->Get(), compact_ctx.pinfo,
                         inode_id, std::move(s3_chunk_info_add),
                         std::move(s3_chunk_info_remove));
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "s3compact: UpdateInode failed, inodeKey = "
               << compact_ctx.fsId << "," << compact_ctx.inodeId
               << ", ret = " << MetaStatusCode_Name(ret);

    return;
  }

  VLOG(6) << "s3compact: finish update inode";

  // 3. delete old objs
  VLOG(6) << "s3compact: start delete old objs";
  for (const auto& index : s3_chunk_info_remove_index) {
    const auto& l = inode.s3chunkinfomap().at(index);
    DeleteObjsOfS3ChunkInfoList(compact_ctx, l);
  }
  VLOG(6) << "s3compact: finish delete objs";
  VLOG(6) << "s3compact: compact successfully";
}

}  // namespace metaserver
}  // namespace dingofs
