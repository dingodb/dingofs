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

#ifndef DINGOFS_SRC_METASERVER_S3COMPACT_INODE_H_
#define DINGOFS_SRC_METASERVER_S3COMPACT_INODE_H_

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dataaccess/block_accesser.h"
#include "metaserver/compaction/s3compact_worker.h"
#include "metaserver/copyset/copyset_node.h"
#include "metaserver/storage/converter.h"

namespace dingofs {
namespace metaserver {

class CopysetNodeWrapper {
 public:
  explicit CopysetNodeWrapper(copyset::CopysetNode* copyset_node)
      : copyset_node_(copyset_node) {}

  virtual ~CopysetNodeWrapper() = default;

  virtual bool IsLeaderTerm() {
    return copyset_node_ != nullptr && copyset_node_->IsLeaderTerm();
  }

  virtual bool IsValid() { return copyset_node_ != nullptr; }

  copyset::CopysetNode* Get() { return copyset_node_; }

 private:
  copyset::CopysetNode* copyset_node_;
};

struct S3CompactionWorkerOptions;

class CompactInodeJob {
 public:
  explicit CompactInodeJob(const S3CompactWorkerOptions* opts) : opts_(opts) {}

  virtual ~CompactInodeJob() = default;

  // compact task for one inode
  struct S3CompactTask {
    std::shared_ptr<InodeManager> inodeManager;
    storage::Key4Inode inodeKey;
    pb::common::PartitionInfo pinfo;
    std::unique_ptr<CopysetNodeWrapper> copysetNodeWrapper;
  };

  struct S3CompactCtx {
    uint64_t inodeId;
    uint64_t fsId;
    pb::common::PartitionInfo pinfo;
    uint64_t blockSize;
    uint64_t chunkSize;
    dataaccess::BlockAccesser* block_accesser;
  };

  struct S3NewChunkInfo {
    uint64_t new_chunk_id;
    uint64_t new_off;
    uint64_t new_compaction;
  };

  struct S3Request {
    uint64_t req_index;
    bool zero;
    std::string obj_name;
    uint64_t off;
    uint64_t len;

    S3Request(uint64_t p_req_index, bool p_zero, std::string p_obj_name,
              uint64_t p_off, uint64_t p_len)
        : req_index(p_req_index),
          zero(p_zero),
          obj_name(std::move(p_obj_name)),
          off(p_off),
          len(p_len) {}
  };

  // node for building valid list
  struct Node {
    uint64_t begin;
    uint64_t end;
    uint64_t chunkid;
    uint64_t compaction;
    uint64_t chunkoff;
    uint64_t chunklen;
    bool zero;
    Node(uint64_t begin, uint64_t end, uint64_t chunkid, uint64_t compaction,
         uint64_t chunkoff, uint64_t chunklen, bool zero)
        : begin(begin),
          end(end),
          chunkid(chunkid),
          compaction(compaction),
          chunkoff(chunkoff),
          chunklen(chunklen),
          zero(zero) {}
  };

  // closure for updating inode, simply wait
  class GetOrModifyS3ChunkInfoClosure : public google::protobuf::Closure {
   private:
    std::mutex mutex_;
    std::condition_variable cond_;
    bool runned_ = false;

   public:
    void Run() override {
      std::lock_guard<std::mutex> l(mutex_);
      runned_ = true;
      cond_.notify_one();
    }

    void WaitRunned() {
      std::unique_lock<std::mutex> ul(mutex_);
      cond_.wait(ul, [this]() { return runned_; });
    }
  };

  std::vector<uint64_t> GetNeedCompact(
      const ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&
          s3_chunk_info_map,
      uint64_t inode_len, uint64_t chunk_size);

  bool CompactPrecheck(const struct S3CompactTask& task,
                       pb::metaserver::Inode* inode);

  std::list<struct Node> BuildValidList(const S3ChunkInfoList& s3chunkinfolist,
                                        uint64_t inode_len, uint64_t index,
                                        uint64_t chunk_size);

  static void GenS3ReadRequests(const struct S3CompactCtx& ctx,
                                const std::list<struct Node>& valid_list,
                                std::vector<struct S3Request>* reqs,
                                struct S3NewChunkInfo* new_chunk_info);

  int ReadFullChunk(const struct S3CompactCtx& ctx,
                    const std::list<struct Node>& valid_list,
                    std::string* full_chunk,
                    struct S3NewChunkInfo* new_chunk_info);

  virtual pb::metaserver::MetaStatusCode UpdateInode(
      copyset::CopysetNode* copyset_node,
      const pb::common::PartitionInfo& pinfo, uint64_t inode_id,
      ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3_chunk_info_add,
      ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&&
          s3_chunk_info_remove);

  Status WriteFullChunk(const struct S3CompactCtx& ctx,
                        const struct S3NewChunkInfo& new_chunk_info,
                        const std::string& full_chunk,
                        std::vector<std::string>* block_added);

  void CompactChunk(
      const struct S3CompactCtx& compact_ctx, uint64_t index,
      const pb::metaserver::Inode& inode,
      std::unordered_map<uint64_t, std::vector<std::string>>* objs_added_map,
      ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3_chunk_info_add,
      ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3_chunk_info_remove);

  void DeleteObjsOfS3ChunkInfoList(const struct S3CompactCtx& ctx,
                                   const S3ChunkInfoList& s3chunkinfolist);
  // func bind with task
  void CompactChunks(const S3CompactTask& task);

 private:
  const S3CompactWorkerOptions* opts_;
  copyset::CopysetNodeManager* copysetNodeMgr_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_S3COMPACT_INODE_H_
