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
 * Project: dingo
 * Created Date: 2021-8-13
 * Author: chengyi
 */

#include "metaserver/s3/metaserver_s3_adaptor.h"

#include <glog/logging.h>

#include <algorithm>
#include <list>
#include <memory>

#include "common/s3util.h"
#include "common/status.h"

namespace dingofs {
namespace metaserver {

Status S3ClientAdaptorImpl::Init(
    const S3ClientAdaptorOption& option,
    dataaccess::BlockAccessOptions block_access_option) {
  blockSize_ = option.blockSize;
  chunkSize_ = option.chunkSize;
  batchSize_ = option.batchSize;
  enableDeleteObjects_ = option.enableDeleteObjects;

  adaptor_option_ = option;
  block_access_option_ = block_access_option;

  block_accesser_ = adaptor_option_.block_accesser_factory->NewBlockAccesser(
      block_access_option);
  Status s = block_accesser_->Init();
  if (!s.ok()) {
    LOG(ERROR) << "Fail init block accesser: " << s.ToString();
  } else {
    is_inited_.store(true);
    VLOG(3) << "S3ClientAdaptorImpl init success";
  }

  return s;
}

Status S3ClientAdaptorImpl::Delete(const pb::metaserver::Inode& inode) {
  if (!is_inited_.load()) {
    LOG(ERROR) << "Fail to delete because S3ClientAdaptorImpl not init, ino: "
               << inode.inodeid();
    return Status::Internal("S3ClientAdaptorImpl not init");
  }

  Status s;
  if (enableDeleteObjects_) {
    s = DeleteInodeByDeleteBatchChunk(inode);
  } else {
    s = DeleteInodeByDeleteSingleChunk(inode);
  }

  return s;
}

Status S3ClientAdaptorImpl::DeleteInodeByDeleteSingleChunk(
    const pb::metaserver::Inode& inode) {
  auto s3_chunk_info_map = inode.s3chunkinfomap();
  LOG(INFO) << "delete data, inode id: " << inode.inodeid()
            << ", len:" << inode.length()
            << ", chunk info map size: " << s3_chunk_info_map.size();
  Status s;

  auto iter = s3_chunk_info_map.begin();
  for (; iter != s3_chunk_info_map.end(); iter++) {
    S3ChunkInfoList& s3_chunk_infolist = iter->second;
    for (int i = 0; i < s3_chunk_infolist.s3chunks_size(); ++i) {
      // traverse chunks to delete blocks
      S3ChunkInfo chunk_info = s3_chunk_infolist.s3chunks(i);
      // delete chunkInfo from client
      uint64_t fs_id = inode.fsid();
      uint64_t inode_id = inode.inodeid();
      uint64_t chunk_id = chunk_info.chunkid();
      uint64_t compaction = chunk_info.compaction();
      uint64_t chunk_pos = chunk_info.offset() % chunkSize_;
      uint64_t length = chunk_info.len();
      Status tmp =
          DeleteChunk(fs_id, inode_id, chunk_id, compaction, chunk_pos, length);
      if (!tmp.ok()) {
        LOG(ERROR) << "Fail delete chunk: " << chunk_id << ", fs_id: " << fs_id
                   << ", inode_id: " << inode_id
                   << ", status: " << tmp.ToString();
        s = tmp;
      }
    }
  }

  LOG(INFO) << "delete data, inode id: " << inode.inodeid()
            << ", len:" << inode.length() << " success";

  return s;
}

Status S3ClientAdaptorImpl::DeleteChunk(uint64_t fs_id, uint64_t inode_id,
                                        uint64_t chunk_id, uint64_t compaction,
                                        uint64_t chunk_pos, uint64_t length) {
  uint64_t block_index = chunk_pos / blockSize_;
  uint64_t block_pos = chunk_pos % blockSize_;
  int count = 0;  // blocks' number
  int ret = 0;

  Status s;
  while (length > blockSize_ * count - block_pos || count == 0) {
    // divide chunks to blocks, and delete these blocks
    std::string object_name = dingofs::common::s3util::GenObjName(
        chunk_id, block_index, compaction, fs_id, inode_id);

    Status tmp = block_accesser_->Delete(object_name);
    if (!tmp.ok()) {
      LOG(ERROR) << "delete block fail. block: " << object_name
                 << ", status: " << tmp.ToString();
      s = tmp;
    }

    ++block_index;
    ++count;
  }

  return s;
}

Status S3ClientAdaptorImpl::DeleteInodeByDeleteBatchChunk(
    const pb::metaserver::Inode& inode) {
  auto s3_chunk_info_map = inode.s3chunkinfomap();
  LOG(INFO) << "delete data, inode id: " << inode.inodeid()
            << ", len:" << inode.length()
            << ", chunk info map size: " << s3_chunk_info_map.size();

  Status s;

  auto iter = s3_chunk_info_map.begin();
  while (iter != s3_chunk_info_map.end()) {
    Status tmp =
        DeleteS3ChunkInfoList(inode.fsid(), inode.inodeid(), iter->second);
    if (!tmp.ok()) {
      LOG(ERROR) << "Fail delete chunk, chunk index is " << iter->first
                 << ", fs_id: " << inode.fsid()
                 << ", inode_id: " << inode.inodeid()
                 << ", status: " << tmp.ToString();
      s = tmp;
      iter++;
    } else {
      iter = s3_chunk_info_map.erase(iter);
    }
  }

  LOG(INFO) << "delete data, inode id: " << inode.inodeid()
            << ", len:" << inode.length() << " , status: " << s.ToString();

  return s;
}

// TOOD: the fail operation maybe need to process
Status S3ClientAdaptorImpl::DeleteS3ChunkInfoList(
    uint32_t fs_id, uint64_t inode_id,
    const S3ChunkInfoList& s3_chunk_infolist) {
  std::list<std::string> obj_list;

  GenObjNameListForChunkInfoList(fs_id, inode_id, s3_chunk_infolist, &obj_list);

  Status s;
  while (obj_list.size() != 0) {
    std::list<std::string> temp_obj_list;
    auto begin = obj_list.begin();
    auto end = obj_list.begin();
    std::advance(end, std::min(batchSize_, obj_list.size()));
    temp_obj_list.splice(temp_obj_list.begin(), obj_list, begin, end);
    Status tmp = block_accesser_->BatchDelete(temp_obj_list);
    if (!tmp.ok()) {
      LOG(ERROR) << "Fail batch delete in DeleteS3ChunkInfoList fs_id: "
                 << fs_id << ", inodeId =  " << inode_id
                 << ", status: " << tmp.ToString();
      s = tmp;
    }
  }

  return s;
}

void S3ClientAdaptorImpl::GenObjNameListForChunkInfoList(
    uint32_t fs_id, uint64_t inode_id, const S3ChunkInfoList& s3_chunk_infolist,
    std::list<std::string>* obj_list) {
  for (int i = 0; i < s3_chunk_infolist.s3chunks_size(); ++i) {
    const S3ChunkInfo& chunk_info = s3_chunk_infolist.s3chunks(i);
    std::list<std::string> temp_obj_list;
    GenObjNameListForChunkInfo(fs_id, inode_id, chunk_info, &temp_obj_list);

    obj_list->splice(obj_list->end(), temp_obj_list);
  }
}

void S3ClientAdaptorImpl::GenObjNameListForChunkInfo(
    uint32_t fs_id, uint64_t inode_id, const S3ChunkInfo& chunk_info,
    std::list<std::string>* obj_list) {
  uint64_t chunk_id = chunk_info.chunkid();
  uint64_t compaction = chunk_info.compaction();
  uint64_t chunk_pos = chunk_info.offset() % chunkSize_;
  uint64_t length = chunk_info.len();
  uint64_t block_index = chunk_pos / blockSize_;
  uint64_t block_pos = chunk_pos % blockSize_;
  VLOG(9) << "delete Chunk start, fsId = " << fs_id
          << ", inodeId = " << inode_id << ", chunk id: " << chunk_id
          << ", compaction:" << compaction << ", chunkPos: " << chunk_pos
          << ", length: " << length;
  int count = (length + block_pos + blockSize_ - 1) / blockSize_;
  for (int i = 0; i < count; i++) {
    // divide chunks to blocks, and delete these blocks
    std::string object_name = dingofs::common::s3util::GenObjName(
        chunk_id, block_index, compaction, fs_id, inode_id);
    obj_list->push_back(object_name);
    VLOG(9) << "gen object name: " << object_name;

    ++block_index;
  }
}

}  // namespace metaserver
}  // namespace dingofs
