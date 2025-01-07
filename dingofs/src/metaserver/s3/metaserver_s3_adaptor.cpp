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

#include "dingofs/src/metaserver/s3/metaserver_s3_adaptor.h"

#include <glog/logging.h>

#include <algorithm>
#include <list>

#include "dingofs/src/common/s3util.h"

namespace dingofs {
namespace metaserver {
void S3ClientAdaptorImpl::Init(const S3ClientAdaptorOption& option,
                               S3Client* client) {
  blockSize_ = option.blockSize;
  chunkSize_ = option.chunkSize;
  batchSize_ = option.batchSize;
  enableDeleteObjects_ = option.enableDeleteObjects;
  objectPrefix_ = option.objectPrefix;
  client_ = client;
}

void S3ClientAdaptorImpl::Reinit(const S3ClientAdaptorOption& option,
                                 const std::string& ak, const std::string& sk,
                                 const std::string& endpoint,
                                 const std::string& bucket_name) {
  blockSize_ = option.blockSize;
  chunkSize_ = option.chunkSize;
  batchSize_ = option.batchSize;
  enableDeleteObjects_ = option.enableDeleteObjects;
  objectPrefix_ = option.objectPrefix;
  client_->Reinit(ak, sk, endpoint, bucket_name);
}

int S3ClientAdaptorImpl::Delete(const pb::metaserver::Inode& inode) {
  if (enableDeleteObjects_) {
    return DeleteInodeByDeleteBatchChunk(inode);
  } else {
    return DeleteInodeByDeleteSingleChunk(inode);
  }
}

int S3ClientAdaptorImpl::DeleteInodeByDeleteSingleChunk(
    const pb::metaserver::Inode& inode) {
  auto s3_chunk_info_map = inode.s3chunkinfomap();
  LOG(INFO) << "delete data, inode id: " << inode.inodeid()
            << ", len:" << inode.length()
            << ", chunk info map size: " << s3_chunk_info_map.size();
  int ret = 0;
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
      int del_stat =
          DeleteChunk(fs_id, inode_id, chunk_id, compaction, chunk_pos, length);
      if (del_stat < 0) {
        LOG(ERROR) << "delete chunk failed, status code is: " << del_stat
                   << " , chunkId is " << chunk_id;
        ret = -1;
      }
    }
  }
  LOG(INFO) << "delete data, inode id: " << inode.inodeid()
            << ", len:" << inode.length() << " success";

  return ret;
}

int S3ClientAdaptorImpl::DeleteChunk(uint64_t fs_id, uint64_t inode_id,
                                     uint64_t chunk_id, uint64_t compaction,
                                     uint64_t chunk_pos, uint64_t length) {
  uint64_t block_index = chunk_pos / blockSize_;
  uint64_t block_pos = chunk_pos % blockSize_;
  int count = 0;  // blocks' number
  int ret = 0;
  while (length > blockSize_ * count - block_pos || count == 0) {
    // divide chunks to blocks, and delete these blocks
    std::string object_name = dingofs::common::s3util::GenObjName(
        chunk_id, block_index, compaction, fs_id, inode_id, objectPrefix_);
    int del_stat = client_->Delete(object_name);
    if (del_stat < 0) {
      // fail
      LOG(ERROR) << "delete object fail. object: " << object_name;
      ret = -1;
    } else if (del_stat > 0) {  // delSat == 1
      // object is not exist
      // 1. overwrite，the object is delete by others
      // 2. last delete failed
      // 3. others
      ret = 1;
    }

    ++block_index;
    ++count;
  }

  return ret;
}

int S3ClientAdaptorImpl::DeleteInodeByDeleteBatchChunk(
    const pb::metaserver::Inode& inode) {
  auto s3_chunk_info_map = inode.s3chunkinfomap();
  LOG(INFO) << "delete data, inode id: " << inode.inodeid()
            << ", len:" << inode.length()
            << ", chunk info map size: " << s3_chunk_info_map.size();
  int return_code = 0;
  auto iter = s3_chunk_info_map.begin();
  while (iter != s3_chunk_info_map.end()) {
    int ret =
        DeleteS3ChunkInfoList(inode.fsid(), inode.inodeid(), iter->second);
    if (ret != 0) {
      LOG(ERROR) << "delete chunk failed, ret = " << ret << " , chunk index is "
                 << iter->first;
      return_code = -1;
      iter++;
    } else {
      iter = s3_chunk_info_map.erase(iter);
    }
  }
  LOG(INFO) << "delete data, inode id: " << inode.inodeid()
            << ", len:" << inode.length() << " , ret = " << return_code;

  return return_code;
}

int S3ClientAdaptorImpl::DeleteS3ChunkInfoList(
    uint32_t fs_id, uint64_t inode_id,
    const S3ChunkInfoList& s3_chunk_infolist) {
  std::list<std::string> obj_list;

  GenObjNameListForChunkInfoList(fs_id, inode_id, s3_chunk_infolist, &obj_list);

  while (obj_list.size() != 0) {
    std::list<std::string> temp_obj_list;
    auto begin = obj_list.begin();
    auto end = obj_list.begin();
    std::advance(end, std::min(batchSize_, obj_list.size()));
    temp_obj_list.splice(temp_obj_list.begin(), obj_list, begin, end);
    int ret = client_->DeleteBatch(temp_obj_list);
    if (ret != 0) {
      LOG(ERROR) << "DeleteS3ChunkInfoList failed, fsId = " << fs_id
                 << ", inodeId =  " << inode_id << ", status code = " << ret;
      return -1;
    }
  }

  return 0;
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
        chunk_id, block_index, compaction, fs_id, inode_id, objectPrefix_);
    obj_list->push_back(object_name);
    VLOG(9) << "gen object name: " << object_name;

    ++block_index;
  }
}

void S3ClientAdaptorImpl::GetS3ClientAdaptorOption(
    S3ClientAdaptorOption* option) {
  option->blockSize = blockSize_;
  option->chunkSize = chunkSize_;
  option->batchSize = batchSize_;
  option->enableDeleteObjects = enableDeleteObjects_;
  option->objectPrefix = objectPrefix_;
}

}  // namespace metaserver
}  // namespace dingofs
