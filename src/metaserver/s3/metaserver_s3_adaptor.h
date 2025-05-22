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

#ifndef DINGOFS_SRC_METASERVER_S3_METASERVER_S3_ADAPTOR_H_
#define DINGOFS_SRC_METASERVER_S3_METASERVER_S3_ADAPTOR_H_

#include <atomic>
#include <list>
#include <memory>
#include <string>

#include "blockaccess/accesser_common.h"
#include "blockaccess/block_accesser.h"
#include "blockaccess/block_accesser_factory.h"
#include "dingofs/metaserver.pb.h"

namespace dingofs {
namespace metaserver {

using pb::metaserver::S3ChunkInfo;
using pb::metaserver::S3ChunkInfoList;

struct S3ClientAdaptorOption {
  uint64_t blockSize; // from fs info
  uint64_t chunkSize; // from fs info
  uint64_t batchSize; // from config
  bool enableDeleteObjects; // from config
  std::shared_ptr<blockaccess::BlockAccesserFactory> block_accesser_factory;
};

class S3ClientAdaptor {
 public:
  S3ClientAdaptor() = default;
  virtual ~S3ClientAdaptor() = default;

  virtual Status Init(const S3ClientAdaptorOption& option,
                      blockaccess::BlockAccessOptions block_access_option) = 0;
  /**
   * @brief delete inode from s3
   * @param inode
   * @return int
   *  0   : delete sucess
   *  -1  : delete fail
   * @details
   * Step.1 get indoe' s3chunkInfoList
   * Step.2 delete chunk from s3 client
   */
  virtual Status Delete(const pb::metaserver::Inode& inode) = 0;
};

class S3ClientAdaptorImpl : public S3ClientAdaptor {
 public:
  S3ClientAdaptorImpl() = default;

  ~S3ClientAdaptorImpl() override {
    if (block_accesser_ != nullptr) {
      block_accesser_->Destroy();
      block_accesser_ = nullptr;
    }
  }

  Status Init(const S3ClientAdaptorOption& option,
              blockaccess::BlockAccessOptions block_access_option) override;

  /**
   * @brief delete inode from s3
   * @param inode
   * @return int
   * @details
   * Step.1 get indoe' s3chunkInfoList
   * Step.2 delete chunk from s3 client
   */
  Status Delete(const pb::metaserver::Inode& inode) override;

 private:
  /**
   * @brief  delete chunk from client
   * @return int
   *  0   : delete sucess or some objects are not exist
   *  -1  : some objects delete fail
   * @param[in] options the options for s3 client
   */
  Status DeleteChunk(uint64_t fs_id, uint64_t inode_id, uint64_t chunk_id,
                     uint64_t compaction, uint64_t chunk_pos, uint64_t length);

  Status DeleteInodeByDeleteSingleChunk(const pb::metaserver::Inode& inode);

  Status DeleteInodeByDeleteBatchChunk(const pb::metaserver::Inode& inode);

  Status DeleteS3ChunkInfoList(uint32_t fs_id, uint64_t inode_id,
                               const S3ChunkInfoList& s3_chunk_infolist);

  void GenObjNameListForChunkInfoList(uint32_t fs_id, uint64_t inode_id,
                                      const S3ChunkInfoList& s3_chunk_infolist,
                                      std::list<std::string>* obj_list);

  void GenObjNameListForChunkInfo(uint32_t fs_id, uint64_t inode_id,
                                  const S3ChunkInfo& chunk_info,
                                  std::list<std::string>* obj_list);

  uint64_t blockSize_;
  uint64_t chunkSize_;
  uint64_t batchSize_;
  bool enableDeleteObjects_;

  std::atomic_bool is_inited_{false};
  S3ClientAdaptorOption adaptor_option_;
  blockaccess::BlockAccessOptions block_access_option_;
  std::unique_ptr<blockaccess::BlockAccesser> block_accesser_{nullptr};
};
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_S3_METASERVER_S3_ADAPTOR_H_
