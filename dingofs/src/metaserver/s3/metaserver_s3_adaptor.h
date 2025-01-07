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

#include <list>
#include <string>

#include "dingofs/proto/metaserver.pb.h"
#include "dingofs/src/metaserver/s3/metaserver_s3.h"

namespace dingofs {
namespace metaserver {

using pb::metaserver::S3ChunkInfo;
using pb::metaserver::S3ChunkInfoList;

struct S3ClientAdaptorOption {
  uint64_t blockSize;
  uint64_t chunkSize;
  uint64_t batchSize;
  uint32_t objectPrefix;
  bool enableDeleteObjects;
};

class S3ClientAdaptor {
 public:
  S3ClientAdaptor() = default;
  virtual ~S3ClientAdaptor() = default;

  /**
   * @brief Initialize s3 client
   * @param[in] options the options for s3 client
   */
  virtual void Init(const S3ClientAdaptorOption& option, S3Client* client) = 0;
  /**
   * @brief Reinitialize s3 client
   */
  virtual void Reinit(const S3ClientAdaptorOption& option,
                      const std::string& ak, const std::string& sk,
                      const std::string& endpoint,
                      const std::string& bucket_name) = 0;

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
  virtual int Delete(const pb::metaserver::Inode& inode) = 0;

  /**
   * @brief get S3ClientAdaptorOption
   *
   * @param option return value
   * @details
   */
  virtual void GetS3ClientAdaptorOption(S3ClientAdaptorOption* option) = 0;
};

class S3ClientAdaptorImpl : public S3ClientAdaptor {
 public:
  S3ClientAdaptorImpl() = default;
  ~S3ClientAdaptorImpl() override {
    if (client_ != nullptr) {
      delete client_;
      client_ = nullptr;
    }
  }

  /**
   * @brief Initialize s3 client
   * @param[in] options the options for s3 client
   */
  void Init(const S3ClientAdaptorOption& option, S3Client* client) override;

  /**
   * @brief Reinitialize s3 client
   */
  void Reinit(const S3ClientAdaptorOption& option, const std::string& ak,
              const std::string& sk, const std::string& endpoint,
              const std::string& bucket_name) override;

  /**
   * @brief delete inode from s3
   * @param inode
   * @return int
   * @details
   * Step.1 get indoe' s3chunkInfoList
   * Step.2 delete chunk from s3 client
   */
  int Delete(const pb::metaserver::Inode& inode) override;

  /**
   * @brief get S3ClientAdaptorOption
   *
   * @param option return value
   * @details
   */
  void GetS3ClientAdaptorOption(S3ClientAdaptorOption* option) override;

 private:
  /**
   * @brief  delete chunk from client
   * @return int
   *  0   : delete sucess or some objects are not exist
   *  -1  : some objects delete fail
   * @param[in] options the options for s3 client
   */
  int DeleteChunk(uint64_t fs_id, uint64_t inode_id, uint64_t chunk_id,
                  uint64_t compaction, uint64_t chunk_pos, uint64_t length);

  int DeleteInodeByDeleteSingleChunk(const pb::metaserver::Inode& inode);

  int DeleteInodeByDeleteBatchChunk(const pb::metaserver::Inode& inode);

  int DeleteS3ChunkInfoList(uint32_t fs_id, uint64_t inode_id,
                            const S3ChunkInfoList& s3_chunk_infolist);

  void GenObjNameListForChunkInfoList(uint32_t fs_id, uint64_t inode_id,
                                      const S3ChunkInfoList& s3_chunk_infolist,
                                      std::list<std::string>* obj_list);

  void GenObjNameListForChunkInfo(uint32_t fs_id, uint64_t inode_id,
                                  const S3ChunkInfo& chunk_info,
                                  std::list<std::string>* obj_list);

  S3Client* client_;
  uint64_t blockSize_;
  uint64_t chunkSize_;
  uint64_t batchSize_;
  uint32_t objectPrefix_;
  bool enableDeleteObjects_;
};
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_S3_METASERVER_S3_ADAPTOR_H_
