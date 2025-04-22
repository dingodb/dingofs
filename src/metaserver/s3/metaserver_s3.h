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

#ifndef DINGOFS_SRC_METASERVER_S3_METASERVER_S3_H_
#define DINGOFS_SRC_METASERVER_S3_METASERVER_S3_H_

#include <list>
#include <memory>
#include <string>

#include "dataaccess/aws/s3_adapter.h"

namespace dingofs {
namespace metaserver {

class S3Client {
 public:
  S3Client() = default;
  virtual ~S3Client() = default;
  virtual void Init(const options::client::S3Option& option) = 0;
  virtual int Delete(const std::string& name) = 0;
  virtual int DeleteBatch(const std::list<std::string>& nameList) = 0;
  virtual void Reinit(const std::string& ak, const std::string& sk,
                      const std::string& endpoint,
                      const std::string& bucketName) = 0;
};

class S3ClientImpl : public S3Client {
 public:
  S3ClientImpl() = default;
  ~S3ClientImpl() override = default;

  void SetAdaptor(std::shared_ptr<dataaccess::aws::S3Adapter> s3Adapter);
  void Init(const options::client::S3Option& option) override;
  void Reinit(const std::string& ak, const std::string& sk,
              const std::string& endpoint,
              const std::string& bucketName) override;
  /**
   * @brief
   *
   * @param name object_key
   * @return int
   *  1:  object is not exist
   *  0:  delete sucess
   *  -1: delete fail
   * @details
   */
  int Delete(const std::string& name) override;

  int DeleteBatch(const std::list<std::string>& nameList) override;

 private:
  std::shared_ptr<dataaccess::aws::S3Adapter> s3Adapter_;
  options::client::S3Option option_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_S3_METASERVER_S3_H_
