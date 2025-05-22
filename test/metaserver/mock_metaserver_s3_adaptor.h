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
 * Created Date: 2021-8-16
 * Author: chengyi
 */
#ifndef DINGOFS_TEST_METASERVER_MOCK_METASERVER_S3_ADAPTOR_H_
#define DINGOFS_TEST_METASERVER_MOCK_METASERVER_S3_ADAPTOR_H_

#include <gmock/gmock.h>

#include "dingofs/metaserver.pb.h"
#include "metaserver/s3/metaserver_s3_adaptor.h"

using ::dingofs::pb::metaserver::Inode;

namespace dingofs {
namespace metaserver {

class MockS3ClientAdaptor : public S3ClientAdaptor {
 public:
  MockS3ClientAdaptor() = default;
  ~MockS3ClientAdaptor() override = default;

  MOCK_METHOD(Status, Init,
              (const S3ClientAdaptorOption& option,
               blockaccess::BlockAccessOptions block_access_option),
              (override));

  MOCK_METHOD(Status, Delete, (const pb::metaserver::Inode& inode), (override));
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_MOCK_METASERVER_S3_ADAPTOR_H_
