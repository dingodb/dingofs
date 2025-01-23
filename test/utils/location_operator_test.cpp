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
 * Created Date: Friday April 12th 2019
 * Author: yangyaokai
 */

#include "utils/location_operator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

namespace dingofs {
namespace utils {

TEST(LocationOperatorTest, GenerateTest) {
  std::string location = LocationOperator::GenerateS3Location("test");
  ASSERT_STREQ("test@s3", location.c_str());

  location = LocationOperator::GenerateDingoLocation("test", 0);
  ASSERT_STREQ("test:0@cs", location.c_str());
}

TEST(LocationOperatorTest, GenerateDingoLocationTest) {
  std::string originPath;
  std::string location;

  location = "test";
  ASSERT_EQ(OriginType::InvalidOrigin,
            LocationOperator::ParseLocation(location, &originPath));

  location = "test@b";
  ASSERT_EQ(OriginType::InvalidOrigin,
            LocationOperator::ParseLocation(location, &originPath));

  location = "test@s3@";
  ASSERT_EQ(OriginType::InvalidOrigin,
            LocationOperator::ParseLocation(location, &originPath));

  location = "test@s3";
  ASSERT_EQ(OriginType::S3Origin,
            LocationOperator::ParseLocation(location, &originPath));
  ASSERT_STREQ(originPath.c_str(), "test");

  location = "test@cs";
  ASSERT_EQ(OriginType::DingoOrigin,
            LocationOperator::ParseLocation(location, &originPath));
  ASSERT_STREQ(originPath.c_str(), "test");

  location = "test@test@cs";
  ASSERT_EQ(OriginType::DingoOrigin,
            LocationOperator::ParseLocation(location, &originPath));
  ASSERT_STREQ(originPath.c_str(), "test@test");

  location = "test@test@cs";
  ASSERT_EQ(OriginType::DingoOrigin,
            LocationOperator::ParseLocation(location, nullptr));
}

TEST(LocationOperatorTest, ParseDingoPathTest) {
  std::string originPath;
  std::string fileName;
  off_t offset;

  originPath = "test";
  ASSERT_EQ(false, LocationOperator::ParseDingoChunkPath(originPath, &fileName,
                                                         &offset));

  originPath = "test:";
  ASSERT_EQ(false, LocationOperator::ParseDingoChunkPath(originPath, &fileName,
                                                         &offset));

  originPath = ":0";
  ASSERT_EQ(false, LocationOperator::ParseDingoChunkPath(originPath, &fileName,
                                                         &offset));

  originPath = "test:0";
  ASSERT_EQ(true, LocationOperator::ParseDingoChunkPath(originPath, &fileName,
                                                        &offset));
  ASSERT_STREQ(fileName.c_str(), "test");
  ASSERT_EQ(offset, 0);

  originPath = "test:0:0";
  ASSERT_EQ(true, LocationOperator::ParseDingoChunkPath(originPath, &fileName,
                                                        &offset));
  ASSERT_STREQ(fileName.c_str(), "test:0");
  ASSERT_EQ(offset, 0);

  originPath = "test:0";
  ASSERT_EQ(true, LocationOperator::ParseDingoChunkPath(originPath, nullptr,
                                                        nullptr));
}

}  // namespace utils
}  // namespace dingofs
