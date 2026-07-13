/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/config_mapper.h"

#include <gtest/gtest.h>

namespace dingofs {

TEST(ConfigMapperTest, FillsS3OptionsFromS3FsInfo) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_type(pb::mds::FsType::S3);
  auto* s3_info = fs_info.mutable_extra()->mutable_s3_info();
  s3_info->set_ak("ak-value");
  s3_info->set_sk("sk-value");
  s3_info->set_endpoint("http://s3.example.com");
  s3_info->set_bucketname("my-bucket");

  blockaccess::BlockAccessOptions options;
  FillBlockAccessOption(fs_info, &options);

  EXPECT_EQ(options.type, blockaccess::AccesserType::kS3);
  EXPECT_EQ(options.s3_options.s3_info.ak, "ak-value");
  EXPECT_EQ(options.s3_options.s3_info.sk, "sk-value");
  EXPECT_EQ(options.s3_options.s3_info.endpoint, "http://s3.example.com");
  EXPECT_EQ(options.s3_options.s3_info.bucket_name, "my-bucket");
}

TEST(ConfigMapperTest, FillsRadosOptionsFromRadosFsInfo) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_type(pb::mds::FsType::RADOS);
  auto* rados_info = fs_info.mutable_extra()->mutable_rados_info();
  rados_info->set_mon_host("1.2.3.4:6789");
  rados_info->set_user_name("admin");
  rados_info->set_key("secret-key");
  rados_info->set_pool_name("pool-a");

  blockaccess::BlockAccessOptions options;
  FillBlockAccessOption(fs_info, &options);

  EXPECT_EQ(options.type, blockaccess::AccesserType::kRados);
  EXPECT_EQ(options.rados_options.mon_host, "1.2.3.4:6789");
  EXPECT_EQ(options.rados_options.user_name, "admin");
  EXPECT_EQ(options.rados_options.key, "secret-key");
  EXPECT_EQ(options.rados_options.pool_name, "pool-a");
}

TEST(ConfigMapperTest, RadosClusterNameDefaultsWhenNotSetInFsInfo) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_type(pb::mds::FsType::RADOS);
  auto* rados_info = fs_info.mutable_extra()->mutable_rados_info();
  rados_info->set_mon_host("1.2.3.4:6789");
  rados_info->set_pool_name("pool-a");
  // cluster_name intentionally left unset.

  blockaccess::RadosOptions defaults;
  const std::string default_cluster_name = defaults.cluster_name;

  blockaccess::BlockAccessOptions options;
  options.rados_options.cluster_name = default_cluster_name;
  FillBlockAccessOption(fs_info, &options);

  EXPECT_EQ(options.rados_options.cluster_name, default_cluster_name);
}

TEST(ConfigMapperTest, RadosClusterNameOverriddenWhenSetInFsInfo) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_type(pb::mds::FsType::RADOS);
  auto* rados_info = fs_info.mutable_extra()->mutable_rados_info();
  rados_info->set_mon_host("1.2.3.4:6789");
  rados_info->set_pool_name("pool-a");
  rados_info->set_cluster_name("custom-cluster");

  blockaccess::BlockAccessOptions options;
  FillBlockAccessOption(fs_info, &options);

  EXPECT_EQ(options.rados_options.cluster_name, "custom-cluster");
}

TEST(ConfigMapperTest, FillsLocalFileOptionsFromLocalFsInfo) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_type(pb::mds::FsType::LOCALFILE);
  fs_info.mutable_extra()->mutable_file_info()->set_path("/mnt/data");

  blockaccess::BlockAccessOptions options;
  FillBlockAccessOption(fs_info, &options);

  EXPECT_EQ(options.type, blockaccess::AccesserType::kLocalFile);
  EXPECT_EQ(options.file_options.path, "/mnt/data");
}

TEST(ConfigMapperDeathTest, S3TypeWithoutS3InfoAborts) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_type(pb::mds::FsType::S3);
  blockaccess::BlockAccessOptions options;
  EXPECT_DEATH(FillBlockAccessOption(fs_info, &options), "S3 info not set");
}

}  // namespace dingofs
