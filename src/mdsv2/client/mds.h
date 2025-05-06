// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <string>

#include "mdsv2/client/interaction.h"

namespace dingofs {
namespace mdsv2 {
namespace client {

class MDSClient {
 public:
  MDSClient() = default;
  ~MDSClient() = default;

  bool Init(const std::string& mds_addr);

  void GetMdsList();

  struct CreateFsParams {
    std::string partition_type;
    uint32_t chunk_size;
    uint32_t block_size;
    std::string s3_endpoint;
    std::string s3_ak;
    std::string s3_sk;
    std::string s3_bucketname;
    std::string owner = "deng";
  };

  void CreateFs(const std::string& fs_name, const CreateFsParams& params);
  void DeleteFs(const std::string& fs_name, bool is_force);
  void UpdateFs(const std::string& fs_name);
  void GetFs(const std::string& fs_name);
  void ListFs();

  void MkDir(uint32_t fs_id, uint64_t parent_ino, const std::string& name);
  void BatchMkDir(uint32_t fs_id, const std::vector<int64_t>& parents, const std::string& prefix, size_t num);

  void MkNod(uint32_t fs_id, uint64_t parent_ino, const std::string& name);
  void BatchMkNod(uint32_t fs_id, const std::vector<int64_t>& parents, const std::string& prefix, size_t num);

  void GetDentry(uint32_t fs_id, uint64_t parent, const std::string& name);
  void ListDentry(uint32_t fs_id, uint64_t parent, bool is_only_dir);
  void GetInode(uint32_t fs_id, uint64_t ino);
  void BatchGetInode(uint32_t fs_id, const std::vector<int64_t>& inos);
  void BatchGetXattr(uint32_t fs_id, const std::vector<int64_t>& inos);

  void SetFsStats(const std::string& fs_name);
  void ContinueSetFsStats(const std::string& fs_name);
  void GetFsStats(const std::string& fs_name);
  void GetFsPerSecondStats(const std::string& fs_name);

 private:
  InteractionPtr interaction_;
};

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs