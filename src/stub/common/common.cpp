// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include "stub/common/common.h"

namespace dingofs {
namespace stub {
namespace common {

std::ostream& operator<<(std::ostream& os, MetaServerOpType optype) {
  switch (optype) {
    case MetaServerOpType::GetDentry:
      os << "GetDentry";
      break;
    case MetaServerOpType::ListDentry:
      os << "ListDentry";
      break;
    case MetaServerOpType::CreateDentry:
      os << "CreateDentry";
      break;
    case MetaServerOpType::DeleteDentry:
      os << "DeleteDentry";
      break;
    case MetaServerOpType::PrepareRenameTx:
      os << "PrepareRenameTx";
      break;
    case MetaServerOpType::GetInode:
      os << "GetInode";
      break;
    case MetaServerOpType::BatchGetInodeAttr:
      os << "BatchGetInodeAttr";
      break;
    case MetaServerOpType::BatchGetXAttr:
      os << "BatchGetXAttr";
      break;
    case MetaServerOpType::UpdateInode:
      os << "UpdateInode";
      break;
    case MetaServerOpType::CreateInode:
      os << "CreateInode";
      break;
    case MetaServerOpType::DeleteInode:
      os << "DeleteInode";
      break;
    case MetaServerOpType::GetOrModifyS3ChunkInfo:
      os << "GetOrModifyS3ChunkInfo";
      break;
    case MetaServerOpType::GetVolumeExtent:
      os << "GetVolumeExtent";
      break;
    case MetaServerOpType::UpdateVolumeExtent:
      os << "UpdateVolumeExtent";
      break;
    case MetaServerOpType::GetFsQuota:
      os << "GetFsQuota";
      break;
    case MetaServerOpType::FlushFsUsage:
      os << "FlushFsUsage";
      break;
    case MetaServerOpType::LoadDirQutoas:
      os << "LoadDirQutoas";
      break;
    case MetaServerOpType::FlushDirUsages:
      os << "FlushDirUsages";
      break;
    default:
      os << "Unknow opType";
  }
  return os;
}
}  // namespace common
}  // namespace stub
}  // namespace dingofs