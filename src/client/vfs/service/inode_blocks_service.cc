// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include "client/vfs/service/inode_blocks_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <fmt/format.h>

#include <cstdint>
#include <memory>

#include "client/common/const.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/data/flat/flat_file.h"
#include "client/vfs/vfs_meta.h"
#include "common/options/client.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

static Status InitFlatFile(VFSHub* vfs_hub, FlatFile* flat_file) {
  auto span = vfs_hub->GetTracer()->StartSpan(kVFSDataMoudule, __func__);
  Attr attr;
  DINGOFS_RETURN_NOT_OK(vfs_hub->GetMetaSystem()->GetAttr(
      span->GetContext(), flat_file->GetIno(), &attr));

  uint64_t chunk_num = (attr.length / flat_file->GetChunkSize()) + 1;

  VLOG(6) << fmt::format("InitFlatFile ino: {}, chunk_num: {} attr: {}",
                         flat_file->GetIno(), chunk_num, Attr2Str(attr));

  for (uint64_t i = 0; i < chunk_num; ++i) {
    std::vector<Slice> slices;
    DINGOFS_RETURN_NOT_OK(vfs_hub->GetMetaSystem()->ReadSlice(
        span->GetContext(), flat_file->GetIno(), i, 0, &slices));

    flat_file->FillChunk(i, std::move(slices));
  }

  return Status::OK();
}

namespace {

struct FormatConfig {
  uint32_t file_offset_width;
  uint32_t len_width;
  uint32_t block_offset_width;
  uint32_t block_name_width;
  uint32_t block_len_width;
  std::string delimiter;
};

void PrintDelimitedHeader(butil::IOBufBuilder& os, const FormatConfig& config) {
  os << std::left << "file_offset" << config.delimiter << "len"
     << config.delimiter << "block_offset" << config.delimiter << "block_name"
     << config.delimiter << "block_len"
     << "\n";
}

void PrintFixedWidthHeader(butil::IOBufBuilder& os,
                           const FormatConfig& config) {
  os << std::left << std::setw(config.file_offset_width) << "file_offset"
     << std::setw(config.len_width) << "len"
     << std::setw(config.block_offset_width) << "block_offset"
     << std::setw(config.block_name_width) << "block_name"
     << std::setw(config.block_len_width) << "block_len"
     << "\n";
}

void PrintDelimitedRow(butil::IOBufBuilder& os, const FormatConfig& config,
                       uint64_t file_pos, uint64_t len, uint64_t block_offset,
                       const std::string& block_key, uint64_t block_len) {
  os << file_pos << config.delimiter << len << config.delimiter << block_offset
     << config.delimiter << block_key << config.delimiter << block_len << '\n';
}

void PrintFixedWidthRow(butil::IOBufBuilder& os, const FormatConfig& config,
                        uint64_t file_pos, uint64_t len, uint64_t block_offset,
                        const std::string& block_key, uint64_t block_len) {
  os << fmt::format("{:<{}}{:<{}}{:<{}}{:<{}}{:<{}}\n", file_pos,
                    config.file_offset_width, len, config.len_width,
                    block_offset, config.block_offset_width, block_key,
                    config.block_name_width, block_len, config.block_len_width);
}

}  // namespace

static void DumpFlatFile(butil::IOBufBuilder& os, FlatFile* flat_file,
                         bool use_delimiter) {
  FormatConfig config{
      .file_offset_width = FLAGS_format_file_offset_width,
      .len_width = FLAGS_format_len_width,
      .block_offset_width = FLAGS_format_block_offset_width,
      .block_name_width = FLAGS_format_block_name_width,
      .block_len_width = FLAGS_format_block_len_width,
      .delimiter = FLAGS_format_delimiter,
  };

  // Print header
  if (use_delimiter) {
    PrintDelimitedHeader(os, config);
  } else {
    PrintFixedWidthHeader(os, config);
  }

  // Get data once
  const auto block_reqs = flat_file->GenBlockReadReqs();
  const uint64_t fs_id = flat_file->GetFsId();
  const uint64_t ino = flat_file->GetIno();

  // Print each row
  for (const auto& req : block_reqs) {
    cache::BlockKey key(fs_id, ino, req.block.slice_id, req.block.index,
                        req.block.version);

    const auto file_pos = req.block.file_offset + req.block_offset;
    const auto& block_key = key.StoreKey();

    if (use_delimiter) {
      PrintDelimitedRow(os, config, file_pos, req.len, req.block_offset,
                        block_key, req.block.block_len);
    } else {
      PrintFixedWidthRow(os, config, file_pos, req.len, req.block_offset,
                         block_key, req.block.block_len);
    }
  }
}

void InodeBlocksServiceImpl::default_method(
    google::protobuf::RpcController* controller,
    const pb::client::InodeBlocksRequest* request,
    pb::client::InodeBlocksResponse* response,
    google::protobuf::Closure* done) {
  (void)request;
  (void)response;
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");

  butil::IOBufBuilder os;

  const std::string& path = cntl->http_request().unresolved_path();
  if (path.empty()) {
    os << "# Use InodeBlocksService/<inode_id> to get inode blocks\n";
  } else {
    char* endptr = nullptr;

    int64_t ino = strtoll(path.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      VLOG(6) << "Get inode blocks, ino: " << ino;
      FsInfo fs_info = vfs_hub_->GetFsInfo();
      VLOG(6) << "Get vfs_fs_info, fs: " << FsInfo2Str(fs_info);

      auto flat_file = std::make_unique<FlatFile>(
          fs_info.id, ino, fs_info.chunk_size, fs_info.block_size);
      Status s = InitFlatFile(vfs_hub_, flat_file.get());
      if (!s.ok()) {
        std::string msg = fmt::format("InitFlatFile failed, ino: {}, error: {}",
                                      ino, s.ToString());
        LOG(INFO) << msg;
        os << msg << "\n";
        return;
      }

      const std::string* delimiter =
          cntl->http_request().uri().GetQuery("delimiter");
      DumpFlatFile(os, flat_file.get(), (delimiter != nullptr));
    } else {
      LOG(INFO) << "Invalid ino: " << path;
      cntl->SetFailed(brpc::ENOMETHOD, "Invalid ino %s", path.c_str());
    }
  }

  os.move_to(cntl->response_attachment());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
