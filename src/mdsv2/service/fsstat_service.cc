// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/service/fsstat_service.h"

#include <sys/types.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "butil/iobuf.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "mdsv2/common/context.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/fs_utils.h"
#include "mdsv2/server.h"

namespace dingofs {
namespace mdsv2 {

static std::string RenderHead() {
  butil::IOBufBuilder os;

  os << fmt::format(R"(<head>{})", brpc::gridtable_style());
  os << fmt::format(R"(<script src="/js/sorttable"></script>)");
  os << fmt::format(R"(<script language="javascript" type="text/javascript" src="/js/jquery_min"></script>)");
  os << brpc::TabsHead();

  os << R"(<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  body {
    font-family: ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace;
  }
  .red-text {
  color: red;
  }
  .blue-text {
  color: blue;
  }
  .green-text {
  color: green;
  }
  .bold-text {
  font-weight: bold;
  }
</style>)";

  os << brpc::TabsHead() << "</head>";

  butil::IOBuf buf;
  os.move_to(buf);

  return buf.to_string();
}

static std::string RenderMountpoint(const pb::mdsv2::FsInfo& fs_info) {
  std::string result;
  for (const auto& mountpoint : fs_info.mount_points()) {
    result += fmt::format("{}:{}", mountpoint.hostname(), mountpoint.port());
    result += "<br>";
    result += mountpoint.path();
    result += "<br>";
  }

  return result;
};

static std::string RenderS3Info(const pb::mdsv2::S3Info& s3_info) {
  std::string result;
  if (!s3_info.endpoint().empty()) {
    result += fmt::format("{}", s3_info.endpoint());
    result += "<br>";
    result += fmt::format("{}", s3_info.bucketname());
  }

  return result;
}

static std::string RenderPartitionPolicy(pb::mdsv2::PartitionPolicy partition_policy) {
  std::string result;

  switch (partition_policy.type()) {
    case pb::mdsv2::PartitionType::MONOLITHIC_PARTITION: {
      const auto& mono = partition_policy.mono();
      result += fmt::format("epoch: {}", mono.epoch());
      result += "<br>";
      result += fmt::format("mds: {}", mono.mds_id());
    } break;

    case pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION: {
      const auto& parent_hash = partition_policy.parent_hash();
      result += fmt::format("epoch: {}", parent_hash.epoch());
      result += "<br>";
      result += fmt::format("bucket_num: {}", parent_hash.bucket_num());
      result += "<br>";
      result += "mds: ";
      for (const auto& [mds_id, _] : parent_hash.distributions()) {
        result += fmt::format("{},", mds_id);
      }
      result.resize(result.size() - 1);
    } break;

    default:
      result = "Unknown";
      break;
  }

  return result;
}

static std::string PartitionTypeName(pb::mdsv2::PartitionType partition_type) {
  switch (partition_type) {
    case pb::mdsv2::PartitionType::MONOLITHIC_PARTITION:
      return "MONOLITHIC";
    case pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION:
      return "PARENT_ID_HASH";
    default:
      return "Unknown";
  }
}

static std::string RenderFsInfo(const std::vector<pb::mdsv2::FsInfo>& fs_infoes) {
  auto render_size_func = [](const pb::mdsv2::FsInfo& fs_info) -> std::string {
    std::string result;
    result += "<div>";
    result += fmt::format("chunk size: {}", fs_info.chunk_size() / (1024 * 1024));
    result += "<br>";
    result += fmt::format("block size: {}", fs_info.block_size() / (1024 * 1024));
    result += "<br>";
    result += fmt::format("capacity: {}", fs_info.capacity() / (1024 * 1024));
    result += "</div>";
    return result;
  };

  auto render_time_func = [](const pb::mdsv2::FsInfo& fs_info) -> std::string {
    std::string result;
    result += "<div>";
    result += fmt::format("update time: {}", Helper::FormatTime(fs_info.last_update_time_ns() / 1000000000));
    result += "<br>";
    result += fmt::format("create time: {}", Helper::FormatTime(fs_info.create_time_s()));
    result += "</div>";
    return result;
  };

  auto render_trash_func = [](const pb::mdsv2::FsInfo& fs_info) -> std::string {
    std::string result;
    result += "<div>";
    result += fmt::format(R"(<a href="FsStatService/delfiles/{}" target="_blank">delfiles</a>)", fs_info.fs_id());
    result += "<br>";
    result += fmt::format(R"(<a href="FsStatService/delslices/{}" target="_blank">delslices</a>)", fs_info.fs_id());
    result += "</div>";
    return result;
  };

  butil::IOBufBuilder os;

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Name</th>";
  os << "<th>Type</th>";
  os << "<th>PartitionType</th>";
  os << "<th>PartitionPolicy</th>";
  os << "<th>Size(MB)</th>";
  os << "<th>Owner</th>";
  os << "<th>RecycleTime</th>";
  os << "<th>MountPoint</th>";
  os << "<th>S3</th>";
  os << "<th>Time</th>";
  os << "<th>Details</th>";
  os << "<th>Trash</th>";
  os << "</tr>";

  for (const auto& fs_info : fs_infoes) {
    const auto& partition_policy = fs_info.partition_policy();

    os << "<tr>";
    os << "<td>"
       << fmt::format(R"(<a href="FsStatService/{}" target="_blank">{}</a>)", fs_info.fs_id(), fs_info.fs_id())
       << "</td>";
    os << "<td>" << fs_info.fs_name() << "</td>";
    os << "<td>" << pb::mdsv2::FsType_Name(fs_info.fs_type()) << "</td>";
    os << "<td>" << PartitionTypeName(partition_policy.type()) << "</td>";
    os << "<td>" << RenderPartitionPolicy(partition_policy) << "</td>";
    os << "<td>" << render_size_func(fs_info) << "</td>";

    os << "<td>" << fs_info.owner() << "</td>";
    os << "<td>" << fs_info.recycle_time_hour() << "</td>";
    os << "<td>" << RenderMountpoint(fs_info) << "</td>";
    os << "<td>" << RenderS3Info(fs_info.extra().s3_info()) << "</td>";
    os << "<td>" << render_time_func(fs_info) << "</td>";
    os << "<td>" << fmt::format(R"(<a href="FsStatService/details/{}" target="_blank">details</a>)", fs_info.fs_id())
       << "</td>";
    os << "<td>" << render_trash_func(fs_info) << "</td>";
    os << "</tr>";
  }

  os << "</table>\n";
  os << "</div>";

  butil::IOBuf buf;
  os.move_to(buf);

  return buf.to_string();
}

static void RenderMainPage(const brpc::Server* server, FileSystemSetSPtr file_system_set, butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>\n";

  os << "<head>";
  os << RenderHead();
  os << "</head>";

  os << "<body>";
  server->PrintTabsBody(os, "fsstat");

  Context ctx;
  std::vector<pb::mdsv2::FsInfo> fs_infoes;
  file_system_set->GetAllFsInfo(ctx, fs_infoes);
  // sort by fs_id
  sort(fs_infoes.begin(), fs_infoes.end(),
       [](const pb::mdsv2::FsInfo& a, const pb::mdsv2::FsInfo& b) { return a.fs_id() < b.fs_id(); });

  os << RenderFsInfo(fs_infoes);

  os << "</body>";
}

static void RenderFsTreePage(FsUtils& fs_utils, uint32_t fs_id, butil::IOBufBuilder& os) {
  if (fs_id == 0) {
    os << "Invalid fs_id";
    return;
  }

  os << "<!DOCTYPE html>";
  os << "<html lang=\"zh-CN\">";

  os << "<head>";
  os << R"(
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>File System Directory Tree</title>
<style>
body {
  font-family: ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace;
}

.tree {
  margin-left: 20px;
}

.tree,
.tree ul {
  list-style-type: none;
  padding-left: 20px;
}

.tree li {
  position: relative;
  padding: 5px 0;
}

.tree li::before {
  content: "";
  position: absolute;
  top: 12px;
  left: -15px;
  width: 10px;
  height: 1px;
  background-color: #666;
}

.tree li::after {
  content: "";
  position: absolute;
  top: 0;
  left: -15px;
  width: 1px;
  height: 100%;
  background-color: #666;
}

.tree li:last-child::after {
  height: 12px;
}

.folder {
  cursor: pointer;
  font-weight: bold;
}

.file {
  color: #333;
}

.collapsed>ul {
  display: none;
}

.icon {
  margin-right: 5px;
}
</style>)";
  os << "</head>";

  os << "<body>";
  os << "<h1>File System Directory Tree</h1>";
  os << "<p style=\"color: gray;\">format: name [ino,version,mode,nlink,uid,gid,length,ctime,mtime,atime]</p>";
  os << R"(
<div class="controls">
  <button id="expandAll">Expand</button>
  <button id="collapseAll">Collapse</button>
</div>
<ul id="fileTree" class="tree"></ul>)";

  os << "<script>";

  os << "const fs_id = " << fs_id << ";";
  os << "const fileSystem =" + fs_utils.GenFsTreeJsonString(fs_id) + ";";

  os << R"(
    function generateTree(item, parentElement) {
      const li = document.createElement('li');

      if (item.type === 'directory') {
        const folderSpan = document.createElement('span');
        folderSpan.className = 'folder';
        folderSpan.innerHTML = `<div><span class="icon">📁</span>${item.name} [${item.ino},${item.description}]</div>`;
        folderSpan.addEventListener('click', function () {
          this.parentElement.classList.toggle('collapsed');
          if (this.parentElement.classList.contains('collapsed')) {
            this.querySelector('.icon').textContent = '📁';
          } else {
            this.querySelector('.icon').textContent = '📂';
          }
        });
        li.appendChild(folderSpan);

        if (item.children && item.children.length > 0) {
          const ul = document.createElement('ul');
          item.children.forEach(child => {
            generateTree(child, ul);
          });
          li.appendChild(ul);
        }
      } else {
        const fileSpan = document.createElement('span');
        fileSpan.className = 'file';
        fileSpan.innerHTML = `<div><span class="icon">📄</span><a href="${fs_id}/${item.ino}" target="_blank">${item.name}</a> [${item.ino},${item.description}]</div>`;
        li.appendChild(fileSpan);
      }

      parentElement.appendChild(li);
    }

    const treeRoot = document.getElementById('fileTree');
    generateTree(fileSystem, treeRoot);

    document.getElementById('expandAll').addEventListener('click', function () {
      const collapsedItems = document.querySelectorAll('.collapsed');
      collapsedItems.forEach(item => {
        item.classList.remove('collapsed');
        item.querySelector('.icon').textContent = '📂';
      });
    });

    document.getElementById('collapseAll').addEventListener('click', function () {
      const folders = document.querySelectorAll('.folder');
      folders.forEach(folder => {
        const li = folder.parentElement;
        if (!li.classList.contains('collapsed') && li.querySelector('ul')) {
          li.classList.add('collapsed');
          folder.querySelector('.icon').textContent = '📁';
        }
      });
    });
  )";
  os << "</script>";
  os << "</body>";
  os << "</html>";
}

void RenderJsonPage(const std::string& header, const std::string& json, butil::IOBufBuilder& os) {
  os << R"(
  <!DOCTYPE html>
<html lang="zh-CN">)";

  os << R"(
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Inode Details</title>
  <style>
    body {
      font-family: ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace;
      margin: 20px;
      background-color: #f5f5f5;
    }

    .container {
      max-width: 800px;
      margin: 0 auto;
      background-color: white;
      border-radius: 8px;
      box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
      padding: 20px;
    }

    h1 {
      text-align: center;
      color: #333;
    }

    pre {
      background-color: #f9f9f9;
      border: 1px solid #ddd;
      border-radius: 4px;
      padding: 15px;
      overflow: auto;
      font-family: monospace;
      white-space: pre-wrap;
      line-height: 1.5;
    }

    .string {
      color: #008000;
    }

    .number {
      color: #0000ff;
    }

    .boolean {
      color: #b22222;
    }

    .null {
      color: #808080;
    }

    .key {
      color: #a52a2a;
    }
  </style>
</head>)";

  os << "<body>";
  os << R"(<div class="container">)";
  os << fmt::format("<h1>{}</h1>", header);
  os << R"(<pre id="json-display"></pre>)";
  os << "</div>";

  os << "<script>";
  if (!json.empty()) {
    os << "const jsonString =`" + json + "`;";
  } else {
    os << "const jsonString = \"{}\";";
  }

  os << R"(
    function syntaxHighlight(json) {
      if (typeof json === 'string') {
        json = JSON.parse(json);
      }

      json = JSON.stringify(json, null, 4);

      json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

      return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        let cls = 'number';

        if (/^"/.test(match)) {
          if (/:$/.test(match)) {
            cls = 'key';
          } else {
            cls = 'string';
          }
        } else if (/true|false/.test(match)) {
          cls = 'boolean';
        } else if (/null/.test(match)) {
          cls = 'null';
        }

        return '<span class="' + cls + '">' + match + '</span>';
      });
    }

    document.addEventListener('DOMContentLoaded', function () {
      try {
        const highlighted = syntaxHighlight(jsonString);
        document.getElementById('json-display').innerHTML = highlighted;
      } catch (e) {
        document.getElementById('json-display').innerHTML = 'Invalid JSON: ' + e.message;
      }
    });)";
  os << "</script>";

  os << "</body>";
  os << "</html>";
}

void RenderFsDetailsPage(const FsInfoType& fs_info, butil::IOBufBuilder& os) {
  std::string header = fmt::format("File System {}({})", fs_info.fs_name(), fs_info.fs_id());
  std::string json;
  Helper::ProtoToJson(fs_info, json);
  RenderJsonPage(header, json, os);
}

void RenderDelfilePage(FileSystemSPtr filesystem, butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead() << "</head>";
  os << "<body>";

  std::vector<AttrType> delfiles;
  auto status = filesystem->GetDelFiles(delfiles);
  if (!status.ok()) {
    os << "Get delfiles fail: " << status.error_str();
    return;
  }

  os << R"(<div style="margin: 12px;font-size:smaller">)";
  os << R"(<table class="gridtable sortable" border=1>)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>Length(byte)</th>";
  os << "<th>Ctime</th>";
  os << "<th>Version</th>";
  os << "</tr>";

  for (const auto& delfile : delfiles) {
    os << "<tr>";

    std::string url = fmt::format("/FsStatService/delfiles/{}/{}", delfile.fs_id(), delfile.ino());
    os << "<td><a href=\"" << url << R"(" target="_blank">)" << delfile.ino() << "</a></td>";
    os << "<td>" << delfile.length() << "</td>";
    os << "<td>" << Helper::FormatTime(delfile.ctime() / 1000000000) << "</td>";
    os << "<td>" << delfile.version() << "</td>";

    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";
  os << "</body>";
}

void RenderDelslicePage(FileSystemSPtr filesystem, butil::IOBufBuilder& os) {
  auto render_range_func = [](const pb::mdsv2::TrashSlice& slice) -> std::string {
    std::string result;
    for (size_t i = 0; i < slice.ranges_size(); ++i) {
      const auto& range = slice.ranges().at(i);
      if (i + 1 < slice.ranges_size()) {
        result += fmt::format("[{},{}),", range.offset(), range.offset() + range.len());
      } else {
        result += fmt::format("[{},{})", range.offset(), range.offset() + range.len());
      }

      result += "<br>";
    }
    return result;
  };

  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead() << "</head>";
  os << "<body>";

  std::vector<TrashSliceList> delslices;
  auto status = filesystem->GetDelSlices(delslices);
  if (!status.ok()) {
    os << "Get delslice fail: " << status.error_str();
    return;
  }

  os << R"(<div style="margin: 12px;font-size:smaller">)";
  os << R"(<table class="gridtable sortable" border=1>)";
  os << "<tr>";
  os << "<th>FsId</th>";
  os << "<th>Ino</th>";
  os << "<th>ChunkIndex</th>";
  os << "<th>SliceId</th>";
  os << "<th>IsPartial</th>";
  os << "<th>Ranges</th>";
  os << "</tr>";

  for (const auto& delslice : delslices) {
    for (const auto& slice : delslice.slices()) {
      os << "<tr>";
      os << "<td>" << slice.fs_id() << "</td>";
      os << "<td>" << slice.ino() << "</td>";
      os << "<td>" << slice.chunk_index() << "</td>";
      os << "<td>" << slice.slice_id() << "</td>";
      os << "<td>" << (slice.is_partial() ? "true" : "false") << "</td>";
      os << "<td>" << render_range_func(slice) << "</td>";
      os << "</tr>";
    }
  }

  os << "</table>";
  os << "</div>";
  os << "</body>";
}

void RenderInodePage(const AttrType& attr, butil::IOBufBuilder& os) {
  std::string header = fmt::format("Inode: {}", attr.ino());
  std::string json;
  Helper::ProtoToJson(attr, json);
  RenderJsonPage(header, json, os);
}

void FsStatServiceImpl::default_method(::google::protobuf::RpcController* controller, const pb::web::FsStatRequest*,
                                       pb::web::FsStatResponse*, ::google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  const brpc::Server* server = cntl->server();
  butil::IOBufBuilder os;
  const bool use_html = brpc::UseHTML(cntl->http_request());
  cntl->http_response().set_content_type(use_html ? "text/html" : "text/plain");
  const std::string& path = cntl->http_request().unresolved_path();

  DINGO_LOG(INFO) << fmt::format("FsStatService path: {}", path);

  std::vector<std::string> params;
  Helper::SplitString(path, '/', params);

  // /FsStatService
  if (params.empty()) {
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    RenderMainPage(server, file_system_set, os);

  } else if (params.size() == 1) {
    // /FsStatService/{fs_id}
    uint32_t fs_id = Helper::StringToInt32(params[0]);
    FsUtils fs_utils(Server::GetInstance().GetKVStorage());
    RenderFsTreePage(fs_utils, fs_id, os);

  } else if (params.size() == 2 && params[0] == "details") {
    // /FsStatService/details/{fs_id}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      RenderFsDetailsPage(file_system->GetFsInfo(), os);

    } else {
      os << fmt::format("Not found file system {}.", fs_id);
    }

  } else if (params.size() == 2 && params[0] == "delfiles") {
    // /FsStatService/delfiles/{fs_id}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      RenderDelfilePage(file_system, os);

    } else {
      os << fmt::format("Not found file system {}.", fs_id);
    }

  } else if (params.size() == 3 && params[0] == "delfiles") {
    // /FsStatService/delfiles/{fs_id}/{ino}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    uint64_t ino = Helper::StringToInt64(params[2]);

    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      AttrType attr;
      auto status = file_system->GetDelFileFromStore(ino, attr);
      if (status.ok()) {
        RenderInodePage(attr, os);

      } else {
        os << fmt::format("Get inode({}) fail, {}.", ino, status.error_str());
      }

    } else {
      os << fmt::format("Not found file system {}.", fs_id);
    }

  } else if (params.size() == 2 && params[0] == "delslices") {
    // /FsStatService/delslices/{fs_id}
    uint32_t fs_id = Helper::StringToInt32(params[1]);
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      RenderDelslicePage(file_system, os);

    } else {
      os << fmt::format("Not found file system {}.", fs_id);
    }

  } else if (params.size() == 2) {
    // /FsStatService/{fs_id}/{ino}

    uint32_t fs_id = Helper::StringToInt32(params[0]);
    uint64_t ino = Helper::StringToInt64(params[1]);

    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      InodeSPtr inode;
      auto status = file_system->GetInodeFromStore(ino, "Stat", inode);
      if (status.ok()) {
        RenderInodePage(inode->CopyTo(), os);

      } else {
        os << fmt::format("Get inode({}) fail, {}.", ino, status.error_str());
      }
    } else {
      os << fmt::format("Not found file system {}", fs_id);
    }

  } else {
    os << fmt::format("Unknown url {}", path);
  }

  os.move_to(cntl->response_attachment());
  cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

void FsStatServiceImpl::GetTabInfo(brpc::TabInfoList* tab_list) const {
  brpc::TabInfo* tab = tab_list->add();
  tab->tab_name = "fs";
  tab->path = "/FsStatService";
}

}  // namespace mdsv2
}  // namespace dingofs
