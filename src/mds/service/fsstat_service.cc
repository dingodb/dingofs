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

#include "mds/service/fsstat_service.h"

#include <sys/types.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "butil/iobuf.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "json/writer.h"
#include "mds/common/context.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/type.h"
#include "mds/common/version.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/fs_utils.h"
#include "mds/server.h"
#include "mds/storage/dingodb_storage.h"
#include "utils/string.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_heartbeat_mds_offline_period_time_ms);
DECLARE_uint32(mds_heartbeat_client_offline_period_ms);
DECLARE_uint32(cache_member_heartbeat_offline_timeout_s);
DECLARE_uint32(cache_member_heartbeat_miss_timeout_s);

static std::string RenderHead(const std::string& title) {
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
  a {
    text-decoration: none;
  }
</style>)";

  os << fmt::format(R"(<title>{}</title>)", title);
  os << "</head>";

  butil::IOBuf buf;
  os.move_to(buf);

  return buf.to_string();
}

static std::string RenderStorageInfo(const pb::mds::FsExtra& fs_extra) {
  std::string result;

  const auto& s3_info = fs_extra.s3_info();
  const auto& rados_info = fs_extra.rados_info();

  if (!s3_info.endpoint().empty()) {
    result += "s3";
    result += "<br>";
    result += fmt::format("endpoint: {}", s3_info.endpoint());
    result += "<br>";
    result += fmt::format("bucket: {}", s3_info.bucketname());

  } else if (!rados_info.mon_host().empty()) {
    result += "rados";
    result += "<br>";
    result += fmt::format("host: {}", rados_info.mon_host());
    result += "<br>";
    result += fmt::format("pool: {}", rados_info.pool_name());
    result += "<br>";
    result += fmt::format("user: {}", rados_info.user_name());
    result += "<br>";
    result += fmt::format("cluster: {}", rados_info.cluster_name());
  }

  return result;
}

static std::string RenderPartitionPolicy(pb::mds::PartitionPolicy partition_policy) {
  std::string result;

  switch (partition_policy.type()) {
    case pb::mds::PartitionType::MONOLITHIC_PARTITION: {
      const auto& mono = partition_policy.mono();
      result += fmt::format("epoch: {}", partition_policy.epoch());
      result += "<br>";
      result += fmt::format("mds: {}", mono.mds_id());
    } break;

    case pb::mds::PartitionType::PARENT_ID_HASH_PARTITION: {
      const auto& parent_hash = partition_policy.parent_hash();
      result += fmt::format("epoch: {}", partition_policy.epoch());
      result += "<br>";
      result += fmt::format("bucket_num: {}", parent_hash.bucket_num());
      result += "<br>";
      result += fmt::format("expect_mds_num: {}", parent_hash.expect_mds_num());
      result += "<br>";
      result += "mds: ";

      std::map<uint64_t, BucketSetEntry> sorted_distributions;
      for (const auto& [mds_id, bucket_set] : parent_hash.distributions()) {
        sorted_distributions[mds_id] = bucket_set;
      }
      for (const auto& [mds_id, bucket_set] : sorted_distributions) {
        result += fmt::format("{}[{}],", mds_id, bucket_set.bucket_ids_size());
      }
      result.resize(result.size() - 1);
    } break;

    default:
      result = "Unknown";
      break;
  }

  return result;
}

static std::string PartitionTypeName(pb::mds::PartitionType partition_type) {
  switch (partition_type) {
    case pb::mds::PartitionType::MONOLITHIC_PARTITION:
      return "MONOLITHIC";
    case pb::mds::PartitionType::PARENT_ID_HASH_PARTITION:
      return "PARENT_ID_HASH";
    default:
      return "Unknown";
  }
}

static void RenderFsInfo(const std::vector<pb::mds::FsInfo>& fs_infoes, butil::IOBufBuilder& os) {
  auto reader_state_fn = [](const pb::mds::FsStatus& status) -> std::string {
    auto name = pb::mds::FsStatus_Name(status);
    switch (status) {
      case pb::mds::FsStatus::INIT:
      case pb::mds::FsStatus::NORMAL:
        return fmt::format(R"(<span style="color:green">{}</span>)", name);
      case pb::mds::FsStatus::DELETED:
      case pb::mds::FsStatus::RECYCLING:
        return fmt::format(R"(<span style="color:red">{}</span>)", name);
      default:
        return fmt::format(R"(<span style="color:blue">{}</span>)", name);
    }
  };
  auto render_size_func = [](const pb::mds::FsInfo& fs_info) -> std::string {
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

  auto render_time_func = [](const pb::mds::FsInfo& fs_info) -> std::string {
    std::string result;
    result += "<div>";
    result += "update time:";
    result += "<br>";
    result += fmt::format("<span>{}</span>", Helper::FormatTime(fs_info.last_update_time_ns() / 1000000000));
    result += "<br>";
    result += "create time:";
    result += "<br>";
    result += fmt::format("<span>{}</span>", Helper::FormatTime(fs_info.create_time_s()));
    result += "</div>";
    return result;
  };

  auto render_navigation_func = [](const pb::mds::FsInfo& fs_info) -> std::string {
    std::string result;
    result += "<div>";
    result += fmt::format(R"(<a href="FsStatService/details/{}" target="_blank">details</a>)", fs_info.fs_name());
    result += "<br>";
    result +=
        fmt::format(R"(<a href="FsStatService/filesession/{}" target="_blank">file session</a>)", fs_info.fs_id());
    result += "<br>";
    result += fmt::format(R"(<a href="FsStatService/quota/{}" target="_blank">quota</a>)", fs_info.fs_id());
    result += "<br>";
    result += fmt::format(R"(<a href="FsStatService/delfiles/{}" target="_blank">delfiles</a>)", fs_info.fs_id());
    result += "<br>";
    result += fmt::format(R"(<a href="FsStatService/delslices/{}" target="_blank">delslices</a>)", fs_info.fs_id());
    result += "<br>";
    result += fmt::format(R"(<a href="FsStatService/oplog/{}" target="_blank">oplog</a>)", fs_info.fs_id());
    result += "</div>";
    return result;
  };

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>FS [{}]</h3>)", fs_infoes.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Name</th>";
  os << "<th>State</th>";
  os << "<th>Type</th>";
  os << "<th>PartitionType</th>";
  os << "<th>PartitionPolicy</th>";
  os << "<th>Size(MB)</th>";
  os << "<th>Owner</th>";
  os << "<th>Navigation</th>";
  os << "<th>Time</th>";
  os << "<th>RecycleTime</th>";
  os << "<th>MountPoint</th>";
  os << "<th>Storage</th>";
  os << "<th>UUID</th>";
  os << "</tr>";

  for (const auto& fs_info : fs_infoes) {
    const auto& partition_policy = fs_info.partition_policy();

    os << "<tr>";
    os << "<td>"
       << fmt::format(R"(<a href="FsStatService/{}" target="_blank">{}</a>[v{}])", fs_info.fs_id(), fs_info.fs_id(),
                      fs_info.version())
       << "</td>";
    os << "<td>" << fs_info.fs_name() << "</td>";
    os << "<td>" << reader_state_fn(fs_info.status()) << "</td>";
    os << "<td>" << pb::mds::FsType_Name(fs_info.fs_type()) << "</td>";
    os << "<td>" << PartitionTypeName(partition_policy.type()) << "</td>";
    os << "<td>" << RenderPartitionPolicy(partition_policy) << "</td>";
    os << "<td>" << render_size_func(fs_info) << "</td>";

    os << "<td>" << fs_info.owner() << "</td>";
    os << "<td>" << render_navigation_func(fs_info) << "</td>";
    os << "<td>" << render_time_func(fs_info) << "</td>";
    os << "<td>" << fmt::format("{}hour", fs_info.recycle_time_hour()) << "</td>";
    os << fmt::format(R"(<td><a href="FsStatService/mountpoint/{}" target="_blank">Goto</a>&nbsp;[{}]</td>)",
                      fs_info.fs_name(), fs_info.mount_points_size());

    os << "<td>" << RenderStorageInfo(fs_info.extra()) << "</td>";
    os << "<td>" << fs_info.uuid() << "</td>";
    os << "</tr>";
  }

  os << "</table>\n";
  os << "</div>";
}

static void RenderMdsList(const std::vector<MdsEntry>& mdses, butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>MDS [{}]</h3>)", mdses.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Addr</th>";
  os << "<th>State</th>";
  os << "<th>Last Online Time</th>";
  os << "<th>Online</th>";
  os << "<th>Details</th>";
  os << "</tr>";

  uint64_t now_ms = Helper::TimestampMs();

  for (const auto& mds : mdses) {
    os << "<tr>";
    os << "<td>" << mds.id() << "</td>";
    os << fmt::format(R"(<td><a href="http://{}:{}/FsStatService" target="_blank">{}:{} </a></td>)",
                      mds.location().host(), mds.location().port(), mds.location().host(), mds.location().port());
    os << "<td>" << MdsEntry::State_Name(mds.state()) << "</td>";
    os << "<td>" << Helper::FormatMsTime(mds.last_online_time_ms()) << "</td>";
    if (mds.last_online_time_ms() + FLAGS_mds_heartbeat_mds_offline_period_time_ms < now_ms) {
      os << "<td style=\"color:red\">NO</td>";
    } else {
      os << R"(<td style="color:green">YES</td>)";
    }

    os << fmt::format(R"(<td><a href="http://{}:{}/FsStatService/server" target="_blank">details</a></td>)",
                      mds.location().host(), mds.location().port());

    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";
}

static void RenderClientList(const std::vector<ClientEntry>& clients, butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Client [{}]</h3>)", clients.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Host</th>";
  os << "<th>MountPoint</th>";
  os << "<th>FsName</th>";
  os << "<th>Last Online Time</th>";
  os << "<th>Online</th>";
  os << "</tr>";

  uint64_t now_ms = Helper::TimestampMs();

  for (const auto& client : clients) {
    os << "<tr>";
    os << "<td>" << client.id() << "</td>";
    os << fmt::format(R"(<td><a href="http://{}:{}/FuseStatService" target="_blank">{}:{} </a></td>)", client.ip(),
                      client.port(), client.hostname(), client.port());
    os << "<td>" << client.mountpoint() << "</td>";
    os << "<td>" << client.fs_name() << "</td>";
    os << "<td>" << Helper::FormatMsTime(client.last_online_time_ms()) << "</td>";
    if (client.last_online_time_ms() + FLAGS_mds_heartbeat_client_offline_period_ms < now_ms) {
      os << R"(<td style="color:red">NO</td>)";
    } else {
      os << R"(<td style="color:green">YES</td>)";
    }

    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";
}

static void RenderCacheMemberList(const std::vector<CacheMemberEntry>& cache_members, butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Cache [{}]</h3>)", cache_members.size());
  os << "<td>" << fmt::format("Available UUID: {}", utils::GenUuid()) << "</td>";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Member_ID</th>";
  os << "<th>Host</th>";
  os << "<th>Group</th>";
  os << "<th>Weight</th>";
  os << "<th>Locked</th>";
  os << "<th>Last Online Time</th>";
  os << "<th>State</th>";
  os << "</tr>";

  uint64_t now_ms = Helper::TimestampMs();

  for (const auto& member : cache_members) {
    os << "<tr>";
    os << "<td>" << member.member_id() << "</td>";
    os << "<td>" << fmt::format("{}:{}", member.ip(), member.port()) << "</td>";
    os << "<td>" << member.group_name() << "</td>";
    os << "<td>" << member.weight() << "</td>";
    if (member.locked()) {
      os << "<td>YES</td>";
    } else {
      os << "<td>NO</td>";
    }
    os << "<td>" << Helper::FormatMsTime(member.last_online_time_ms()) << "</td>";
    if (member.last_online_time_ms() == 0) {
      os << R"(<td>UNKNOW</td>)";
    } else if (member.last_online_time_ms() + FLAGS_cache_member_heartbeat_offline_timeout_s * 1000 < now_ms) {
      os << R"(<td style="color:red">OFFLINE</td>)";
    } else if (member.last_online_time_ms() + FLAGS_cache_member_heartbeat_miss_timeout_s * 1000 < now_ms) {
      os << R"(<td style="color:orange">UNSTABLE</td>)";
    } else {
      os << R"(<td style="color:green">ONLINE</td>)";
    }

    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";
}

static void RenderDistributedLock(const std::vector<StoreDistributionLock::LockEntry>& lock_entries,
                                  butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller;">)";
  os << R"(<h3>Distributed Lock</h3>)";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Name</th>";
  os << "<th>Owner</th>";
  os << "<th>Epoch</th>";
  os << "<th>Expired Time</th>";
  os << "</tr>";

  for (const auto& lock_entry : lock_entries) {
    os << "<tr>";
    os << "<td>" << lock_entry.name << "</td>";
    os << "<td>" << lock_entry.owner << "</td>";
    os << "<td>" << lock_entry.epoch << "</td>";
    os << "<td>" << Helper::FormatMsTime(lock_entry.expire_time_ms) << "</td>";
    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";
}

void FsStatServiceImpl::RenderAutoIncrementIdGenerator(FileSystemSetSPtr file_system_set, butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller">)";
  os << R"(<h3>Autoincrement ID Generator</h3>)";
  os << R"(<div style="font-size:smaller;">)";

  os << file_system_set->GetFsIdGenerator().Describe();
  os << "<br>";
  os << file_system_set->GetSliceIdGenerator().Describe();
  os << "<br>";

  auto fses = file_system_set->GetAllFileSystem();

  for (const auto& fs : fses) {
    os << fs->GetInoIdGenerator().Describe();
    os << "<br>";
  }

  os << R"(</div>)";
  os << R"(</div>)";
}

static void RenderGitInfo(butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller">)";
  os << R"(<h3>Git</h3>)";
  os << R"(<div style="font-size:smaller;">)";

  auto infos = DingoVersion();
  for (const auto& info : infos) {
    os << fmt::format("{}: {}", info.first, info.second);
    os << "<br>";
  }

  auto dingo_sdk_infos = DingodbStorage::GetSdkVersion();
  for (const auto& info : dingo_sdk_infos) {
    os << fmt::format("DINGO_SDK_{}: {}", info.first, info.second);
    os << "<br>";
  }

  os << R"(</div>)";
  os << R"(</div>)";
}

static void RenderJsonPage(const std::string& title, const std::string& header, const std::string& json,
                           butil::IOBufBuilder& os) {
  os << R"(<!DOCTYPE html><html lang="zh-CN">)";

  os << R"(<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  )";
  os << fmt::format(R"(<title>{}</title>)", title);
  os << R"(
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

static void RenderGitVersion(butil::IOBufBuilder& os) {
  os << R"(<div style="margin:2px;font-size:smaller;text-align:center">)";
  os << fmt::format(R"(<p >{} {} {}</p>)", GetGitVersion(), GetGitCommitHash(), GetGitCommitTime());
  os << "</div>";
}

static void RenderCoordinatorAddr(const std::string& addrs, butil::IOBufBuilder& os) {
  os << R"(<div style="margin:8px;font-size:smaller;text-align:center">)";
  os << R"(<h3>Coordinator: )";

  std::vector<std::string> addr_vec;
  Helper::SplitString(addrs, ',', addr_vec);

  for (const auto& addr : addr_vec) {
    os << fmt::format(R"(<a href="http://{}" target="_blank">{}</a>&nbsp)", addr, addr);
  }

  os << "</h3>";
  os << "</div>";
}

void FsStatServiceImpl::RenderMainPage(const brpc::Server* server, FileSystemSetSPtr file_system_set,
                                       butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>\n";

  os << "<head>";
  os << RenderHead("dingofs-mds dashboard");
  os << "</head>";

  os << "<body>";
  server->PrintTabsBody(os, "fsstat");
  os << R"(<h1 style="text-align:center;">dingofs-mds dashboard</h1>)";

  // git version
  RenderGitVersion(os);

  // coordinator addr
  std::string coor_addr = Server::GetInstance().GetCoordinatorClient()->GetAddr();
  RenderCoordinatorAddr(coor_addr, os);

  // fs stats
  Context ctx;
  std::vector<pb::mds::FsInfo> fs_infoes;
  auto status = file_system_set->GetAllFsInfo(ctx, true, fs_infoes);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[mdsstat] get fs list fail, error({}).", status.error_str());
    os << fmt::format(R"(<div style="color:red;">get fs list fail, error({}).</div>)", status.error_str());

  } else {
    std::sort(fs_infoes.begin(), fs_infoes.end(),  // NOLINT
              [](const pb::mds::FsInfo& a, const pb::mds::FsInfo& b) { return a.fs_id() < b.fs_id(); });

    RenderFsInfo(fs_infoes, os);
  }

  // mds stats
  auto heartbeat = Server::GetInstance().GetHeartbeat();
  std::vector<MdsEntry> mdses;
  status = heartbeat->GetMDSList(ctx, mdses);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[mdsstat] get mds list fail, error({}).", status.error_str());
    os << fmt::format(R"(<div style="color:red;">get mds list fail, error({}).</div>)", status.error_str());

  } else {
    std::sort(mdses.begin(), mdses.end(),  // NOLINT
              [](const MdsEntry& a, const MdsEntry& b) { return a.id() < b.id(); });
    RenderMdsList(mdses, os);
  }

  // cache member stats
  std::vector<CacheMemberEntry> cache_members;
  status = heartbeat->GetCacheMemberList(cache_members);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[mdsstat] get cache member list fail, error({}).", status.error_str());
    os << fmt::format(R"(<div style="color:red;">get cache member list fail, error({}).</div>)", status.error_str());

  } else {
    // sort by last_online_time
    std::sort(cache_members.begin(), cache_members.end(),  // NOLINT
              [](const CacheMemberEntry& a, const CacheMemberEntry& b) {
                butil::EndPoint endpoint_a, endpoint_b;
                if (butil::str2endpoint(a.ip().c_str(), a.port(), &endpoint_a) != 0) {
                  return false;
                }
                if (butil::str2endpoint(b.ip().c_str(), b.port(), &endpoint_b) != 0) {
                  return false;
                }
                return endpoint_a > endpoint_b;
              });

    RenderCacheMemberList(cache_members, os);
  }

  // client stats
  std::vector<ClientEntry> clients;
  status = heartbeat->GetClientList(clients);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[mdsstat] get client list fail, error({}).", status.error_str());
    os << fmt::format(R"(<div style="color:red;">get client list fail, error({}).</div>)", status.error_str());

  } else {
    // sort by last_online_time
    std::sort(  // NOLINT
        clients.begin(), clients.end(),
        [](const ClientEntry& a, const ClientEntry& b) { return a.last_online_time_ms() > b.last_online_time_ms(); });

    RenderClientList(clients, os);
  }

  // distribution lock
  std::vector<StoreDistributionLock::LockEntry> lock_entries;
  status = StoreDistributionLock::GetAllLockInfo(Server::GetInstance().GetOperationProcessor(), lock_entries);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[mdsstat] get distributed lock info fail, error({}).", status.error_str());
    os << fmt::format(R"(<div style="color:red;">get distributed lock info fail, error({}).</div>)",
                      status.error_str());

  } else {
    RenderDistributedLock(lock_entries, os);
  }

  // auto increment id generator
  RenderAutoIncrementIdGenerator(file_system_set, os);

  // git info
  RenderGitInfo(os);

  os << "</body>";
  os << "</html>";
}

void FsStatServiceImpl::RenderServerPage(butil::IOBufBuilder& os) {
  auto& server = Server::GetInstance();

  const auto& self_mds_meta = server.GetMDSMeta();

  Json::Value root(Json::objectValue);
  server.DescribeByJson(root);

  Json::StreamWriterBuilder writer;
  std::string header = fmt::format("MDS Server({})", self_mds_meta.ID());

  RenderJsonPage("dingofs mds details", header, Json::writeString(writer, root), os);
}

static void RenderQuotaPage(FileSystemSPtr fs, butil::IOBufBuilder& os) {
  auto render_bytes_func = [](int64_t bytes, const std::string& unit) -> std::string {
    if (bytes == INT64_MAX) {
      return "unlimited";
    }
    if (unit == "GB") {
      return fmt::format("{:.2f}", static_cast<double>(bytes) / (1024 * 1024 * 1024));
    } else if (unit == "MB") {
      return fmt::format("{:.2f}", static_cast<double>(bytes) / (1024 * 1024));
    } else if (unit == "KB") {
      return fmt::format("{:.2f}", static_cast<double>(bytes) / 1024);
    } else {
      return fmt::format("{:.2f}", static_cast<double>(bytes));
    }
  };

  auto render_inode_func = [](int64_t inodes) -> std::string {
    if (inodes == INT64_MAX) {
      return "unlimited";
    }
    return fmt::format("{}", inodes);
  };

  os << "<!DOCTYPE html><html>\n";

  os << "<head>";
  os << RenderHead("dinfofs quota");
  os << "</head>";

  os << "<body>";
  os << R"(<h1 style="text-align:center;">Quota</h1>)";
  auto& quota_manager = fs->GetQuotaManager();

  // fs quota
  Trace trace;
  QuotaEntry quota;
  auto status = quota_manager.GetFsQuota(trace, false, quota);

  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller;">)";
  os << R"(<h3>FS Quota</h3>)";
  if (status.ok()) {
    os << "<div>";
    os << fmt::format(R"(Max Bytes: {} GB)", render_bytes_func(quota.max_bytes(), "GB"));
    os << "<br>";
    os << fmt::format(R"(Used Bytes: {} KB)", render_bytes_func(quota.used_bytes(), "KB"));
    os << "<br>";
    os << fmt::format(R"(Max Inode: {})", render_inode_func(quota.max_inodes()));
    os << "<br>";
    os << fmt::format(R"(Used Inode: {})", render_inode_func(quota.used_inodes()));
    os << "</div>";
  } else {
    os << fmt::format(R"(<span class="red-text">Get fs quota fail, status({}).</span>)", status.error_str());
  }

  os << "</div>";

  // dir quota
  std::map<Ino, QuotaEntry> dir_quota_entry_map;
  status = quota_manager.LoadDirQuotas(trace, dir_quota_entry_map);

  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Dir Quota [{}]</h3>)", dir_quota_entry_map.size());
  if (status.ok()) {
    os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
    os << "<tr>";
    os << "<th>Ino</th>";
    os << "<th>MaxBytes(GB)</th>";
    os << "<th>MaxInodes</th>";
    os << "<th>UsedBytes(KB)</th>";
    os << "<th>UsedInodes</th>";
    os << "</tr>";

    for (const auto& [ino, quota_entry] : dir_quota_entry_map) {
      os << "<tr>";

      os << fmt::format(R"(<td>{}</td>)", ino);
      os << fmt::format(R"(<td>{}</td>)", render_bytes_func(quota_entry.max_bytes(), "GB"));
      os << fmt::format(R"(<td>{}</td>)", render_inode_func(quota_entry.max_inodes()));
      os << fmt::format(R"(<td>{}</td>)", render_bytes_func(quota_entry.used_bytes(), "KB"));
      os << fmt::format(R"(<td>{}</td>)", render_inode_func(quota_entry.used_inodes()));

      os << "</tr>";
    }

    os << "</table>";
  } else {
    os << fmt::format(R"(<span class="red-text">Load dir quota fail, status({}).</span>)", status.error_str());
  }
  os << "</div>";

  os << "</body>";
  os << "</html>";
}

static void RenderFsTreePage(const FsInfoEntry& fs_info, butil::IOBufBuilder& os) {
  const auto fs_id = fs_info.fs_id();
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
<title>dingofs tree</title>
<style>
body {
  font-family: ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace;
  margin: 20px;
}

.tree {
  margin-left: 20px;
}

.tree ul {
  list-style-type: none;
  padding-left: 20px;
  margin: 0;
}

.tree li {
  position: relative;
  padding: 5px 0;
  margin: 0;
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

.folder-name {
  cursor: pointer;
  font-weight: bold;
  color: #0066cc;
  user-select: none;
}

.folder-name:hover {
  color: #004499;
  background-color: #f0f8ff;
  padding: 2px 4px;
  border-radius: 3px;
}

.file {
  color: #333;
}

.file a {
  color: #0066cc;
  text-decoration: none;
}

.file a:hover {
  text-decoration: underline;
}

.icon {
  margin-right: 5px;
  font-size: 14px;
}

.loading {
  color: #666;
  font-style: italic;
  margin-left: 20px;
}

.error {
  color: #cc0000;
  font-style: italic;
  margin-left: 20px;
}

.expanded > .folder > .icon:before {
  content: "📂";
}

.collapsed > .folder > .icon:before,
.folder > .icon:before {
  content: "📁";
}

.children {
  display: none;
}

.expanded > .children {
  display: block;
}

.info {
  color: #666;
  font-size: 12px;
  margin-left: 4px;
}

.controls {
  margin-bottom: 20px;
}

.controls button {
  margin-right: 10px;
  padding: 8px 16px;
  background-color: #0066cc;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
}

.controls button:hover {
  background-color: #004499;
}
</style>)";
  os << "</head>";

  os << "<body>";
  os << R"(<h1 style="text-align:center;">FileSystem Directory Tree</h1>)";
  os << fmt::format(R"(<h3>{}({})</h3>)", fs_info.fs_name(), fs_id);
  os << "<p style=\"color: gray;\">format: name "
        "[ino,version,mode,nlink,uid,gid,length,ctime,mtime,atime][mds]</p>";

  os << R"(
<div class="controls">
  <button id="collapseAll">Collapse All</button>
  <button id="refreshRoot">Refresh</button>
</div>
<ul id="fileTree" class="tree"></ul>)";

  os << "<script>";

  os << "const fs_id = " << fs_id << ";";

  os << R"(
    // API endpoints
    const API_BASE = '/FsStatService/partition';
    
    // Cache for loaded directories
    const directoryCache = new Map();
    
    // Get children of a directory
    async function getDirectoryChildren(parentIno) {
      const cacheKey = `${fs_id}_${parentIno}`;
      
      if (directoryCache.has(cacheKey)) {
        return directoryCache.get(cacheKey);
      }
      
      try {
        const response = await fetch(`${API_BASE}/${fs_id}/${parentIno}`);
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        directoryCache.set(cacheKey, data);
        return data;
      } catch (error) {
        console.error('Failed to fetch directory children:', error);
        throw error;
      }
    }
    
    // Create tree node element
    function createTreeNode(item) {
      const li = document.createElement('li');
      li.dataset.ino = item.ino;
      li.dataset.type = item.type;
      
      if (item.type === 'directory') {
        li.className = 'collapsed';
        
        const folderDiv = document.createElement('div');
        folderDiv.className = 'folder';
        
        const icon = document.createElement('span');
        icon.className = 'icon';
        
        const folderName = document.createElement('span');
        folderName.className = 'folder-name';
        folderName.textContent = item.name;
        
        const info = document.createElement('span');
        info.className = 'info';
        info.textContent = `[${item.ino},${item.description}][${item.node}]`;
        
        folderDiv.appendChild(icon);
        folderDiv.appendChild(folderName);
        folderDiv.appendChild(info);
        
        // 只给目录名添加点击事件
        folderName.addEventListener('click', async function(e) {
          e.preventDefault();
          e.stopPropagation();
          
          const listItem = this.closest('li');
          
          if (listItem.classList.contains('expanded')) {
            // Collapse
            listItem.classList.remove('expanded');
            listItem.classList.add('collapsed');
          } else {
            // Expand
            await expandDirectory(listItem, item.ino);
          }
        });
        
        li.appendChild(folderDiv);
        
        // Children container
        const childrenUl = document.createElement('ul');
        childrenUl.className = 'children';
        li.appendChild(childrenUl);
        
      } else {
        const fileDiv = document.createElement('div');
        fileDiv.className = 'file';
        
        const icon = document.createElement('span');
        icon.className = 'icon';
        icon.textContent = '📄';
        
        const link = document.createElement('a');
        link.href = `${fs_id}/${item.ino}`;
        link.target = '_blank';
        link.textContent = item.name;

        const chunk_link = document.createElement('a');
        chunk_link.href = `chunk/${fs_id}/${item.ino}`;
        chunk_link.target = '_blank';
        chunk_link.textContent = 'chunk';
        
        const info = document.createElement('span');
        info.className = 'info';
        info.textContent = `[${item.ino},${item.description}]`;
        
        fileDiv.appendChild(icon);
        fileDiv.appendChild(link);
        fileDiv.appendChild(info);
        fileDiv.appendChild(chunk_link);
        
        li.appendChild(fileDiv);
      }
      
      return li;
    }
    
    // Expand directory and load children
    async function expandDirectory(listItem, ino) {
      const childrenContainer = listItem.querySelector('.children');
      
      // 如果已经加载过子项，直接展开
      if (childrenContainer.children.length > 0) {
        listItem.classList.remove('collapsed');
        listItem.classList.add('expanded');
        return;
      }
      
      // Show loading indicator
      const loadingDiv = document.createElement('li');
      loadingDiv.className = 'loading';
      loadingDiv.textContent = 'Loading...';
      childrenContainer.appendChild(loadingDiv);
      
      listItem.classList.remove('collapsed');
      listItem.classList.add('expanded');
      
      try {
        const children = await getDirectoryChildren(ino);
        
        // Remove loading indicator
        childrenContainer.removeChild(loadingDiv);
        
        // Add children
        if (children && children.length > 0) {
          children.forEach(child => {
            const childNode = createTreeNode(child);
            childrenContainer.appendChild(childNode);
          });
        } else {
          const emptyDiv = document.createElement('li');
          emptyDiv.className = 'info';
          emptyDiv.textContent = 'Empty directory';
          childrenContainer.appendChild(emptyDiv);
        }
        
      } catch (error) {
        // Remove loading indicator
        if (childrenContainer.contains(loadingDiv)) {
          childrenContainer.removeChild(loadingDiv);
        }
        
        // Show error
        const errorDiv = document.createElement('li');
        errorDiv.className = 'error';
        errorDiv.textContent = `Failed to load: ${error.message}`;
        childrenContainer.appendChild(errorDiv);
        
        // Collapse on error
        listItem.classList.remove('expanded');
        listItem.classList.add('collapsed');
      }
    }
    
    // Initialize tree with root directory
    async function initializeTree() {
      const treeRoot = document.getElementById('fileTree');
      
      try {
        // Load root directory (ino = 1 typically)
        const rootChildren = await getDirectoryChildren(0);
        
        rootChildren.forEach(child => {
          const childNode = createTreeNode(child);
          treeRoot.appendChild(childNode);
        });
        
      } catch (error) {
        const errorDiv = document.createElement('li');
        errorDiv.className = 'error';
        errorDiv.textContent = `Failed to load root directory: ${error.message}`;
        treeRoot.appendChild(errorDiv);
      }
    }
    
    // Collapse all directories
    function collapseAll() {
      const expandedDirs = document.querySelectorAll('.expanded');
      expandedDirs.forEach(dir => {
        dir.classList.remove('expanded');
        dir.classList.add('collapsed');
      });
    }
    
    // Refresh root directory
    function refreshRoot() {
      const treeRoot = document.getElementById('fileTree');
      treeRoot.innerHTML = '';
      directoryCache.clear();
      initializeTree();
    }
    
    // Event listeners
    document.getElementById('collapseAll').addEventListener('click', collapseAll);
    document.getElementById('refreshRoot').addEventListener('click', refreshRoot);
    
    // Initialize the tree when page loads
    document.addEventListener('DOMContentLoaded', initializeTree);
  )";
  os << "</script>";
  os << "</body>";
  os << "</html>";
}

static void RenderFsDetailsPage(const FsInfoEntry& fs_info, butil::IOBufBuilder& os) {
  std::string header = fmt::format("FileSystem: {}({})", fs_info.fs_name(), fs_info.fs_id());
  std::string json;
  Helper::ProtoToJson(fs_info, json);
  RenderJsonPage("dingofs fs details", header, json, os);
}

static void RenderFsMountpointPage(const FsInfoEntry& fs_info, butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dinfofs mountpoint") << "</head>";
  os << "<body>";
  os << fmt::format(R"(<h1 style="text-align:center;">FileSystem({}) MountPoint</h1>)", fs_info.fs_id());

  os << R"(<div style="margin: 12px;font-size:smaller">)";
  os << fmt::format(R"(<h3>Mountpoint [{}]</h3>)", fs_info.mount_points_size());
  os << R"(<table class="gridtable sortable" border=1>)";
  os << "<tr>";
  os << "<th>ClientId</th>";
  os << "<th>Hostname</th>";
  os << "<th>Port</th>";
  os << "<th>Path</th>";
  os << "</tr>";

  for (const auto& mountpoint : fs_info.mount_points()) {
    os << "<tr>";

    os << "<td>" << mountpoint.client_id() << "</td>";
    os << "<td>" << mountpoint.hostname() << "</td>";
    os << "<td>" << mountpoint.port() << "</td>";
    os << "<td>" << mountpoint.path() << "</td>";

    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";
  os << "</body>";
}

static void RenderFileSessionPage(uint32_t fs_id, const std::vector<FileSessionEntry>& file_sessions,
                                  butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dinfofs filesession") << "</head>";
  os << "<body>";
  os << fmt::format(R"(<h1 style="text-align:center;">FileSystem({}) File Session</h1>)", fs_id);

  os << R"(<div style="margin: 12px;font-size:smaller">)";
  os << fmt::format(R"(<h3>FileSession [{}]</h3>)", file_sessions.size());
  os << R"(<table class="gridtable sortable" border=1>)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>SessionID</th>";
  os << "<th>ClientID</th>";
  os << "<th>Time</th>";
  os << "</tr>";

  for (const auto& file_session : file_sessions) {
    os << "<tr>";

    os << "<td>" << file_session.ino() << "</td>";
    os << "<td>" << file_session.session_id() << "</td>";
    os << "<td>" << file_session.client_id() << "</td>";
    os << "<td>" << Helper::FormatTime(file_session.create_time_s()) << "</td>";

    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";
  os << "</body>";
}

static void RenderDelfilePage(const std::vector<AttrEntry>& delfiles, butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs delfile") << "</head>";
  os << "<body>";
  os << R"(<h1 style="text-align:center;">Deleted File</h1>)";

  os << R"(<div style="margin: 12px;font-size:smaller">)";
  os << fmt::format(R"(<h3>DelFile [{}]</h3>)", delfiles.size());
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

static void RenderDelslicePage(const std::vector<TrashSliceList>& delslices, butil::IOBufBuilder& os) {
  auto render_range_func = [](const pb::mds::TrashSlice& slice) -> std::string {
    std::string result;
    for (int i = 0; i < slice.ranges_size(); ++i) {
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

  os << "<head>" << RenderHead("dingofs delslice") << "</head>";
  os << "<body>";
  os << R"(<h1 style="text-align:center;">Deleted Slice</h1>)";

  auto get_slice_count_fn = [&]() -> size_t {
    size_t count = 0;
    for (const auto& delslice : delslices) {
      count += delslice.slices_size();
    }
    return count;
  };

  os << R"(<div style="margin: 12px;font-size:smaller">)";
  os << fmt::format(R"(<h3>DelSlice [{}] [{}]</h3>)", delslices.size(), get_slice_count_fn());
  os << R"(<table class="gridtable sortable" border=1>)";
  os << "<tr>";
  os << "<th>FsId</th>";
  os << "<th>Ino</th>";
  os << "<th>ChunkIndex</th>";
  os << "<th>SliceId</th>";
  os << "<th>IsPartial</th>";
  os << "<th>Ranges</th>";
  os << "<th>Time</th>";
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
      os << "<td>" << Helper::FormatMsTime(delslice.time_ms()) << "</td>";
      os << "</tr>";
    }
  }

  os << "</table>";
  os << "</div>";
  os << "</body>";
}

static void RenderOplogPage(const std::vector<FsOpLog>& oplogs, butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs fsoplog") << "</head>";
  os << "<body>";
  os << R"(<h1 style="text-align:center;">Fs OpLog</h1>)";

  os << R"(<div style="margin: 12px;font-size:smaller">)";
  os << fmt::format(R"(<h3>Oplog [{}]</h3>)", oplogs.size());
  os << R"(<table class="gridtable sortable" border=1>)";
  os << "<tr>";
  os << "<th>No.</th>";
  os << "<th>Type</th>";
  os << "<th>Parameter</th>";
  os << "<th>comment</th>";
  os << "<th>Time</th>";
  os << "</tr>";

  uint32_t no = 0;
  for (const auto& oplog : oplogs) {
    os << "<tr>";
    os << "<td>" << ++no << "</td>";
    os << "<td>" << pb::mds::FsOpLog::Type_Name(oplog.type()) << "</td>";
    switch (oplog.type()) {
      case pb::mds::FsOpLog::CREATE_FS:
        os << "<td>" << oplog.create_fs().ShortDebugString() << "</td>";
        break;
      case pb::mds::FsOpLog::JOIN_FS:
        os << "<td>" << oplog.join_fs().ShortDebugString() << "</td>";
        break;
      case pb::mds::FsOpLog::QUIT_FS:
        os << "<td>" << oplog.quit_fs().ShortDebugString() << "</td>";
        break;
      case pb::mds::FsOpLog::DELETE_FS:
        os << "<td>" << oplog.delete_fs().ShortDebugString() << "</td>";
        break;
      case pb::mds::FsOpLog::QUIT_AND_JOIN_FS:
        os << "<td>" << oplog.quit_and_join_fs().ShortDebugString() << "</td>";
        break;
      case pb::mds::FsOpLog::UPDATE_STATE_FS:
        os << "<td>" << oplog.update_state_fs().ShortDebugString() << "</td>";
        break;
      default:
        os << "<td>UNKNOWN</td>";
    }
    os << "<td>" << oplog.comment() << "</td>";
    os << "<td>" << Helper::FormatMsTime(oplog.time_ms()) << "</td>";

    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";
  os << "</body>";
}

static void RenderInodePage(const AttrEntry& attr, butil::IOBufBuilder& os) {
  std::string header = fmt::format("Inode: {}", attr.ino());
  std::string json;
  Helper::ProtoToJson(attr, json);
  RenderJsonPage("dingofs inode details", header, json, os);
}

static void RenderChunk(uint64_t& count, uint64_t chunk_size, ChunkEntry chunk, butil::IOBufBuilder& os) {
  struct OffsetRange {
    uint64_t start;
    uint64_t end;
    std::vector<SliceEntry> slices;
  };

  // sort by offset
  std::stable_sort(chunk.mutable_slices()->begin(), chunk.mutable_slices()->end(),
                   [](const SliceEntry& a, const SliceEntry& b) { return a.offset() < b.offset(); });

  // get offset ranges
  std::set<uint64_t> offsets;
  if (!chunk.slices().empty()) {
    for (const auto& slice : chunk.slices()) {
      offsets.insert(slice.offset());
      offsets.insert(slice.offset() + slice.len());
    }
  } else {
    offsets.insert(chunk.index() * chunk_size);
    offsets.insert((chunk.index() + 1) * chunk_size);
  }

  std::vector<OffsetRange> offset_ranges;
  for (auto it = offsets.begin(); it != offsets.end(); ++it) {
    auto next_it = std::next(it);
    if (next_it != offsets.end()) {
      offset_ranges.push_back({.start = *it, .end = *next_it});
    }
  }

  for (auto& offset_range : offset_ranges) {
    for (const auto& slice : chunk.slices()) {
      uint64_t slice_start = slice.offset();
      uint64_t slice_end = slice.offset() + slice.len();

      // check intersect
      if (slice_end <= offset_range.start || slice_start >= offset_range.end) {
        continue;
      }
      offset_range.slices.push_back(slice);
    }
  }

  uint64_t chunk_index = chunk.index();

  std::set<uint64_t> uncontinuous_offsets;
  uint64_t prev_offset = chunk_index * chunk_size;
  for (auto& offset_range : offset_ranges) {
    if (offset_range.start != prev_offset) {
      uncontinuous_offsets.insert(offset_range.start);
    }

    prev_offset = offset_range.end;
  }

  for (size_t i = 0; i < offset_ranges.size(); ++i) {
    const auto& offset_range = offset_ranges[i];
    os << "<tr>";

    os << "<td>" << ++count << "</td>";

    os << ((i == 0) ? fmt::format(R"(<td>{} [{}, {})</td>)", chunk_index, chunk_index * chunk_size,
                                  (chunk_index + 1) * chunk_size)
                    : fmt::format(R"(<td>{}</td>)", chunk_index));

    os << fmt::format(R"(<td>{}</td>)", chunk.version());

    os << ((uncontinuous_offsets.count(offset_range.start) == 0)
               ? fmt::format(R"(<td>[{}, {})</td>)", offset_range.start, offset_range.end)
               : fmt::format(R"(<td style="color:red;">[{}, {})</td>)", offset_range.start, offset_range.end));

    os << R"(<td><ul style="list-style:disc">)";
    const auto& slices = offset_range.slices;
    for (size_t i = 0; i < slices.size(); ++i) {
      const auto& slice = slices.at(i);
      if (i + 1 < slices.size()) {
        os << fmt::format(R"(<li style="color:gray;">{} [{},{}) {} {} {}</li>)", slice.id(), slice.offset(),
                          slice.offset() + slice.len(), slice.len(), slice.size(), slice.zero());

      } else {
        os << fmt::format(R"(<li>{} [{},{}) {} {} {}</li>)", slice.id(), slice.offset(), slice.offset() + slice.len(),
                          slice.len(), slice.size(), slice.zero());
      }
    }
    os << "</ul></td>";

    os << "</tr>";
  }
}

static void RenderChunksPage(Ino ino, const std::vector<ChunkEntry>& chunks, butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs chunk") << "</head>";
  os << "<body>";
  os << fmt::format(R"(<h1 style="text-align:center;">File({}) Chunk</h1>)", ino);

  os << R"(<div style="margin: 12px;font-size:smaller">)";
  os << fmt::format(R"(<h3>Chunk [{}]</h3>)", chunks.size());
  if (!chunks.empty()) {
    const auto& chunk = chunks.at(0);
    os << fmt::format(R"(<h5>ChunkSize: {}MB BlockSize: {}KB LastCompactTime: {}</h5>)",
                      chunk.chunk_size() / (1024 * 1024), chunk.block_size() / 1024,
                      Helper::FormatMsTime(chunk.last_compaction_time_ms()));
  }
  os << R"(<table class="gridtable sortable" border=1>)";
  os << "<tr>";
  os << "<th>No.</th>";
  os << "<th>Index</th>";
  os << "<th>Version</th>";
  os << "<th>Range(byte)</th>";
  os << "<th>Slice<div>id range len size zero</div></th>";
  os << "</tr>";

  uint64_t count = 0;
  for (const auto& chunk : chunks) {
    RenderChunk(count, chunk.chunk_size(), chunk, os);
  }

  os << "</table>";
  os << "</div>";
  os << "</body>";
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

  } else if (params.size() == 1 && params[0] == "server") {
    // /FsStatService/server
    RenderServerPage(os);

  } else if (params.size() == 1) {
    // /FsStatService/{fs_id}
    uint32_t fs_id = Helper::StringToInt32(params[0]);

    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      auto fs_info = file_system->GetFsInfo();
      RenderFsTreePage(fs_info, os);

    } else {
      os << fmt::format("Not found file system {}.", fs_id);
    }

  } else if (params.size() == 2 && params[0] == "details") {
    // /FsStatService/details/{fs_name}

    std::string fs_name = params[1];
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    Context ctx;
    FsInfoEntry fs_info;
    auto status = file_system_set->GetFsInfo(ctx, fs_name, fs_info);
    if (status.ok()) {
      RenderFsDetailsPage(fs_info, os);

    } else {
      os << fmt::format("Get fs({}) info fail, status({}).", fs_name, status.error_str());
    }

  } else if (params.size() == 2 && params[0] == "mountpoint") {
    // /FsStatService/mountpoint/{fs_name}

    std::string fs_name = params[1];
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    Context ctx;
    FsInfoEntry fs_info;
    auto status = file_system_set->GetFsInfo(ctx, fs_name, fs_info);
    if (status.ok()) {
      RenderFsMountpointPage(fs_info, os);

    } else {
      os << fmt::format("Get fs({}) info fail, status({}).", fs_name, status.error_str());
    }

  } else if (params.size() == 2 && params[0] == "filesession") {
    // /FsStatService/filesession/{fs_id}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    std::vector<FileSessionEntry> file_sessions;
    auto status = file_system_set->GetFileSessions(fs_id, file_sessions);
    if (status.ok()) {
      RenderFileSessionPage(fs_id, file_sessions, os);

    } else {
      os << fmt::format("Not found file system {} status({}).", fs_id, status.error_str());
    }

  } else if (params.size() == 2 && params[0] == "quota") {
    // /FsStatService/quota/{fs_id}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      RenderQuotaPage(file_system, os);

    } else {
      os << fmt::format("Not found file system {}.", fs_id);
    }

  } else if (params.size() == 2 && params[0] == "delfiles") {
    // /FsStatService/delfiles/{fs_id}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    std::vector<AttrEntry> delfiles;
    auto status = file_system_set->GetDelFiles(fs_id, delfiles);
    if (status.ok()) {
      RenderDelfilePage(delfiles, os);

    } else {
      os << fmt::format("Not found file system {} status({}).", fs_id, status.error_str());
    }

  } else if (params.size() == 3 && params[0] == "delfiles") {
    // /FsStatService/delfiles/{fs_id}/{ino}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    uint64_t ino = Helper::StringToInt64(params[2]);

    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      AttrEntry attr;
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
    std::vector<TrashSliceList> delslices;
    auto status = file_system_set->GetDelSlices(fs_id, delslices);
    if (status.ok()) {
      RenderDelslicePage(delslices, os);

    } else {
      os << fmt::format("Not found file system {} status({}).", fs_id, status.error_str());
    }

  } else if (params.size() == 2 && params[0] == "oplog") {
    // /FsStatService/oplog/{fs_id}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    auto file_system_set = Server::GetInstance().GetFileSystemSet();

    std::vector<FsOpLog> oplogs;
    auto status = file_system_set->GetFsOpLogs(fs_id, oplogs);
    if (status.ok()) {
      RenderOplogPage(oplogs, os);

    } else {
      os << fmt::format("Not found file system {} status({}).", fs_id, status.error_str());
    }

  } else if (params.size() == 2) {
    // /FsStatService/{fs_id}/{ino}

    uint32_t fs_id = Helper::StringToInt32(params[0]);
    uint64_t ino = Helper::StringToInt64(params[1]);

    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      InodeSPtr inode;
      auto status = file_system->GetInodeFromStore(ino, "Stat", false, inode);
      if (status.ok()) {
        RenderInodePage(inode->Copy(), os);

      } else {
        os << fmt::format("Get inode({}) fail, {}.", ino, status.error_str());
      }
    } else {
      os << fmt::format("Not found file system {}", fs_id);
    }

  } else if (params.size() == 3 && params[0] == "partition") {
    // /FsStatService/partition/{fs_id}/{ino}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    uint64_t ino = Helper::StringToInt64(params[2]);

    DINGO_LOG(INFO) << fmt::format("Get dir json, fs_id: {}, ino: {}", fs_id, ino);

    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    auto file_system = file_system_set->GetFileSystem(fs_id);
    if (file_system != nullptr) {
      auto fs_info = file_system->GetFsInfo();
      FsUtils fs_utils(Server::GetInstance().GetOperationProcessor(), fs_info);
      std::string result;
      auto status = fs_utils.GenDirJsonString(ino, result);
      if (status.ok()) {
        cntl->http_response().set_content_type("application/json");
        os << result;

      } else {
        cntl->SetFailed(fmt::format("Get dir({}) json fail, {}.", path, status.error_str()));
      }

    } else {
      cntl->SetFailed(fmt::format("Not found file system {}.", fs_id));
    }

  } else if (params.size() == 3 && params[0] == "chunk") {
    // /FsStatService/chunk/{fs_id}/{ino}

    uint32_t fs_id = Helper::StringToInt32(params[1]);
    uint64_t ino = Helper::StringToInt64(params[2]);

    FsUtils fs_utils(Server::GetInstance().GetOperationProcessor());

    std::vector<ChunkEntry> chunks;
    auto status = fs_utils.GetChunks(fs_id, ino, chunks);
    if (status.ok()) {
      RenderChunksPage(ino, chunks, os);

    } else {
      os << fmt::format("Get chunk({}) fail, {}.", ino, status.error_str());
    }

  } else {
    cntl->SetFailed("unknown path: " + path);
  }

  os.move_to(cntl->response_attachment());
  cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

void FsStatServiceImpl::GetTabInfo(brpc::TabInfoList* tab_list) const {
  brpc::TabInfo* tab = tab_list->add();
  tab->tab_name = "dingofs";
  tab->path = "/FsStatService";
}

}  // namespace mds
}  // namespace dingofs
