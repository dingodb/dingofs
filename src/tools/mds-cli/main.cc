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

#include <fmt/format.h>
#include <glog/logging.h>

#include <string>
#include <vector>

#include "common/const.h"
#include "common/flag.h"
#include "common/helper.h"
#include "gflags/gflags.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"
#include "tools/mds-cli/br.h"
#include "tools/mds-cli/mds.h"
#include "tools/mds-cli/output.h"
#include "tools/mds-cli/store.h"

DEFINE_string(
    coor_addr, "",
    "coordinator address, etc: list://127.0.0.1:22001 or file://./coor_list");
DEFINE_string(mds_addr, "", "mds address, e.g. 127.0.0.1:7400 (required by mds commands)");

DEFINE_string(cmd, "", "command to run, see Usage (required)");
DEFINE_string(format, "pretty", "output format: pretty or json");
DEFINE_string(color, "auto", "ANSI color: auto, always, or never");
DEFINE_bool(verbose, false, "show verbose command output");

DEFINE_uint32(cluster_id, 0, "dingofs cluster id, used in meta table names");

DEFINE_string(s3_endpoint, "", "s3 endpoint, e.g. http://127.0.0.1:9000");
DEFINE_string(s3_ak, "", "s3 access key");
DEFINE_string(s3_sk, "", "s3 secret key");
DEFINE_string(s3_bucketname, "", "s3 bucket name");
DEFINE_string(s3_objectname, "", "s3 object name (backup/restore to s3)");

DEFINE_string(rados_user_name, "", "rados user name");
DEFINE_string(rados_pool_name, "", "rados pool name");
DEFINE_string(rados_mon_host, "", "rados mon host");
DEFINE_string(rados_key, "", "rados key");
DEFINE_string(rados_cluster_name, "", "rados cluster name");

DEFINE_string(fs_name, "", "filesystem name");
DEFINE_uint32(fs_id, 0, "filesystem id (0: unset)");
DEFINE_string(fs_partition_type, "mono", "createfs: fs partition type, mono or parent_hash");

DEFINE_uint32(chunk_size, 64 * 1024 * 1024, "createfs: chunk size in bytes");
DEFINE_uint32(block_size, 4 * 1024 * 1024, "createfs: block size in bytes");

DEFINE_string(name, "", "dentry name (mkdir, mknod, getdentry)");
DEFINE_string(prefix, "", "name prefix (batchmkdir, batchmknod)");

DEFINE_uint64(ino, 0, "inode number (getinode, quota, dir-stats commands)");
DEFINE_string(
    path, "",
    "directory path for dir-stats commands; an absolute OS mount path (e.g. "
    "/mnt/dingofs/a/b) is recommended -- it also renders the full info header; "
    "a mount-relative path (e.g. a/b) still resolves the inode but the path "
    "header is degraded; alternative to --ino");
DEFINE_uint64(parent, 0, "parent inode number (mkdir, mknod, getdentry, listdentry)");
DEFINE_string(parents, "", "comma-separated inode list, e.g. 1,2,3 (batch commands)");
DEFINE_uint32(num, 3, "createfs: expected mds num; batchmkdir/batchmknod: count per parent");

DEFINE_uint64(max_bytes, 1024 * 1024 * 1024, "quota max bytes");
DEFINE_uint64(max_inodes, 1000000, "quota max inodes");

DEFINE_bool(is_force, false, "force the operation (deletefs, restore)");
DEFINE_bool(keep_data, false, "deletefs: only delete metadata, keep file data");
DEFINE_bool(immediately, false, "deletefs: clean metadata immediately, requires keep_data");

DEFINE_string(type, "", "backup/restore type: meta or fsmeta (fsmeta needs fs_id)");
DEFINE_string(output_type, "stdout", "backup output: stdout, file or s3");
DEFINE_string(input_type, "stdout", "restore input: stdout, file or s3");
DEFINE_string(out, "./output", "backup output file path");
DEFINE_string(in, "./input", "restore input file path");
DEFINE_bool(is_binary, false, "backup in binary format");

DEFINE_string(mds_id_list, "", "mds id list for joinfs, quitfs or deletemds, e.g. 1,2,3. "
              "deletemds: only for offline mds, quitfs first");

DEFINE_string(member_id, "", "cache member id (must be uuid)");
DEFINE_string(cache_member_ip, "", "cache member ip");
DEFINE_uint32(cache_member_port, 0, "cache member port");
DEFINE_string(group_name, "", "cache group name");
DEFINE_uint32(weight, 0, "cache member weight");

DEFINE_string(storage_path, "", "createfs: local storage path for a local-file fs");

DEFINE_string(storage_engine, "dingo-store", "store commands: storage engine, dingo-store or tikv");

// Trash restore (`restoretrash` subcommand).
DEFINE_string(hours, "",
              "comma-separated trash hour buckets to restore, e.g. "
              "2026-04-05-14,2026-04-05-15 (UTC)");
DEFINE_bool(put_back, false,
            "if true, restore files back to their original directories");
DEFINE_uint32(restore_threads, 10, "concurrency for trash restore workers");
DEFINE_uint32(
    trash_days, 0,
    "per-fs trash retention in days: deleted files are kept in trash for this "
    "many days before GC; "
    "0 means trash disabled. Applied at createfs and at updatefs trash_days.");
DEFINE_bool(
    immediate_trash_quota, true,
    "per-fs: when true, trash-move immediately debits the parent/ancestor "
    "per-dir quota "
    "(credited back on restore); when false, the debit is deferred to GC. "
    "Applied at createfs only and immutable thereafter.");

DEFINE_bool(enable_uid_gid_map, true,
            "per-fs: when true, client hashes local user/group names into "
            "internal ids in [10000, 2^32). "
            "ALL mounting clients of this fs must support the feature.");

DEFINE_bool(enable_dir_stats, true,
            "per-fs: when true, tracks per-directory usage statistics. "
            "Set at createfs, or hot-switched later via the "
            "updatefsenabledirstats command.");

// dir-stats subcommand flags
DEFINE_uint32(
    depth, 2,
    "summary tree depth (0-10; values > 10 are clamped to 10 with a warning)");
DEFINE_uint32(entries, 10,
              "summary top-N entries per level (0-100; values > 100 are "
              "clamped to 100 with a warning)");
DEFINE_uint32(dir_threads, 50,
              "concurrency for the client-side directory-tree walk "
              "(info -r / summary / syncdirstat)");
DEFINE_bool(strict, false, "summary: strict (accurate, full traversal)");
DEFINE_bool(recursive, false, "info/syncdirstat: walk sub-directories recursively");
DEFINE_bool(repair, false, "repair inconsistent dir stats");
DEFINE_bool(
    raw, false,
    "info: for a file, show raw chunks/slices (sliceId) instead of objects");
static std::string GetDefaultCoorAddrPath() {
  if (!FLAGS_coor_addr.empty()) {
    return FLAGS_coor_addr;
  }

  std::vector<std::string> paths = {"./coor_list", "./conf/coor_list",
                                    "./bin/coor_list"};
  for (const auto& path : paths) {
    if (::dingofs::Helper::IsExistPath(path)) {
      return "file://" + path;
    }
  }

  return "";
}

// get the last name from the path
// e.g. /path/to/file.txt -> file.txt
static std::string GetLastName(const std::string& name) {
  size_t pos = name.find_last_of('/');
  if (pos == std::string::npos) {
    return name;
  }
  return name.substr(pos + 1);
}

static dingofs::FlagExtraInfo extras = {
    .program = "dingo-mds-client",
    .usage = R"(  dingo-mds-client --mds_addr=<ip:port[,..]> --cmd=<command> [OPTIONS]
  dingo-mds-client --coor_addr=<list://..|file://..> --cmd=<command> [OPTIONS]

  --mds_addr  : used by MDS commands (fs / dir / quota / cache / trash / dir-stats).
  --coor_addr : used by backup / restore / store commands; when empty, it falls
                back to ./coor_list, ./conf/coor_list or ./bin/coor_list.
  Command names are case-insensitive. Options are listed at the bottom.

Commands (key options in brackets):
  Filesystem (MDS)
    createfs   [fs_name fs_id fs_partition_type chunk_size block_size num
                trash_days immediate_trash_quota enable_uid_gid_map
                enable_dir_stats s3_*|rados_*|storage_path]
    deletefs   [fs_name is_force keep_data immediately]
    getfs / listfs / getmdslist           [fs_name]
    updatefs / updatefss3info / updatefsradosinfo   [fs_name s3_*|rados_*]
    updatefstrashdays / updatefsenableuidgidmap / updatefsenabledirstats
               [fs_name trash_days|enable_uid_gid_map|enable_dir_stats]
    joinfs / quitfs                       [fs_name|fs_id mds_id_list]
    setfsstats / continuesetfsstats / getfsstats / getfspersecondstats [fs_name]
    restoretrash [fs_id hours put_back restore_threads]
  Metadata (MDS)
    mkdir / mknod [fs_id parent name]     getdentry [fs_id parent name]
    listdentry [fs_id parent]             getinode  [fs_id ino]
    batchmkdir / batchmknod [fs_id parents prefix num]
    batchgetinode / batchgetxattr [fs_id parents(as inode list)]
  Quota (MDS)
    setfsquota [fs_id max_bytes max_inodes]    getfsquota [fs_id]
    setdirquota [fs_id ino max_bytes max_inodes]
    getdirquota / deletedirquota [fs_id ino]
  Dir stats (MDS)
    info / summary [fs_id ino|path depth entries dir_threads recursive raw]
    syncdirstat    [fs_id ino|path recursive repair dir_threads]
  Cache group (MDS)
    joincachegroup [member_id cache_member_ip cache_member_port group_name weight]
    leavecachegroup [member_id cache_member_ip cache_member_port group_name]
    reweightmember [member_id cache_member_ip cache_member_port weight]
    unlockmember   [member_id cache_member_ip cache_member_port]
    deletemember   [member_id]   listgroups   listmembers [group_name]
  Backup / restore (coordinator)
    backup  [type=meta|fsmeta fs_id output_type=stdout|file|s3 out is_binary s3_*]
    restore [type=meta|fsmeta fs_id input_type=stdout|file|s3 in is_force s3_*]
  Store (coordinator, offline maintenance)
    createmetatable / createfsstatstable / createalltable
    dropmetatable / dropfsstatstable / dropfsmetatable [fs_id]
    updatefss3infobystore / updatefsradosinfobystore [fs_name s3_*|rados_*]
    deletemds [mds_id_list]   tree [fs_id]   (uses cluster_id, storage_engine)
)",
    .examples =
        R"(  $ dingo-mds-client --mds_addr=127.0.0.1:7400 --cmd=listfs
  $ dingo-mds-client --mds_addr=127.0.0.1:7400 --cmd=createfs --fs_name=myfs --s3_endpoint=http://s3:9000 --s3_ak=AK --s3_sk=SK --s3_bucketname=bkt
  $ dingo-mds-client --mds_addr=127.0.0.1:7400 --cmd=setdirquota --fs_id=1 --ino=1024 --max_bytes=10737418240 --max_inodes=100000
  $ dingo-mds-client --mds_addr=127.0.0.1:7400 --cmd=info --fs_id=1 --path=/mnt/dingofs/a/b
  $ dingo-mds-client --mds_addr=127.0.0.1:7400 --cmd=summary --fs_id=1 --ino=1024 --depth=3 --format=json
  $ dingo-mds-client --coor_addr=file://./coor_list --cmd=backup --type=meta --output_type=file --out=backup_restore/meta_backup1
  $ dingo-mds-client --coor_addr=file://./coor_list --cmd=restore --type=meta --input_type=file --in=backup_restore/meta_backup1
)",
    .patterns = {"tools/mds-cli"},
};

int main(int argc, char* argv[]) {
  //  parse gflags
  int rc = dingofs::ParseFlags(&argc, &argv, extras);
  if (rc != 0) return 1;

  dingofs::mds::client::OutputConfig output_config;
  const auto output_format = ::dingofs::Helper::ToLowerCase(FLAGS_format);
  if (output_format == "pretty") {
    output_config.format = dingofs::mds::client::OutputFormat::kPretty;
  } else if (output_format == "json") {
    output_config.format = dingofs::mds::client::OutputFormat::kJson;
  } else {
    std::cerr << "invalid --format: " << FLAGS_format
              << " (expected pretty or json)\n";
    return 2;
  }
  const auto color_mode = ::dingofs::Helper::ToLowerCase(FLAGS_color);
  if (color_mode == "never") {
    output_config.color = dingofs::mds::client::ColorMode::kNever;
  } else if (color_mode == "auto") {
    output_config.color = dingofs::mds::client::ColorMode::kAuto;
  } else if (color_mode == "always") {
    output_config.color = dingofs::mds::client::ColorMode::kAlways;
  } else {
    std::cerr << "invalid --color: " << FLAGS_color
              << " (expected auto, always, or never)\n";
    return 2;
  }
  output_config.verbose = FLAGS_verbose;
  dingofs::mds::client::SetOutputConfig(output_config);

  dingofs::mds::MetaCodec::SetClusterID(FLAGS_cluster_id);

  std::string program_name = GetLastName(std::string(argv[0]));
  dingofs::Logger::Init(program_name);
  // Keep diagnostics in the configured log files; command output is rendered
  // explicitly by OutputFormatter.
  FLAGS_stderrthreshold = google::GLOG_FATAL;

  std::string lower_cmd = ::dingofs::Helper::ToLowerCase(FLAGS_cmd);

  // run backup command
  {
    dingofs::mds::br::BackupCommandRunner::Options options;
    options.cluster_id = FLAGS_cluster_id;
    options.type = ::dingofs::Helper::ToLowerCase(FLAGS_type);
    options.output_type = ::dingofs::Helper::ToLowerCase(FLAGS_output_type);
    options.fs_id = FLAGS_fs_id;
    options.fs_name = FLAGS_fs_name;
    options.file_path = FLAGS_out;
    options.is_binary = FLAGS_is_binary;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    if (dingofs::mds::br::BackupCommandRunner::Run(
            options, GetDefaultCoorAddrPath(), lower_cmd)) {
      return 0;
    }
  }

  // run restore command
  {
    dingofs::mds::br::RestoreCommandRunner::Options options;
    options.cluster_id = FLAGS_cluster_id;
    options.type = ::dingofs::Helper::ToLowerCase(FLAGS_type);
    options.input_type = ::dingofs::Helper::ToLowerCase(FLAGS_input_type);
    options.fs_id = FLAGS_fs_id;
    options.fs_name = FLAGS_fs_name;
    options.file_path = FLAGS_in;
    options.is_force = FLAGS_is_force;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    if (dingofs::mds::br::RestoreCommandRunner::Run(
            options, GetDefaultCoorAddrPath(), lower_cmd)) {
      return 0;
    }
  }

  // run mds command
  {
    dingofs::mds::client::MdsCommandRunner::Options options;
    options.cluster_id = FLAGS_cluster_id;
    options.fs_id = FLAGS_fs_id;
    options.ino = FLAGS_ino;
    options.path = FLAGS_path;
    options.parent = FLAGS_parent;
    options.parents = FLAGS_parents;
    options.name = FLAGS_name;
    options.fs_name = FLAGS_fs_name;
    options.mds_id_list = FLAGS_mds_id_list;
    options.prefix = FLAGS_prefix;
    options.num = FLAGS_num;
    options.max_bytes = FLAGS_max_bytes;
    options.max_inodes = FLAGS_max_inodes;
    options.fs_partition_type = FLAGS_fs_partition_type;
    options.chunk_size = FLAGS_chunk_size;
    options.block_size = FLAGS_block_size;

    options.storage_path = FLAGS_storage_path;

    options.is_force = FLAGS_is_force;
    options.keep_data = FLAGS_keep_data;
    options.immediately = FLAGS_immediately;

    // cache member
    options.member_id = FLAGS_member_id;
    options.ip = FLAGS_cache_member_ip;
    options.port = FLAGS_cache_member_port;
    options.group_name = FLAGS_group_name;
    options.weight = FLAGS_weight;

    options.trash_days = FLAGS_trash_days;
    options.immediate_trash_quota = FLAGS_immediate_trash_quota;
    options.enable_uid_gid_map = FLAGS_enable_uid_gid_map;
    options.enable_dir_stats = FLAGS_enable_dir_stats;
    // trash restore
    options.trash_put_back = FLAGS_put_back;
    options.trash_threads = FLAGS_restore_threads;
    if (!FLAGS_hours.empty()) {
      dingofs::Helper::SplitString(FLAGS_hours, ',', options.trash_hours);
    }

    options.depth = FLAGS_depth;
    options.entries = FLAGS_entries;
    options.dir_threads = FLAGS_dir_threads;
    options.strict = FLAGS_strict;
    options.recursive = FLAGS_recursive;
    options.repair = FLAGS_repair;
    options.raw = FLAGS_raw;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    auto& rados_info = options.rados_info;
    rados_info.user_name = FLAGS_rados_user_name;
    rados_info.pool_name = FLAGS_rados_pool_name;
    rados_info.mon_host = FLAGS_rados_mon_host;
    rados_info.key = FLAGS_rados_key;
    rados_info.cluster_name = FLAGS_rados_cluster_name;

    if (dingofs::mds::client::MdsCommandRunner::Run(options, FLAGS_mds_addr,
                                                    lower_cmd, FLAGS_fs_id)) {
      return dingofs::mds::client::GetOutputExitCode();
    }
  }

  // run store command
  {
    dingofs::mds::client::StoreCommandRunner::Options options;
    options.cluster_id = FLAGS_cluster_id;
    options.fs_id = FLAGS_fs_id;
    options.fs_name = FLAGS_fs_name;
    options.mds_id_list = FLAGS_mds_id_list;
    options.meta_table_name =
        fmt::format("{}[{}]", dingofs::kMetaTableName, FLAGS_cluster_id);
    options.fsstats_table_name =
        fmt::format("{}[{}]", dingofs::kFsStatsTableName, FLAGS_cluster_id);
    options.storage_engine = FLAGS_storage_engine;

    auto& s3_info = options.s3_info;
    s3_info.ak = FLAGS_s3_ak;
    s3_info.sk = FLAGS_s3_sk;
    s3_info.endpoint = FLAGS_s3_endpoint;
    s3_info.bucket_name = FLAGS_s3_bucketname;
    s3_info.object_name = FLAGS_s3_objectname;

    auto& rados_info = options.rados_info;
    rados_info.user_name = FLAGS_rados_user_name;
    rados_info.pool_name = FLAGS_rados_pool_name;
    rados_info.mon_host = FLAGS_rados_mon_host;
    rados_info.key = FLAGS_rados_key;
    rados_info.cluster_name = FLAGS_rados_cluster_name;

    if (dingofs::mds::client::StoreCommandRunner::Run(
            options, GetDefaultCoorAddrPath(), lower_cmd)) {
      return dingofs::mds::client::GetOutputExitCode();
    }
  }

  std::cerr << (lower_cmd.empty() ? "missing --cmd"
                                  : "unknown --cmd: " + FLAGS_cmd)
            << ", run with --help to see supported commands\n";
  return 2;
}
