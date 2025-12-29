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

#include "client/vfs/metasystem/local/metasystem.h"

#include <fcntl.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/mutex.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "common/const.h"
#include "common/helper.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"
#include "mds/common/codec.h"
#include "utils/uuid.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace local {

using dingofs::mds::AttrEntry;
using dingofs::mds::DentryEntry;
using dingofs::mds::MetaCodec;

const std::string kDbDir = fmt::format("{}/store", kDefaultRuntimeBaseDir);

constexpr uint32_t kFsId = 10000;
const std::string kFsDefaultName = "local_fs";
constexpr Ino kRootParentIno = 0;
constexpr Ino kRootIno = 1;
constexpr uint32_t kBlockSize = 4 * 1024 * 1024;
constexpr uint32_t kChunkSize = 64 * 1024 * 1024;

const int kEmptyDirMinLinkNum = 2;
const int kFileMinLinkNum = 1;

const uint32_t kCleanDelfileIntervalS = 60;

const std::vector<std::string> kLocalParams = {"path"};
const std::vector<std::string> kS3Params = {"ak", "sk", "endpoint",
                                            "bucketname"};
const std::vector<std::string> kRadosParams = {"key", "username", "mon", "pool",
                                               "cluster"};

static uint64_t ToTimestamp(const struct timespec& ts) {
  return (ts.tv_sec * 1000000000) + ts.tv_nsec;
}

static uint64_t ToTimestamp(uint64_t tv_sec, uint32_t tv_nsec) {
  return (tv_sec * 1000000000) + tv_nsec;
}

static void AddParentIno(AttrEntry& attr, Ino parent) {
  auto it = std::find(attr.parents().begin(), attr.parents().end(), parent);
  if (it == attr.parents().end()) {
    attr.add_parents(parent);
  }
}

static void DelParentIno(AttrEntry& attr, Ino parent) {
  auto it = std::find(attr.parents().begin(), attr.parents().end(), parent);
  if (it != attr.parents().end()) {
    attr.mutable_parents()->erase(it);
  }
}
// storage info may be like:
// storage=file&path=/data/dingofs
// storage=rados&key=<key>&username=<name>&pool=<pool>&cluster=<cluster-name>&mon=<mon1,mon2..>
// storage=s3&ak=<ak>&sk=<sk>&endpoint=<endpoint>&bucketname=<bucketname>
static std::unordered_map<std::string, std::string> ParseStorageInfo(
    const std::string& storage_info) {
  std::unordered_map<std::string, std::string> result;
  std::stringstream ss(storage_info);
  std::string token;

  while (std::getline(ss, token, '&')) {
    size_t equalsPos = token.find('=');
    if (equalsPos != std::string::npos) {
      std::string key = token.substr(0, equalsPos);
      std::string value = token.substr(equalsPos + 1);
      result[key] = value;
    }
  }

  return result;
}

OpenFileMemo::OpenFileMemo() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}
OpenFileMemo::~OpenFileMemo() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail.";
}

bool OpenFileMemo::IsOpened(Ino ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto iter = file_map_.find(ino);
  return iter != file_map_.end();
}

void OpenFileMemo::Open(Ino ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto iter = file_map_.find(ino);
  if (iter != file_map_.end()) {
    iter->second.ref_count++;
    return;
  }

  State state;
  state.ref_count = 1;
  file_map_[ino] = state;
}

void OpenFileMemo::Close(Ino ino, bool& is_erased) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto iter = file_map_.find(ino);
  if (iter == file_map_.end()) {
    return;
  }

  CHECK_GT(iter->second.ref_count, 0);
  iter->second.ref_count--;

  if (iter->second.ref_count == 0) {
    file_map_.erase(iter);
    is_erased = true;
  }
}

bool OpenFileMemo::Dump(Json::Value& value) {
  BAIDU_SCOPED_LOCK(mutex_);

  Json::Value items = Json::arrayValue;
  for (auto& [ino, state] : file_map_) {
    Json::Value item = Json::objectValue;

    item["ino"] = ino;
    item["ref_count"] = state.ref_count;

    items.append(item);
  }

  value["file_map"] = items;

  return true;
}

bool OpenFileMemo::Load(const Json::Value& value) {
  BAIDU_SCOPED_LOCK(mutex_);

  const Json::Value& items = value["file_map"];
  if (!items.isArray()) return false;

  file_map_.clear();
  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();
    State state;
    state.ref_count = item["ref_count"].asUInt();

    file_map_.insert(std::make_pair(ino, state));
  }

  return true;
}

LocalMetaSystem::LocalMetaSystem(const std::string& db_path,
                                 const std::string& fs_name,
                                 const std::string& storage_info)
    : db_path_(db_path.empty() ? kDbDir : db_path),
      fs_name_(fs_name.empty() ? kFsDefaultName : fs_name),
      storage_info_(storage_info),
      inode_cache_(meta::InodeCache::New(kFsId)) {}  // NOLINT

Status LocalMetaSystem::Init(bool upgrade) {
  std::string db_path = fmt::format("{}/{}", db_path_, fs_name_);
  // create db path if not exist
  if (!dingofs::Helper::CreateDirectory(db_path)) {
    return Status::Internal(
        fmt::format("create db path dir fail, path : {}", db_path_));
  }

  if (!OpenLevelDB(db_path)) {
    return Status::Internal("open leveldb fail.");
  }

  if (!InitIdGenerators()) {
    return Status::Internal("init id generators fail.");
  }

  auto status = InitFsInfo();
  if (!status.ok()) return status;

  status = InitFsQuota();
  if (!status.ok()) return status;

  status = InitRoot();
  if (!status.ok()) return status;

  if (!InitCrontab()) {
    return Status::Internal("init crontab fail.");
  }

  return Status::OK();
}

void LocalMetaSystem::UnInit(bool upgrade) {
  crontab_manager_.Destroy();

  CloseLevelDB();
}

bool LocalMetaSystem::Dump(ContextSPtr, Json::Value& value) {
  DumpOption options;
  options.file_session = true;
  options.dir_iterator = true;

  return Dump(options, value);
}

bool LocalMetaSystem::Dump(const DumpOption& options, Json::Value& value) {
  if (options.file_session && !open_file_memo_.Dump(value)) {
    return false;
  }

  if (options.dir_iterator && !dir_iterator_manager_.Dump(value)) {
    return false;
  }

  return true;
}

bool LocalMetaSystem::Load(ContextSPtr, const Json::Value& value) {
  if (!open_file_memo_.Load(value)) return false;

  if (!dir_iterator_manager_.Load(value)) return false;

  return true;
}

Status LocalMetaSystem::Lookup(ContextSPtr, Ino parent, const std::string& name,
                               Attr* attr) {
  const uint32_t fs_id = fs_info_.fs_id();

  // get dentry
  std::string value;
  auto status = Get(MetaCodec::EncodeDentryKey(fs_id, parent, name), value);
  if (!status.ok()) return status;

  auto dentry_entry = MetaCodec::DecodeDentryValue(value);

  // get inode
  AttrEntry attr_entry;
  status = GetAttrEntry(dentry_entry.ino(), attr_entry);
  if (!status.ok()) return status;

  *attr = meta::Helper::ToAttr(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::MkNod(ContextSPtr, Ino parent, const std::string& name,
                              uint32_t uid, uint32_t gid, uint32_t mode,
                              uint64_t rdev, Attr* attr) {
  const uint64_t now_ns = utils::TimestampNs();
  const uint32_t fs_id = fs_info_.fs_id();
  const Ino ino = GenFileIno();

  AttrEntry attr_entry;
  attr_entry.set_fs_id(fs_id);
  attr_entry.set_ino(ino);
  attr_entry.set_length(0);
  attr_entry.set_ctime(now_ns);
  attr_entry.set_mtime(now_ns);
  attr_entry.set_atime(now_ns);
  attr_entry.set_uid(uid);
  attr_entry.set_gid(gid);
  attr_entry.set_mode(mode);
  attr_entry.set_nlink(kFileMinLinkNum);
  attr_entry.set_type(pb::mds::FileType::FILE);
  attr_entry.set_rdev(rdev);
  attr_entry.add_parents(parent);
  attr_entry.set_version(1);

  DentryEntry dentry_entry;
  dentry_entry.set_fs_id(fs_id);
  dentry_entry.set_ino(ino);
  dentry_entry.set_parent(parent);
  dentry_entry.set_name(name);
  dentry_entry.set_flag(0);
  dentry_entry.set_type(pb::mds::FileType::FILE);

  AttrEntry parent_attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    std::string value;
    std::string parent_inode_key = MetaCodec::EncodeInodeKey(fs_id, parent);
    auto status = Get(parent_inode_key, value);
    if (!status.ok()) return status;
    CHECK(!value.empty()) << "not found parent inode.";

    parent_attr_entry = MetaCodec::DecodeInodeValue(value);
    parent_attr_entry.set_ctime(std::max(parent_attr_entry.ctime(), now_ns));
    parent_attr_entry.set_mtime(std::max(parent_attr_entry.mtime(), now_ns));
    parent_attr_entry.set_version(parent_attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, MetaCodec::EncodeInodeKey(fs_id, ino),
         MetaCodec::EncodeInodeValue(attr_entry)},
        {KeyValue::OpType::kPut,
         MetaCodec::EncodeDentryKey(fs_id, parent, name),
         MetaCodec::EncodeDentryValue(dentry_entry)},
        {KeyValue::OpType::kPut, parent_inode_key,
         MetaCodec::EncodeInodeValue(parent_attr_entry)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  UpdateFsUsage(0, 1, "mknod");

  *attr = meta::Helper::ToAttr(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::Open(ContextSPtr, Ino ino, int flags, uint64_t) {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t now_ns = utils::TimestampNs();

  // O_ACCMODE	         0003
  // O_RDONLY	             00
  // O_WRONLY	             01
  // O_RDWR		             02
  // O_CREAT		         0100
  // O_TRUNC		        01000
  // O_APPEND		        02000
  // O_NONBLOCK	        04000
  // O_SYNC	         04010000
  // O_ASYNC	         020000
  if ((flags & O_TRUNC) && !(flags & O_WRONLY || flags & O_RDWR)) {
    return Status::NoPermission("O_TRUNC without O_WRONLY or O_RDWR");
  }

  open_file_memo_.Open(ino);

  int64_t file_length = 0;
  AttrEntry attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    // get inode
    std::string value;
    std::string inode_key = MetaCodec::EncodeInodeKey(fs_info_.fs_id(), ino);
    auto status = Get(inode_key, value);

    attr_entry = MetaCodec::DecodeInodeValue(value);

    if (flags & O_TRUNC) {
      file_length = attr_entry.length();
      attr_entry.set_length(0);

      attr_entry.set_ctime(std::max(attr_entry.ctime(), now_ns));
      attr_entry.set_mtime(std::max(attr_entry.mtime(), now_ns));
    }

    attr_entry.set_atime(std::max(attr_entry.atime(), now_ns));

    attr_entry.set_version(attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, inode_key,
         MetaCodec::EncodeInodeValue(attr_entry)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);

  if (flags & O_TRUNC) UpdateFsUsage(-file_length, 0, "open");

  return Status::OK();
}

Status LocalMetaSystem::Create(ContextSPtr ctx, Ino parent,
                               const std::string& name, uint32_t uid,
                               uint32_t gid, uint32_t mode, int flags,
                               Attr* attr, uint64_t fh) {
  auto status = MkNod(ctx, parent, name, uid, gid, mode, 0, attr);
  if (!status.ok()) return status;

  status = Open(ctx, attr->ino, flags, fh);
  if (!status.ok()) return status;

  return Status::OK();
}

Status LocalMetaSystem::Flush(ContextSPtr ctx, Ino ino, uint64_t fh) {
  return Status::OK();
}

Status LocalMetaSystem::Close(ContextSPtr, Ino ino, uint64_t) {
  if (!open_file_memo_.IsOpened(ino)) return Status::OK();

  bool is_erased = false;
  open_file_memo_.Close(ino, is_erased);

  if (is_erased) {
    // clean inode cache
    auto inode = GetInodeFromCache(ino);
    if (inode != nullptr && inode->Nlink() == 0) DeleteInodeFromCache(ino);
  }

  return Status::OK();
}

Status LocalMetaSystem::ReadSlice(ContextSPtr, Ino ino, uint64_t index,
                                  uint64_t, std::vector<Slice>* slices,
                                  uint64_t& version) {
  const uint32_t fs_id = fs_info_.fs_id();

  // get chunk info
  std::string value;
  std::string chunk_key = MetaCodec::EncodeChunkKey(fs_id, ino, index);
  auto status = Get(chunk_key, value);
  if (!status.ok()) {
    if (status.IsNotFound()) return Status::OK();
    return status;
  }

  auto chunk_entry = MetaCodec::DecodeChunkValue(value);

  for (const auto& slice_info : chunk_entry.slices()) {
    Slice slice;
    slice.id = slice_info.id();
    slice.offset = slice_info.offset();
    slice.size = slice_info.size();
    slice.length = slice_info.len();
    slice.compaction = slice_info.compaction_version();
    slice.is_zero = slice_info.zero();

    slices->push_back(slice);
  }

  version = chunk_entry.version();

  return Status::OK();
}

Status LocalMetaSystem::NewSliceId(ContextSPtr, Ino, uint64_t* id) {
  CHECK(slice_id_generator_->GenID(1, *id)) << "gen slice id fail.";

  return Status::OK();
}

Status LocalMetaSystem::WriteSlice(ContextSPtr, Ino ino, uint64_t index,
                                   uint64_t, const std::vector<Slice>& slices) {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t now_ns = utils::TimestampNs();

  int64_t delta_size = 0;
  AttrEntry attr_entry;
  {
    utils::WriteLockGuard lk(lock_);
    // get inode
    std::string value;
    std::string inode_key = MetaCodec::EncodeInodeKey(fs_id, ino);
    auto status = Get(inode_key, value);
    if (!status.ok() && !status.IsNotFound()) return status;

    if (status.ok()) attr_entry = MetaCodec::DecodeInodeValue(value);

    // get chunk info
    std::string chunk_key = MetaCodec::EncodeChunkKey(fs_id, ino, index);
    status = Get(chunk_key, value);

    mds::ChunkEntry chunk_entry;
    if (!status.ok()) {
      if (!status.IsNotFound()) return status;

      chunk_entry.set_index(index);
      chunk_entry.set_block_size(fs_info_.block_size());
      chunk_entry.set_chunk_size(fs_info_.chunk_size());
      chunk_entry.set_version(0);

    } else {
      chunk_entry = MetaCodec::DecodeChunkValue(value);
    }

    uint64_t new_length = 0;
    for (const auto& slice : slices) {
      new_length = std::max(new_length, slice.offset + slice.size);

      auto* slice_entry = chunk_entry.add_slices();

      slice_entry->set_id(slice.id);
      slice_entry->set_offset(slice.offset);
      slice_entry->set_len(slice.length);
      slice_entry->set_size(slice.size);
      slice_entry->set_compaction_version(slice.compaction);
      slice_entry->set_zero(slice.is_zero);
    }

    chunk_entry.set_version(chunk_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, chunk_key,
         MetaCodec::EncodeChunkValue(chunk_entry)},
    };
    if (attr_entry.ino() > 0 && attr_entry.length() < new_length) {
      delta_size = new_length - attr_entry.length();
      attr_entry.set_length(new_length);
      attr_entry.set_ctime(std::max(attr_entry.ctime(), now_ns));
      attr_entry.set_mtime(std::max(attr_entry.mtime(), now_ns));
      attr_entry.set_version(attr_entry.version() + 1);

      kvs.push_back({KeyValue::OpType::kPut, inode_key,
                     MetaCodec::EncodeInodeValue(attr_entry)});
    }

    status = Put(kvs);
    if (!status.ok()) return status;
  }

  if (attr_entry.ino() > 0) PutInodeToCache(attr_entry);

  if (delta_size != 0) UpdateFsUsage(delta_size, 0, "symlink");

  return Status::OK();
}

Status LocalMetaSystem::AsyncWriteSlice(ContextSPtr ctx, Ino ino,
                                        uint64_t index, uint64_t fh,
                                        const std::vector<Slice>& slices,
                                        DoneClosure done) {
  auto status = WriteSlice(ctx, ino, index, fh, slices);

  done(status);

  return Status::OK();
}

Status LocalMetaSystem::Write(ContextSPtr, Ino ino, uint64_t offset,
                              uint64_t size, uint64_t) {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t now_ns = utils::TimestampNs();

  AttrEntry attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    // get inode
    std::string value;
    std::string inode_key = MetaCodec::EncodeInodeKey(fs_id, ino);
    auto status = Get(inode_key, value);
    if (!status.ok()) return status;

    attr_entry = MetaCodec::DecodeInodeValue(value);

    if (attr_entry.length() < offset + size) {
      attr_entry.set_length(offset + size);
    }
    attr_entry.set_ctime(std::max(attr_entry.ctime(), now_ns));
    attr_entry.set_mtime(std::max(attr_entry.mtime(), now_ns));
    attr_entry.set_version(attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, inode_key,
         MetaCodec::EncodeInodeValue(attr_entry)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::MkDir(ContextSPtr, Ino parent, const std::string& name,
                              uint32_t uid, uint32_t gid, uint32_t mode,
                              Attr* attr) {
  const uint64_t now_ns = utils::TimestampNs();
  const uint32_t fs_id = fs_info_.fs_id();
  const Ino ino = GenDirIno();

  AttrEntry attr_entry;
  attr_entry.set_fs_id(fs_id);
  attr_entry.set_ino(ino);
  attr_entry.set_length(4096);
  attr_entry.set_ctime(now_ns);
  attr_entry.set_mtime(now_ns);
  attr_entry.set_atime(now_ns);
  attr_entry.set_uid(uid);
  attr_entry.set_gid(gid);
  attr_entry.set_mode(S_IFDIR | mode);
  attr_entry.set_nlink(kEmptyDirMinLinkNum);
  attr_entry.set_type(pb::mds::FileType::DIRECTORY);
  attr_entry.set_rdev(0);
  attr_entry.add_parents(parent);
  attr_entry.set_version(1);

  DentryEntry dentry_entry;
  dentry_entry.set_fs_id(fs_id);
  dentry_entry.set_ino(ino);
  dentry_entry.set_parent(parent);
  dentry_entry.set_name(name);
  dentry_entry.set_flag(0);
  dentry_entry.set_type(pb::mds::FileType::DIRECTORY);

  AttrEntry parent_attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    std::string value;
    std::string parent_inode_key = MetaCodec::EncodeInodeKey(fs_id, parent);
    auto status = Get(parent_inode_key, value);
    if (!status.ok()) return status;
    CHECK(!value.empty()) << "not found parent inode.";

    parent_attr_entry = MetaCodec::DecodeInodeValue(value);
    parent_attr_entry.set_nlink(parent_attr_entry.nlink() + 1);
    parent_attr_entry.set_ctime(std::max(parent_attr_entry.ctime(), now_ns));
    parent_attr_entry.set_mtime(std::max(parent_attr_entry.mtime(), now_ns));
    parent_attr_entry.set_version(parent_attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, MetaCodec::EncodeInodeKey(fs_id, ino),
         MetaCodec::EncodeInodeValue(attr_entry)},
        {KeyValue::OpType::kPut,
         MetaCodec::EncodeDentryKey(fs_id, parent, name),
         MetaCodec::EncodeDentryValue(dentry_entry)},
        {KeyValue::OpType::kPut, parent_inode_key,
         MetaCodec::EncodeInodeValue(parent_attr_entry)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  UpdateFsUsage(0, 1, "mkdir");

  *attr = meta::Helper::ToAttr(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::RmDir(ContextSPtr, Ino parent,
                              const std::string& name) {
  const uint64_t now_ns = utils::TimestampNs();
  const uint32_t fs_id = fs_info_.fs_id();

  AttrEntry attr_entry;
  AttrEntry parent_attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    // get parent/name dentry
    std::string value;
    std::string dentry_key = MetaCodec::EncodeDentryKey(fs_id, parent, name);
    auto status = Get(dentry_key, value);
    if (!status.ok()) return status;

    auto dentry_entry = MetaCodec::DecodeDentryValue(value);

    // get parent/name inode
    std::string inode_key =
        MetaCodec::EncodeInodeKey(fs_id, dentry_entry.ino());
    status = Get(inode_key, value);
    if (!status.ok()) return status;

    attr_entry = MetaCodec::DecodeInodeValue(value);
    CHECK(attr_entry.type() == pb::mds::FileType::DIRECTORY)
        << fmt::format("not directory type, ino({}).", attr_entry.ino());

    bool is_empty = false;
    status = CheckDirEmpty(attr_entry.ino(), is_empty);
    if (!status.ok()) return status;
    if (!is_empty) return Status::NotEmpty("directory not empty.");

    // get parent inode
    std::string parent_inode_key = MetaCodec::EncodeInodeKey(fs_id, parent);
    status = Get(parent_inode_key, value);
    if (!status.ok()) return status;
    parent_attr_entry = MetaCodec::DecodeInodeValue(value);

    // update parent inode
    parent_attr_entry.set_nlink(parent_attr_entry.nlink() - 1);
    parent_attr_entry.set_ctime(std::max(parent_attr_entry.ctime(), now_ns));
    parent_attr_entry.set_mtime(std::max(parent_attr_entry.mtime(), now_ns));
    parent_attr_entry.set_version(parent_attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kDelete, inode_key, ""},
        {KeyValue::OpType::kDelete, dentry_key, ""},
        {KeyValue::OpType::kPut, parent_inode_key,
         MetaCodec::EncodeInodeValue(parent_attr_entry)},
    };

    status = Put(kvs);
    if (!status.ok()) return status;
  }

  DeleteInodeFromCache(attr_entry.ino());
  PutInodeToCache(parent_attr_entry);

  UpdateFsUsage(0, -1, "rmdir");

  return Status::OK();
}

Status LocalMetaSystem::OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh) {
  std::vector<DentryEntry> dentries;
  auto status = GetDentries(ino, dentries);
  if (!status.ok()) return status;

  auto dir_iterator = DirIterator::New(ino, fh, std::move(dentries));

  bool need_cache = false;
  dir_iterator_manager_.PutWithFunc(
      ino, fh, dir_iterator,
      [&need_cache](const std::vector<DirIteratorSPtr>& vec) {
        if (vec.size() <= 1) {
          need_cache = true;
        }
      });

  ctx->need_cache = need_cache;

  return Status::OK();
}

Status LocalMetaSystem::ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh,
                                uint64_t offset, bool with_attr,
                                ReadDirHandler handler) {
  auto dir_iterator = dir_iterator_manager_.Get(ino, fh);
  CHECK(dir_iterator != nullptr) << "dir_iterator is null";

  dir_iterator->Remember(offset);

  while (true) {
    DirEntry entry;
    auto status = dir_iterator->GetValue(ctx, offset++, entry);
    if (!status.ok()) {
      if (status.IsNoData()) break;
      return status;
    }

    AttrEntry attr_entry;
    status = GetAttrEntry(ino, attr_entry);
    if (!status.ok()) return status;

    if (with_attr) entry.attr = meta::Helper::ToAttr(attr_entry);

    if (!handler(entry, offset)) {
      break;
    }
  }

  return Status::OK();
}

Status LocalMetaSystem::ReleaseDir(ContextSPtr, Ino ino, uint64_t fh) {
  dir_iterator_manager_.Delete(ino, fh);
  return Status::OK();
}

Status LocalMetaSystem::Link(ContextSPtr, Ino ino, Ino new_parent,
                             const std::string& new_name, Attr* attr) {
  const uint64_t now_ns = utils::TimestampNs();
  const uint32_t fs_id = fs_info_.fs_id();

  AttrEntry attr_entry;
  AttrEntry parent_attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    // get inode
    std::string value;
    std::string inode_key = MetaCodec::EncodeInodeKey(fs_id, ino);
    auto status = Get(inode_key, value);
    if (!status.ok()) return status;

    attr_entry = MetaCodec::DecodeInodeValue(value);
    CHECK(attr_entry.type() != pb::mds::FileType::DIRECTORY)
        << fmt::format("not file type, ino({}).", attr_entry.ino());

    attr_entry.set_nlink(attr_entry.nlink() + 1);
    attr_entry.set_ctime(std::max(attr_entry.ctime(), now_ns));
    attr_entry.set_mtime(std::max(attr_entry.mtime(), now_ns));
    AddParentIno(attr_entry, new_parent);
    attr_entry.set_version(attr_entry.version() + 1);

    // get parent inode
    std::string parent_inode_key = MetaCodec::EncodeInodeKey(fs_id, new_parent);
    status = Get(parent_inode_key, value);
    if (!status.ok()) return status;
    parent_attr_entry = MetaCodec::DecodeInodeValue(value);

    parent_attr_entry.set_ctime(std::max(parent_attr_entry.ctime(), now_ns));
    parent_attr_entry.set_mtime(std::max(parent_attr_entry.mtime(), now_ns));
    parent_attr_entry.set_version(parent_attr_entry.version() + 1);

    // create new_name dentry
    DentryEntry dentry_entry;
    dentry_entry.set_fs_id(fs_id);
    dentry_entry.set_ino(ino);
    dentry_entry.set_parent(new_parent);
    dentry_entry.set_name(new_name);
    dentry_entry.set_flag(0);
    dentry_entry.set_type(pb::mds::FileType::FILE);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, inode_key,
         MetaCodec::EncodeInodeValue(attr_entry)},
        {KeyValue::OpType::kPut,
         MetaCodec::EncodeDentryKey(fs_id, new_parent, new_name),
         MetaCodec::EncodeDentryValue(dentry_entry)},
        {KeyValue::OpType::kPut, parent_inode_key,
         MetaCodec::EncodeInodeValue(parent_attr_entry)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  *attr = meta::Helper::ToAttr(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::Unlink(ContextSPtr, Ino parent,
                               const std::string& name) {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t now_ns = utils::TimestampNs();

  AttrEntry attr_entry;
  AttrEntry parent_attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    // get parent inode key
    std::string value;
    std::string parent_inode_key = MetaCodec::EncodeInodeKey(fs_id, parent);
    auto status = Get(parent_inode_key, value);
    if (!status.ok()) return status;
    parent_attr_entry = MetaCodec::DecodeInodeValue(value);

    // update parent inode
    parent_attr_entry.set_ctime(std::max(parent_attr_entry.ctime(), now_ns));
    parent_attr_entry.set_mtime(std::max(parent_attr_entry.mtime(), now_ns));
    parent_attr_entry.set_version(parent_attr_entry.version() + 1);

    // get parent/name dentry
    std::string dentry_key = MetaCodec::EncodeDentryKey(fs_id, parent, name);
    status = Get(dentry_key, value);
    if (!status.ok()) return status;

    auto dentry_entry = MetaCodec::DecodeDentryValue(value);

    // get parent/name inode
    std::string inode_key =
        MetaCodec::EncodeInodeKey(fs_id, dentry_entry.ino());
    status = Get(inode_key, value);
    if (!status.ok()) return status;

    attr_entry = MetaCodec::DecodeInodeValue(value);
    CHECK(attr_entry.type() != pb::mds::FileType::DIRECTORY)
        << fmt::format("not file type, ino({}).", attr_entry.ino());

    attr_entry.set_nlink(attr_entry.nlink() - 1);
    attr_entry.set_ctime(std::max(attr_entry.ctime(), now_ns));
    DelParentIno(attr_entry, parent);
    attr_entry.set_version(attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs;
    if (attr_entry.nlink() == 0) {
      kvs.push_back({KeyValue::OpType::kDelete, inode_key, ""});

      // save delete file info
      kvs.push_back({KeyValue::OpType::kPut,
                     MetaCodec::EncodeDelFileKey(fs_id, dentry_entry.ino()),
                     MetaCodec::EncodeDelFileValue(attr_entry)});

    } else {
      kvs.push_back({KeyValue::OpType::kPut, inode_key,
                     MetaCodec::EncodeInodeValue(attr_entry)});
    }
    kvs.push_back({KeyValue::OpType::kDelete, dentry_key, ""});
    kvs.push_back({KeyValue::OpType::kPut, parent_inode_key,
                   MetaCodec::EncodeInodeValue(parent_attr_entry)});
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  if (attr_entry.nlink() == 0) UpdateFsUsage(0, -1, "unlink");

  return Status::OK();
}

Status LocalMetaSystem::Symlink(ContextSPtr, Ino parent,
                                const std::string& name, uint32_t uid,
                                uint32_t gid, const std::string& link,
                                Attr* attr) {
  const uint64_t now_ns = utils::TimestampNs();
  const uint32_t fs_id = fs_info_.fs_id();
  const Ino ino = GenFileIno();

  AttrEntry attr_entry;
  attr_entry.set_fs_id(fs_id);
  attr_entry.set_ino(ino);
  attr_entry.set_length(link.size());
  attr_entry.set_ctime(now_ns);
  attr_entry.set_mtime(now_ns);
  attr_entry.set_atime(now_ns);
  attr_entry.set_uid(uid);
  attr_entry.set_gid(gid);
  attr_entry.set_nlink(kFileMinLinkNum);
  attr_entry.set_mode(S_IFLNK | 0777);
  attr_entry.set_type(pb::mds::FileType::SYM_LINK);
  attr_entry.add_parents(parent);
  attr_entry.set_symlink(link);
  attr_entry.set_rdev(1);
  attr_entry.set_version(1);

  DentryEntry dentry_entry;
  dentry_entry.set_fs_id(fs_id);
  dentry_entry.set_ino(ino);
  dentry_entry.set_parent(parent);
  dentry_entry.set_name(name);
  dentry_entry.set_flag(0);
  dentry_entry.set_type(pb::mds::FileType::SYM_LINK);

  AttrEntry parent_attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    std::string value;
    const std::string parent_inode_key =
        MetaCodec::EncodeInodeKey(fs_id, parent);
    auto status = Get(parent_inode_key, value);
    if (!status.ok()) return status;
    CHECK(!value.empty()) << "not found parent inode.";

    parent_attr_entry = MetaCodec::DecodeInodeValue(value);
    parent_attr_entry.set_ctime(std::max(parent_attr_entry.ctime(), now_ns));
    parent_attr_entry.set_mtime(std::max(parent_attr_entry.mtime(), now_ns));
    parent_attr_entry.set_version(parent_attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, MetaCodec::EncodeInodeKey(fs_id, ino),
         MetaCodec::EncodeInodeValue(attr_entry)},
        {KeyValue::OpType::kPut,
         MetaCodec::EncodeDentryKey(fs_id, parent, name),
         MetaCodec::EncodeDentryValue(dentry_entry)},
        {KeyValue::OpType::kPut, parent_inode_key,
         MetaCodec::EncodeInodeValue(parent_attr_entry)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  UpdateFsUsage(0, 1, "symlink");

  *attr = meta::Helper::ToAttr(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::ReadLink(ContextSPtr, Ino ino, std::string* link) {
  const uint32_t fs_id = fs_info_.fs_id();

  // get inode
  AttrEntry attr_entry;
  auto status = GetAttrEntry(ino, attr_entry);
  if (!status.ok()) return status;

  CHECK(attr_entry.type() == pb::mds::FileType::SYM_LINK)
      << fmt::format("not symlink type, ino({}).", attr_entry.ino());

  *link = attr_entry.symlink();

  return Status::OK();
}

Status LocalMetaSystem::GetAttr(ContextSPtr, Ino ino, Attr* attr) {
  // get inode
  AttrEntry attr_entry;
  auto status = GetAttrEntry(ino, attr_entry);
  if (!status.ok()) return status;

  *attr = meta::Helper::ToAttr(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::SetAttr(ContextSPtr, Ino ino, int to_set,
                                const Attr& in_attr, Attr* out_attr) {
  const uint32_t fs_id = fs_info_.fs_id();

  AttrEntry attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    // get inode
    std::string value;
    std::string inode_key = MetaCodec::EncodeInodeKey(fs_id, ino);
    auto status = Get(inode_key, value);
    if (!status.ok()) return status;

    attr_entry = MetaCodec::DecodeInodeValue(value);

    if (to_set & kSetAttrMode) {
      attr_entry.set_mode(in_attr.mode);
    }
    if (to_set & kSetAttrUid) {
      attr_entry.set_uid(in_attr.uid);
    }
    if (to_set & kSetAttrGid) {
      attr_entry.set_gid(in_attr.gid);
    }

    struct timespec now;
    CHECK(clock_gettime(CLOCK_REALTIME, &now) == 0) << "get current time fail.";

    if (to_set & kSetAttrAtime) {
      attr_entry.set_atime(in_attr.atime);
    } else if (to_set & kSetAttrAtimeNow) {
      attr_entry.set_atime(ToTimestamp(now));
    }

    if (to_set & kSetAttrMtime) {
      attr_entry.set_mtime(in_attr.mtime);

    } else if (to_set & kSetAttrMtimeNow) {
      attr_entry.set_mtime(ToTimestamp(now));
    }

    if (to_set & kSetAttrCtime) {
      attr_entry.set_ctime(in_attr.ctime);
    } else {
      attr_entry.set_ctime(ToTimestamp(now));
    }

    if (to_set & kSetAttrSize) {
      attr_entry.set_length(in_attr.length);
    }

    if (to_set & kSetAttrFlags) {
      attr_entry.set_flags(in_attr.flags);
    }

    attr_entry.set_version(attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, inode_key,
         MetaCodec::EncodeInodeValue(attr_entry)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);

  *out_attr = meta::Helper::ToAttr(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::GetXattr(ContextSPtr, Ino ino, const std::string& name,
                                 std::string* value) {
  // get inode
  AttrEntry attr_entry;
  auto status = GetAttrEntry(ino, attr_entry);
  if (!status.ok()) return status;

  auto it = attr_entry.xattrs().find(name);
  if (it != attr_entry.xattrs().end()) *value = it->second;

  return Status::OK();
}

Status LocalMetaSystem::SetXattr(ContextSPtr, Ino ino, const std::string& name,
                                 const std::string& value, int) {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t now_ns = utils::TimestampNs();

  AttrEntry attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    // get inode
    std::string inode_value;
    std::string inode_key = MetaCodec::EncodeInodeKey(fs_id, ino);
    auto status = Get(inode_key, inode_value);
    if (!status.ok()) return status;

    attr_entry = MetaCodec::DecodeInodeValue(inode_value);

    (*attr_entry.mutable_xattrs())[name] = value;
    attr_entry.set_ctime(std::max(attr_entry.ctime(), now_ns));
    attr_entry.set_mtime(std::max(attr_entry.mtime(), now_ns));
    attr_entry.set_version(attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, inode_key,
         MetaCodec::EncodeInodeValue(attr_entry)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::RemoveXattr(ContextSPtr, Ino ino,
                                    const std::string& name) {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t now_ns = utils::TimestampNs();

  AttrEntry attr_entry;
  {
    utils::WriteLockGuard lk(lock_);

    // get inode
    std::string inode_key = MetaCodec::EncodeInodeKey(fs_id, ino);
    std::string inode_value;
    auto status = Get(inode_key, inode_value);
    if (!status.ok()) return status;

    attr_entry = MetaCodec::DecodeInodeValue(inode_value);

    (*attr_entry.mutable_xattrs()).erase(name);
    attr_entry.set_ctime(std::max(attr_entry.ctime(), now_ns));
    attr_entry.set_mtime(std::max(attr_entry.mtime(), now_ns));
    attr_entry.set_version(attr_entry.version() + 1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, inode_key,
         MetaCodec::EncodeInodeValue(attr_entry)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  PutInodeToCache(attr_entry);

  return Status::OK();
}

Status LocalMetaSystem::ListXattr(ContextSPtr, Ino ino,
                                  std::vector<std::string>* xattrs) {
  // get inode
  AttrEntry attr_entry;
  auto status = GetAttrEntry(ino, attr_entry);
  if (!status.ok()) return status;

  for (const auto& pair : attr_entry.xattrs()) {
    xattrs->push_back(pair.first);
  }

  return Status::OK();
}

Status LocalMetaSystem::Rename(ContextSPtr, Ino old_parent,
                               const std::string& old_name, Ino new_parent,
                               const std::string& new_name) {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t now_ns = utils::TimestampNs();

  const bool is_same_parent = (old_parent == new_parent);

  AttrEntry old_parent_attr_entry;
  DentryEntry old_dentry;
  AttrEntry old_attr_entry;

  AttrEntry new_parent_attr_entry;
  DentryEntry prev_new_dentry;
  AttrEntry prev_new_attr_entry;

  bool is_exist_new_dentry = false;
  {
    utils::WriteLockGuard lk(lock_);

    std::vector<KeyValue> kvs;

    // get old_parent inode
    std::string value;
    std::string old_parent_inode_key =
        MetaCodec::EncodeInodeKey(fs_id, old_parent);
    auto status = Get(old_parent_inode_key, value);
    if (!status.ok()) return status;
    old_parent_attr_entry = MetaCodec::DecodeInodeValue(value);

    // get old_parent/name dentry
    std::string old_dentry_key =
        MetaCodec::EncodeDentryKey(fs_id, old_parent, old_name);
    status = Get(old_dentry_key, value);
    if (!status.ok()) return status;
    old_dentry = MetaCodec::DecodeDentryValue(value);

    // get old_parent/name inode
    std::string old_inode_key =
        MetaCodec::EncodeInodeKey(fs_id, old_dentry.ino());
    status = Get(old_inode_key, value);
    if (!status.ok()) return status;
    old_attr_entry = MetaCodec::DecodeInodeValue(value);

    // get new_parent inode
    if (is_same_parent) {
      new_parent_attr_entry = old_parent_attr_entry;
    } else {
      std::string new_parent_inode_key =
          MetaCodec::EncodeInodeKey(fs_id, new_parent);
      status = Get(new_parent_inode_key, value);
      if (!status.ok()) return status;
      new_parent_attr_entry = MetaCodec::DecodeInodeValue(value);
    }

    // get new_parent/name dentry
    std::string new_dentry_key =
        MetaCodec::EncodeDentryKey(fs_id, new_parent, new_name);
    status = Get(new_dentry_key, value);
    if (status.ok()) {
      prev_new_dentry = MetaCodec::DecodeDentryValue(value);
    }
    is_exist_new_dentry = (prev_new_dentry.ino() != 0);
    if (is_exist_new_dentry) {
      // get pre new inode
      std::string prev_new_inode_key =
          MetaCodec::EncodeInodeKey(fs_id, prev_new_dentry.ino());
      status = Get(prev_new_inode_key, value);
      if (!status.ok()) return status;

      prev_new_attr_entry = MetaCodec::DecodeInodeValue(value);

      if (prev_new_dentry.type() == pb::mds::DIRECTORY) {
        // check new dentry is empty
        bool is_empty = false;
        status = CheckDirEmpty(prev_new_dentry.ino(), is_empty);
        if (!status.ok()) return status;
        if (!is_empty) {
          return Status::NotEmpty(fmt::format("new dentry({}/{}) is not empty.",
                                              new_parent, new_name));
        }

        // delete exist new inode
        kvs.push_back({
            KeyValue::OpType::kDelete,
            prev_new_inode_key,
        });

      } else {
        // update exist new inode nlink
        prev_new_attr_entry.set_nlink(prev_new_attr_entry.nlink() - 1);
        prev_new_attr_entry.set_ctime(
            std::max(prev_new_attr_entry.ctime(), now_ns));
        prev_new_attr_entry.set_mtime(
            std::max(prev_new_attr_entry.mtime(), now_ns));
        prev_new_attr_entry.set_version(prev_new_attr_entry.version() + 1);
        if (prev_new_attr_entry.nlink() <= 0) {
          // delete exist new inode
          kvs.push_back({
              KeyValue::OpType::kDelete,
              prev_new_inode_key,
          });

          // save delete file info
          kvs.push_back(
              {KeyValue::OpType::kPut,
               MetaCodec::EncodeDelFileKey(fs_id, prev_new_attr_entry.ino()),
               MetaCodec::EncodeDelFileValue(prev_new_attr_entry)});

        } else {
          // update exist new inode attr
          kvs.push_back({KeyValue::OpType::kPut, prev_new_inode_key,
                         MetaCodec::EncodeInodeValue(prev_new_attr_entry)});
        }
      }
    }

    // delete old dentry
    kvs.push_back({
        KeyValue::OpType::kDelete,
        old_dentry_key,
    });

    // add new dentry
    DentryEntry new_dentry;
    new_dentry.set_fs_id(fs_id);
    new_dentry.set_name(new_name);
    new_dentry.set_ino(old_dentry.ino());
    new_dentry.set_type(old_dentry.type());
    new_dentry.set_parent(new_parent);

    kvs.push_back({KeyValue::OpType::kPut, new_dentry_key,
                   MetaCodec::EncodeDentryValue(new_dentry)});

    // update old inode attr
    old_attr_entry.set_ctime(std::max(old_attr_entry.ctime(), now_ns));
    if (!is_same_parent) {
      DelParentIno(old_attr_entry, old_parent);
      AddParentIno(old_attr_entry, new_parent);
    }
    old_attr_entry.set_version(old_attr_entry.version() + 1);

    kvs.push_back({KeyValue::OpType::kPut, old_inode_key,
                   MetaCodec::EncodeInodeValue(old_attr_entry)});

    // update old parent inode attr
    old_parent_attr_entry.set_ctime(
        std::max(old_parent_attr_entry.ctime(), now_ns));
    old_parent_attr_entry.set_mtime(
        std::max(old_parent_attr_entry.mtime(), now_ns));
    if (old_dentry.type() == pb::mds::FileType::DIRECTORY &&
        (!is_same_parent || (is_same_parent && is_exist_new_dentry))) {
      old_parent_attr_entry.set_nlink(old_parent_attr_entry.nlink() - 1);
    }
    old_parent_attr_entry.set_version(old_parent_attr_entry.version() + 1);

    kvs.push_back({KeyValue::OpType::kPut, old_parent_inode_key,
                   MetaCodec::EncodeInodeValue(old_parent_attr_entry)});

    if (!is_same_parent) {
      // update new parent inode attr
      new_parent_attr_entry.set_ctime(
          std::max(new_parent_attr_entry.ctime(), now_ns));
      new_parent_attr_entry.set_mtime(
          std::max(new_parent_attr_entry.mtime(), now_ns));
      if (new_dentry.type() == pb::mds::FileType::DIRECTORY &&
          !is_exist_new_dentry)
        new_parent_attr_entry.set_nlink(new_parent_attr_entry.nlink() + 1);
      new_parent_attr_entry.set_version(new_parent_attr_entry.version() + 1);

      kvs.push_back({KeyValue::OpType::kPut,
                     MetaCodec::EncodeInodeKey(fs_id, new_parent),
                     MetaCodec::EncodeInodeValue(new_parent_attr_entry)});
    }

    // write to leveldb
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  // update inode cache
  PutInodeToCache(old_parent_attr_entry);
  if (!is_same_parent) PutInodeToCache(new_parent_attr_entry);
  PutInodeToCache(old_attr_entry);

  if (prev_new_dentry.type() == pb::mds::DIRECTORY) {
    DeleteInodeFromCache(prev_new_dentry.ino());
  } else {
    PutInodeToCache(prev_new_attr_entry);
  }

  // update fs quota
  if (is_exist_new_dentry) {
    int64_t fs_delta_bytes = 0;
    if (prev_new_attr_entry.type() == pb::mds::FileType::FILE &&
        prev_new_attr_entry.nlink() == 0) {
      fs_delta_bytes -= prev_new_attr_entry.length();
    }
    UpdateFsUsage(fs_delta_bytes, -1, "rename");
  }

  return Status::OK();
}

Status LocalMetaSystem::StatFs(ContextSPtr, Ino, FsStat* fs_stat) {
  auto fs_quota = GetFsQuota();

  fs_stat->max_bytes = fs_quota.max_bytes();
  fs_stat->max_inodes = fs_quota.max_inodes();

  fs_stat->used_bytes = fs_quota.used_bytes();
  fs_stat->used_inodes = fs_quota.used_inodes();

  return Status::OK();
}

Status LocalMetaSystem::GetFsInfo(ContextSPtr, FsInfo* fs_info) {
  fs_info->name = fs_info_.fs_name();
  fs_info->id = fs_info_.fs_id();
  fs_info->chunk_size = fs_info_.chunk_size();
  fs_info->block_size = fs_info_.block_size();
  fs_info->uuid = fs_info_.uuid();
  fs_info->status = meta::Helper::ToFsStatus(fs_info_.status());

  fs_info->storage_info.store_type =
      meta::Helper::ToStoreType(fs_info_.fs_type());
  if (fs_info->storage_info.store_type == StoreType::kS3) {
    CHECK(fs_info_.extra().has_s3_info())
        << "fs type is S3, but s3 info is not set";

    fs_info->storage_info.s3_info =
        meta::Helper::ToS3Info(fs_info_.extra().s3_info());

  } else if (fs_info->storage_info.store_type == StoreType::kRados) {
    CHECK(fs_info_.extra().has_rados_info())
        << "fs type is Rados, but rados info is not set";

    fs_info->storage_info.rados_info =
        meta::Helper::ToRadosInfo(fs_info_.extra().rados_info());

  } else if (fs_info->storage_info.store_type == StoreType::kLocalFile) {
    CHECK(fs_info_.extra().has_file_info())
        << "fs type is LocalFile, but file info is not set";

    fs_info->storage_info.file_info =
        meta::Helper::ToLocalFileInfo(fs_info_.extra().file_info());

  } else {
    return Status::InvalidParam("unknown fs type");
  }

  return Status::OK();
}

bool LocalMetaSystem::GetDescription(Json::Value& value) {
  // fs info
  Json::Value fs_info;

  auto fs_info_entry = fs_info_;
  fs_info["id"] = fs_info_entry.fs_id();
  fs_info["name"] = fs_info_entry.fs_name();
  fs_info["owner"] = fs_info_entry.owner();
  fs_info["block_size"] = fs_info_entry.block_size();
  fs_info["chunk_size"] = fs_info_entry.chunk_size();
  fs_info["capacity"] = fs_info_entry.capacity();
  fs_info["create_time_s"] = fs_info_entry.create_time_s();
  fs_info["last_update_time_ns"] = fs_info_entry.last_update_time_ns();
  fs_info["recycle_time"] = fs_info_entry.recycle_time_hour();
  fs_info["s3_endpoint"] = fs_info_entry.extra().s3_info().endpoint();
  fs_info["s3_bucket"] = fs_info_entry.extra().s3_info().bucketname();
  fs_info["rados_mon_host"] = fs_info_entry.extra().rados_info().mon_host();
  fs_info["rados_pool_name"] = fs_info_entry.extra().rados_info().pool_name();
  fs_info["rados_user_name"] = fs_info_entry.extra().rados_info().user_name();
  fs_info["rados_cluster_name"] =
      fs_info_entry.extra().rados_info().cluster_name();

  value["fs_info"] = fs_info;

  return true;
}

void LocalMetaSystem::SetFsStorageInfo(mds::FsInfoEntry& fs_info,
                                       const std::string& storage_info) {
  LOG(INFO) << "generate storage info from: " << storage_info;

  auto storage_info_map = ParseStorageInfo(storage_info);

  if (storage_info_map.empty()) {  // use default storage path
    fs_info.set_fs_type(pb::mds::FsType::LOCALFILE);
    auto* file_info = fs_info.mutable_extra()->mutable_file_info();
    auto data_path = fmt::format(
        "{}/{}", dingofs::Helper::ExpandPath(kDefaultDataDir), fs_name_);
    LOG(INFO) << "use default local storage data path: " << data_path;

    file_info->set_path(data_path);

    return;
  }

  auto storage_it = storage_info_map.find("storage");
  if (storage_it == storage_info_map.end()) {
    LOG(FATAL) << "storage info missing storage type, storage_info: "
               << storage_info_;
  }

  if (storage_it->second == "file") {  // local storage
    for (const auto& param : kLocalParams) {
      auto it = storage_info_map.find(param);
      if (it == storage_info_map.end() || it->second.empty()) {
        LOG(FATAL) << "Localfile storage missing required param: " << param
                   << ", storage_info: " << storage_info;
      }
    }

    fs_info.set_fs_type(pb::mds::FsType::LOCALFILE);
    auto* file_info = fs_info.mutable_extra()->mutable_file_info();

    auto data_path = fmt::format("{}/{}", storage_info_map["path"], fs_name_);
    file_info->set_path(data_path);

  } else if (storage_it->second == "s3") {  // s3 storage
    for (const auto& param : kS3Params) {
      auto it = storage_info_map.find(param);
      if (it == storage_info_map.end() || it->second.empty()) {
        LOG(FATAL) << "S3 storage missing required param: " << param
                   << ", storage_info: " << storage_info;
      }
    }

    fs_info.set_fs_type(pb::mds::FsType::S3);
    auto* s3_info = fs_info.mutable_extra()->mutable_s3_info();
    s3_info->set_ak(storage_info_map["ak"]);
    s3_info->set_sk(storage_info_map["sk"]);
    s3_info->set_endpoint(storage_info_map["endpoint"]);
    s3_info->set_bucketname(storage_info_map["bucketname"]);

  } else if (storage_it->second == "rados") {  // rados storage
    for (const auto& param : kRadosParams) {
      auto it = storage_info_map.find(param);
      if (it == storage_info_map.end() || it->second.empty()) {
        LOG(FATAL) << "Rados storage missing required param: " << param
                   << ", storage_info: " << storage_info;
      }
    }

    fs_info.set_fs_type(pb::mds::FsType::RADOS);
    auto* rados_info = fs_info.mutable_extra()->mutable_rados_info();
    rados_info->set_key(storage_info_map["key"]);
    rados_info->set_user_name(storage_info_map["username"]);
    rados_info->set_mon_host(storage_info_map["mon"]);
    rados_info->set_pool_name(storage_info_map["pool"]);
    rados_info->set_cluster_name(storage_info_map["cluster"]);
  } else {
    LOG(FATAL) << "unknown storage type: " << storage_it->second
               << ", storage_info: " << storage_info;
  }
}

mds::FsInfoEntry LocalMetaSystem::GenFsInfo() {
  mds::FsInfoEntry fs_info;

  fs_info.set_fs_id(kFsId);
  fs_info.set_fs_name(fs_name_);
  fs_info.set_root_ino(kRootIno);
  fs_info.set_status(pb::mds::FsStatus::NORMAL);
  fs_info.set_block_size(kBlockSize);
  fs_info.set_chunk_size(kChunkSize);
  fs_info.set_enable_sum_in_dir(false);
  fs_info.set_owner(std::getenv("USER"));
  fs_info.set_capacity(INT64_MAX);
  fs_info.set_recycle_time_hour(24);
  fs_info.set_create_time_s(utils::Timestamp());
  fs_info.set_last_update_time_ns(utils::TimestampNs());
  fs_info.set_uuid(utils::UUIDGenerator::GenerateUUID());

  fs_info.set_version(1);

  fs_info.set_status(pb::mds::FsStatus::NORMAL);

  SetFsStorageInfo(fs_info, storage_info_);

  return fs_info;
}

Status LocalMetaSystem::InitFsInfo() {
  std::string value;
  auto status = Get(MetaCodec::EncodeFsKey(fs_name_), value);
  if (!status.ok() && !status.IsNotFound()) {
    return status;
  }

  if (!status.IsNotFound()) {
    fs_info_ = MetaCodec::DecodeFsValue(value);
    return Status::OK();
  }

  // not found, create new fs_info
  fs_info_ = GenFsInfo();

  // write to leveldb
  std::vector<KeyValue> kvs = {
      {KeyValue::OpType::kPut, MetaCodec::EncodeFsKey(fs_name_),
       MetaCodec::EncodeFsValue(fs_info_)},
  };

  status = Put(kvs);
  if (!status.ok()) return status;

  return Status::OK();
}

Status LocalMetaSystem::InitFsQuota() {
  std::string value;
  auto status = Get(MetaCodec::EncodeFsQuotaKey(fs_info_.fs_id()), value);
  if (!status.ok() && !status.IsNotFound()) return status;
  if (status.ok()) {
    fs_quota_ = MetaCodec::DecodeFsQuotaValue(value);

  } else {
    fs_quota_.set_max_bytes(INT64_MAX);
    fs_quota_.set_max_inodes(INT64_MAX);
    fs_quota_.set_used_bytes(0);
    fs_quota_.set_used_inodes(1);

    // write to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kPut, MetaCodec::EncodeFsQuotaKey(fs_info_.fs_id()),
         MetaCodec::EncodeFsQuotaValue(fs_quota_)},
    };
    status = Put(kvs);
    if (!status.ok()) return status;
  }

  return Status::OK();
}

Status LocalMetaSystem::InitRoot() {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t now_ns = utils::TimestampNs();

  AttrEntry attr_entry;
  attr_entry.set_fs_id(fs_id);
  attr_entry.set_ino(kRootIno);
  attr_entry.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR |
                      S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);

  attr_entry.set_length(4096);
  attr_entry.set_ctime(now_ns);
  attr_entry.set_mtime(now_ns);
  attr_entry.set_atime(now_ns);
  attr_entry.set_uid(1008);
  attr_entry.set_gid(1008);
  attr_entry.set_nlink(kEmptyDirMinLinkNum);
  attr_entry.set_type(pb::mds::FileType::DIRECTORY);
  attr_entry.set_version(1);

  DentryEntry dentry_entry;
  dentry_entry.set_fs_id(fs_id);
  dentry_entry.set_ino(kRootIno);
  dentry_entry.set_parent(kRootParentIno);
  dentry_entry.set_name("/");
  dentry_entry.set_flag(0);
  dentry_entry.set_type(pb::mds::FileType::DIRECTORY);

  // get root inode
  AttrEntry old_attr_entry;
  auto status = GetAttrEntry(kRootIno, old_attr_entry);
  if (status.ok()) return Status::OK();

  // write to leveldb
  std::vector<KeyValue> kvs = {
      {KeyValue::OpType::kPut, MetaCodec::EncodeInodeKey(fs_id, kRootIno),
       MetaCodec::EncodeInodeValue(attr_entry)},
      {KeyValue::OpType::kPut,
       MetaCodec::EncodeDentryKey(fs_id, kRootParentIno, "/"),
       MetaCodec::EncodeDentryValue(dentry_entry)},
  };
  status = Put(kvs);
  if (!status.ok()) return status;

  PutInodeToCache(attr_entry);

  return Status::OK();
}

bool LocalMetaSystem::InitIdGenerators() {
  CHECK(db_ != nullptr) << "leveldb is null.";

  static const std::string kInoIdGeneratorName = "InoGenerator";
  static const int64_t kInoStartId = 100000000;
  static const int kInoIdBatchSize = 1000;

  static const std::string kSliceIdGeneratorName = "SliceIdGenerator";
  static const int64_t kSliceIdStartId = 100000000;
  static const int kSliceIdBatchSize = 1000;

  ino_generator_ = LevelIdGenerator::New(db_, kInoIdGeneratorName, kInoStartId,
                                         kInoIdBatchSize);
  if (!ino_generator_->Init()) {
    LOG(ERROR) << "init ino generator fail.";
    return false;
  }

  slice_id_generator_ = LevelIdGenerator::New(
      db_, kSliceIdGeneratorName, kSliceIdStartId, kSliceIdBatchSize);
  if (!slice_id_generator_->Init()) {
    LOG(ERROR) << "init slice id generator fail.";
    return false;
  }

  return true;
}

bool LocalMetaSystem::InitCrontab() {
  // add clean expired crontab
  crontab_configs_.push_back({
      "CLEAN_DELFILE",
      kCleanDelfileIntervalS * 1000,
      true,
      [this](void*) { this->CleanDelfile(); },
  });

  crontab_manager_.AddCrontab(crontab_configs_);

  return true;
}

Ino LocalMetaSystem::GenDirIno() {
  Ino ino;
  CHECK(ino_generator_->GenID(2, ino)) << "generate dir ino fail.";

  // ensure odd number for dir inode
  return (ino & 1) ? ino : (ino + 1);
}

Ino LocalMetaSystem::GenFileIno() {
  Ino ino;
  CHECK(ino_generator_->GenID(2, ino)) << "generate file ino fail.";

  // ensure even number for file inode
  return (ino & 1) ? (ino + 1) : ino;
}

Status LocalMetaSystem::GetAttrEntry(Ino ino, AttrEntry& attr_entry) {
  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    attr_entry = inode->ToAttrEntry();
    return Status::OK();
  }

  {
    utils::ReadLockGuard lk(lock_);

    std::string value;
    auto status = Get(MetaCodec::EncodeInodeKey(fs_info_.fs_id(), ino), value);
    if (!status.ok()) return status;

    attr_entry = MetaCodec::DecodeInodeValue(value);
  }

  return Status::OK();
}

Status LocalMetaSystem::GetDentries(Ino parent,
                                    std::vector<DentryEntry>& dentries) {
  auto* iter = db_->NewIterator(leveldb::ReadOptions());

  iter->Seek(MetaCodec::EncodeInodeKey(fs_info_.fs_id(), parent));
  if (iter->Valid()) {
    CHECK(MetaCodec::IsInodeKey(iter->key().ToString())) << "not inode key.";
    iter->Next();  // skip inode key
  }

  while (iter->Valid()) {
    if (!MetaCodec::IsDentryKey(iter->key().ToString())) {
      break;
    }

    dentries.push_back(MetaCodec::DecodeDentryValue(iter->value().ToString()));

    iter->Next();
  }

  delete iter;

  return Status::OK();
}

Status LocalMetaSystem::CheckDirEmpty(Ino ino, bool& is_empty) {
  auto* iter = db_->NewIterator(leveldb::ReadOptions());
  iter->Seek(MetaCodec::EncodeInodeKey(fs_info_.fs_id(), ino));

  if (!iter->Valid() || !MetaCodec::IsInodeKey(iter->key().ToString())) {
    LOG(ERROR) << fmt::format("[meta.fs] check dir empty, not found inode {}.",
                              ino);
    delete iter;
    return Status::NotFound("not found inode.");
  }

  uint32_t fs_id;
  Ino temp_ino;
  MetaCodec::DecodeInodeKey(iter->key().ToString(), fs_id, temp_ino);
  if (temp_ino != ino) {
    LOG(ERROR) << fmt::format("[meta.fs] decode ino not match, {}!={}.",
                              temp_ino, ino);
    delete iter;
    return Status::NotFound("not found inode.");
  }

  iter->Next();  // skip inode key

  if (iter->Valid() && MetaCodec::IsDentryKey(iter->key().ToString())) {
    is_empty = false;
  } else {
    is_empty = true;
  }

  delete iter;

  return Status::OK();
}

Status LocalMetaSystem::CleanChunk(Ino ino) {
  const uint32_t fs_id = fs_info_.fs_id();

  // scan chunk keys
  std::vector<KeyValue> delete_kvs;
  auto* iter = db_->NewIterator(leveldb::ReadOptions());
  iter->Seek(MetaCodec::EncodeChunkKey(fs_id, ino, 0));

  while (iter->Valid()) {
    if (!MetaCodec::IsChunkKey(iter->key().ToString())) break;

    uint32_t temp_fs_id;
    Ino temp_ino;
    uint64_t chunk_index;
    MetaCodec::DecodeChunkKey(iter->key().ToString(), temp_fs_id, temp_ino,
                              chunk_index);
    if (temp_ino != ino) break;

    delete_kvs.push_back({KeyValue::OpType::kDelete, iter->key().ToString()});

    iter->Next();
  }

  // write delete chunk keys to leveldb
  auto status = Put(delete_kvs);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs] clean chunk fail, ino({}), {}.", ino,
                              status.ToString());
  }

  delete iter;

  return status;
}

void LocalMetaSystem::CleanDelfile() {
  static std::atomic<bool> is_running{false};
  if (is_running.exchange(true)) return;

  DoCleanDelfile();

  is_running.store(false);
}

Status LocalMetaSystem::DoCleanDelfile() {
  const uint32_t fs_id = fs_info_.fs_id();

  // scan del file keys
  std::vector<std::string> keys;
  auto* iter = db_->NewIterator(leveldb::ReadOptions());
  iter->Seek(MetaCodec::EncodeDelFileKey(fs_id, 0));

  while (iter->Valid()) {
    if (!MetaCodec::IsDelFileKey(iter->key().ToString())) break;

    keys.push_back(iter->key().ToString());

    iter->Next();
  }
  delete iter;

  LOG(INFO) << fmt::format("[meta.fs] start clean del file, count({}).",
                           keys.size());

  for (const auto& key : keys) {
    uint32_t temp_fs_id;
    Ino ino;
    MetaCodec::DecodeDelFileKey(key, temp_fs_id, ino);

    // delete chunk
    auto status = CleanChunk(ino);
    if (!status.ok()) continue;

    // write delete del file key to leveldb
    std::vector<KeyValue> kvs = {
        {KeyValue::OpType::kDelete, key},
    };
    status = Put(kvs);
    if (!status.ok()) {
      LOG(ERROR) << fmt::format(
          "[meta.fs] clean del file key fail, ino({}), {}.", ino,
          status.ToString());
    }
  }

  return Status::OK();
}

void LocalMetaSystem::PutInodeToCache(const AttrEntry& attr_entry) {
  inode_cache_->Put(attr_entry.ino(), attr_entry);
}

void LocalMetaSystem::DeleteInodeFromCache(Ino ino) {
  inode_cache_->Delete(ino);
}

meta::InodeSPtr LocalMetaSystem::GetInodeFromCache(Ino ino) {
  return inode_cache_->Get(ino);
}

void LocalMetaSystem::UpdateFsUsage(int64_t byte_delta, int64_t inode_delta,
                                    const std::string& reason) {
  utils::WriteLockGuard lk(fs_quota_lock_);

  fs_quota_.set_used_bytes(fs_quota_.used_bytes() + byte_delta);
  fs_quota_.set_used_inodes(fs_quota_.used_inodes() + inode_delta);

  // write to leveldb
  std::vector<KeyValue> kvs = {
      {KeyValue::OpType::kPut, MetaCodec::EncodeFsQuotaKey(fs_info_.fs_id()),
       MetaCodec::EncodeFsQuotaValue(fs_quota_)},
  };
  auto status = Put(kvs);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.fs] update fs quota fail, reason({}), byte_delta({}), "
        "inode_delta({}), {}.",
        reason, byte_delta, inode_delta, status.ToString());
  }
}

mds::QuotaEntry LocalMetaSystem::GetFsQuota() {
  utils::ReadLockGuard lk(fs_quota_lock_);

  return fs_quota_;
}

bool LocalMetaSystem::OpenLevelDB(const std::string& db_path) {
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, db_path, &db_);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[local] init leveldb fail, {} {}.", db_path,
                              status.ToString());
    return false;
  }

  return true;
}

void LocalMetaSystem::CloseLevelDB() {
  LOG(INFO) << "[meta.fs] close leveldb.";
  delete db_;
  db_ = nullptr;
}

Status LocalMetaSystem::Get(const std::string& key, std::string& value) {
  auto status = db_->Get(leveldb::ReadOptions(), key, &value);
  if (!status.ok()) {
    if (status.IsNotFound()) return Status::NotFound("not found");
    return Status::IoError(fmt::format("get fail, {}.", status.ToString()));
  }

  return Status::OK();
}

Status LocalMetaSystem::Put(std::vector<KeyValue>& kvs) {
  leveldb::WriteBatch batch;

  for (const auto& kv : kvs) {
    if (kv.opt_type == KeyValue::OpType::kPut) {
      batch.Put(kv.key, kv.value);
    } else if (kv.opt_type == KeyValue::OpType::kDelete) {
      batch.Delete(kv.key);
    }
  }

  auto status = db_->Write(leveldb::WriteOptions(), &batch);
  if (!status.ok()) {
    return Status::IoError(fmt::format("put fail, {}.", status.ToString()));
  }

  return Status::OK();
}

}  // namespace local
}  // namespace vfs
}  // namespace client
}  // namespace dingofs