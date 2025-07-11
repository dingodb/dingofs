/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Mon Sept 1 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_
#define DINGOFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_

#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "dingofs/cachegroup.pb.h"
#include "dingofs/mds.pb.h"
#include "dingofs/topology.pb.h"
#include "stub/common/config.h"
#include "stub/common/metacache_struct.h"
#include "stub/rpcclient/base_client.h"

namespace dingofs {
namespace stub {
namespace rpcclient {

class RPCExcutorRetryPolicy {
 public:
  RPCExcutorRetryPolicy()
      : retryOpt_(), currentWorkingMDSAddrIndex_(0), cntlID_(1) {}

  void SetOption(const common::MdsOption::RpcRetryOption& option) {
    retryOpt_ = option;
  }
  using RPCFunc = std::function<int(int addrindex, uint64_t rpctimeoutMS,
                                    brpc::Channel*, brpc::Controller*)>;
  /**
   * 将client与mds的重试相关逻辑抽离
   * @param: task为当前要进行的具体rpc任务
   * @param: maxRetryTimeMS是当前执行最大的重试时间
   * @return: 返回当前RPC的结果
   */
  int DoRPCTask(RPCFunc task, uint64_t maxRetryTimeMS);

  /**
   * 测试使用: 设置当前正在服务的mdsindex
   */
  void SetCurrentWorkIndex(int index) {
    currentWorkingMDSAddrIndex_.store(index);
  }

  /**
   * 测试使用：获取当前正在服务的mdsindex
   */
  int GetCurrentWorkIndex() const { return currentWorkingMDSAddrIndex_.load(); }

 private:
  /**
   * rpc失败需要重试，根据cntl返回的不同的状态，确定应该做什么样的预处理。
   * 主要做了以下几件事：
   * 1. 如果上一次的RPC是超时返回，那么执行rpc 超时指数退避逻辑
   * 2. 如果上一次rpc返回not connect等返回值，会主动触发切换mds地址重试
   * 3. 更新重试信息，比如在当前mds上连续重试的次数
   * @param[in]: status为当前rpc的失败返回的状态
   * @param normalRetryCount The total count of normal retry
   * @param[in][out]: curMDSRetryCount当前mds节点上的重试次数，如果切换mds
   *             该值会被重置为1.
   * @param[in]: curRetryMDSIndex代表当前正在重试的mds索引
   * @param[out]: lastWorkingMDSIndex上一次正在提供服务的mds索引
   * @param[out]: timeOutMS根据status对rpctimeout进行调整
   *
   * @return: 返回下一次重试的mds索引
   */
  int PreProcessBeforeRetry(int status, bool retryUnlimit,
                            uint64_t* normalRetryCount,
                            uint64_t* curMDSRetryCount, int curRetryMDSIndex,
                            int* lastWorkingMDSIndex, uint64_t* timeOutMS);
  /**
   * 执行rpc发送任务
   * @param[in]: mdsindex为mds对应的地址索引
   * @param[in]: rpcTimeOutMS是rpc超时时间
   * @param[in]: task为待执行的任务
   * @return: channel获取成功则返回0，否则-1
   */
  int ExcuteTask(int mdsindex, uint64_t rpcTimeOutMS,
                 RPCExcutorRetryPolicy::RPCFunc task);
  /**
   * 根据输入状态获取下一次需要重试的mds索引，mds切换逻辑：
   * 记录三个状态：curRetryMDSIndex、lastWorkingMDSIndex、
   *             currentWorkingMDSIndex
   * 1. 开始的时候curRetryMDSIndex = currentWorkingMDSIndex
   *            lastWorkingMDSIndex = currentWorkingMDSIndex
   * 2.
   * 如果rpc失败，会触发切换curRetryMDSIndex，如果这时候lastWorkingMDSIndex
   *    与currentWorkingMDSIndex相等，这时候会顺序切换到下一个mds索引，
   *    如果lastWorkingMDSIndex与currentWorkingMDSIndex不相等，那么
   *    说明有其他接口更新了currentWorkingMDSAddrIndex_，那么本次切换
   *    直接切换到currentWorkingMDSAddrIndex_
   * @param[in]: needChangeMDS表示当前外围需不需要切换mds，这个值由
   *              PreProcessBeforeRetry函数确定
   * @param[in]: currentRetryIndex为当前正在重试的mds索引
   * @param[in][out]:
   * lastWorkingindex为上一次正在服务的mds索引，正在重试的mds
   *              与正在服务的mds索引可能是不同的mds。
   * @return: 返回下一次要重试的mds索引
   */
  int GetNextMDSIndex(bool needChangeMDS, int currentRetryIndex,
                      int* lastWorkingindex);
  /**
   * 根据输入参数，决定是否继续重试，重试退出条件是重试时间超出最大允许时间
   * IO路径上和非IO路径上的重试时间不一样，非IO路径的重试时间由配置文件的
   * mdsMaxRetryMS参数指定，IO路径为无限循环重试。
   * @param[in]: startTimeMS
   * @param[in]: maxRetryTimeMS为最大重试时间
   * @return:需要继续重试返回true， 否则返回false
   */
  bool GoOnRetry(uint64_t startTimeMS, uint64_t maxRetryTimeMS);

  /**
   * 递增controller id并返回id
   */
  uint64_t GetLogId() {
    return cntlID_.fetch_add(1, std::memory_order_relaxed);
  }

 private:
  // 执行rpc时必要的配置信息
  common::MdsOption::RpcRetryOption retryOpt_;

  // 记录上一次重试过的leader信息
  std::atomic<int> currentWorkingMDSAddrIndex_;

  // controller id，用于trace整个rpc IO链路
  // 这里直接用uint64即可，在可预测的范围内，不会溢出
  std::atomic<uint64_t> cntlID_;
};

class MdsClient {
 public:
  MdsClient() = default;
  virtual ~MdsClient() = default;

  virtual pb::mds::FSStatusCode Init(const common::MdsOption& mdsOpt,
                                     MDSBaseClient* baseclient) = 0;

  virtual pb::mds::FSStatusCode MountFs(const std::string& fsName,
                                        const pb::mds::Mountpoint& mountPt,
                                        pb::mds::FsInfo* fsInfo) = 0;

  virtual pb::mds::FSStatusCode UmountFs(
      const std::string& fsName, const pb::mds::Mountpoint& mountPt) = 0;

  virtual pb::mds::FSStatusCode GetFsInfo(const std::string& fsName,
                                          pb::mds::FsInfo* fsInfo) = 0;

  virtual pb::mds::FSStatusCode GetFsInfo(uint32_t fsId,
                                          pb::mds::FsInfo* fsInfo) = 0;

  virtual bool GetMetaServerInfo(
      const common::PeerAddr& addr,
      common::CopysetPeerInfo<common::MetaserverID>* metaserverInfo) = 0;

  virtual bool GetMetaServerListInCopysets(
      const common::LogicPoolID& logicalpooid,
      const std::vector<common::CopysetID>& copysetidvec,
      std::vector<common::CopysetInfo<common::MetaserverID>>* cpinfoVec) = 0;

  virtual bool CreatePartition(
      uint32_t fsid, uint32_t count,
      std::vector<pb::common::PartitionInfo>* partitionInfos) = 0;

  virtual bool GetCopysetOfPartitions(
      const std::vector<uint32_t>& partitionIDList,
      std::map<uint32_t, pb::mds::topology::Copyset>* copysetMap) = 0;

  virtual bool ListPartition(
      uint32_t fsID,
      std::vector<pb::common::PartitionInfo>* partitionInfos) = 0;

  virtual bool AllocOrGetMemcacheCluster(
      uint32_t fsId, pb::mds::topology::MemcacheClusterInfo* cluster) = 0;

  virtual pb::mds::FSStatusCode AllocS3ChunkId(uint32_t fsId, uint32_t idNum,
                                               uint64_t* chunkId) = 0;

  virtual pb::mds::FSStatusCode RefreshSession(
      const std::vector<pb::mds::topology::PartitionTxId>& txIds,
      std::vector<pb::mds::topology::PartitionTxId>* latestTxIdList,
      const std::string& fsName, const pb::mds::Mountpoint& mountpoint) = 0;

  virtual pb::mds::FSStatusCode GetLatestTxId(
      uint32_t fsId, std::vector<pb::mds::topology::PartitionTxId>* txIds) = 0;

  virtual pb::mds::FSStatusCode GetLatestTxIdWithLock(
      uint32_t fsId, const std::string& fsName, const std::string& uuid,
      std::vector<pb::mds::topology::PartitionTxId>* txIds,
      uint64_t* sequence) = 0;

  virtual pb::mds::FSStatusCode CommitTx(
      const std::vector<pb::mds::topology::PartitionTxId>& txIds) = 0;

  virtual pb::mds::FSStatusCode CommitTxWithLock(
      const std::vector<pb::mds::topology::PartitionTxId>& txIds,
      const std::string& fsName, const std::string& uuid,
      uint64_t sequence) = 0;

  // set filesystem runtime statistics
  virtual pb::mds::FSStatusCode SetFsStats(
      const std::string& fsName, const pb::mds::FsStatsData& fsStatsData) = 0;

  virtual pb::mds::cachegroup::CacheGroupErrCode RegisterCacheGroupMember(
      uint64_t old_id, uint64_t* member_id) = 0;

  virtual pb::mds::cachegroup::CacheGroupErrCode AddCacheGroupMember(
      const std::string& group_name,
      const pb::mds::cachegroup::CacheGroupMember& member) = 0;

  virtual pb::mds::cachegroup::CacheGroupErrCode LoadCacheGroupMembers(
      const std::string& group_name,
      std::vector<pb::mds::cachegroup::CacheGroupMember>* members) = 0;

  virtual pb::mds::cachegroup::CacheGroupErrCode ReweightCacheGroupMember(
      const std::string& group_name, uint64_t member_id, uint32_t weight) = 0;

  virtual pb::mds::cachegroup::CacheGroupErrCode SendCacheGroupHeartbeat(
      const std::string& group_name, uint64_t member_id,
      const pb::mds::cachegroup::HeartbeatRequest::Statistic& stat) = 0;
};

class MdsClientImpl : public MdsClient {
 public:
  MdsClientImpl() = default;

  pb::mds::FSStatusCode Init(const common::MdsOption& mdsOpt,
                             MDSBaseClient* baseclient) override;

  pb::mds::FSStatusCode MountFs(const std::string& fsName,
                                const pb::mds::Mountpoint& mountPt,
                                pb::mds::FsInfo* fsInfo) override;

  pb::mds::FSStatusCode UmountFs(const std::string& fsName,
                                 const pb::mds::Mountpoint& mountPt) override;

  pb::mds::FSStatusCode GetFsInfo(const std::string& fs_name,
                                  pb::mds::FsInfo* fs_info) override;

  pb::mds::FSStatusCode GetFsInfo(uint32_t fs_id,
                                  pb::mds::FsInfo* fs_info) override;

  bool GetMetaServerInfo(
      const common::PeerAddr& addr,
      common::CopysetPeerInfo<common::MetaserverID>* metaserver_info) override;

  bool GetMetaServerListInCopysets(
      const common::LogicPoolID& logicalpooid,
      const std::vector<common::CopysetID>& copysetidvec,
      std::vector<common::CopysetInfo<common::MetaserverID>>* copyset_infos)
      override;

  bool CreatePartition(
      uint32_t fs_id, uint32_t count,
      std::vector<pb::common::PartitionInfo>* partition_infos) override;

  bool GetCopysetOfPartitions(
      const std::vector<uint32_t>& partition_id_list,
      std::map<uint32_t, pb::mds::topology::Copyset>* copyset_map) override;

  bool ListPartition(
      uint32_t fs_id,
      std::vector<pb::common::PartitionInfo>* partition_infos) override;

  bool AllocOrGetMemcacheCluster(
      uint32_t fsId, pb::mds::topology::MemcacheClusterInfo* cluster) override;

  pb::mds::FSStatusCode AllocS3ChunkId(uint32_t fs_id, uint32_t id_num,
                                       uint64_t* chunk_id) override;

  pb::mds::FSStatusCode RefreshSession(
      const std::vector<pb::mds::topology::PartitionTxId>& tx_ids,
      std::vector<pb::mds::topology::PartitionTxId>* latest_tx_id_list,
      const std::string& fs_name,
      const pb::mds::Mountpoint& mountpoint) override;

  pb::mds::FSStatusCode GetLatestTxId(
      uint32_t fsId,
      std::vector<pb::mds::topology::PartitionTxId>* txIds) override;

  pb::mds::FSStatusCode GetLatestTxIdWithLock(
      uint32_t fsId, const std::string& fsName, const std::string& uuid,
      std::vector<pb::mds::topology::PartitionTxId>* txIds,
      uint64_t* sequence) override;

  pb::mds::FSStatusCode CommitTx(
      const std::vector<pb::mds::topology::PartitionTxId>& txIds) override;

  pb::mds::FSStatusCode CommitTxWithLock(
      const std::vector<pb::mds::topology::PartitionTxId>& txIds,
      const std::string& fsName, const std::string& uuid,
      uint64_t sequence) override;

  // set filesystem runtime statistics
  pb::mds::FSStatusCode SetFsStats(
      const std::string& fsname,
      const pb::mds::FsStatsData& fs_stat_data) override;

  // cache group
  pb::mds::cachegroup::CacheGroupErrCode RegisterCacheGroupMember(
      uint64_t old_id, uint64_t* member_id) override;

  pb::mds::cachegroup::CacheGroupErrCode AddCacheGroupMember(
      const std::string& group_name,
      const pb::mds::cachegroup::CacheGroupMember& member) override;

  pb::mds::cachegroup::CacheGroupErrCode LoadCacheGroupMembers(
      const std::string& group_name,
      std::vector<pb::mds::cachegroup::CacheGroupMember>* members) override;

  pb::mds::cachegroup::CacheGroupErrCode ReweightCacheGroupMember(
      const std::string& group_name, uint64_t member_id,
      uint32_t weight) override;

  pb::mds::cachegroup::CacheGroupErrCode SendCacheGroupHeartbeat(
      const std::string& group_name, uint64_t member_id,
      const pb::mds::cachegroup::HeartbeatRequest::Statistic& stat) override;

 private:
  pb::mds::FSStatusCode DoGetFsInfo(const std::string& fs_name,
                                    pb::mds::FsInfo* fs_info);

  pb::mds::FSStatusCode DoGetFsInfo(uint32_t fs_id, pb::mds::FsInfo* fs_info);

  bool DoGetMetaServerInfo(
      const common::PeerAddr& addr,
      common::CopysetPeerInfo<common::MetaserverID>* metaserver_info);

  bool DoGetMetaServerListInCopysets(
      const common::LogicPoolID& logicalpooid,
      const std::vector<common::CopysetID>& copysetidvec,
      std::vector<common::CopysetInfo<common::MetaserverID>>* copyset_infos);

  bool DoCreatePartition(
      uint32_t fs_id, uint32_t count,
      std::vector<pb::common::PartitionInfo>* partition_infos);

  bool DoGetCopysetOfPartitions(
      const std::vector<uint32_t>& partition_id_list,
      std::map<uint32_t, pb::mds::topology::Copyset>* copyset_map);

  bool DoListPartition(uint32_t fs_id,
                       std::vector<pb::common::PartitionInfo>* partition_infos);

  pb::mds::FSStatusCode DoAllocS3ChunkId(uint32_t fs_id, uint32_t id_num,
                                         uint64_t* chunk_id);

  pb::mds::FSStatusCode DoRefreshSession(
      const std::vector<pb::mds::topology::PartitionTxId>& tx_ids,
      std::vector<pb::mds::topology::PartitionTxId>* latest_tx_id_list,
      const std::string& fs_name, const pb::mds::Mountpoint& mountpoint);
  pb::mds::FSStatusCode ReturnError(int retcode);

  pb::mds::FSStatusCode GetLatestTxId(
      const pb::mds::GetLatestTxIdRequest& request,
      pb::mds::GetLatestTxIdResponse* response);

  pb::mds::FSStatusCode CommitTx(const pb::mds::CommitTxRequest& request);

  MDSBaseClient* mdsbasecli_;
  RPCExcutorRetryPolicy rpcexcutor_;
  common::MdsOption mdsOpt_;
};

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_
