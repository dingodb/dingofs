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
 * Project: curve
 * Created Date: Mon Sept 1 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_

#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/stub/common/config.h"
#include "curvefs/src/stub/common/metacache_struct.h"
#include "curvefs/src/stub/metric/metric.h"
#include "curvefs/src/stub/rpcclient/base_client.h"

using ::curvefs::common::PartitionInfo;
using ::curvefs::common::Peer;
using ::curvefs::common::Volume;
using ::curvefs::mds::FsInfo;
using ::curvefs::mds::FSStatusCode;
using ::curvefs::mds::Mountpoint;
using ::curvefs::mds::space::SpaceErrCode;
using ::curvefs::mds::topology::Copyset;
using ::curvefs::mds::topology::TopoStatusCode;

using ::curvefs::stub::common::CopysetID;
using ::curvefs::stub::common::CopysetInfo;
using ::curvefs::stub::common::CopysetPeerInfo;
using ::curvefs::stub::common::LogicPoolID;
using ::curvefs::stub::common::MetaserverID;
using ::curvefs::stub::common::PeerAddr;
using ::curvefs::stub::metric::MDSClientMetric;

namespace curvefs {
namespace stub {
namespace rpcclient {

using curvefs::mds::CommitTxRequest;
using curvefs::mds::CommitTxResponse;
using curvefs::mds::GetLatestTxIdRequest;
using curvefs::mds::GetLatestTxIdResponse;
using curvefs::mds::Mountpoint;
using curvefs::mds::topology::MemcacheClusterInfo;

using ::curvefs::stub::common::MdsOption;

class RPCExcutorRetryPolicy {
 public:
  RPCExcutorRetryPolicy()
      : retryOpt_(), currentWorkingMDSAddrIndex_(0), cntlID_(1) {}

  void SetOption(const MdsOption::RpcRetryOption& option) {
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
  MdsOption::RpcRetryOption retryOpt_;

  // 记录上一次重试过的leader信息
  std::atomic<int> currentWorkingMDSAddrIndex_;

  // controller id，用于trace整个rpc IO链路
  // 这里直接用uint64即可，在可预测的范围内，不会溢出
  std::atomic<uint64_t> cntlID_;
};

class MdsClient {
 public:
  MdsClient() {}
  virtual ~MdsClient() {}

  virtual FSStatusCode Init(const ::curvefs::stub::common::MdsOption& mdsOpt,
                            MDSBaseClient* baseclient) = 0;

  virtual FSStatusCode MountFs(const std::string& fsName,
                               const Mountpoint& mountPt, FsInfo* fsInfo) = 0;

  virtual FSStatusCode UmountFs(const std::string& fsName,
                                const Mountpoint& mountPt) = 0;

  virtual FSStatusCode GetFsInfo(const std::string& fsName, FsInfo* fsInfo) = 0;

  virtual FSStatusCode GetFsInfo(uint32_t fsId, FsInfo* fsInfo) = 0;

  virtual bool GetMetaServerInfo(
      const PeerAddr& addr, CopysetPeerInfo<MetaserverID>* metaserverInfo) = 0;

  virtual bool GetMetaServerListInCopysets(
      const LogicPoolID& logicalpooid,
      const std::vector<CopysetID>& copysetidvec,
      std::vector<CopysetInfo<MetaserverID>>* cpinfoVec) = 0;

  virtual bool CreatePartition(uint32_t fsid, uint32_t count,
                               std::vector<PartitionInfo>* partitionInfos) = 0;

  virtual bool GetCopysetOfPartitions(
      const std::vector<uint32_t>& partitionIDList,
      std::map<uint32_t, Copyset>* copysetMap) = 0;

  virtual bool ListPartition(uint32_t fsID,
                             std::vector<PartitionInfo>* partitionInfos) = 0;

  virtual bool AllocOrGetMemcacheCluster(uint32_t fsId,
                                         MemcacheClusterInfo* cluster) = 0;

  virtual FSStatusCode AllocS3ChunkId(uint32_t fsId, uint32_t idNum,
                                      uint64_t* chunkId) = 0;

  virtual FSStatusCode RefreshSession(
      const std::vector<PartitionTxId>& txIds,
      std::vector<PartitionTxId>* latestTxIdList, const std::string& fsName,
      const Mountpoint& mountpoint, std::atomic<bool>* enableSumInDir) = 0;

  virtual FSStatusCode GetLatestTxId(uint32_t fsId,
                                     std::vector<PartitionTxId>* txIds) = 0;

  virtual FSStatusCode GetLatestTxIdWithLock(uint32_t fsId,
                                             const std::string& fsName,
                                             const std::string& uuid,
                                             std::vector<PartitionTxId>* txIds,
                                             uint64_t* sequence) = 0;

  virtual FSStatusCode CommitTx(const std::vector<PartitionTxId>& txIds) = 0;

  virtual FSStatusCode CommitTxWithLock(const std::vector<PartitionTxId>& txIds,
                                        const std::string& fsName,
                                        const std::string& uuid,
                                        uint64_t sequence) = 0;

  // allocate block group
  virtual SpaceErrCode AllocateVolumeBlockGroup(
      uint32_t fsId, uint32_t count, const std::string& owner,
      std::vector<curvefs::mds::space::BlockGroup>* groups) = 0;

  // acquire block group
  virtual SpaceErrCode AcquireVolumeBlockGroup(
      uint32_t fsId, uint64_t blockGroupOffset, const std::string& owner,
      curvefs::mds::space::BlockGroup* groups) = 0;

  // release block group
  virtual SpaceErrCode ReleaseVolumeBlockGroup(
      uint32_t fsId, const std::string& owner,
      const std::vector<curvefs::mds::space::BlockGroup>& blockGroups) = 0;
};

class MdsClientImpl : public MdsClient {
 public:
  MdsClientImpl() = default;

  FSStatusCode Init(const ::curvefs::stub::common::MdsOption& mdsOpt,
                    MDSBaseClient* baseclient) override;

  FSStatusCode MountFs(const std::string& fsName, const Mountpoint& mountPt,
                       FsInfo* fsInfo) override;

  FSStatusCode UmountFs(const std::string& fsName,
                        const Mountpoint& mountPt) override;

  FSStatusCode GetFsInfo(const std::string& fsName, FsInfo* fsInfo) override;

  FSStatusCode GetFsInfo(uint32_t fsId, FsInfo* fsInfo) override;

  bool GetMetaServerInfo(
      const PeerAddr& addr,
      CopysetPeerInfo<MetaserverID>* metaserverInfo) override;

  bool GetMetaServerListInCopysets(
      const LogicPoolID& logicalpooid,
      const std::vector<CopysetID>& copysetidvec,
      std::vector<CopysetInfo<MetaserverID>>* cpinfoVec) override;

  bool CreatePartition(uint32_t fsID, uint32_t count,
                       std::vector<PartitionInfo>* partitionInfos) override;

  bool GetCopysetOfPartitions(const std::vector<uint32_t>& partitionIDList,
                              std::map<uint32_t, Copyset>* copysetMap) override;

  bool ListPartition(uint32_t fsID,
                     std::vector<PartitionInfo>* partitionInfos) override;

  bool AllocOrGetMemcacheCluster(uint32_t fsId,
                                 MemcacheClusterInfo* cluster) override;

  FSStatusCode AllocS3ChunkId(uint32_t fsId, uint32_t idNum,
                              uint64_t* chunkId) override;

  FSStatusCode RefreshSession(const std::vector<PartitionTxId>& txIds,
                              std::vector<PartitionTxId>* latestTxIdList,
                              const std::string& fsName,
                              const Mountpoint& mountpoint,
                              std::atomic<bool>* enableSumInDir) override;

  FSStatusCode GetLatestTxId(uint32_t fsId,
                             std::vector<PartitionTxId>* txIds) override;

  FSStatusCode GetLatestTxIdWithLock(uint32_t fsId, const std::string& fsName,
                                     const std::string& uuid,
                                     std::vector<PartitionTxId>* txIds,
                                     uint64_t* sequence) override;

  FSStatusCode CommitTx(const std::vector<PartitionTxId>& txIds) override;

  FSStatusCode CommitTxWithLock(const std::vector<PartitionTxId>& txIds,
                                const std::string& fsName,
                                const std::string& uuid,
                                uint64_t sequence) override;

  // allocate block group
  SpaceErrCode AllocateVolumeBlockGroup(
      uint32_t fsId, uint32_t size, const std::string& owner,
      std::vector<curvefs::mds::space::BlockGroup>* groups) override;

  // acquire block group
  SpaceErrCode AcquireVolumeBlockGroup(
      uint32_t fsId, uint64_t blockGroupOffset, const std::string& owner,
      curvefs::mds::space::BlockGroup* groups) override;

  // release block group
  SpaceErrCode ReleaseVolumeBlockGroup(
      uint32_t fsId, const std::string& owner,
      const std::vector<curvefs::mds::space::BlockGroup>& blockGroups) override;

 private:
  FSStatusCode ReturnError(int retcode);

  FSStatusCode GetLatestTxId(const GetLatestTxIdRequest& request,
                             GetLatestTxIdResponse* response);

  FSStatusCode CommitTx(const CommitTxRequest& request);

 private:
  MDSBaseClient* mdsbasecli_;
  RPCExcutorRetryPolicy rpcexcutor_;
  ::curvefs::stub::common::MdsOption mdsOpt_;

  MDSClientMetric mdsClientMetric_;
};

}  // namespace rpcclient
}  // namespace stub
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_
