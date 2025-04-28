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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef DINGOFS_SRC_MDS_FS_MANAGER_H_
#define DINGOFS_SRC_MDS_FS_MANAGER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dataaccess/aws/aws_s3_common.h"
#include "dingofs/mds.pb.h"
#include "dingofs/topology.pb.h"
#include "mds/common/types.h"
#include "mds/dlock/dlock.h"
#include "mds/fs_info_wrapper.h"
#include "mds/fs_storage.h"
#include "mds/metaserverclient/metaserver_client.h"
#include "mds/topology/topology_manager.h"
#include "utils/concurrent/concurrent.h"
#include "utils/interruptible_sleeper.h"

namespace dingofs {
namespace mds {

struct FsManagerOption {
  uint32_t backEndThreadRunInterSec;
  uint32_t spaceReloadConcurrency = 10;
  uint32_t clientTimeoutSec = 20;
  dataaccess::aws::S3AdapterOption s3AdapterOption;
};

class FsManager {
 public:
  FsManager(const std::shared_ptr<FsStorage>& fs_storage,
            const std::shared_ptr<MetaserverClient>& metaserver_client,
            const std::shared_ptr<topology::TopologyManager>& topo_manager,
            const std::shared_ptr<dlock::DLock>& dlock,
            const FsManagerOption& option)
      : fsStorage_(fs_storage),
        metaserverClient_(metaserver_client),
        topoManager_(topo_manager),
        dlock_(dlock),
        isStop_(true),
        option_(option) {}

  bool Init();
  void Run();
  void Stop();
  void Uninit();
  void BackEndFunc();
  void ScanFs(const FsInfoWrapper& wrapper);

  static bool CheckFsName(const std::string& fs_name);

  /**
   * @brief create fs, the fs name can not repeat
   *
   * @param CreateFsRequest request
   * @return FSStatusCode If success return OK; if fsName exist, return
   * FS_EXIST;
   *         else return error code
   */
  FSStatusCode CreateFs(const pb::mds::CreateFsRequest* request,
                        pb::mds::FsInfo* fs_info);

  /**
   * @brief delete fs, fs must unmount first
   *
   * @param[in] fsName: the fsName of fs which want to delete
   *
   * @return If success return OK; if fs has mount point, return FS_BUSY;
   *         else return error code
   */
  FSStatusCode DeleteFs(const std::string& fs_name);

  /**
   * @brief Mount fs, mount point can not repeat. It will increate
   * mountNum.
   *        If before mount, the mountNum is 0, call spaceClient to
   * InitSpace.
   *
   * @param[in] fsName: fsname of fs
   * @param[in] mountpoint: where the fs mount
   * @param[out] fsInfo: return the fsInfo
   *
   * @return If success return OK;
   *         if fs has same mount point or cto not consistent, return
   *         MOUNT_POINT_CONFLICT; else return error code
   */
  FSStatusCode MountFs(const std::string& fs_name,
                       const pb::mds::Mountpoint& mountpoint,
                       pb::mds::FsInfo* fs_info);

  /**
   * @brief Umount fs, it will decrease mountNum.
   *        If mountNum decrease to zero, call spaceClient to UnInitSpace
   *
   * @param[in] fsName: fsname of fs
   * @param[in] mountpoint: the mountpoint need to umount
   *
   * @return If success return OK;
   *         else return error code
   */
  FSStatusCode UmountFs(const std::string& fs_name,
                        const pb::mds::Mountpoint& mountpoint);

  /**
   * @brief get fs info by fsname
   *
   * @param[in] fsName: the fsname want to get
   * @param[out] fsInfo: the fsInfo got
   *
   * @return If success return OK; else return error code
   */
  FSStatusCode GetFsInfo(const std::string& fs_name, pb::mds::FsInfo* fs_info);

  /**
   * @brief get fs info by fsid
   *
   * @param[in] fsId: the fsid want to get
   * @param[out] fsInfo: the fsInfo got
   *
   * @return If success return OK; else return error code
   */
  FSStatusCode GetFsInfo(uint32_t fs_id, pb::mds::FsInfo* fs_info);

  /**
   * @brief get fs info by fsid and fsname
   *
   * @param[in] fsId: the fsid of fs want to get
   * @param[in] fsName: the fsname of fs want to get
   * @param[out] fsInfo: the fsInfo got
   *
   * @return If success return OK; else return error code
   */
  FSStatusCode GetFsInfo(const std::string& fs_name, uint32_t fs_id,
                         pb::mds::FsInfo* fs_info);

  void GetAllFsInfo(
      ::google::protobuf::RepeatedPtrField<pb::mds::FsInfo>* fs_info_vec);

  void RefreshSession(const pb::mds::RefreshSessionRequest* request,
                      pb::mds::RefreshSessionResponse* response);

  void GetLatestTxId(const pb::mds::GetLatestTxIdRequest* request,
                     pb::mds::GetLatestTxIdResponse* response);

  void CommitTx(const pb::mds::CommitTxRequest* request,
                pb::mds::CommitTxResponse* response);

  void SetFsStats(const pb::mds::SetFsStatsRequest* request,
                  pb::mds::SetFsStatsResponse* response);

  void GetFsStats(const pb::mds::GetFsStatsRequest* request,
                  pb::mds::GetFsStatsResponse* response);

  void GetFsPerSecondStats(const pb::mds::GetFsPerSecondStatsRequest* request,
                           pb::mds::GetFsPerSecondStatsResponse* response);

  // periodically check if the mount point is alive
  void BackEndCheckMountPoint();
  void CheckMountPoint();

  // for utest
  bool GetClientAliveTime(const std::string& mountpoint,
                          std::pair<std::string, uint64_t>* out);

 private:
  // return 0: ExactlySame; 1: uncomplete, -1: neither
  int IsExactlySameOrCreateUnComplete(const std::string& fs_name,
                                      pb::common::FSType fs_type,
                                      uint64_t blocksize,
                                      const pb::mds::FsDetail& detail);

  // send request to metaserver to DeletePartition, if response returns
  // FSStatusCode::OK or FSStatusCode::UNDER_DELETING, returns true;
  // else returns false
  bool DeletePartiton(std::string fs_name,
                      const pb::common::PartitionInfo& partition);

  // set partition status to DELETING in topology
  bool SetPartitionToDeleting(const pb::common::PartitionInfo& partition);

  void GetLatestTxId(uint32_t fs_id,
                     std::vector<pb::mds::topology::PartitionTxId>* tx_ids);

  FSStatusCode IncreaseFsTxSequence(const std::string& fs_name,
                                    const std::string& owner,
                                    uint64_t* sequence);

  FSStatusCode GetFsTxSequence(const std::string& fs_name, uint64_t* sequence);

  void UpdateClientAliveTime(const pb::mds::Mountpoint& mountpoint,
                             const std::string& fs_name,
                             bool add_mount_point = true);

  // add mount point to fs if client restore session
  FSStatusCode AddMountPoint(const pb::mds::Mountpoint& mountpoint,
                             const std::string& fs_name);

  void DeleteClientAliveTime(const std::string& mountpoint);

  void RebuildTimeRecorder();

  uint64_t GetRootId();

  void UpdateFsMountMetrics();

  std::shared_ptr<FsStorage> fsStorage_;
  std::shared_ptr<MetaserverClient> metaserverClient_;
  dingofs::utils::GenericNameLock<Mutex> nameLock_;
  std::shared_ptr<topology::TopologyManager> topoManager_;
  std::shared_ptr<dlock::DLock> dlock_;

  // Manage fs background delete threads
  utils::Thread backEndThread_;
  utils::Atomic<bool> isStop_;
  utils::InterruptibleSleeper sleeper_;
  const FsManagerOption option_;

  // deal with check mountpoint alive
  utils::Thread checkMountPointThread_;
  utils::InterruptibleSleeper checkMountPointSleeper_;
  // <mountpoint, <fsname,last update time>>
  std::map<std::string, std::pair<std::string, uint64_t>> mpTimeRecorder_;
  mutable RWLock recorderMutex_;
};
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_FS_MANAGER_H_
