/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * File Created: Monday, 15th October 2018 10:52:48 am
 * Author: tongguangxun
 */

#ifndef SRC_STUB_COMMON_METACACHE_STRUCT_H_
#define SRC_STUB_COMMON_METACACHE_STRUCT_H_

#include <atomic>
#include <vector>

#include "stub/common/common.h"
#include "utils/bitmap.h"
#include "utils/concurrent/rw_lock.h"
#include "utils/concurrent/spinlock.h"
#include "utils/dingo_compiler_specific.h"

namespace dingofs {
namespace stub {
namespace common {

using dingofs::utils::Bitmap;
using dingofs::utils::BthreadRWLock;
using dingofs::utils::ReadLockGuard;
using dingofs::utils::SpinLock;
using dingofs::utils::WriteLockGuard;

// copyset内的chunkserver节点的基本信息
// 包含当前chunkserver的id信息，以及chunkserver的地址信息
template <typename T>
struct DINGO_CACHELINE_ALIGNMENT CopysetPeerInfo {
  // 当前chunkserver节点的ID
  T peerID = 0;
  // 当前chunkserver节点的内部地址
  PeerAddr internalAddr;
  // 当前chunkserver节点的外部地址
  PeerAddr externalAddr;

  CopysetPeerInfo() = default;

  CopysetPeerInfo(const CopysetPeerInfo&) = default;
  CopysetPeerInfo& operator=(const CopysetPeerInfo& other) = default;

  CopysetPeerInfo(const T& cid, const PeerAddr& internal,
                  const PeerAddr& external)
      : peerID(cid), internalAddr(internal), externalAddr(external) {}

  bool operator==(const CopysetPeerInfo& other) const {
    return this->internalAddr == other.internalAddr &&
           this->externalAddr == other.externalAddr;
  }

  bool IsEmpty() const {
    return this->peerID == 0 && this->internalAddr.IsEmpty() &&
           this->externalAddr.IsEmpty();
  }
};

template <typename T>
inline std::ostream& operator<<(std::ostream& os, const CopysetPeerInfo<T>& c) {
  os << "peer id : " << c.peerID
     << ", internal address : " << c.internalAddr.ToString()
     << ", external address : " << c.externalAddr.ToString();

  return os;
}

// copyset的基本信息，包含peer信息、leader信息、appliedindex信息

template <typename T>
struct DINGO_CACHELINE_ALIGNMENT CopysetInfo {
  // leader存在变更可能标志位
  bool leaderMayChange_ = false;
  // 当前copyset的节点信息
  std::vector<CopysetPeerInfo<T>> csinfos_;
  // 当前节点的apply信息，在read的时候需要，用来避免读IO进入raft
  std::atomic<uint64_t> lastappliedindex_{0};
  // leader在本copyset信息中的索引，用于后面避免重复尝试同一个leader
  int16_t leaderindex_ = -1;
  // 当前copyset的id信息
  CopysetID cpid_ = 0;
  LogicPoolID lpid_ = 0;
  // 用于保护对copyset信息的修改
  SpinLock spinlock_;

  CopysetInfo() = default;
  ~CopysetInfo() = default;

  CopysetInfo& operator=(const CopysetInfo& other) {
    this->cpid_ = other.cpid_;
    this->lpid_ = other.lpid_;
    this->csinfos_ = other.csinfos_;
    this->leaderindex_ = other.leaderindex_;
    this->lastappliedindex_.store(other.lastappliedindex_);
    this->leaderMayChange_ = other.leaderMayChange_;
    return *this;
  }

  CopysetInfo(const CopysetInfo& other)
      : leaderMayChange_(other.leaderMayChange_),
        csinfos_(other.csinfos_),
        lastappliedindex_(other.lastappliedindex_.load()),
        leaderindex_(other.leaderindex_),
        cpid_(other.cpid_),
        lpid_(other.lpid_) {}

  uint64_t GetAppliedIndex() const {
    return lastappliedindex_.load(std::memory_order_acquire);
  }

  void SetLeaderUnstableFlag() { leaderMayChange_ = true; }

  void ResetSetLeaderUnstableFlag() { leaderMayChange_ = false; }

  bool LeaderMayChange() const { return leaderMayChange_; }

  bool HasValidLeader() const {
    return !leaderMayChange_ && leaderindex_ >= 0 &&
           leaderindex_ < csinfos_.size();
  }

  /**
   * read,write返回时，会携带最新的appliedindex更新当前的appliedindex
   * 如果read，write失败，那么会将appliedindex更新为0
   * @param: appliedindex为待更新的值
   */
  void UpdateAppliedIndex(uint64_t appliedindex) {
    uint64_t curIndex = lastappliedindex_.load(std::memory_order_acquire);

    if (appliedindex != 0 && appliedindex <= curIndex) {
      return;
    }

    while (!lastappliedindex_.compare_exchange_strong(
        curIndex, appliedindex, std::memory_order_acq_rel)) {
      if (curIndex >= appliedindex) {
        break;
      }
    }
  }

  /**
   * 获取当前leader的索引
   */
  int16_t GetCurrentLeaderIndex() const { return leaderindex_; }

  bool GetCurrentLeaderID(T* id) const {
    if (leaderindex_ >= 0) {
      if (static_cast<int>(csinfos_.size()) < leaderindex_) {
        return false;
      } else {
        *id = csinfos_[leaderindex_].peerID;
        return true;
      }
    } else {
      return false;
    }
  }

  /**
   * 更新leaderindex，如果leader不在当前配置组中，则返回-1
   * @param: addr为新的leader的地址信息
   */
  int UpdateLeaderInfo(const PeerAddr& addr,
                       CopysetPeerInfo<T> csInfo = CopysetPeerInfo<T>()) {
    VLOG(3) << "update leader info, pool " << lpid_ << ", copyset " << cpid_
            << ", current leader " << addr.ToString();

    spinlock_.Lock();
    bool exists = false;
    uint16_t tempindex = 0;
    for (auto iter : csinfos_) {
      if (iter.internalAddr == addr || iter.externalAddr == addr) {
        exists = true;
        break;
      }
      tempindex++;
    }

    // 新的addr不在当前copyset内，如果csInfo不为空，那么将其插入copyset
    if (!exists && !csInfo.IsEmpty()) {
      csinfos_.push_back(csInfo);
    } else if (exists == false) {
      LOG(WARNING) << addr.ToString() << " not in current copyset and "
                   << "its peer info not supplied";
      spinlock_.UnLock();
      return -1;
    }
    leaderindex_ = tempindex;
    spinlock_.UnLock();
    return 0;
  }

  /**
   * get leader info
   * @param[out]: peer id
   * @param[out]: ep
   */
  int GetLeaderInfo(T* peerid, EndPoint* ep) {
    // 第一次获取leader,如果当前leader信息没有确定，返回-1，由外部主动发起更新leader
    if (leaderindex_ < 0 || leaderindex_ >= static_cast<int>(csinfos_.size())) {
      LOG(INFO) << "GetLeaderInfo pool " << lpid_ << ", copyset " << cpid_
                << " has no leader";

      return -1;
    }

    *peerid = csinfos_[leaderindex_].peerID;
    *ep = csinfos_[leaderindex_].externalAddr.addr_;

    VLOG(12) << "GetLeaderInfo pool " << lpid_ << ", copyset " << cpid_
             << " leader id " << *peerid << ", end point "
             << butil::endpoint2str(*ep).c_str();

    return 0;
  }

  /**
   * 添加copyset的peerinfo
   * @param: csinfo为待添加的peer信息
   */
  void AddCopysetPeerInfo(const CopysetPeerInfo<T>& csinfo) {
    spinlock_.Lock();
    csinfos_.push_back(csinfo);
    spinlock_.UnLock();
  }

  /**
   * 当前CopysetInfo是否合法
   */
  bool IsValid() const { return !csinfos_.empty(); }

  /**
   * 更新leaderindex
   */
  void UpdateLeaderIndex(int index) { leaderindex_ = index; }

  /**
   * 当前copyset是否存在对应的chunkserver address
   * @param: addr需要检测的chunkserver
   * @return: true存在；false不存在
   */
  bool HasPeerInCopyset(const PeerAddr& addr) const {
    for (const auto& peer : csinfos_) {
      if (peer.internalAddr == addr || peer.externalAddr == addr) {
        return true;
      }
    }

    return false;
  }
};

template <typename T>
inline std::ostream& operator<<(std::ostream& os,
                                const CopysetInfo<T>& copyset) {
  os << "pool id : " << copyset.lpid_ << ", copyset id : " << copyset.cpid_
     << ", leader index : " << copyset.leaderindex_
     << ", applied index : " << copyset.lastappliedindex_
     << ", leader may change : " << copyset.leaderMayChange_ << ", peers : ";

  for (auto& p : copyset.csinfos_) {
    os << p << " ";
  }

  return os;
}

struct CopysetIDInfo {
  LogicPoolID lpid = 0;
  CopysetID cpid = 0;

  CopysetIDInfo(LogicPoolID logicpoolid, CopysetID copysetid)
      : lpid(logicpoolid), cpid(copysetid) {}

  CopysetIDInfo(const CopysetIDInfo& other) = default;
  CopysetIDInfo& operator=(const CopysetIDInfo& other) = default;
};

inline bool operator<(const CopysetIDInfo& cpidinfo1,
                      const CopysetIDInfo& cpidinfo2) {
  return cpidinfo1.lpid <= cpidinfo2.lpid && cpidinfo1.cpid < cpidinfo2.cpid;
}

inline bool operator==(const CopysetIDInfo& cpidinfo1,
                       const CopysetIDInfo& cpidinfo2) {
  return cpidinfo1.cpid == cpidinfo2.cpid && cpidinfo1.lpid == cpidinfo2.lpid;
}

}  // namespace common
}  // namespace stub
}  // namespace dingofs

#endif  // SRC_STUB_COMMON_METACACHE_STRUCT_H_
