package quota

import (
	"context"
	"fmt"
	"log"
	"math"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	cmderror "github.com/dingodb/dingofs/tools-v2/internal/error"
	cobrautil "github.com/dingodb/dingofs/tools-v2/internal/utils"
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	cmdCommon "github.com/dingodb/dingofs/tools-v2/pkg/cli/command/common"
	curvefs "github.com/dingodb/dingofs/tools-v2/pkg/cli/command/query/fs"
	"github.com/dingodb/dingofs/tools-v2/pkg/config"
	"github.com/dingodb/dingofs/tools-v2/pkg/output"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/common"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/heartbeat"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/metaserver"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/topology"
	"github.com/dustin/go-humanize"
	"github.com/gookit/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

type LeaderInfoMeta struct {
	LeaderInfoMap map[uint32][]string // partitionid ->leader address
	mux           sync.RWMutex
}

type PartitionListMeta struct {
	PartitionListMap map[uint32][]*common.PartitionInfo // fsid -> partitionListinfo
	mux              sync.RWMutex
}

type CopysetInfoMeta struct {
	CopysetInfoMap map[uint64]*heartbeat.CopySetInfo // copysetkey -> CopySetInfo
	mux            sync.RWMutex
}

var (
	// metadata cache
	leaderInfoMeta    *LeaderInfoMeta    = &LeaderInfoMeta{make(map[uint32][]string), sync.RWMutex{}}
	partitionListMeta *PartitionListMeta = &PartitionListMeta{make(map[uint32][]*common.PartitionInfo), sync.RWMutex{}}
	copysetInfoMeta   *CopysetInfoMeta   = &CopysetInfoMeta{make(map[uint64]*heartbeat.CopySetInfo), sync.RWMutex{}}
)

// Summary represents the total length and inodes of directory
type Summary struct {
	Length uint64
	Inodes uint64
}

type GetDentryRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.GetDentryRequest
	metaServerClient metaserver.MetaServerServiceClient
}

var _ basecmd.RpcFunc = (*GetDentryRpc)(nil) // check interface

func (getDentryRpc *GetDentryRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getDentryRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (getDentryRpc *GetDentryRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getDentryRpc.metaServerClient.GetDentry(ctx, getDentryRpc.Request)
	output.ShowRpcData(getDentryRpc.Request, response, getDentryRpc.Info.RpcDataShow)
	return response, err
}

// check fsid and fsname
func CheckAndGetFsIdOrFsNameValue(cmd *cobra.Command) (uint32, string, error) {
	var fsId uint32
	var fsName string
	if !cmd.Flag(config.CURVEFS_FSNAME).Changed && !cmd.Flag(config.CURVEFS_FSID).Changed {
		return 0, "", fmt.Errorf("fsname or fsid is required")
	}
	if cmd.Flag(config.CURVEFS_FSID).Changed {
		fsId = config.GetFlagUint32(cmd, config.CURVEFS_FSID)
	} else {
		fsName = config.GetFlagString(cmd, config.CURVEFS_FSNAME)
	}
	if fsId == 0 && len(fsName) == 0 {
		return 0, "", fmt.Errorf("fsname or fsid is invalid")
	}

	return fsId, fsName, nil
}

// get fs oid
func GetFsId(cmd *cobra.Command) (uint32, error) {
	fsId, _, fsErr := CheckAndGetFsIdOrFsNameValue(cmd)
	if fsErr != nil {
		return 0, fsErr
	}
	// fsId is not set,need to get fsId by fsName (fsName -> fsId)
	if fsId == 0 {
		fsData, fsErr := curvefs.GetFsInfo(cmd)
		if fsErr != nil {
			return 0, fsErr
		}
		fsId = uint32(fsData["fsId"].(float64))
		if fsId == 0 {
			return 0, fmt.Errorf("fsid is invalid")
		}
	}
	return fsId, nil
}

// get fs name
func GetFsName(cmd *cobra.Command) (string, error) {
	_, fsName, fsErr := CheckAndGetFsIdOrFsNameValue(cmd)
	if fsErr != nil {
		return "", fsErr
	}
	if len(fsName) == 0 { // fsName is not set,need to get fsName by fsId (fsId->fsName)
		fsData, fsErr := curvefs.GetFsInfo(cmd)
		if fsErr != nil {
			return "", fsErr
		}
		fsName = fsData["fsName"].(string)
		if len(fsName) == 0 {
			return "", fmt.Errorf("fsName is invalid")
		}
	}
	return fsName, nil
}

// check the quota value from command line
func CheckAndGetQuotaValue(cmd *cobra.Command) (uint64, uint64, error) {
	var capacity uint64
	var inodes uint64
	if !cmd.Flag(config.CURVEFS_QUOTA_CAPACITY).Changed && !cmd.Flag(config.CURVEFS_QUOTA_INODES).Changed {
		return 0, 0, fmt.Errorf("capacity or inodes is required")
	}
	if cmd.Flag(config.CURVEFS_QUOTA_CAPACITY).Changed {
		capacity = config.GetFlagUint64(cmd, config.CURVEFS_QUOTA_CAPACITY)
	}
	if cmd.Flag(config.CURVEFS_QUOTA_INODES).Changed {
		inodes = config.GetFlagUint64(cmd, config.CURVEFS_QUOTA_INODES)
	}
	return capacity * 1024 * 1024 * 1024, inodes, nil
}

// convert number value to Humanize Value
func ConvertQuotaToHumanizeValue(capacity uint64, usedBytes int64, maxInodes uint64, usedInodes int64) []string {
	var capacityStr string
	var usedPercentStr string
	var maxInodesStr string
	var maxInodesPercentStr string
	var result []string

	if capacity == 0 {
		capacityStr = "unlimited"
		usedPercentStr = ""
	} else {
		capacityStr = humanize.IBytes(capacity)
		usedPercentStr = fmt.Sprintf("%d", int(math.Round((float64(usedBytes) * 100.0 / float64(capacity)))))
	}
	result = append(result, capacityStr)
	result = append(result, humanize.IBytes(uint64(usedBytes))) //TODO usedBytes  may be negative
	result = append(result, usedPercentStr)
	if maxInodes == 0 {
		maxInodesStr = "unlimited"
		maxInodesPercentStr = ""
	} else {
		maxInodesStr = humanize.Comma(int64(maxInodes))
		maxInodesPercentStr = fmt.Sprintf("%d", int(math.Round((float64(usedInodes) * 100.0 / float64(maxInodes)))))
	}
	result = append(result, maxInodesStr)
	result = append(result, humanize.Comma(int64(usedInodes)))
	result = append(result, maxInodesPercentStr)
	return result
}

// get partitionList by fsid
func GetPartitionList(cmd *cobra.Command, fsId uint32) ([]*common.PartitionInfo, error) {
	partitionListMeta.mux.RLock()
	partitionList, ok := partitionListMeta.PartitionListMap[fsId]
	partitionListMeta.mux.RUnlock()
	if ok {
		return partitionList, nil
	}
	addrs, addrErr := config.GetFsMdsAddrSlice(cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, fmt.Errorf(addrErr.Message)
	}
	request := &topology.ListPartitionRequest{
		FsId: &fsId,
	}
	listPartitionRpc := &cmdCommon.ListPartitionRpc{
		Request: request,
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	listPartitionRpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "ListPartition")
	listPartitionRpc.Info.RpcDataShow = config.GetFlagBool(cmd, "verbose")
	result, err := basecmd.GetRpcResponse(listPartitionRpc.Info, listPartitionRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err.ToError()
	}
	response := result.(*topology.ListPartitionResponse)
	if response.GetStatusCode() != topology.TopoStatusCode_TOPO_OK {
		return nil, fmt.Errorf("get partition failed in fs[%d], error[%s]", fsId, response.GetStatusCode().String())
	}
	partitionList = response.GetPartitionInfoList()
	if partitionList == nil {
		return nil, fmt.Errorf("partition not found in fs[%d]", fsId)
	}
	partitionListMeta.mux.Lock()
	partitionListMeta.PartitionListMap[fsId] = partitionList
	partitionListMeta.mux.Unlock()
	return partitionList, nil
}

// get partition by inodeid
func GetPartitionInfo(cmd *cobra.Command, fsId uint32, inodeId uint64) (*common.PartitionInfo, error) {
	partitionList, err := GetPartitionList(cmd, fsId)
	if err != nil {
		return nil, err
	}
	index := slices.IndexFunc(partitionList,
		func(p *common.PartitionInfo) bool {
			return p.GetFsId() == fsId && p.GetStart() <= inodeId && p.GetEnd() >= inodeId
		})
	if index < 0 {
		return nil, fmt.Errorf("inode[%d] is not on any partition of fs[%d]", inodeId, fsId)
	}
	partitionInfo := partitionList[index]
	return partitionInfo, nil
}

func GetCopysetInfo(cmd *cobra.Command, poolId uint32, copyetId uint32) (*heartbeat.CopySetInfo, error) {
	copysetKeyId := cobrautil.GetCopysetKey(uint64(poolId), uint64(copyetId))
	copysetInfoMeta.mux.RLock()
	copysetInfo, ok := copysetInfoMeta.CopysetInfoMap[copysetKeyId]
	copysetInfoMeta.mux.RUnlock()
	if ok {
		return copysetInfo, nil
	}
	addrs, addrErr := config.GetFsMdsAddrSlice(cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, fmt.Errorf(addrErr.Message)
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	copysetKey := topology.CopysetKey{
		PoolId:    &poolId,
		CopysetId: &copyetId,
	}
	request := &topology.GetCopysetsInfoRequest{}
	request.CopysetKeys = append(request.CopysetKeys, &copysetKey)

	rpc := &cmdCommon.QueryCopysetRpc{
		Request: request,
	}
	rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetCopysetsInfo")
	rpc.Info.RpcDataShow = config.GetFlagBool(cmd, "verbose")
	result, err := basecmd.GetRpcResponse(rpc.Info, rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err.ToError()
	}
	response := result.(*topology.GetCopysetsInfoResponse)
	copysetValues := response.GetCopysetValues()
	if len(copysetValues) == 0 {
		return nil, fmt.Errorf("no copysetinfo found")
	}
	copysetValue := copysetValues[0] //only one copyset
	if copysetValue.GetStatusCode() == topology.TopoStatusCode_TOPO_OK {
		copysetInfo := copysetValue.GetCopysetInfo()
		copysetInfoMeta.mux.Lock()
		copysetInfoMeta.CopysetInfoMap[copysetKeyId] = copysetInfo
		copysetInfoMeta.mux.Unlock()
		return copysetInfo, nil
	} else {
		err := cmderror.ErrGetCopysetsInfo(int(copysetValue.GetStatusCode()))
		return nil, err.ToError()
	}
}

// get leader address
func GetLeaderPeerAddr(cmd *cobra.Command, fsId uint32, inodeId uint64) ([]string, error) {
	//get partition info
	partitionInfo, partErr := GetPartitionInfo(cmd, fsId, inodeId)
	if partErr != nil {
		return nil, partErr
	}
	partitionId := partitionInfo.GetPartitionId()
	leaderInfoMeta.mux.RLock()
	leadInfo, ok := leaderInfoMeta.LeaderInfoMap[partitionId]
	leaderInfoMeta.mux.RUnlock()
	if ok {
		return leadInfo, nil
	}
	poolId := partitionInfo.GetPoolId()
	copyetId := partitionInfo.GetCopysetId()
	copysetInfo, copysetErr := GetCopysetInfo(cmd, poolId, copyetId)
	if copysetErr != nil {
		return nil, copysetErr
	}
	leader := copysetInfo.GetLeaderPeer()
	addr, peerErr := cobrautil.PeertoAddr(leader)
	if peerErr.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, fmt.Errorf("pares leader peer[%s] failed: %s", leader, peerErr.Message)
	}
	addrs := []string{addr}
	leaderInfoMeta.mux.Lock()
	leaderInfoMeta.LeaderInfoMap[partitionId] = addrs
	leaderInfoMeta.mux.Unlock()
	return addrs, nil
}

// check quota is consistent
func CheckQuota(capacity uint64, usedBytes int64, maxInodes uint64, usedInodes int64, realUsedBytes int64, realUsedInodes int64) ([]string, bool) {
	var capacityStr string
	var usedStr string
	var realUsedStr string
	var maxInodesStr string
	var inodeUsedStr string
	var realUsedInodesStr string
	var result []string

	checkResult := true

	if capacity == 0 {
		capacityStr = "unlimited"
	} else { //quota is set
		capacityStr = humanize.Comma(int64(capacity))
	}
	usedStr = humanize.Comma(usedBytes)
	realUsedStr = humanize.Comma(realUsedBytes)
	if usedBytes != realUsedBytes {
		checkResult = false
	}
	result = append(result, capacityStr)
	result = append(result, usedStr)
	result = append(result, realUsedStr)

	if maxInodes == 0 {
		maxInodesStr = "unlimited"
	} else { //inode quota is set
		maxInodesStr = humanize.Comma(int64(maxInodes))
	}
	inodeUsedStr = humanize.Comma(usedInodes)
	realUsedInodesStr = humanize.Comma(int64(realUsedInodes))
	if usedInodes != realUsedInodes {
		checkResult = false
	}
	result = append(result, maxInodesStr)
	result = append(result, inodeUsedStr)
	result = append(result, realUsedInodesStr)

	if checkResult {
		result = append(result, "success")
	} else {
		result = append(result, color.Red.Sprint("failed"))
	}
	return result, checkResult
}

// align 512 bytes
func align512(length uint64) int64 {
	if length == 0 {
		return 0
	}
	return int64((((length - 1) >> 9) + 1) << 9)
}

// get inode
func GetInodeAttr(cmd *cobra.Command, fsId uint32, inodeId uint64) (*metaserver.InodeAttr, error) {
	partitionInfo, partErr := GetPartitionInfo(cmd, fsId, inodeId)
	if partErr != nil {
		return nil, partErr
	}
	poolId := partitionInfo.GetPoolId()
	copyetId := partitionInfo.GetCopysetId()
	partitionId := partitionInfo.GetPartitionId()
	inodeIds := []uint64{inodeId}
	inodeRequest := &metaserver.BatchGetInodeAttrRequest{
		PoolId:      &poolId,
		CopysetId:   &copyetId,
		PartitionId: &partitionId,
		FsId:        &fsId,
		InodeId:     inodeIds,
	}
	getInodeRpc := &cmdCommon.GetInodeAttrRpc{
		Request: inodeRequest,
	}
	addrs, addrErr := GetLeaderPeerAddr(cmd, fsId, inodeId)
	if addrErr != nil {
		return nil, addrErr
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	getInodeRpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetInodeAttr")
	getInodeRpc.Info.RpcDataShow = config.GetFlagBool(cmd, "verbose")

	inodeResult, err := basecmd.GetRpcResponse(getInodeRpc.Info, getInodeRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, fmt.Errorf("get inode failed: %s", err.Message)
	}
	getInodeResponse := inodeResult.(*metaserver.BatchGetInodeAttrResponse)
	if getInodeResponse.GetStatusCode() != metaserver.MetaStatusCode_OK {
		if getInodeResponse.GetStatusCode() == metaserver.MetaStatusCode_NOT_FOUND {
			return nil, syscall.ENOENT
		}
		return nil, fmt.Errorf("get inode failed: %s", getInodeResponse.GetStatusCode().String())
	}
	inodesAttrs := getInodeResponse.GetAttr()
	if len(inodesAttrs) != 1 {
		return nil, fmt.Errorf("GetInodeAttr return inodesAttrs size != 1, which is %d", len(inodesAttrs))
	}
	return inodesAttrs[0], nil
}

// ListDentry by inodeid
func ListDentry(cmd *cobra.Command, fsId uint32, inodeId uint64) ([]*metaserver.Dentry, error) {
	partitionInfo2, partErr2 := GetPartitionInfo(cmd, fsId, inodeId)
	if partErr2 != nil {
		return nil, partErr2
	}
	poolId2 := partitionInfo2.GetPoolId()
	copyetId2 := partitionInfo2.GetCopysetId()
	partitionId2 := partitionInfo2.GetPartitionId()
	txId := partitionInfo2.GetTxId()
	dentryRequest := &metaserver.ListDentryRequest{
		PoolId:      &poolId2,
		CopysetId:   &copyetId2,
		PartitionId: &partitionId2,
		FsId:        &fsId,
		DirInodeId:  &inodeId,
		TxId:        &txId,
	}
	listDentryRpc := &cmdCommon.ListDentryRpc{
		Request: dentryRequest,
	}

	addrs, addrErr := GetLeaderPeerAddr(cmd, fsId, inodeId)
	if addrErr != nil {
		return nil, addrErr
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	listDentryRpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "ListDentry")
	listDentryRpc.Info.RpcDataShow = config.GetFlagBool(cmd, "verbose")

	listDentryResult, err := basecmd.GetRpcResponse(listDentryRpc.Info, listDentryRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, fmt.Errorf("list dentry failed: %s", err.Message)
	}
	listDentryResponse := listDentryResult.(*metaserver.ListDentryResponse)

	if listDentryResponse.GetStatusCode() != metaserver.MetaStatusCode_OK {
		return nil, fmt.Errorf("list dentry failed: %s", listDentryResponse.GetStatusCode().String())
	}
	return listDentryResponse.GetDentrys(), nil
}

// get dir path
func GetInodePath(cmd *cobra.Command, fsId uint32, inodeId uint64) (string, error) {

	reverse := func(s []string) {
		for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
			s[i], s[j] = s[j], s[i]
		}
	}
	if inodeId == config.ROOTINODEID {
		return "/", nil
	}
	var names []string
	for inodeId != config.ROOTINODEID {
		inode, inodeErr := GetInodeAttr(cmd, fsId, inodeId)
		if inodeErr != nil {
			return "", inodeErr
		}
		//do list entry rpc
		parentIds := inode.GetParent()
		parentId := parentIds[0]
		entries, entryErr := ListDentry(cmd, fsId, parentId)
		if entryErr != nil {
			return "", entryErr
		}
		for _, e := range entries {
			if e.GetInodeId() == inodeId {
				names = append(names, *e.Name)
				break
			}
		}
		inodeId = parentId
	}
	if len(names) == 0 { //directory may be deleted
		return "", nil
	}
	names = append(names, "/") // add root
	reverse(names)
	return path.Join(names...), nil
}

// GetDentry
func GetDentry(cmd *cobra.Command, fsId uint32, parentId uint64, name string) (*metaserver.Dentry, error) {
	partitionInfo, partErr := GetPartitionInfo(cmd, fsId, parentId)
	if partErr != nil {
		return nil, partErr
	}
	poolId := partitionInfo.GetPoolId()
	copyetId := partitionInfo.GetCopysetId()
	partitionId := partitionInfo.GetPartitionId()
	txId := partitionInfo.GetTxId()
	getDentryRequest := &metaserver.GetDentryRequest{
		PoolId:        &poolId,
		CopysetId:     &copyetId,
		PartitionId:   &partitionId,
		FsId:          &fsId,
		ParentInodeId: &parentId,
		Name:          &name,
		TxId:          &txId,
	}
	getDentryRpc := &GetDentryRpc{
		Request: getDentryRequest,
	}
	addrs, addrErr := GetLeaderPeerAddr(cmd, fsId, parentId)
	if addrErr != nil {
		return nil, addrErr
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	getDentryRpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetDentry")
	getDentryRpc.Info.RpcDataShow = config.GetFlagBool(cmd, "verbose")

	inodeResult, err := basecmd.GetRpcResponse(getDentryRpc.Info, getDentryRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, fmt.Errorf("get dentry failed: %s", err.Message)
	}
	getDentryResponse := inodeResult.(*metaserver.GetDentryResponse)
	if getDentryResponse.GetStatusCode() != metaserver.MetaStatusCode_OK {
		return nil, fmt.Errorf("get dentry failed: %s", getDentryResponse.GetStatusCode().String())
	}
	return getDentryResponse.GetDentry(), nil
}

// parse directory path -> inodeId
func GetDirPathInodeId(cmd *cobra.Command, fsId uint32, path string) (uint64, error) {
	if path == "/" {
		return config.ROOTINODEID, nil
	}
	inodeId := config.ROOTINODEID

	for path != "" {
		names := strings.SplitN(path, "/", 2)
		if names[0] != "" {
			dentry, err := GetDentry(cmd, fsId, inodeId, names[0])
			if err != nil {
				return 0, err
			}
			if dentry.GetType() != metaserver.FsFileType_TYPE_DIRECTORY {
				return 0, syscall.ENOTDIR
			}
			inodeId = dentry.GetInodeId()
		}
		if len(names) == 1 {
			break
		}
		path = names[1]
	}
	return inodeId, nil
}

// get directory size and inodes by inode
func GetDirSummarySize(cmd *cobra.Command, fsId uint32, inode uint64, summary *Summary, concurrent chan struct{}, ctx context.Context, cancel context.CancelFunc) error {
	var err error
	entries, entErr := ListDentry(cmd, fsId, inode)
	if entErr != nil {
		return entErr
	}
	var wg sync.WaitGroup
	var errCh = make(chan error, 1)
	for _, entry := range entries {
		atomic.AddUint64(&summary.Inodes, 1)
		if entry.GetType() == metaserver.FsFileType_TYPE_S3 || entry.GetType() == metaserver.FsFileType_TYPE_FILE {
			inodeAttr, err := GetInodeAttr(cmd, fsId, entry.GetInodeId())
			if err != nil {
				return err
			}
			atomic.AddUint64(&summary.Length, inodeAttr.GetLength())
		}
		if entry.GetType() != metaserver.FsFileType_TYPE_DIRECTORY {
			continue
		}
		select {
		case err := <-errCh:
			cancel()
			return err
		case <-ctx.Done():
			return fmt.Errorf("cancel scan directory for other goroutine error")
		case concurrent <- struct{}{}:
			wg.Add(1)
			go func(e *metaserver.Dentry) {
				defer wg.Done()
				sumErr := GetDirSummarySize(cmd, fsId, e.GetInodeId(), summary, concurrent, ctx, cancel)
				<-concurrent
				if sumErr != nil {
					select {
					case errCh <- sumErr:
					default:
					}
				}
			}(entry)
		default:
			if sumErr := GetDirSummarySize(cmd, fsId, entry.GetInodeId(), summary, concurrent, ctx, cancel); sumErr != nil {
				return sumErr
			}
		}
	}
	wg.Wait()
	select {
	case err = <-errCh:
	default:
	}
	return err
}

// get directory size and inodes by path name
func GetDirectorySizeAndInodes(cmd *cobra.Command, fsId uint32, dirInode uint64) (int64, int64, error) {
	log.Printf("start to summary directory statistics, inode[%d]", dirInode)
	summary := &Summary{0, 0}
	concurrent := make(chan struct{}, 50)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sumErr := GetDirSummarySize(cmd, fsId, dirInode, summary, concurrent, ctx, cancel)
	log.Printf("end summary directory statistics, inode[%d],inodes[%d],size[%d]", dirInode, summary.Inodes, summary.Length)
	if sumErr != nil {
		return 0, 0, sumErr
	}
	return int64(summary.Length), int64(summary.Inodes), nil
}
