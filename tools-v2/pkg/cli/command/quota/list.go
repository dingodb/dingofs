// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

package quota

import (
	"context"
	"fmt"
	"syscall"

	cmderror "github.com/dingodb/dingofs/tools-v2/internal/error"
	cobrautil "github.com/dingodb/dingofs/tools-v2/internal/utils"
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/config"
	"github.com/dingodb/dingofs/tools-v2/pkg/output"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/metaserver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type ListQuotaRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.LoadDirQuotasRequest
	metaServerClient metaserver.MetaServerServiceClient
}

var _ basecmd.RpcFunc = (*ListQuotaRpc)(nil) // check interface

type ListQuotaCommand struct {
	basecmd.FinalCurveCmd
	Rpc *ListQuotaRpc
}

var _ basecmd.FinalCurveCmdFunc = (*ListQuotaCommand)(nil) // check interface

func (listQuotaRpc *ListQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	listQuotaRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (listQuotaRpc *ListQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := listQuotaRpc.metaServerClient.LoadDirQuotas(ctx, listQuotaRpc.Request)
	output.ShowRpcData(listQuotaRpc.Request, response, listQuotaRpc.Info.RpcDataShow)
	return response, err
}

func NewListQuotaCommand() *cobra.Command {
	listQuotaCmd := &ListQuotaCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "list",
			Short: "list all directory quotas of fileSystem by fsid",
			Example: `$ curve quota list --fsid 1
$ curve quota list --fsname dingofs`,
		},
	}
	basecmd.NewFinalCurveCli(&listQuotaCmd.FinalCurveCmd, listQuotaCmd)
	return listQuotaCmd.Cmd
}

func (listQuotaCmd *ListQuotaCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(listQuotaCmd.Cmd)
	config.AddRpcTimeoutFlag(listQuotaCmd.Cmd)
	config.AddFsMdsAddrFlag(listQuotaCmd.Cmd)
	config.AddFsIdUint32OptionFlag(listQuotaCmd.Cmd)
	config.AddFsNameStringOptionFlag(listQuotaCmd.Cmd)
}

func (listQuotaCmd *ListQuotaCommand) Init(cmd *cobra.Command, args []string) error {
	_, getAddrErr := config.GetFsMdsAddrSlice(listQuotaCmd.Cmd)
	if getAddrErr.TypeCode() != cmderror.CODE_SUCCESS {
		listQuotaCmd.Error = getAddrErr
		return fmt.Errorf(getAddrErr.Message)
	}
	//check flags values
	fsId, fsErr := GetFsId(cmd)
	if fsErr != nil {
		return fsErr
	}
	// get poolid copysetid
	partitionInfo, partErr := GetPartitionInfo(listQuotaCmd.Cmd, fsId, config.ROOTINODEID)
	if partErr != nil {
		return partErr
	}
	poolId := partitionInfo.GetPoolId()
	copyetId := partitionInfo.GetCopysetId()
	request := &metaserver.LoadDirQuotasRequest{
		PoolId:    &poolId,
		CopysetId: &copyetId,
		FsId:      &fsId,
	}
	listQuotaCmd.Rpc = &ListQuotaRpc{
		Request: request,
	}
	//get request addr leader
	addrs, addrErr := GetLeaderPeerAddr(listQuotaCmd.Cmd, fsId, config.ROOTINODEID)
	if addrErr != nil {
		return addrErr
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	listQuotaCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "LoadDirQuotas")
	listQuotaCmd.Rpc.Info.RpcDataShow = config.GetFlagBool(listQuotaCmd.Cmd, config.VERBOSE)

	header := []string{cobrautil.ROW_ID, cobrautil.ROW_PATH, cobrautil.ROW_CAPACITY, cobrautil.ROW_USED, cobrautil.ROW_USED_PERCNET,
		cobrautil.ROW_INODES, cobrautil.ROW_INODES_IUSED, cobrautil.ROW_INODES_PERCENT}
	listQuotaCmd.SetHeader(header)

	return nil
}

func (listQuotaCmd *ListQuotaCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&listQuotaCmd.FinalCurveCmd, listQuotaCmd)
}

func (listQuotaCmd *ListQuotaCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(listQuotaCmd.Rpc.Info, listQuotaCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	response := result.(*metaserver.LoadDirQuotasResponse)
	if statusCode := response.GetStatusCode(); statusCode != metaserver.MetaStatusCode_OK {
		return cmderror.ErrQuota(int(statusCode)).ToError()
	}
	//fill tables
	rows := make([]map[string]string, 0)
	for dirInode, quota := range response.GetQuotas() {
		row := make(map[string]string)
		quotaValueSlice := ConvertQuotaToHumanizeValue(quota.GetMaxBytes(), quota.GetUsedBytes(), quota.GetMaxInodes(), quota.GetUsedInodes())
		dirPath, dirErr := GetInodePath(listQuotaCmd.Cmd, listQuotaCmd.Rpc.Request.GetFsId(), dirInode)
		if dirErr == syscall.ENOENT {
			continue
		}
		if dirErr != nil {
			return dirErr
		}
		if dirPath == "" { // directory may be deleted,not show
			continue
		}
		row[cobrautil.ROW_ID] = fmt.Sprintf("%d", dirInode)
		row[cobrautil.ROW_PATH] = dirPath
		row[cobrautil.ROW_CAPACITY] = quotaValueSlice[0]
		row[cobrautil.ROW_USED] = quotaValueSlice[1]
		row[cobrautil.ROW_USED_PERCNET] = quotaValueSlice[2]
		row[cobrautil.ROW_INODES] = quotaValueSlice[3]
		row[cobrautil.ROW_INODES_IUSED] = quotaValueSlice[4]
		row[cobrautil.ROW_INODES_PERCENT] = quotaValueSlice[5]
		rows = append(rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, listQuotaCmd.Header, []string{cobrautil.ROW_PATH})
	listQuotaCmd.TableNew.AppendBulk(list)

	res, errTranslate := output.MarshalProtoJson(response)
	if errTranslate != nil {
		return errTranslate
	}
	mapRes := res.(map[string]interface{})
	listQuotaCmd.Result = mapRes
	listQuotaCmd.Error = cmderror.ErrSuccess()

	return nil
}

func (listQuotaCmd *ListQuotaCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&listQuotaCmd.FinalCurveCmd)
}
