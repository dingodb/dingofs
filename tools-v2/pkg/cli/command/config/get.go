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

package config

import (
	"context"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/metaserver"
	"strconv"

	cmderror "github.com/dingodb/dingofs/tools-v2/internal/error"
	cobrautil "github.com/dingodb/dingofs/tools-v2/internal/utils"
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/quota"
	"github.com/dingodb/dingofs/tools-v2/pkg/config"
	"github.com/dingodb/dingofs/tools-v2/pkg/output"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type GetFsQuotaRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.GetFsQuotaRequest
	metaServerClient metaserver.MetaServerServiceClient
}

var _ basecmd.RpcFunc = (*GetFsQuotaRpc)(nil) // check interface

type GetFsQuotaCommand struct {
	basecmd.FinalCurveCmd
	Rpc *GetFsQuotaRpc
}

var _ basecmd.FinalCurveCmdFunc = (*GetFsQuotaCommand)(nil) // check interface

func (getFsQuotaRpc *GetFsQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getFsQuotaRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (getFsQuotaRpc *GetFsQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getFsQuotaRpc.metaServerClient.GetFsQuota(ctx, getFsQuotaRpc.Request)
	output.ShowRpcData(getFsQuotaRpc.Request, response, getFsQuotaRpc.Info.RpcDataShow)
	return response, err
}

func NewGetFsQuotaCommand() *cobra.Command {
	fsQuotaCmd := &GetFsQuotaCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "get",
			Short: "get fs quota for curvefs",
			Example: `$ curve config get --fsid 1 
$ curve config fs --fsname dingofs
`,
		},
	}
	basecmd.NewFinalCurveCli(&fsQuotaCmd.FinalCurveCmd, fsQuotaCmd)
	return fsQuotaCmd.Cmd
}

func (fsQuotaCmd *GetFsQuotaCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(fsQuotaCmd.Cmd)
	config.AddRpcTimeoutFlag(fsQuotaCmd.Cmd)
	config.AddFsMdsAddrFlag(fsQuotaCmd.Cmd)
	config.AddFsIdUint32OptionFlag(fsQuotaCmd.Cmd)
	config.AddFsNameStringOptionFlag(fsQuotaCmd.Cmd)
}

func (fsQuotaCmd *GetFsQuotaCommand) Init(cmd *cobra.Command, args []string) error {
	fsId, fsErr := quota.GetFsId(fsQuotaCmd.Cmd)
	if fsErr != nil {
		return fsErr
	}
	// get poolid copysetid
	partitionInfo, partErr := quota.GetPartitionInfo(fsQuotaCmd.Cmd, fsId, config.ROOTINODEID)
	if partErr != nil {
		return partErr
	}
	poolId := partitionInfo.GetPoolId()
	copyetId := partitionInfo.GetCopysetId()
	request := &metaserver.GetFsQuotaRequest{
		PoolId:    &poolId,
		CopysetId: &copyetId,
		FsId:      &fsId,
	}
	fsQuotaCmd.Rpc = &GetFsQuotaRpc{
		Request: request,
	}
	addrs, addrErr := quota.GetLeaderPeerAddr(fsQuotaCmd.Cmd, uint32(fsId), config.ROOTINODEID)
	if addrErr != nil {
		return addrErr
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	fsQuotaCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "getFsQuota")
	fsQuotaCmd.Rpc.Info.RpcDataShow = config.GetFlagBool(fsQuotaCmd.Cmd, "verbose")
	header := []string{cobrautil.ROW_FS_ID, cobrautil.ROW_FS_NAME, cobrautil.ROW_CAPACITY, cobrautil.ROW_USED, cobrautil.ROW_USED_PERCNET,
		cobrautil.ROW_INODES, cobrautil.ROW_INODES_IUSED, cobrautil.ROW_INODES_PERCENT}
	fsQuotaCmd.SetHeader(header)
	return nil
}

func (fsQuotaCmd *GetFsQuotaCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fsQuotaCmd.FinalCurveCmd, fsQuotaCmd)
}

func (fsQuotaCmd *GetFsQuotaCommand) RunCommand(cmd *cobra.Command, args []string) error {

	result, err := basecmd.GetRpcResponse(fsQuotaCmd.Rpc.Info, fsQuotaCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	response := result.(*metaserver.GetFsQuotaResponse)
	if statusCode := response.GetStatusCode(); statusCode != metaserver.MetaStatusCode_OK {
		return cmderror.ErrQuota(int(statusCode)).ToError()
	}
	fsQuota := response.GetQuota()
	quotaValueSlice := quota.ConvertQuotaToHumanizeValue(fsQuota.GetMaxBytes(), fsQuota.GetUsedBytes(), fsQuota.GetMaxInodes(), fsQuota.GetUsedInodes())
	//get filesystem name
	fsName, fsErr := quota.GetFsName(cmd)
	if fsErr != nil {
		return fsErr
	}
	row := map[string]string{
		cobrautil.ROW_FS_ID:          strconv.FormatUint(uint64(fsQuotaCmd.Rpc.Request.GetFsId()), 10),
		cobrautil.ROW_FS_NAME:        fsName,
		cobrautil.ROW_CAPACITY:       quotaValueSlice[0],
		cobrautil.ROW_USED:           quotaValueSlice[1],
		cobrautil.ROW_USED_PERCNET:   quotaValueSlice[2],
		cobrautil.ROW_INODES:         quotaValueSlice[3],
		cobrautil.ROW_INODES_IUSED:   quotaValueSlice[4],
		cobrautil.ROW_INODES_PERCENT: quotaValueSlice[5],
	}
	fsQuotaCmd.TableNew.Append(cobrautil.Map2List(row, fsQuotaCmd.Header))

	res, errTranslate := output.MarshalProtoJson(response)
	if errTranslate != nil {
		return errTranslate
	}
	mapRes := res.(map[string]interface{})
	fsQuotaCmd.Result = mapRes
	fsQuotaCmd.Error = cmderror.ErrSuccess()
	return nil
}

func (fsQuotaCmd *GetFsQuotaCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fsQuotaCmd.FinalCurveCmd)
}
