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

type DeleteQuotaRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.DeleteDirQuotaRequest
	metaServerClient metaserver.MetaServerServiceClient
}

var _ basecmd.RpcFunc = (*DeleteQuotaRpc)(nil) // check interface

type DeleteQuotaCommand struct {
	basecmd.FinalCurveCmd
	Rpc *DeleteQuotaRpc
}

var _ basecmd.FinalCurveCmdFunc = (*DeleteQuotaCommand)(nil) // check interface

func (deleteQuotaRpc *DeleteQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	deleteQuotaRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (deleteQuotaRpc *DeleteQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := deleteQuotaRpc.metaServerClient.DeleteDirQuota(ctx, deleteQuotaRpc.Request)
	output.ShowRpcData(deleteQuotaRpc.Request, response, deleteQuotaRpc.Info.RpcDataShow)
	return response, err
}

func NewDeleteQuotaCommand() *cobra.Command {
	deleteQuotaCmd := &DeleteQuotaCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "delete",
			Short:   "delete quota of a directory",
			Example: `$ curve quota delete --fsid 1 --path /quotadir`,
		},
	}
	basecmd.NewFinalCurveCli(&deleteQuotaCmd.FinalCurveCmd, deleteQuotaCmd)
	return deleteQuotaCmd.Cmd
}

func (deleteQuotaCmd *DeleteQuotaCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(deleteQuotaCmd.Cmd)
	config.AddRpcTimeoutFlag(deleteQuotaCmd.Cmd)
	config.AddFsMdsAddrFlag(deleteQuotaCmd.Cmd)
	config.AddFsIdUint32OptionFlag(deleteQuotaCmd.Cmd)
	config.AddFsNameStringOptionFlag(deleteQuotaCmd.Cmd)
	config.AddFsPathRequiredFlag(deleteQuotaCmd.Cmd)
}

func (deleteQuotaCmd *DeleteQuotaCommand) Init(cmd *cobra.Command, args []string) error {
	_, getAddrErr := config.GetFsMdsAddrSlice(deleteQuotaCmd.Cmd)
	if getAddrErr.TypeCode() != cmderror.CODE_SUCCESS {
		deleteQuotaCmd.Error = getAddrErr
		return fmt.Errorf(getAddrErr.Message)
	}
	//check flags values
	fsId, fsErr := GetFsId(cmd)
	if fsErr != nil {
		return fsErr
	}
	path := config.GetFlagString(deleteQuotaCmd.Cmd, config.CURVEFS_QUOTA_PATH)
	if len(path) == 0 {
		return fmt.Errorf("path is required")
	}
	//get inodeid
	dirInodeId, inodeErr := GetDirPathInodeId(deleteQuotaCmd.Cmd, fsId, path)
	if inodeErr != nil {
		return inodeErr
	}
	// get poolid copysetid
	partitionInfo, partErr := GetPartitionInfo(deleteQuotaCmd.Cmd, fsId, config.ROOTINODEID)
	if partErr != nil {
		return partErr
	}
	poolId := partitionInfo.GetPoolId()
	copyetId := partitionInfo.GetCopysetId()
	//set rpc request
	request := &metaserver.DeleteDirQuotaRequest{
		PoolId:     &poolId,
		CopysetId:  &copyetId,
		FsId:       &fsId,
		DirInodeId: &dirInodeId,
	}
	deleteQuotaCmd.Rpc = &DeleteQuotaRpc{
		Request: request,
	}
	//get request addr leader
	addrs, addrErr := GetLeaderPeerAddr(deleteQuotaCmd.Cmd, fsId, config.ROOTINODEID)
	if addrErr != nil {
		return addrErr
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	deleteQuotaCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "DeleteDirQuota")
	deleteQuotaCmd.Rpc.Info.RpcDataShow = config.GetFlagBool(deleteQuotaCmd.Cmd, config.VERBOSE)

	header := []string{cobrautil.ROW_RESULT}
	deleteQuotaCmd.SetHeader(header)
	return nil
}

func (deleteQuotaCmd *DeleteQuotaCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&deleteQuotaCmd.FinalCurveCmd, deleteQuotaCmd)
}

func (deleteQuotaCmd *DeleteQuotaCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(deleteQuotaCmd.Rpc.Info, deleteQuotaCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	response := result.(*metaserver.DeleteDirQuotaResponse)
	//mock rpc data
	//status := metaserver.MetaStatusCode_OK
	//response := &metaserver.DeleteDirQuotaResponse{
	//	StatusCode: &status,
	//}
	errQuota := cmderror.ErrQuota(int(response.GetStatusCode()))
	row := map[string]string{
		cobrautil.ROW_RESULT: errQuota.Message,
	}
	deleteQuotaCmd.TableNew.Append(cobrautil.Map2List(row, deleteQuotaCmd.Header))

	res, errTranslate := output.MarshalProtoJson(response)
	if errTranslate != nil {
		return errTranslate
	}
	mapRes := res.(map[string]interface{})
	deleteQuotaCmd.Result = mapRes
	deleteQuotaCmd.Error = cmderror.ErrSuccess()

	return nil
}

func (deleteQuotaCmd *DeleteQuotaCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&deleteQuotaCmd.FinalCurveCmd)
}
