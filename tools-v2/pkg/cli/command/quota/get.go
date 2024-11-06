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
	"strconv"

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

type GetQuotaRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.GetDirQuotaRequest
	metaServerClient metaserver.MetaServerServiceClient
}

var _ basecmd.RpcFunc = (*GetQuotaRpc)(nil) // check interface

type GetQuotaCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *GetQuotaRpc
	Path     string
	Response *metaserver.GetDirQuotaResponse
}

var _ basecmd.FinalCurveCmdFunc = (*GetQuotaCommand)(nil) // check interface

func (getQuotaRpc *GetQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getQuotaRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (getQuotaRpc *GetQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getQuotaRpc.metaServerClient.GetDirQuota(ctx, getQuotaRpc.Request)
	output.ShowRpcData(getQuotaRpc.Request, response, getQuotaRpc.Info.RpcDataShow)
	return response, err
}

func NewGetQuotaDataCommand() *GetQuotaCommand {
	getQuotaCmd := &GetQuotaCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "get",
			Short:   "get quota of a directory",
			Example: `$ curve quota get --fsid 1 --path /quotadir`,
		},
	}
	basecmd.NewFinalCurveCli(&getQuotaCmd.FinalCurveCmd, getQuotaCmd)
	return getQuotaCmd
}

func NewGetQuotaCommand() *cobra.Command {
	return NewGetQuotaDataCommand().Cmd
}

func (getQuotaCmd *GetQuotaCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(getQuotaCmd.Cmd)
	config.AddRpcTimeoutFlag(getQuotaCmd.Cmd)
	config.AddFsMdsAddrFlag(getQuotaCmd.Cmd)
	config.AddFsIdUint32OptionFlag(getQuotaCmd.Cmd)
	config.AddFsNameStringOptionFlag(getQuotaCmd.Cmd)
	config.AddFsPathRequiredFlag(getQuotaCmd.Cmd)
}

func (getQuotaCmd *GetQuotaCommand) Init(cmd *cobra.Command, args []string) error {
	//check flags values
	fsId, fsErr := GetFsId(getQuotaCmd.Cmd)
	if fsErr != nil {
		return fsErr
	}
	path := config.GetFlagString(getQuotaCmd.Cmd, config.CURVEFS_QUOTA_PATH)
	if len(path) == 0 {
		return fmt.Errorf("path is required")
	}
	getQuotaCmd.Path = path
	//get inodeid
	dirInodeId, inodeErr := GetDirPathInodeId(getQuotaCmd.Cmd, fsId, getQuotaCmd.Path)
	if inodeErr != nil {
		return inodeErr
	}
	// get poolid copysetid
	partitionInfo, partErr := GetPartitionInfo(getQuotaCmd.Cmd, fsId, config.ROOTINODEID)
	if partErr != nil {
		return partErr
	}
	poolId := partitionInfo.GetPoolId()
	copyetId := partitionInfo.GetCopysetId()
	//set rpc request
	request := &metaserver.GetDirQuotaRequest{
		PoolId:     &poolId,
		CopysetId:  &copyetId,
		FsId:       &fsId,
		DirInodeId: &dirInodeId,
	}
	getQuotaCmd.Rpc = &GetQuotaRpc{
		Request: request,
	}
	//get request addr leader
	addrs, addrErr := GetLeaderPeerAddr(getQuotaCmd.Cmd, fsId, config.ROOTINODEID)
	if addrErr != nil {
		return addrErr
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	getQuotaCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetDirQuota")
	getQuotaCmd.Rpc.Info.RpcDataShow = config.GetFlagBool(getQuotaCmd.Cmd, config.VERBOSE)

	header := []string{cobrautil.ROW_ID, cobrautil.ROW_PATH, cobrautil.ROW_CAPACITY, cobrautil.ROW_USED, cobrautil.ROW_USED_PERCNET,
		cobrautil.ROW_INODES, cobrautil.ROW_INODES_IUSED, cobrautil.ROW_INODES_PERCENT}
	getQuotaCmd.SetHeader(header)
	return nil
}

func (getQuotaCmd *GetQuotaCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&getQuotaCmd.FinalCurveCmd, getQuotaCmd)
}

func (getQuotaCmd *GetQuotaCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(getQuotaCmd.Rpc.Info, getQuotaCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	response := result.(*metaserver.GetDirQuotaResponse)
	getQuotaCmd.Response = response

	if statusCode := response.GetStatusCode(); statusCode != metaserver.MetaStatusCode_OK {
		return cmderror.ErrQuota(int(statusCode)).ToError()
	}
	quota := response.GetQuota()
	quotaValueSlice := ConvertQuotaToHumanizeValue(quota.GetMaxBytes(), quota.GetUsedBytes(), quota.GetMaxInodes(), quota.GetUsedInodes())
	row := map[string]string{
		cobrautil.ROW_ID:             strconv.FormatUint(getQuotaCmd.Rpc.Request.GetDirInodeId(), 10),
		cobrautil.ROW_PATH:           getQuotaCmd.Path,
		cobrautil.ROW_CAPACITY:       quotaValueSlice[0],
		cobrautil.ROW_USED:           quotaValueSlice[1],
		cobrautil.ROW_USED_PERCNET:   quotaValueSlice[2],
		cobrautil.ROW_INODES:         quotaValueSlice[3],
		cobrautil.ROW_INODES_IUSED:   quotaValueSlice[4],
		cobrautil.ROW_INODES_PERCENT: quotaValueSlice[5],
	}
	getQuotaCmd.TableNew.Append(cobrautil.Map2List(row, getQuotaCmd.Header))

	res, errTranslate := output.MarshalProtoJson(response)
	if errTranslate != nil {
		return errTranslate
	}
	mapRes := res.(map[string]interface{})
	getQuotaCmd.Result = mapRes
	getQuotaCmd.Error = cmderror.ErrSuccess()

	return nil
}

func (getQuotaCmd *GetQuotaCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&getQuotaCmd.FinalCurveCmd)
}

func GetDirQuotaData(caller *cobra.Command) (*metaserver.GetDirQuotaRequest, *metaserver.GetDirQuotaResponse, error) {
	dirQuotaDataCmd := NewGetQuotaDataCommand()
	dirQuotaDataCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	config.AlignFlagsValue(caller, dirQuotaDataCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEFS_MDSADDR, config.CURVEFS_FSID, config.CURVEFS_FSNAME,
		config.CURVEFS_QUOTA_PATH,
	})
	dirQuotaDataCmd.Cmd.SilenceErrors = true
	err := dirQuotaDataCmd.Cmd.Execute()
	if err != nil {
		return nil, nil, err
	}
	return dirQuotaDataCmd.Rpc.Request, dirQuotaDataCmd.Response, nil
}
