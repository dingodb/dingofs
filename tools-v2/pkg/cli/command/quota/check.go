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

type CheckQuotaRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.SetDirQuotaRequest
	metaServerClient metaserver.MetaServerServiceClient
}

var _ basecmd.RpcFunc = (*CheckQuotaRpc)(nil) // check interface

type CheckQuotaCommand struct {
	basecmd.FinalCurveCmd
	Rpc *CheckQuotaRpc
}

var _ basecmd.FinalCurveCmdFunc = (*CheckQuotaCommand)(nil) // check interface

func (checkQuotaRpc *CheckQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	checkQuotaRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (checkQuotaRpc *CheckQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := checkQuotaRpc.metaServerClient.SetDirQuota(ctx, checkQuotaRpc.Request)
	output.ShowRpcData(checkQuotaRpc.Request, response, checkQuotaRpc.Info.RpcDataShow)
	return response, err
}

func NewCheckQuotaCommand() *cobra.Command {
	checkQuotaCmd := &CheckQuotaCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "check",
			Short:   "check quota of a directory",
			Example: `$ curve quota check --fsid 1 --path /quotadir`,
		},
	}
	basecmd.NewFinalCurveCli(&checkQuotaCmd.FinalCurveCmd, checkQuotaCmd)
	return checkQuotaCmd.Cmd
}

func (checkQuotaCmd *CheckQuotaCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(checkQuotaCmd.Cmd)
	config.AddRpcTimeoutFlag(checkQuotaCmd.Cmd)
	config.AddFsMdsAddrFlag(checkQuotaCmd.Cmd)
	config.AddFsIdUint32OptionFlag(checkQuotaCmd.Cmd)
	config.AddFsNameStringOptionFlag(checkQuotaCmd.Cmd)
	config.AddFsPathRequiredFlag(checkQuotaCmd.Cmd)
	config.AddBoolOptionPFlag(checkQuotaCmd.Cmd, config.CURVEFS_QUOTA_REPAIR, "r", "repair inconsistent quota (default: false)")
}

func (checkQuotaCmd *CheckQuotaCommand) Init(cmd *cobra.Command, args []string) error {
	return nil
}

func (checkQuotaCmd *CheckQuotaCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&checkQuotaCmd.FinalCurveCmd, checkQuotaCmd)
}

func (checkQuotaCmd *CheckQuotaCommand) RunCommand(cmd *cobra.Command, args []string) error {

	path := config.GetFlagString(checkQuotaCmd.Cmd, config.CURVEFS_QUOTA_PATH)
	dirQuotaRequest, dirQuotaResponse, err := GetDirQuotaData(checkQuotaCmd.Cmd)
	if err != nil {
		return err
	}
	fsId := dirQuotaRequest.GetFsId()
	dirQuota := dirQuotaResponse.GetQuota()
	//get real used space
	realUsedBytes, realUsedInodes, getErr := GetDirectorySizeAndInodes(checkQuotaCmd.Cmd, fsId, path)
	if getErr != nil {
		return getErr
	}
	checkResult, ok := CheckQuota(dirQuota.GetMaxBytes(), dirQuota.GetUsedBytes(), dirQuota.GetMaxInodes(), dirQuota.GetUsedInodes(), realUsedBytes, realUsedInodes)
	repair := config.GetFlagBool(checkQuotaCmd.Cmd, config.CURVEFS_QUOTA_REPAIR)
	dirInodeId := dirQuotaRequest.GetDirInodeId()

	if repair && !ok { // inconsistent and need to repair
		// get poolid copysetid
		partitionInfo, partErr := GetPartitionInfo(checkQuotaCmd.Cmd, fsId, config.ROOTINODEID)
		if partErr != nil {
			return partErr
		}
		poolId := partitionInfo.GetPoolId()
		copyetId := partitionInfo.GetCopysetId()

		request := &metaserver.SetDirQuotaRequest{
			PoolId:     &poolId,
			CopysetId:  &copyetId,
			FsId:       &fsId,
			DirInodeId: &dirInodeId,
			Quota:      &metaserver.Quota{UsedBytes: &realUsedBytes, UsedInodes: &realUsedInodes},
		}
		checkQuotaCmd.Rpc = &CheckQuotaRpc{
			Request: request,
		}
		addrs, addrErr := GetLeaderPeerAddr(checkQuotaCmd.Cmd, fsId, config.ROOTINODEID)
		if addrErr != nil {
			return addrErr
		}
		timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
		retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
		checkQuotaCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "SetDirQuota")
		checkQuotaCmd.Rpc.Info.RpcDataShow = config.GetFlagBool(checkQuotaCmd.Cmd, config.VERBOSE)

		result, err := basecmd.GetRpcResponse(checkQuotaCmd.Rpc.Info, checkQuotaCmd.Rpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		response := result.(*metaserver.SetDirQuotaResponse)

		errQuota := cmderror.ErrQuota(int(response.GetStatusCode()))
		header := []string{cobrautil.ROW_RESULT}
		checkQuotaCmd.SetHeader(header)
		row := map[string]string{
			cobrautil.ROW_RESULT: errQuota.Message,
		}
		checkQuotaCmd.TableNew.Append(cobrautil.Map2List(row, checkQuotaCmd.Header))

	} else {
		header := []string{cobrautil.ROW_ID, cobrautil.ROW_NAME, cobrautil.ROW_CAPACITY, cobrautil.ROW_USED, cobrautil.ROW_REAL_USED, cobrautil.ROW_INODES, cobrautil.ROW_INODES_IUSED, cobrautil.ROW_INODES_REAL_IUSED, cobrautil.ROW_STATUS}
		checkQuotaCmd.SetHeader(header)
		row := map[string]string{
			cobrautil.ROW_ID:                strconv.FormatUint(dirInodeId, 10),
			cobrautil.ROW_NAME:              path,
			cobrautil.ROW_CAPACITY:          checkResult[0],
			cobrautil.ROW_USED:              checkResult[1],
			cobrautil.ROW_REAL_USED:         checkResult[2],
			cobrautil.ROW_INODES:            checkResult[3],
			cobrautil.ROW_INODES_IUSED:      checkResult[4],
			cobrautil.ROW_INODES_REAL_IUSED: checkResult[5],
			cobrautil.ROW_STATUS:            checkResult[6],
		}
		checkQuotaCmd.TableNew.Append(cobrautil.Map2List(row, checkQuotaCmd.Header))
	}
	return nil
}

func (checkQuotaCmd *CheckQuotaCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&checkQuotaCmd.FinalCurveCmd)
}
