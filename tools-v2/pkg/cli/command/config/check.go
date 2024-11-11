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
	"fmt"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/common"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/quota"
	"strconv"

	cmderror "github.com/dingodb/dingofs/tools-v2/internal/error"
	cobrautil "github.com/dingodb/dingofs/tools-v2/internal/utils"
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/config"
	"github.com/dingodb/dingofs/tools-v2/pkg/output"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/metaserver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type CheckQuotaCommand struct {
	basecmd.FinalCurveCmd
	Rpc *common.SetFsQuotaRpc
}

var _ basecmd.FinalCurveCmdFunc = (*CheckQuotaCommand)(nil) // check interface

func NewCheckQuotaCommand() *cobra.Command {
	checkQuotaCmd := &CheckQuotaCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "check",
			Short:   "check quota of fs",
			Example: `$ curve config check --fsid 1`,
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
	config.AddBoolOptionPFlag(checkQuotaCmd.Cmd, config.CURVEFS_QUOTA_REPAIR, "r", "repair inconsistent quota (default: false)")
}

func (checkQuotaCmd *CheckQuotaCommand) Init(cmd *cobra.Command, args []string) error {
	_, getAddrErr := config.GetFsMdsAddrSlice(checkQuotaCmd.Cmd)
	if getAddrErr.TypeCode() != cmderror.CODE_SUCCESS {
		checkQuotaCmd.Error = getAddrErr
		return fmt.Errorf(getAddrErr.Message)
	}
	return nil
}

func (checkQuotaCmd *CheckQuotaCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&checkQuotaCmd.FinalCurveCmd, checkQuotaCmd)
}

func (checkQuotaCmd *CheckQuotaCommand) RunCommand(cmd *cobra.Command, args []string) error {
	fsId, fsErr := quota.GetFsId(checkQuotaCmd.Cmd)
	if fsErr != nil {
		return fsErr
	}
	_, fsResponse, err := GetFsQuotaData(checkQuotaCmd.Cmd, fsId)
	if err != nil {
		return err
	}
	fsQuota := fsResponse.GetQuota()
	// get root director real used space
	realUsedBytes, realUsedInodes, getErr := quota.GetDirectorySizeAndInodes(checkQuotaCmd.Cmd, fsId, config.ROOTINODEID)
	if getErr != nil {
		return getErr
	}
	checkResult, ok := quota.CheckQuota(fsQuota.GetMaxBytes(), fsQuota.GetUsedBytes(), fsQuota.GetMaxInodes(), fsQuota.GetUsedInodes(), realUsedBytes, realUsedInodes)
	repair := config.GetFlagBool(checkQuotaCmd.Cmd, config.CURVEFS_QUOTA_REPAIR)

	if repair && !ok { // inconsistent and need to repair
		// get poolid copysetid
		partitionInfo, partErr := quota.GetPartitionInfo(checkQuotaCmd.Cmd, fsId, config.ROOTINODEID)
		if partErr != nil {
			return partErr
		}
		poolId := partitionInfo.GetPoolId()
		copyetId := partitionInfo.GetCopysetId()
		request := &metaserver.SetFsQuotaRequest{
			PoolId:    &poolId,
			CopysetId: &copyetId,
			FsId:      &fsId,
			Quota:     &metaserver.Quota{UsedBytes: &realUsedBytes, UsedInodes: &realUsedInodes},
		}
		checkQuotaCmd.Rpc = &common.SetFsQuotaRpc{
			Request: request,
		}
		addrs, addrErr := quota.GetLeaderPeerAddr(checkQuotaCmd.Cmd, fsId, config.ROOTINODEID)
		if addrErr != nil {
			return addrErr
		}
		timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
		retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
		checkQuotaCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "SetFsQuota")
		checkQuotaCmd.Rpc.Info.RpcDataShow = config.GetFlagBool(checkQuotaCmd.Cmd, config.VERBOSE)

		result, err := basecmd.GetRpcResponse(checkQuotaCmd.Rpc.Info, checkQuotaCmd.Rpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		response := result.(*metaserver.SetFsQuotaResponse)

		errQuota := cmderror.ErrQuota(int(response.GetStatusCode()))
		header := []string{cobrautil.ROW_RESULT}
		checkQuotaCmd.SetHeader(header)
		row := map[string]string{
			cobrautil.ROW_RESULT: errQuota.Message,
		}
		checkQuotaCmd.TableNew.Append(cobrautil.Map2List(row, checkQuotaCmd.Header))

	} else {
		header := []string{cobrautil.ROW_FS_ID, cobrautil.ROW_FS_NAME, cobrautil.ROW_CAPACITY, cobrautil.ROW_USED, cobrautil.ROW_REAL_USED, cobrautil.ROW_INODES, cobrautil.ROW_INODES_IUSED, cobrautil.ROW_INODES_REAL_IUSED, cobrautil.ROW_STATUS}
		checkQuotaCmd.SetHeader(header)
		fsName, fsErr := quota.GetFsName(checkQuotaCmd.Cmd)
		if fsErr != nil {
			return fsErr
		}
		row := map[string]string{
			cobrautil.ROW_FS_ID:             strconv.FormatUint(uint64(fsId), 10),
			cobrautil.ROW_FS_NAME:           fsName,
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
