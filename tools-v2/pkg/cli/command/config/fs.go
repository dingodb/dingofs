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
	cmderror "github.com/dingodb/dingofs/tools-v2/internal/error"
	cobrautil "github.com/dingodb/dingofs/tools-v2/internal/utils"
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/common"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/quota"
	"github.com/dingodb/dingofs/tools-v2/pkg/config"
	"github.com/dingodb/dingofs/tools-v2/pkg/output"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/metaserver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type ConfigFsQuotaCommand struct {
	basecmd.FinalCurveCmd
	Rpc *common.SetFsQuotaRpc
}

var _ basecmd.FinalCurveCmdFunc = (*ConfigFsQuotaCommand)(nil) // check interface

func NewConfigFsQuotaCommand() *cobra.Command {
	fsQuotaCmd := &ConfigFsQuotaCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "fs",
			Short: "config fs quota for dingofs",
			Example: `$ dingo config fs --mdsaddr "10.20.61.2:6700,172.20.61.3:6700,10.20.61.4:6700" --fsid 1 --capacity 10 --inodes 1000
$ dingo config fs --fsid 1 --capacity 10 --inodes 1000
$ dingo config fs --fsname dingofs --capacity 10 --inodes 1000
`,
		},
	}
	basecmd.NewFinalCurveCli(&fsQuotaCmd.FinalCurveCmd, fsQuotaCmd)
	return fsQuotaCmd.Cmd
}

func (fsQuotaCmd *ConfigFsQuotaCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(fsQuotaCmd.Cmd)
	config.AddRpcTimeoutFlag(fsQuotaCmd.Cmd)
	config.AddFsMdsAddrFlag(fsQuotaCmd.Cmd)
	config.AddFsIdUint32OptionFlag(fsQuotaCmd.Cmd)
	config.AddFsNameStringOptionFlag(fsQuotaCmd.Cmd)
	config.AddFsCapacityOptionalFlag(fsQuotaCmd.Cmd)
	config.AddFsInodesOptionalFlag(fsQuotaCmd.Cmd)
}

func (fsQuotaCmd *ConfigFsQuotaCommand) Init(cmd *cobra.Command, args []string) error {
	_, getAddrErr := config.GetFsMdsAddrSlice(fsQuotaCmd.Cmd)
	if getAddrErr.TypeCode() != cmderror.CODE_SUCCESS {
		fsQuotaCmd.Error = getAddrErr
		return fmt.Errorf(getAddrErr.Message)
	}
	// check flags values
	capacity, inodes, quotaErr := quota.CheckAndGetQuotaValue(fsQuotaCmd.Cmd)
	if quotaErr != nil {
		return quotaErr
	}
	// get fs id
	fsId, fsErr := quota.GetFsId(cmd)
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
	//set request info
	request := &metaserver.SetFsQuotaRequest{
		FsId:      &fsId,
		PoolId:    &poolId,
		CopysetId: &copyetId,
		Quota:     &metaserver.Quota{MaxBytes: &capacity, MaxInodes: &inodes},
	}
	fsQuotaCmd.Rpc = &common.SetFsQuotaRpc{
		Request: request,
	}
	// get leader
	addrs, addrErr := quota.GetLeaderPeerAddr(fsQuotaCmd.Cmd, fsId, config.ROOTINODEID)
	if addrErr != nil {
		return addrErr
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	fsQuotaCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "setFsQuota")
	fsQuotaCmd.Rpc.Info.RpcDataShow = config.GetFlagBool(fsQuotaCmd.Cmd, "verbose")

	header := []string{cobrautil.ROW_RESULT}
	fsQuotaCmd.SetHeader(header)
	return nil
}

func (fsQuotaCmd *ConfigFsQuotaCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fsQuotaCmd.FinalCurveCmd, fsQuotaCmd)
}

func (fsQuotaCmd *ConfigFsQuotaCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(fsQuotaCmd.Rpc.Info, fsQuotaCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	response := result.(*metaserver.SetFsQuotaResponse)
	errQuota := cmderror.ErrQuota(int(response.GetStatusCode()))
	row := map[string]string{
		cobrautil.ROW_RESULT: errQuota.Message,
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

func (fsQuotaCmd *ConfigFsQuotaCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fsQuotaCmd.FinalCurveCmd)
}
