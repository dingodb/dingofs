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
)

type GetFsQuotaCommand struct {
	basecmd.FinalCurveCmd
	Rpc *common.GetFsQuotaRpc
}

var _ basecmd.FinalCurveCmdFunc = (*GetFsQuotaCommand)(nil) // check interface

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
	_, getAddrErr := config.GetFsMdsAddrSlice(fsQuotaCmd.Cmd)
	if getAddrErr.TypeCode() != cmderror.CODE_SUCCESS {
		fsQuotaCmd.Error = getAddrErr
		return fmt.Errorf(getAddrErr.Message)
	}
	header := []string{cobrautil.ROW_FS_ID, cobrautil.ROW_FS_NAME, cobrautil.ROW_CAPACITY, cobrautil.ROW_USED, cobrautil.ROW_USED_PERCNET,
		cobrautil.ROW_INODES, cobrautil.ROW_INODES_IUSED, cobrautil.ROW_INODES_PERCENT}
	fsQuotaCmd.SetHeader(header)
	return nil
}

func (fsQuotaCmd *GetFsQuotaCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fsQuotaCmd.FinalCurveCmd, fsQuotaCmd)
}

func (fsQuotaCmd *GetFsQuotaCommand) RunCommand(cmd *cobra.Command, args []string) error {

	fsId, fsErr := quota.GetFsId(fsQuotaCmd.Cmd)
	if fsErr != nil {
		return fsErr
	}
	request, response, err := GetFsQuotaData(fsQuotaCmd.Cmd, fsId)
	if err != nil {
		return err
	}
	fsQuota := response.GetQuota()
	quotaValueSlice := quota.ConvertQuotaToHumanizeValue(fsQuota.GetMaxBytes(), fsQuota.GetUsedBytes(), fsQuota.GetMaxInodes(), fsQuota.GetUsedInodes())
	//get filesystem name
	fsName, fsErr := quota.GetFsName(cmd)
	if fsErr != nil {
		return fsErr
	}
	row := map[string]string{
		cobrautil.ROW_FS_ID:          strconv.FormatUint(uint64(request.GetFsId()), 10),
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

func GetFsQuotaData(cmd *cobra.Command, fsId uint32) (*metaserver.GetFsQuotaRequest, *metaserver.GetFsQuotaResponse, error) {
	// get poolid copysetid
	partitionInfo, partErr := quota.GetPartitionInfo(cmd, fsId, config.ROOTINODEID)
	if partErr != nil {
		return nil, nil, partErr
	}
	poolId := partitionInfo.GetPoolId()
	copyetId := partitionInfo.GetCopysetId()
	request := &metaserver.GetFsQuotaRequest{
		PoolId:    &poolId,
		CopysetId: &copyetId,
		FsId:      &fsId,
	}
	requestRpc := &common.GetFsQuotaRpc{
		Request: request,
	}
	addrs, addrErr := quota.GetLeaderPeerAddr(cmd, uint32(fsId), config.ROOTINODEID)
	if addrErr != nil {
		return nil, nil, addrErr
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	requestRpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "getFsQuota")
	requestRpc.Info.RpcDataShow = config.GetFlagBool(cmd, "verbose")

	result, err := basecmd.GetRpcResponse(requestRpc.Info, requestRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, nil, err.ToError()
	}
	response := result.(*metaserver.GetFsQuotaResponse)
	if statusCode := response.GetStatusCode(); statusCode != metaserver.MetaStatusCode_OK {
		return request, response, cmderror.ErrQuota(int(statusCode)).ToError()
	}
	return request, response, nil
}
