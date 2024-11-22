/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: CurveCli
 * Created Date: 2022-06-07
 * Author: chengyi (Cyber-SiKu)
 */

package metaserver

import (
	"fmt"

	cmderror "github.com/dingodb/dingofs/tools-v2/internal/error"
	cobrautil "github.com/dingodb/dingofs/tools-v2/internal/utils"
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/list/topology"
	config "github.com/dingodb/dingofs/tools-v2/pkg/config"
	"github.com/dingodb/dingofs/tools-v2/pkg/output"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type MetaserverCommand struct {
	basecmd.FinalCurveCmd
	metrics []*basecmd.Metric
	rows    []map[string]string
	health  cobrautil.ClUSTER_HEALTH_STATUS
}

const (
	STATUS_SUBURI  = "/vars/pid"
	VERSION_SUBURI = "/vars/curve_version"
)

const (
	metaserverExample = `$ dingo status metaserver`
)

var _ basecmd.FinalCurveCmdFunc = (*MetaserverCommand)(nil) // check interface

func NewMetaserverCommand() *cobra.Command {
	mdsCmd := &MetaserverCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "metaserver",
			Short:   "get metaserver status of dingofs",
			Example: metaserverExample,
		},
	}
	basecmd.NewFinalCurveCli(&mdsCmd.FinalCurveCmd, mdsCmd)
	return mdsCmd.Cmd
}

func (mCmd *MetaserverCommand) AddFlags() {
	config.AddFsMdsAddrFlag(mCmd.Cmd)
	config.AddHttpTimeoutFlag(mCmd.Cmd)
	config.AddFsMdsDummyAddrFlag(mCmd.Cmd)
}

func (mCmd *MetaserverCommand) Init(cmd *cobra.Command, args []string) error {
	mCmd.health = cobrautil.HEALTH_ERROR
	externalAddrs, internalAddrs, errMetaserver := topology.GetMetaserverAddrs(mCmd.Cmd)
	if errMetaserver.TypeCode() != cmderror.CODE_SUCCESS {
		mCmd.Error = errMetaserver
		return fmt.Errorf(errMetaserver.Message)
	}

	header := []string{cobrautil.ROW_EXTERNAL_ADDR, cobrautil.ROW_INTERNAL_ADDR, cobrautil.ROW_VERSION, cobrautil.ROW_STATUS}
	mCmd.SetHeader(header)
	mCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		mCmd.Header, []string{cobrautil.ROW_STATUS, cobrautil.ROW_VERSION},
	))

	for i, addr := range externalAddrs {
		if !config.IsValidAddr(addr) {
			return fmt.Errorf("invalid metaserver external addr: %s", addr)
		}

		// set metrics
		timeout := viper.GetDuration(config.VIPER_GLOBALE_HTTPTIMEOUT)
		addrs := []string{addr}
		statusMetric := basecmd.NewMetric(addrs, STATUS_SUBURI, timeout)
		mCmd.metrics = append(mCmd.metrics, statusMetric)
		versionMetric := basecmd.NewMetric(addrs, VERSION_SUBURI, timeout)
		mCmd.metrics = append(mCmd.metrics, versionMetric)

		// set rows
		row := make(map[string]string)
		row[cobrautil.ROW_EXTERNAL_ADDR] = addr
		row[cobrautil.ROW_INTERNAL_ADDR] = internalAddrs[i]
		row[cobrautil.ROW_STATUS] = cobrautil.ROW_VALUE_OFFLINE
		row[cobrautil.ROW_VERSION] = cobrautil.ROW_VALUE_UNKNOWN
		mCmd.rows = append(mCmd.rows, row)
	}
	return nil
}

func (mCmd *MetaserverCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&mCmd.FinalCurveCmd, mCmd)
}

func (mCmd *MetaserverCommand) RunCommand(cmd *cobra.Command, args []string) error {
	results := make(chan basecmd.MetricResult, config.MaxChannelSize())
	size := 0
	for _, metric := range mCmd.metrics {
		size++
		go func(m *basecmd.Metric) {
			result, err := basecmd.QueryMetric(m)
			var key string
			if m.SubUri == STATUS_SUBURI {
				key = "status"
			} else {
				key = "version"
			}
			var value string
			if err.TypeCode() == cmderror.CODE_SUCCESS {
				value, err = basecmd.GetMetricValue(result)
			}
			results <- basecmd.MetricResult{
				Addr:  m.Addrs[0],
				Key:   key,
				Value: value,
				Err:   err,
			}
		}(metric)
	}

	count := 0
	var errs []*cmderror.CmdError
	for res := range results {
		for _, row := range mCmd.rows {
			if res.Err.TypeCode() == cmderror.CODE_SUCCESS && row[cobrautil.ROW_EXTERNAL_ADDR] == res.Addr {
				if res.Key == "status" {
					row[res.Key] = "online"
				} else {
					row[res.Key] = res.Value
				}
			} else if res.Err.TypeCode() != cmderror.CODE_SUCCESS {
				errs = append(errs, res.Err)
			}
		}
		count++
		if count >= size {
			break
		}
	}

	mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	mCmd.Error = mergeErr

	if len(errs) > 0 && len(errs) < len(mCmd.rows) {
		mCmd.health = cobrautil.HEALTH_WARN
	} else if len(errs) == 0 {
		mCmd.health = cobrautil.HEALTH_OK
	}

	list := cobrautil.ListMap2ListSortByKeys(mCmd.rows, mCmd.Header, []string{
		cobrautil.ROW_STATUS, cobrautil.ROW_VERSION,
	})
	mCmd.TableNew.AppendBulk(list)
	mCmd.Result = mCmd.rows
	return nil
}

func (mCmd *MetaserverCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&mCmd.FinalCurveCmd)
}

func NewStatusMetaserverCommand() *MetaserverCommand {
	mdsCmd := &MetaserverCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "metaserver",
			Short: "get metaserver status of dingofs",
		},
	}
	basecmd.NewFinalCurveCli(&mdsCmd.FinalCurveCmd, mdsCmd)
	return mdsCmd
}

func GetMetaserverStatus(caller *cobra.Command) (*interface{}, *tablewriter.Table, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS) {
	metaserverCmd := NewStatusMetaserverCommand()
	metaserverCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, metaserverCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEFS_MDSADDR,
	})
	metaserverCmd.Cmd.SilenceErrors = true
	metaserverCmd.Cmd.Execute()
	return &metaserverCmd.Result, metaserverCmd.TableNew, metaserverCmd.Error, metaserverCmd.health
}
