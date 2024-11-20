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
 * Created Date: 2022-05-09
 * Author: chengyi (Cyber-SiKu)
 */

package basecmd

import (
	"context"
	"encoding/json"
	"github.com/dingodb/dingofs/tools-v2/internal/utils/process"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	cmderror "github.com/dingodb/dingofs/tools-v2/internal/error"
	cobrautil "github.com/dingodb/dingofs/tools-v2/internal/utils"
	cobratemplate "github.com/dingodb/dingofs/tools-v2/internal/utils/template"
	config "github.com/dingodb/dingofs/tools-v2/pkg/config"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	CURL_VERSION = "curl/7.54.0"
)

type LeaderMetaCache struct {
	mutex      sync.RWMutex
	leaderAddr string
}

var (
	leaderMetaCache *LeaderMetaCache = &LeaderMetaCache{}
	pool            *ConnectionPool  = NewConnectionPool()
)

// FinalCurveCmd is the final executable command,
// it has no subcommands.
// The execution process is Init->RunCommand->Print.
// Error Use to indicate whether the command is wrong
// and the reason for the execution error
type FinalCurveCmd struct {
	Use      string             `json:"-"`
	Short    string             `json:"-"`
	Long     string             `json:"-"`
	Example  string             `json:"-"`
	Error    *cmderror.CmdError `json:"error"`
	Result   interface{}        `json:"result"`
	TableNew *tablewriter.Table `json:"-"`
	Header   []string           `json:"-"`
	Cmd      *cobra.Command     `json:"-"`
}

func (fc *FinalCurveCmd) SetHeader(header []string) {
	fc.Header = header
	fc.TableNew.SetHeader(header)
	// width := 80
	// if ws, err := term.GetWinsize(0); err == nil {
	// 	if width < int(ws.Width) {
	// 		width = int(ws.Width)
	// 	}
	// }
	// if len(header) != 0 {
	// 	fc.TableNew.SetColWidth(width/len(header) - 1)
	// }
}

// FinalCurveCmdFunc is the function type for final command
// If there is flag[required] related code should not be placed in init,
// the check for it is placed between PreRun and Run
type FinalCurveCmdFunc interface {
	Init(cmd *cobra.Command, args []string) error
	RunCommand(cmd *cobra.Command, args []string) error
	Print(cmd *cobra.Command, args []string) error
	// result in plain format string
	ResultPlainOutput() error
	AddFlags()
}

// MidCurveCmd is the middle command and has subcommands.
// If you execute this command
// you will be prompted which subcommands are included
type MidCurveCmd struct {
	Use   string
	Short string
	Cmd   *cobra.Command
}

// Add subcommand for MidCurveCmd
type MidCurveCmdFunc interface {
	AddSubCommands()
}

func NewFinalCurveCli(cli *FinalCurveCmd, funcs FinalCurveCmdFunc) *cobra.Command {
	cli.Cmd = &cobra.Command{
		Use:     cli.Use,
		Short:   cli.Short,
		Long:    cli.Long,
		Example: cli.Example,
		RunE: func(cmd *cobra.Command, args []string) error {
			show := config.GetFlagBool(cli.Cmd, config.VERBOSE)
			process.SetShow(show)
			cmd.SilenceUsage = true
			err := funcs.Init(cmd, args)
			if err != nil {
				return err
			}
			err = funcs.RunCommand(cmd, args)
			if err != nil {
				return err
			}
			return funcs.Print(cmd, args)
		},
		SilenceUsage: false,
	}
	config.AddFormatFlag(cli.Cmd)
	funcs.AddFlags()
	cobratemplate.SetFlagErrorFunc(cli.Cmd)

	// set table
	cli.TableNew = tablewriter.NewWriter(os.Stdout)
	cli.TableNew.SetRowLine(true)
	cli.TableNew.SetAutoFormatHeaders(true)
	cli.TableNew.SetAutoWrapText(true)
	cli.TableNew.SetAlignment(tablewriter.ALIGN_LEFT)

	return cli.Cmd
}

func NewMidCurveCli(cli *MidCurveCmd, add MidCurveCmdFunc) *cobra.Command {
	cli.Cmd = &cobra.Command{
		Use:   cli.Use,
		Short: cli.Short,
		Args:  cobratemplate.NoArgs,
	}
	add.AddSubCommands()
	return cli.Cmd
}

type Metric struct {
	Addrs   []string
	SubUri  string
	timeout time.Duration
}

type MetricResult struct {
	Addr  string
	Key   string
	Value string
	Err   *cmderror.CmdError
}

func NewMetric(addrs []string, subUri string, timeout time.Duration) *Metric {
	return &Metric{
		Addrs:   addrs,
		SubUri:  subUri,
		timeout: timeout,
	}
}

func QueryMetric(m *Metric) (string, *cmderror.CmdError) {
	response := make(chan string, 1)
	size := len(m.Addrs)
	if size > config.MaxChannelSize() {
		size = config.MaxChannelSize()
	}
	errs := make(chan *cmderror.CmdError, size)
	for _, host := range m.Addrs {
		url := "http://" + host + m.SubUri
		go httpGet(url, m.timeout, response, errs)
	}
	var retStr string
	var vecErrs []*cmderror.CmdError
	count := 0
	for err := range errs {
		if err.Code != cmderror.CODE_SUCCESS {
			vecErrs = append(vecErrs, err)
		} else {
			retStr = <-response
			vecErrs = append(vecErrs, cmderror.ErrSuccess())
			break
		}
		count++
		if count >= len(m.Addrs) {
			// all host failed
			break
		}
	}
	retErr := cmderror.MergeCmdError(vecErrs)
	return retStr, retErr
}

func GetMetricValue(metricRet string) (string, *cmderror.CmdError) {
	kv := cobrautil.RmWitespaceStr(metricRet)
	kvVec := strings.Split(kv, ":")
	if len(kvVec) != 2 {
		err := cmderror.ErrParseMetric()
		err.Format(metricRet)
		return "", err
	}
	kvVec[1] = strings.Replace(kvVec[1], "\"", "", -1)
	return kvVec[1], cmderror.ErrSuccess()
}

func GetKeyValueFromJsonMetric(metricRet string, key string) (string, *cmderror.CmdError) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(metricRet), &data); err != nil {
		err := cmderror.ErrParseMetric()
		err.Format(metricRet)
		return "", err
	}
	return data[key].(string), cmderror.ErrSuccess()
}

// get mds leader server
func GetMdsLeader(mdsAddrs []string) (string, bool) {
	leaderMetaCache.mutex.RLock()
	if leaderMetaCache.leaderAddr != "" {
		return leaderMetaCache.leaderAddr, true
	}
	leaderMetaCache.mutex.RUnlock()
	timeout := viper.GetDuration(config.VIPER_GLOBALE_HTTPTIMEOUT)
	for _, addr := range mdsAddrs {
		addrs := []string{addr}
		statusMetric := NewMetric(addrs, config.STATUS_SUBURI, timeout)
		result, err := QueryMetric(statusMetric)
		if err.TypeCode() == cmderror.CODE_SUCCESS {
			value, err := GetMetricValue(result)
			if err.TypeCode() == cmderror.CODE_SUCCESS && value == "leader" {
				leaderMetaCache.mutex.Lock()
				leaderMetaCache.leaderAddr = addr
				leaderMetaCache.mutex.Unlock()
				return addr, true
			}
		}
	}
	return "", false
}

// get request hosts
func GetResuestHosts(reqAddrs []string) []string {
	var result []string
	if size := len(reqAddrs); size > 1 {
		// mutible host,  get leader
		leaderAddr, ok := GetMdsLeader(reqAddrs)
		if ok {
			result = append(result, leaderAddr)
		} else {
			// fail,remain origin host list
			result = reqAddrs
		}
	} else {
		// only one host
		result = reqAddrs
	}
	return result
}

func httpGet(url string, timeout time.Duration, response chan string, errs chan *cmderror.CmdError) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		interErr := cmderror.ErrHttpCreateGetRequest()
		interErr.Format(err.Error())
		errs <- interErr
	}
	// for get curl url
	req.Header.Set("User-Agent", CURL_VERSION)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		interErr := cmderror.ErrHttpClient()
		interErr.Format(err.Error())
		errs <- interErr
	} else if resp.StatusCode != http.StatusOK {
		statusErr := cmderror.ErrHttpStatus(resp.StatusCode)
		statusErr.Format(url, resp.StatusCode)
		errs <- statusErr
	} else {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			interErr := cmderror.ErrHttpUnreadableResult()
			interErr.Format(url, err.Error())
			errs <- interErr
		}
		// get response
		response <- string(body)
		errs <- cmderror.ErrSuccess()
	}
}

type Rpc struct {
	Addrs         []string
	RpcTimeout    time.Duration
	RpcRetryTimes int32
	RpcFuncName   string
	RpcDataShow   bool
}

// TODO field RpcDataShow may be pass by parameter
func NewRpc(addrs []string, timeout time.Duration, retryTimes int32, funcName string) *Rpc {
	return &Rpc{
		Addrs:         addrs,
		RpcTimeout:    timeout,
		RpcRetryTimes: retryTimes,
		RpcFuncName:   funcName,
		RpcDataShow:   false,
	}
}

type RpcFunc interface {
	NewRpcClient(cc grpc.ClientConnInterface)
	Stub_Func(ctx context.Context) (interface{}, error)
}

type Result struct {
	addr   string
	err    *cmderror.CmdError
	result interface{}
}

func GetRpcResponse(rpc *Rpc, rpcFunc RpcFunc) (interface{}, *cmderror.CmdError) {
	reqAddrs := GetResuestHosts(rpc.Addrs)
	// start rpc request
	results := make([]Result, 0)
	for _, address := range reqAddrs {
		conn, err := pool.GetConnection(address, rpc.RpcTimeout)
		if err != nil {
			errDial := cmderror.ErrRpcDial()
			errDial.Format(address, err.Error())
			results = append(results, Result{address, errDial, nil})
		} else {
			rpcFunc.NewRpcClient(conn)
			retryTimes := rpc.RpcRetryTimes
			for {
				log.Printf("%s: start to rpc [%s],timeout[%v],retrytimes[%d]", address, rpc.RpcFuncName, rpc.RpcTimeout, retryTimes)
				ctx, _ := context.WithTimeout(context.Background(), rpc.RpcTimeout)
				res, err := rpcFunc.Stub_Func(ctx)
				retryTimes = retryTimes - 1
				if err != nil {
					if retryTimes > 0 {
						log.Printf("%s: fail to get rpc [%s] response,retrying...", address, rpc.RpcFuncName)
						continue
					} else {
						errRpc := cmderror.ErrRpcCall()
						errRpc.Format(rpc.RpcFuncName, err.Error())
						results = append(results, Result{address, errRpc, nil})
						log.Printf("%s: fail to get rpc [%s] response", address, rpc.RpcFuncName)
						break
					}
				} else {
					results = append(results, Result{address, cmderror.ErrSuccess(), res})
					log.Printf("%s: get rpc [%s] response successfully", address, rpc.RpcFuncName)
					break
				}
			}
			pool.PutConnection(address, conn)
		}
	}
	// get the rpc response result
	var ret interface{}
	var vecErrs []*cmderror.CmdError
	for _, res := range results {
		if res.err.TypeCode() != cmderror.CODE_SUCCESS {
			vecErrs = append(vecErrs, res.err)
		} else {
			ret = res.result
			break
		}
	}
	if len(vecErrs) >= len(reqAddrs) {
		retErr := cmderror.MergeCmdError(vecErrs)
		return ret, retErr
	}

	return ret, cmderror.ErrSuccess()
}

type RpcResult struct {
	position int
	Response interface{}
	Error    *cmderror.CmdError
}

func GetRpcListResponse(rpcList []*Rpc, rpcFunc []RpcFunc) ([]interface{}, []*cmderror.CmdError) {
	results := make([]RpcResult, 0)
	for i := range rpcList {
		res, err := GetRpcResponse(rpcList[i], rpcFunc[i])
		results = append(results, RpcResult{i, res, err})
	}

	retRes := make([]interface{}, 0)
	var vecErrs []*cmderror.CmdError
	for i := 0; i < len(results); i++ {
		res := results[i]
		if res.Error.TypeCode() != cmderror.CODE_SUCCESS {
			// get fail
			vecErrs = append(vecErrs, res.Error)
		} else {
			// success
			retRes = append(retRes, res.Response)
		}
	}

	return retRes, vecErrs
}

// get mountPoint inode
func GetFileInode(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if sst, ok := fi.Sys().(*syscall.Stat_t); ok {
		return sst.Ino, nil
	}
	return 0, nil
}
