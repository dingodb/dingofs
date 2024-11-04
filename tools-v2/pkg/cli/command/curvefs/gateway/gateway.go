/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2024-10-22
 * Author: Wei Dong (jackblack369)
 */

package gateway

import (
	"fmt"
	cmderror "github.com/dingodb/dingofs/tools-v2/internal/error"
	utils "github.com/dingodb/dingofs/tools-v2/internal/utils"
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/curvefs/list/fs"
	"github.com/dingodb/dingofs/tools-v2/pkg/config"
	mcli "github.com/minio/cli"
	mnas "github.com/minio/minio/cmd/gateway/nas"
	"github.com/spf13/cobra"
	"os"
)

var logger = utils.GetLogger("dingofs gateway")

type GatewayCommand struct {
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*GatewayCommand)(nil) // check interface

const (
	gatewayExample = `$ curve gateway {mount_point} {gateway_addr}`
)

func NewGatewayCommand() *cobra.Command {
	fsCmd := &GatewayCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "gateway",
			Short:   "Start an S3-compatible gateway",
			Example: gatewayExample,
		},
	}
	basecmd.NewFinalCurveCli(&fsCmd.FinalCurveCmd, fsCmd)
	return fsCmd.Cmd
}

func (gCmd *GatewayCommand) AddFlags() {
	config.AddFsMdsAddrFlag(gCmd.Cmd)
	config.AddFsIdRequiredFlag(gCmd.Cmd)
}

func (gCmd *GatewayCommand) Init(cmd *cobra.Command, args []string) error {
	return nil
}

func (gCmd *GatewayCommand) Print(cmd *cobra.Command, args []string) error {
	return nil
}

func (gCmd *GatewayCommand) RunCommand(cmd *cobra.Command, args []string) error {
	err := gateway(gCmd.Cmd)
	if err != nil {
		return err
	}
	return nil
}

func (gCmd *GatewayCommand) ResultPlainOutput() error {
	return nil
}

func gateway(cmd *cobra.Command) error {
	ak := os.Getenv("MINIO_ROOT_USER")
	if ak == "" {
		ak = os.Getenv("MINIO_ACCESS_KEY")
	}
	if len(ak) < 3 {
		logger.Fatalf("MINIO_ROOT_USER should be specified as an environment variable with at least 3 characters")
	}
	sk := os.Getenv("MINIO_ROOT_PASSWORD")
	if sk == "" {
		sk = os.Getenv("MINIO_SECRET_KEY")
	}
	if len(sk) < 8 {
		logger.Fatalf("MINIO_ROOT_PASSWORD should be specified as an environment variable with at least 8 characters")
	}

	addrs, addrErr := config.GetFsMdsAddrSlice(cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	fsId := config.GetFlagUint32(cmd, config.CURVEFS_FSID)

	// locate mount point path
	mountPoint, err := getMountPoint(addrs[0], fsId, "")
	if err != nil {
		return err
	}
	listenAddr := "127.0.0.1" + ":" + "19000"

	args := []string{"gateway", "--address", listenAddr, "--anonymous", mountPoint}

	app := &mcli.App{
		Action: gateway2,
		Flags: []mcli.Flag{
			mcli.StringFlag{
				Name:  "address",
				Value: ":9000",
				Usage: "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
			},
			mcli.BoolFlag{
				Name:  "anonymous",
				Usage: "hide sensitive information from logging",
			},
			mcli.BoolFlag{
				Name:  "json",
				Usage: "output server logs and startup information in json format",
			},
			mcli.BoolFlag{
				Name:  "quiet",
				Usage: "disable MinIO startup information",
			},
		},
	}
	return app.Run(args)
}

func gateway2(ctx *mcli.Context) error {
	// minio.ServerMainForJFS(ctx, jfsGateway)
	// minio.StartGateway(ctx, &mnas.NAS{ctx.Args().First()})
	mnas.NasGatewayMain(ctx)
	return nil
}

func getMountPoint(mdsaddr string, fsId uint32, fsName string) (string, error) {
	caller := fs.NewFsCommand()
	caller.Flag(config.CURVEFS_MDSADDR).Changed = true
	caller.Flag(config.CURVEFS_MDSADDR).Value.Set(mdsaddr)
	// Call the function being tested
	listCluster, err := fs.GetClusterFsInfo(caller)
	if err.Code != 0 {
		return "", fmt.Errorf(err.Message)
	}

	if listCluster == nil {
		return "", fmt.Errorf("not found fs info")
	}

	fsInfo := listCluster.GetFsInfo()
	if fsInfo == nil {
		return "", fmt.Errorf("not found fs info")
	}

	// get mount point by fsName
	for _, fs := range fsInfo {
		if fs.GetFsId() == fsId || fs.GetFsName() == fsName {
			mountPoints := fs.GetMountpoints()
			if len(mountPoints) > 0 {
				return mountPoints[0].GetPath(), nil
			}
		}
	}
	return "", fmt.Errorf("not found mount point")
}
