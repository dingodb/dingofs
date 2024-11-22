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
 * Created Date: 2022-06-06
 * Author: chengyi (Cyber-SiKu)
 */

package status

import (
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status/cluster"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status/copyset"
	etcd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status/etcd"
	mds "github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status/mds"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status/metaserver"
	"github.com/spf13/cobra"
)

type StatusCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*StatusCommand)(nil) // check interface

func (statusCmd *StatusCommand) AddSubCommands() {
	statusCmd.Cmd.AddCommand(
		mds.NewMdsCommand(),
		metaserver.NewMetaserverCommand(),
		etcd.NewEtcdCommand(),
		copyset.NewCopysetCommand(),
		cluster.NewClusterCommand(),
	)
}

func NewStatusCommand() *cobra.Command {
	statusCmd := &StatusCommand{
		basecmd.MidCurveCmd{
			Use:   "status",
			Short: "get the status of dingofs",
		},
	}
	return basecmd.NewMidCurveCli(&statusCmd.MidCurveCmd, statusCmd)
}
