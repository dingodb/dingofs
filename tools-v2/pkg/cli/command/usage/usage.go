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
 * Created Date: 2022-05-25
 * Author: chengyi (Cyber-SiKu)
 */

package usage

import (
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	inode "github.com/dingodb/dingofs/tools-v2/pkg/cli/command/usage/inode"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/usage/metadata"
	"github.com/spf13/cobra"
)

type UsageCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*UsageCommand)(nil) // check interface

func (usageCmd *UsageCommand) AddSubCommands() {
	usageCmd.Cmd.AddCommand(
		inode.NewInodeNumCommand(),
		metadata.NewMetadataCommand(),
	)
}

func NewUsageCommand() *cobra.Command {
	usageCmd := &UsageCommand{
		basecmd.MidCurveCmd{
			Use:   "usage",
			Short: "get the usage info of dingofs",
		},
	}
	return basecmd.NewMidCurveCli(&usageCmd.MidCurveCmd, usageCmd)
}
