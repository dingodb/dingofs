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
 * Created Date: 2022-08-10
 * Author: chengyi (Cyber-SiKu)
 */

package warmup

import (
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/warmup/add"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/warmup/query"
	"github.com/spf13/cobra"
)

type WarmupCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*WarmupCommand)(nil) // check interface

func (warmupCmd *WarmupCommand) AddSubCommands() {
	warmupCmd.Cmd.AddCommand(
		add.NewAddCommand(),
		query.NewQueryCommand(),
	)
}

func NewWarmupCommand() *cobra.Command {
	warmupCmd := &WarmupCommand{
		basecmd.MidCurveCmd{
			Use:   "warmup",
			Short: "add warmup file to local in dingofs",
		},
	}
	return basecmd.NewMidCurveCli(&warmupCmd.MidCurveCmd, warmupCmd)
}
