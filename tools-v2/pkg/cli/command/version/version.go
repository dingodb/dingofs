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

package version

import (
	"fmt"

	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/spf13/cobra"
)

type VersionCommand struct {
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*VersionCommand)(nil) // check interface

const (
	versionExample = `$ curve version`
)

func NewVersionCommand() *cobra.Command {
	return NewStatusVersionCommand().Cmd
}

func (cCmd *VersionCommand) AddFlags() {
}

func (cCmd *VersionCommand) Init(cmd *cobra.Command, args []string) error {
	return nil
}

func (cCmd *VersionCommand) Print(cmd *cobra.Command, args []string) error {
	return nil
}

func (cCmd *VersionCommand) RunCommand(cmd *cobra.Command, args []string) error {
	fmt.Println("Version:", Version)
	fmt.Println("Build Date:", BuildDate)
	fmt.Println("Git Commit:", GitCommit)
	fmt.Println("Go Version:", GoVersion)
	fmt.Println("OS / Arch:", OsArch)
	return nil
}

func (cCmd *VersionCommand) ResultPlainOutput() error {
	return nil
}

func NewStatusVersionCommand() *VersionCommand {
	versionCmd := &VersionCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "version",
			Short:   "Show the curve version information",
			Example: versionExample,
		},
	}
	basecmd.NewFinalCurveCli(&versionCmd.FinalCurveCmd, versionCmd)
	return versionCmd
}
