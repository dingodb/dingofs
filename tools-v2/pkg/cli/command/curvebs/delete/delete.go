/*
 * Project: tools-v2
 * Created Date: 2022-11-14
 * Author: shentupenghui@gmail.com
 */

package delete

import (
	"github.com/spf13/cobra"

	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/curvebs/delete/file"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/curvebs/delete/peer"
)

type DeleteCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*DeleteCommand)(nil) // check interface

func (dCmd *DeleteCommand) AddSubCommands() {
	dCmd.Cmd.AddCommand(
		file.NewFileCommand(),
		peer.NewCommand(),
	)
}

func NewDeleteCommand() *cobra.Command {
	dCmd := &DeleteCommand{
		basecmd.MidCurveCmd{
			Use:   "delete",
			Short: "delete resources in the curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&dCmd.MidCurveCmd, dCmd)
}
