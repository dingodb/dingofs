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

package cli

import (
	"fmt"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/check"
	quotaconfig "github.com/dingodb/dingofs/tools-v2/pkg/cli/command/config"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/create"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/delete"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/gateway"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/list"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/quota"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/stats"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/umount"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/usage"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/warmup"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/warmup/query"
	"github.com/dingodb/dingofs/tools-v2/pkg/config"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	cobratemplate "github.com/dingodb/dingofs/tools-v2/internal/utils/template"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/version"
)

func addSubCommands(cmd *cobra.Command) {
	cmd.AddCommand(
		usage.NewUsageCommand(),
		list.NewListCommand(),
		status.NewStatusCommand(),
		umount.NewUmountCommand(),
		query.NewQueryCommand(),
		delete.NewDeleteCommand(),
		create.NewCreateCommand(),
		check.NewCheckCommand(),
		warmup.NewWarmupCommand(),
		stats.NewStatsCommand(),
		quota.NewQuotaCommand(),
		quotaconfig.NewConfigCommand(),
		gateway.NewGatewayCommand(),
		version.NewVersionCommand(),
	)
}

func setupRootCommand(cmd *cobra.Command) {
	cmd.SetVersionTemplate("curve {{.Version}}\n")
	cobratemplate.SetFlagErrorFunc(cmd)
	cobratemplate.SetHelpTemplate(cmd)
	cobratemplate.SetUsageTemplate(cmd)
}

func newCurveCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:     "curve COMMAND [ARGS...]",
		Short:   "curve is a tool for managing curvefs",
		Version: version.Version,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cobratemplate.ShowHelp(os.Stderr)(cmd, args)
			}
			return fmt.Errorf("curve: '%s' is not a curve command.\n"+
				"See 'curve --help'", args[0])
		},
		SilenceUsage: false, // silence usage when an error occurs
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true,
		},
	}

	rootCmd.Flags().BoolP("version", "v", false, "print curve version")
	rootCmd.PersistentFlags().BoolP("help", "h", false, "print help")
	rootCmd.PersistentFlags().StringVarP(&config.ConfPath, "conf", "c", "", "config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)")
	config.AddShowErrorPFlag(rootCmd)
	rootCmd.PersistentFlags().BoolP("verbose", "", false, "show some extra info")
	viper.BindPFlag("useViper", rootCmd.PersistentFlags().Lookup("viper"))

	addSubCommands(rootCmd)
	setupRootCommand(rootCmd)

	return rootCmd
}

func Execute() {
	cobra.OnInitialize(config.InitConfig)
	res := newCurveCommand().Execute()
	if res != nil {
		os.Exit(1)
	}
}
