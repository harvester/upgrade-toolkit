package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	upgradelog "github.com/harvester/upgrade-toolkit/pkg/log"
	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/restorevm"
)

// RestoreVMCommand implements the restore-vm subcommand.
type RestoreVMCommand struct {
	nodeName   string
	upgrade    string
	kubeconfig string
	logLevel   int
	logFormat  string
	fs         *flag.FlagSet
}

func (c *RestoreVMCommand) Name() string {
	return "restore-vm"
}

func (c *RestoreVMCommand) FlagSet() *flag.FlagSet {
	if c.fs == nil {
		c.fs = flag.NewFlagSet("restore-vm", flag.ExitOnError)
		c.fs.StringVar(&c.nodeName, "node", "", "Node name (required)")
		c.fs.StringVar(&c.upgrade, "upgrade", "", "UpgradePlan name (required)")
		c.fs.StringVar(&c.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "Path to kubeconfig file")
		c.fs.IntVar(&c.logLevel, "log-level", 0, "Log verbosity level (0=info, 1=debug, 2=trace)")
		c.fs.StringVar(&c.logFormat, "log-format", "json", "Log format (json or console)")
	}
	return c.fs
}

func (c *RestoreVMCommand) Run() error {
	log, err := upgradelog.NewLogger(c.logFormat == upgradelog.FormatConsole, c.logLevel)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	log = log.WithName(c.Name())

	if c.nodeName == "" {
		return fmt.Errorf("--node is required")
	}
	if c.upgrade == "" {
		return fmt.Errorf("--upgrade is required")
	}

	handler, err := restorevm.NewRestoreVMHandler(log, c.kubeconfig, "", c.nodeName, c.upgrade)
	if err != nil {
		return fmt.Errorf("failed to initialize restore-vm handler: %w", err)
	}

	ctx := context.Background()
	return handler.Run(ctx)
}
