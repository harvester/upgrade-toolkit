package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/restorevm"
)

// RestoreVMCommand implements the restore-vm subcommand.
type RestoreVMCommand struct {
	nodeName   string
	upgrade    string
	kubeconfig string
	debug      bool
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
		c.fs.BoolVar(&c.debug, "debug", false, "Enable debug logging")
	}
	return c.fs
}

func (c *RestoreVMCommand) Run() error {
	if c.debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	logrus.SetOutput(os.Stdout)

	if c.nodeName == "" {
		return fmt.Errorf("--node is required")
	}
	if c.upgrade == "" {
		return fmt.Errorf("--upgrade is required")
	}

	handler, err := restorevm.NewRestoreVMHandler(c.kubeconfig, "", c.nodeName, c.upgrade)
	if err != nil {
		return fmt.Errorf("failed to initialize restore-vm handler: %w", err)
	}

	ctx := context.Background()
	return handler.Run(ctx)
}
