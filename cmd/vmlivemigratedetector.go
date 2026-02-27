package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/vmlivemigratedetector"
)

// VMLiveMigrateDetectorCommand implements the vm-live-migrate-detector subcommand.
type VMLiveMigrateDetectorCommand struct {
	shutdown   bool
	upgrade    string
	kubeconfig string
	debug      bool
	fs         *flag.FlagSet
}

func (c *VMLiveMigrateDetectorCommand) Name() string {
	return "vm-live-migrate-detector"
}

func (c *VMLiveMigrateDetectorCommand) FlagSet() *flag.FlagSet {
	if c.fs == nil {
		c.fs = flag.NewFlagSet("vm-live-migrate-detector", flag.ExitOnError)
		c.fs.BoolVar(&c.shutdown, "shutdown", false, "Shutdown non-migratable VMs")
		c.fs.StringVar(&c.upgrade, "upgrade", "", "UpgradePlan name; if set, stores non-migratable VM names in a ConfigMap")
		c.fs.StringVar(&c.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "Path to kubeconfig file")
		c.fs.BoolVar(&c.debug, "debug", false, "Enable debug logging")
	}
	return c.fs
}

func (c *VMLiveMigrateDetectorCommand) Run() error {
	if c.debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	logrus.SetOutput(os.Stdout)

	args := c.fs.Args()
	if len(args) < 1 {
		return fmt.Errorf("usage: upgrade-toolkit vm-live-migrate-detector NODENAME [--shutdown] [--upgrade NAME]")
	}
	nodeName := args[0]

	detector := vmlivemigratedetector.NewVMLiveMigrateDetector(vmlivemigratedetector.DetectorOptions{
		KubeConfigPath: c.kubeconfig,
		Shutdown:       c.shutdown,
		NodeName:       nodeName,
		Upgrade:        c.upgrade,
	})

	if err := detector.Init(); err != nil {
		return fmt.Errorf("failed to initialize detector: %w", err)
	}

	ctx := context.Background()
	return detector.Run(ctx)
}
