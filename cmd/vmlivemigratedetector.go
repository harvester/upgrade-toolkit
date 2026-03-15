package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	upgradelog "github.com/harvester/upgrade-toolkit/pkg/log"
	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/vmlivemigratedetector"
)

// VMLiveMigrateDetectorCommand implements the vm-live-migrate-detector subcommand.
type VMLiveMigrateDetectorCommand struct {
	shutdown   bool
	upgrade    string
	kubeconfig string
	logLevel   int
	logFormat  string
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
		c.fs.IntVar(&c.logLevel, "log-level", 0, "Log verbosity level (0=info, 1=debug, 2=trace)")
		c.fs.StringVar(&c.logFormat, "log-format", "json", "Log format (json or console)")
	}
	return c.fs
}

func (c *VMLiveMigrateDetectorCommand) Run() error {
	log, err := upgradelog.NewLogger(c.logFormat == upgradelog.FormatConsole, c.logLevel)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	log = log.WithName(c.Name())

	args := c.fs.Args()
	if len(args) < 1 {
		return fmt.Errorf("usage: upgrade-toolkit vm-live-migrate-detector NODENAME [--shutdown] [--upgrade NAME]")
	}
	nodeName := args[0]

	detector := vmlivemigratedetector.NewVMLiveMigrateDetector(log, vmlivemigratedetector.DetectorOptions{
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
