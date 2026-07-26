/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/harvester/upgrade-toolkit/pkg/logshipper"
)

// LogCollectorCommand implements the log-collector subcommand.
type LogCollectorCommand struct {
	listen      string
	logDir      string
	upgradePlan string
	fs          *flag.FlagSet
}

func (c *LogCollectorCommand) Name() string {
	return "log-collector"
}

func (c *LogCollectorCommand) FlagSet() *flag.FlagSet {
	if c.fs == nil {
		c.fs = flag.NewFlagSet("log-collector", flag.ExitOnError)
		c.fs.StringVar(&c.listen, "listen", ":9500", "Address to listen on for gRPC connections.")
		c.fs.StringVar(&c.logDir, "log-dir", "/logs", "Directory to write collected logs to.")
		c.fs.StringVar(&c.upgradePlan, "upgrade-plan", "", "Name of the UpgradePlan this collector serves.")
	}
	return c.fs
}

func (c *LogCollectorCommand) Run() error {
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("log-collector")

	if c.upgradePlan == "" {
		return fmt.Errorf("--upgrade-plan is required")
	}

	if err := os.MkdirAll(c.logDir, 0o755); err != nil {
		return fmt.Errorf("creating log directory: %w", err)
	}

	collector := logshipper.NewCollector(c.logDir, log)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		log.Info("received shutdown signal, stopping collector")
		collector.GracefulStop()
	}()

	return collector.Serve(c.listen)
}
