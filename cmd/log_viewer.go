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
	"context"
	"errors"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/harvester/upgrade-toolkit/pkg/logviewer"
)

// LogViewerCommand implements the log-viewer subcommand.
type LogViewerCommand struct {
	scanInterval time.Duration
	fs           *flag.FlagSet
}

func (c *LogViewerCommand) Name() string {
	return "log-viewer"
}

func (c *LogViewerCommand) FlagSet() *flag.FlagSet {
	if c.fs == nil {
		c.fs = flag.NewFlagSet("log-viewer", flag.ExitOnError)
		c.fs.DurationVar(&c.scanInterval, "scan-interval", logviewer.DefaultScanInterval,
			"How often to re-scan directories for new .log files.")
	}
	return c.fs
}

func (c *LogViewerCommand) Run() error {
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("log-viewer")

	dirs := c.fs.Args()
	if len(dirs) == 0 {
		return fmt.Errorf("at least one directory path is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	v := logviewer.NewViewer(logviewer.Config{
		WatchDirs:    dirs,
		ScanInterval: c.scanInterval,
	}, log)

	if err := v.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
