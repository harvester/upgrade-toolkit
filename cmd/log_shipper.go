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
	"os"
	"os/signal"
	"syscall"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/harvester/upgrade-toolkit/pkg/logshipper"
)

// LogShipperCommand implements the log-shipper subcommand.
type LogShipperCommand struct {
	logDir            string
	collectorEndpoint string
	podName           string
	podNamespace      string
	component         string
	nodeName          string
	fs                *flag.FlagSet
}

func (c *LogShipperCommand) Name() string {
	return "log-shipper"
}

func (c *LogShipperCommand) FlagSet() *flag.FlagSet {
	if c.fs == nil {
		c.fs = flag.NewFlagSet("log-shipper", flag.ExitOnError)
		c.fs.StringVar(&c.logDir, "log-dir", "/upgrade-log-shared", "Directory containing log files to ship.")
		c.fs.StringVar(&c.collectorEndpoint, "collector-endpoint", "", "gRPC endpoint of the log collector service.")
		c.fs.StringVar(&c.podName, "pod-name", "", "Name of the pod this shipper runs in.")
		c.fs.StringVar(&c.podNamespace, "pod-namespace", "", "Namespace of the pod this shipper runs in.")
		c.fs.StringVar(&c.component, "component", "", "Upgrade component label (e.g., cluster-upgrade, node-upgrade).")
		c.fs.StringVar(&c.nodeName, "node-name", "", "Node name where this pod is running.")
	}
	return c.fs
}

func (c *LogShipperCommand) Run() error {
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("log-shipper")

	if c.collectorEndpoint == "" {
		return fmt.Errorf("--collector-endpoint is required")
	}

	// Allow env vars as fallbacks for downward API values
	if c.podName == "" {
		c.podName = os.Getenv("POD_NAME")
	}
	if c.podNamespace == "" {
		c.podNamespace = os.Getenv("POD_NAMESPACE")
	}
	if c.nodeName == "" {
		c.nodeName = os.Getenv("NODE_NAME")
	}
	if c.component == "" {
		c.component = os.Getenv("COMPONENT")
	}

	config := logshipper.ShipperConfig{
		LogDir:            c.logDir,
		CollectorEndpoint: c.collectorEndpoint,
		PodName:           c.podName,
		PodNamespace:      c.podNamespace,
		Component:         c.component,
		NodeName:          c.nodeName,
	}

	shipper := logshipper.NewShipper(config, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		log.Info("received shutdown signal, stopping shipper")
		cancel()
	}()

	if err := shipper.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
