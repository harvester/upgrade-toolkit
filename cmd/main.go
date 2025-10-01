/*
Copyright 2025.

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
	"crypto/tls"
	"flag"
	"fmt"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/internal/controller"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(harvesterv1beta1.AddToScheme(scheme))
	utilruntime.Must(upgradev1.AddToScheme(scheme))

	utilruntime.Must(managementv1beta1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// Command interface for subcommands
type Command interface {
	Name() string
	FlagSet() *flag.FlagSet
	Run() error
}

var commands = []Command{
	&ManagerCommand{},
	// Add new commands here
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmdName := os.Args[1]
	for _, cmd := range commands {
		if cmd.Name() == cmdName {
			_ = cmd.FlagSet().Parse(os.Args[2:])
			if err := cmd.Run(); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmdName)
	printUsage()
	os.Exit(1)
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <command>\n\nAvailable commands:\n", os.Args[0])
	for _, cmd := range commands {
		fmt.Fprintf(os.Stderr, "  %s\n", cmd.Name())
	}
}

// ManagerCommand implements the manager subcommand
type ManagerCommand struct {
	metricsAddr          string
	metricsCertPath      string
	metricsCertName      string
	metricsCertKey       string
	webhookCertPath      string
	webhookCertName      string
	webhookCertKey       string
	enableLeaderElection bool
	probeAddr            string
	secureMetrics        bool
	enableHTTP2          bool
	fs                   *flag.FlagSet
	zapOpts              zap.Options
}

func (c *ManagerCommand) Name() string {
	return "manager"
}

func (c *ManagerCommand) FlagSet() *flag.FlagSet {
	if c.fs == nil {
		c.fs = flag.NewFlagSet("manager", flag.ExitOnError)
		c.fs.StringVar(&c.metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
			"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
		c.fs.StringVar(&c.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
		c.fs.BoolVar(&c.enableLeaderElection, "leader-elect", false,
			"Enable leader election for controller manager. "+
				"Enabling this will ensure there is only one active controller manager.")
		c.fs.BoolVar(&c.secureMetrics, "metrics-secure", true,
			"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
		c.fs.StringVar(&c.webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
		c.fs.StringVar(&c.webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
		c.fs.StringVar(&c.webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
		c.fs.StringVar(&c.metricsCertPath, "metrics-cert-path", "",
			"The directory that contains the metrics server certificate.")
		c.fs.StringVar(&c.metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
		c.fs.StringVar(&c.metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
		c.fs.BoolVar(&c.enableHTTP2, "enable-http2", false,
			"If set, HTTP/2 will be enabled for the metrics and webhook servers")

		c.zapOpts = zap.Options{Development: true}
		c.zapOpts.BindFlags(c.fs)
	}
	return c.fs
}

func (c *ManagerCommand) Run() error {
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&c.zapOpts)))

	var tlsOpts []func(*tls.Config)

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(config *tls.Config) {
		setupLog.Info("disabling http/2")
		config.NextProtos = []string{"http/1.1"}
	}

	if !c.enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(c.webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", c.webhookCertPath, "webhook-cert-name", c.webhookCertName, "webhook-cert-key", c.webhookCertKey)

		webhookServerOptions.CertDir = c.webhookCertPath
		webhookServerOptions.CertName = c.webhookCertName
		webhookServerOptions.KeyName = c.webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   c.metricsAddr,
		SecureServing: c.secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if c.secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(c.metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", c.metricsCertPath, "metrics-cert-name", c.metricsCertName, "metrics-cert-key", c.metricsCertKey)

		metricsServerOptions.CertDir = c.metricsCertPath
		metricsServerOptions.CertName = c.metricsCertName
		metricsServerOptions.KeyName = c.metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				"harvester-system": {},
				"cattle-system":    {},
				"kube-system":      {},
			},
		},
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: c.probeAddr,
		LeaderElection:         c.enableLeaderElection,
		LeaderElectionID:       "3509939c.harvesterhci.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		return err
	}

	ctx := ctrl.SetupSignalHandler()

	if err := (&controller.UpgradePlanReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    logf.FromContext(ctx).WithName("upgrade-plan-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "UpgradePlan")
		return err
	}
	if err := (&controller.VersionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    logf.FromContext(ctx).WithName("version-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Version")
		return err
	}
	if err := (&controller.JobReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    logf.FromContext(ctx).WithName("job-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Job")
		return err
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		return err
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		return err
	}

	return nil
}
