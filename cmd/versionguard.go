package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
	"github.com/harvester/upgrade-toolkit/pkg/upgradehelper/versionguard"
)

// VersionGuardCommand implements the version-guard subcommand.
type VersionGuardCommand struct {
	strict           bool
	minUpgradableVer string
	kubeconfig       string
	debug            bool
	fs               *flag.FlagSet
}

func (c *VersionGuardCommand) Name() string {
	return "version-guard"
}

func (c *VersionGuardCommand) FlagSet() *flag.FlagSet {
	if c.fs == nil {
		c.fs = flag.NewFlagSet("version-guard", flag.ExitOnError)
		c.fs.BoolVar(&c.strict, "strict", true, "Enable strict mode (prohibit dev-to-release upgrades)")
		c.fs.StringVar(
			&c.minUpgradableVer, "min-upgradable-version", "",
			"Override minimum upgradable version from the upgrade object",
		)
		c.fs.StringVar(&c.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "Path to kubeconfig file")
		c.fs.BoolVar(&c.debug, "debug", false, "Enable debug logging")
	}
	return c.fs
}

func (c *VersionGuardCommand) Run() error {
	if c.debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	logrus.SetOutput(os.Stdout)

	args := c.fs.Args()
	if len(args) < 1 {
		return fmt.Errorf(
			"usage: upgrade-toolkit version-guard UPGRADEPLANNAME" +
				" [--strict] [--min-upgradable-version VERSION]",
		)
	}
	upgradePlanName := args[0]

	restConfig, err := clientcmd.BuildConfigFromFlags("", c.kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to build REST config: %w", err)
	}

	upgradePlan, err := getUpgradePlan(restConfig, upgradePlanName)
	if err != nil {
		return fmt.Errorf("failed to get UpgradePlan %s: %w", upgradePlanName, err)
	}

	return versionguard.Check(upgradePlan, c.strict, c.minUpgradableVer)
}

func getUpgradePlan(restConfig *rest.Config, name string) (*managementv1beta1.UpgradePlan, error) {
	s := runtime.NewScheme()
	_ = managementv1beta1.AddToScheme(s)

	crdConfig := *restConfig
	crdConfig.GroupVersion = &managementv1beta1.GroupVersion
	crdConfig.APIPath = "/apis"
	crdConfig.NegotiatedSerializer = serializer.NewCodecFactory(s)

	client, err := rest.RESTClientFor(&crdConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST client: %w", err)
	}

	upgradePlan := &managementv1beta1.UpgradePlan{}
	err = client.Get().
		Resource("upgradeplans").
		Name(name).
		Do(context.Background()).
		Into(upgradePlan)
	if err != nil {
		return nil, err
	}
	return upgradePlan, nil
}
