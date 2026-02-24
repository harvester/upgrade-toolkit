package upgradeplan

import (
	"context"
	"fmt"

	upgradev1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// NodeUpgradePhase performs sequential k8s + OS upgrades per node via Plans.
type NodeUpgradePhase struct {
	*PhaseDeps
}

func NewNodeUpgradePhase(deps *PhaseDeps) *NodeUpgradePhase {
	return &NodeUpgradePhase{PhaseDeps: deps}
}

func (p *NodeUpgradePhase) Name() string { return "NodeUpgrade" }

func (p *NodeUpgradePhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle node upgrade")

	kubernetesUpgradePlan, err := p.getOrCreatePlanForKubernetesUpgrade(ctx, upgradePlan)
	if err != nil {
		p.Log.Error(err, "unable to retrieve kubernetes-upgrade plan from upgradeplan")
		return ctrl.Result{}, err
	}

	if !isPlanFinished(kubernetesUpgradePlan) {
		if isAnyPlanJobFailed(kubernetesUpgradePlan) {
			p.Log.V(0).Info("kubernetes-upgrade job failed")
			updateProgressingPhase(
				upgradePlan,
				managementv1beta1.UpgradePlanPhaseFailed,
				"kubernetes-upgrade plan job(s) failed",
			)
			return ctrl.Result{}, nil
		}

		p.Log.V(1).Info("kubernetes-upgrade plan running")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgrading, "")
		return ctrl.Result{}, nil
	}

	if !isSkipOSUpgrade(upgradePlan) {
		osUpgradePlan, err := p.getOrCreatePlanForOSUpgrade(ctx, upgradePlan)
		if err != nil {
			p.Log.Error(err, "unable to retrieve os-upgrade plan from upgradeplan")
			return ctrl.Result{}, err
		}

		if !isPlanFinished(osUpgradePlan) {
			if isAnyPlanJobFailed(osUpgradePlan) {
				p.Log.V(0).Info("os-upgrade job failed")
				updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "os-upgrade plan job(s) failed")
				return ctrl.Result{}, nil
			}

			p.Log.V(1).Info("os-upgrade plan running")
			updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgrading, "")
			return ctrl.Result{}, nil
		}
	}

	// Check that all nodes reached their desired terminal upgrade state
	for nodeName, status := range upgradePlan.Status.NodeUpgradeStatuses {
		if !isTerminalState(status, upgradePlan.Spec.SkipOSUpgrade) {
			updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgrading, "")
			return ctrl.Result{}, nil
		}
		p.Log.V(1).Info("node has reached the desired node upgrade state", "nodeName", nodeName)
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseNodeUpgraded, "")
	return ctrl.Result{}, nil
}

func (p *NodeUpgradePhase) getOrCreatePlanForKubernetesUpgrade(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*upgradev1.Plan, error) {
	nn := types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      fmt.Sprintf("%s-%s-%s", up.Name, NodeComponent, NodeUpgradeTypeKubernetes),
	}
	single, err := p.isSingleNodeCluster(ctx)
	if err != nil {
		return nil, err
	}
	return getOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *upgradev1.Plan { return &upgradev1.Plan{} },
		func() *upgradev1.Plan { return constructPlanForKubernetesUpgrade(up, !single) },
		up,
	)
}

func (p *NodeUpgradePhase) getOrCreatePlanForOSUpgrade(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*upgradev1.Plan, error) {
	nn := types.NamespacedName{
		Namespace: cattleSystemNamespace,
		Name:      fmt.Sprintf("%s-%s-%s", up.Name, NodeComponent, NodeUpgradeTypeOS),
	}
	single, err := p.isSingleNodeCluster(ctx)
	if err != nil {
		return nil, err
	}
	return getOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *upgradev1.Plan { return &upgradev1.Plan{} },
		func() *upgradev1.Plan { return constructPlanForOSUpgrade(up, !single) },
		up,
	)
}

func (p *NodeUpgradePhase) isSingleNodeCluster(ctx context.Context) (bool, error) {
	var nodeList corev1.NodeList
	if err := p.Client.List(ctx, &nodeList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			harvesterManagedLabel: "true",
		}),
	}); err != nil {
		return false, err
	}
	return len(nodeList.Items) == 1, nil
}

func constructPlanForKubernetesUpgrade(upgradePlan *managementv1beta1.UpgradePlan, maintenance bool) *upgradev1.Plan {
	selector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      harvesterManagedLabel,
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"true"},
			},
			{
				Key:      fmt.Sprintf("%s/%s", LabelPrefix, upgradePlan.Name),
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{KubernetesUpgradeState},
			},
		},
	}
	prepare := &upgradev1.ContainerSpec{
		Image:   fmt.Sprintf("%s:%s", upgradeToolkitImage, getUpgradeVersion(upgradePlan)),
		Command: []string{"upgrade_node.sh"},
		Args:    []string{"pre-drain"},
		Env: []corev1.EnvVar{
			{
				Name:  "HARVESTER_UPGRADEPLAN_NAME",
				Value: upgradePlan.Name,
			},
			{
				Name: "HARVESTER_UPGRADE_NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
		},
	}
	container := &upgradev1.ContainerSpec{
		Image: rke2UpgradeImage,
	}
	version := getKubernetesVersion(upgradePlan)

	plan := constructPlan(upgradePlan.Name, NodeComponent, 1, selector, maintenance, prepare, container, version)
	plan.Name += "-" + NodeUpgradeTypeKubernetes
	plan.Labels[HarvesterNodeUpgradeTypeLabel] = NodeUpgradeTypeKubernetes

	return plan
}

func constructPlanForOSUpgrade(upgradePlan *managementv1beta1.UpgradePlan, maintenance bool) *upgradev1.Plan {
	selector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      harvesterManagedLabel,
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"true"},
			},
			{
				Key:      fmt.Sprintf("%s/%s", LabelPrefix, upgradePlan.Name),
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{OSUpgradeState},
			},
		},
	}
	prepare := &upgradev1.ContainerSpec{
		Image:   upgradeToolkitImage,
		Command: []string{"upgrade_node.sh"},
		Args:    []string{"pre-drain"},
		Env: []corev1.EnvVar{
			{
				Name:  "HARVESTER_UPGRADEPLAN_NAME",
				Value: upgradePlan.Name,
			},
			{
				Name: "HARVESTER_UPGRADE_NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
		},
	}
	container := &upgradev1.ContainerSpec{
		Image:   upgradeToolkitImage,
		Command: []string{"upgrade_node.sh"},
		Args:    []string{"post-drain"},
		Env: []corev1.EnvVar{
			{
				Name:  "HARVESTER_UPGRADEPLAN_NAME",
				Value: upgradePlan.Name,
			},
			{
				Name: "HARVESTER_UPGRADE_NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
			{
				Name: "HARVESTER_UPGRADE_POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
		},
	}
	version := getUpgradeVersion(upgradePlan)

	plan := constructPlan(upgradePlan.Name, NodeComponent, 1, selector, maintenance, prepare, container, version)
	plan.Name += "-" + NodeUpgradeTypeOS
	plan.Labels[HarvesterNodeUpgradeTypeLabel] = NodeUpgradeTypeOS

	return plan
}
