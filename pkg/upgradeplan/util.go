package upgradeplan

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

func getPreviousVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan == nil {
		return "nonexistent"
	}

	if upgradePlan.Spec.Upgrade != nil {
		return *upgradePlan.Spec.Upgrade
	}

	if upgradePlan.Status.PreviousVersion != nil {
		return *upgradePlan.Status.PreviousVersion
	}

	return upgradePlan.Spec.Version
}

func getUpgradeVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan == nil {
		return "nonexistent"
	}

	if upgradePlan.Spec.Upgrade != nil {
		return *upgradePlan.Spec.Upgrade
	}

	return upgradePlan.Spec.Version
}

func sanitizedVersion(version string) string {
	return strings.Replace(version, "+", "-", 1)
}

func getKubernetesVersion(upgradePlan *managementv1beta1.UpgradePlan) string {
	if upgradePlan != nil && upgradePlan.Status.ReleaseMetadata != nil {
		return sanitizedVersion(upgradePlan.Status.ReleaseMetadata.Kubernetes)
	}
	return ""
}

func getDefaultTolerations() []corev1.Toleration {
	return []corev1.Toleration{
		{
			Key:      corev1.TaintNodeUnschedulable,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		{
			Key:      corev1.TaintNodeUnreachable,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
		{
			Key:      "node-role.kubernetes.io/control-plane",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
		{
			Key:      "node-role.kubernetes.io/etcd",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
		{
			Key:      "kubernetes.io/arch",
			Operator: corev1.TolerationOpEqual,
			Effect:   corev1.TaintEffectNoSchedule,
			Value:    "amd64",
		},
		{
			Key:      "kubernetes.io/arch",
			Operator: corev1.TolerationOpEqual,
			Effect:   corev1.TaintEffectNoSchedule,
			Value:    "arm64",
		},
		{
			Key:      "kubernetes.io/arch",
			Operator: corev1.TolerationOpEqual,
			Effect:   corev1.TaintEffectNoSchedule,
			Value:    "arm",
		},
		{
			Key:      "kubevirt.io/drain",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		{
			Key:      "CriticalAddonsOnly",
			Operator: corev1.TolerationOpExists,
		},
	}
}

// finalize wraps up a finished UpgradePlan, whether it's a success or a failure.
func finalize(upgradePlan *managementv1beta1.UpgradePlan) {
	markUpgradePlanComplete(upgradePlan)

	upgradePlan.Status.Phase = managementv1beta1.UpgradePlanPhaseSucceeded
}

func isTerminalPhase(phase managementv1beta1.UpgradePlanPhase) bool {
	return phase == managementv1beta1.UpgradePlanPhaseSucceeded ||
		phase == managementv1beta1.UpgradePlanPhaseFailed
}

// markUpgradePlanComplete marks UpgradePlan as no longer being processed due to reaching one of the terminating phases.
func markUpgradePlanComplete(upgradePlan *managementv1beta1.UpgradePlan) {
	if isTerminalPhase(upgradePlan.Status.Phase) {
		upgradePlan.SetCondition(
			managementv1beta1.UpgradePlanAvailable,
			metav1.ConditionFalse,
			"Executed",
			"Entered one of the terminal phases",
		)
		upgradePlan.SetCondition(
			managementv1beta1.UpgradePlanProgressing,
			metav1.ConditionFalse,
			string(upgradePlan.Status.Phase),
			"UpgradePlan has completed",
		)
	}
}
