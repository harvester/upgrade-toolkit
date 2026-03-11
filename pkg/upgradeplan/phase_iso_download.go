package upgradeplan

import (
	"context"
	"fmt"
	"strings"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	"github.com/rancher/wrangler/v3/pkg/name"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

// ISODownloadPhase downloads the upgrade ISO via VirtualMachineImage.
type ISODownloadPhase struct {
	*PhaseDeps
}

func NewISODownloadPhase(deps *PhaseDeps) *ISODownloadPhase {
	return &ISODownloadPhase{PhaseDeps: deps}
}

func (p *ISODownloadPhase) Name() string { return "ISODownload" }

func (p *ISODownloadPhase) Run(
	ctx context.Context,
	upgradePlan *managementv1beta1.UpgradePlan,
) (ctrl.Result, error) {
	p.Log.V(1).Info("handle iso download")

	var vmImage *harvesterv1beta1.VirtualMachineImage
	var err error

	if upgradePlan.Spec.Image != nil {
		vmImage, err = p.fetchExistingVirtualMachineImage(ctx, *upgradePlan.Spec.Image)
	} else {
		vmImage, err = p.getOrCreateVirtualMachineImageForRepo(ctx, upgradePlan)
	}
	if err != nil {
		p.Log.Error(err, "unable to retrieve iso vmimage from upgradeplan")
		return ctrl.Result{}, err
	}

	if upgradePlan.Status.ISOImageID == nil {
		upgradePlan.Status.ISOImageID = ptr.To(fmt.Sprintf("%s/%s", vmImage.Namespace, vmImage.Name))
	}

	imported, success := isVirtualMachineImageImported(vmImage)

	if !imported {
		p.Log.V(1).Info("iso image downloading")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseISODownloading, "")
		return ctrl.Result{}, nil
	}

	if !success {
		p.Log.V(0).Info("iso image download failed")
		updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseFailed, "ISO image download failed")
		return ctrl.Result{}, nil
	}

	updateProgressingPhase(upgradePlan, managementv1beta1.UpgradePlanPhaseISODownloaded, "")
	return ctrl.Result{}, nil
}

// fetchExistingVirtualMachineImage retrieves a user-provided VirtualMachineImage
// by its namespaced name (format: "namespace/name").
func (p *ISODownloadPhase) fetchExistingVirtualMachineImage(
	ctx context.Context,
	imageRef string,
) (*harvesterv1beta1.VirtualMachineImage, error) {
	ns, n, ok := strings.Cut(imageRef, "/")
	if !ok {
		return nil, fmt.Errorf("invalid image reference %q: expected namespace/name format", imageRef)
	}

	var vmImage harvesterv1beta1.VirtualMachineImage
	if err := p.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: n}, &vmImage); err != nil {
		return nil, fmt.Errorf("failed to get VirtualMachineImage %s: %w", imageRef, err)
	}
	return &vmImage, nil
}

func (p *ISODownloadPhase) getOrCreateVirtualMachineImageForRepo(
	ctx context.Context,
	up *managementv1beta1.UpgradePlan,
) (*harvesterv1beta1.VirtualMachineImage, error) {
	nn := types.NamespacedName{
		Namespace: harvesterSystemNamespace,
		Name:      name.SafeConcatName(up.Name, imageComponent),
	}
	return GetOrCreate(
		ctx, p.Client, p.Scheme, nn,
		func() *harvesterv1beta1.VirtualMachineImage { return &harvesterv1beta1.VirtualMachineImage{} },
		func() *harvesterv1beta1.VirtualMachineImage { return constructVirtualMachineImage(up) },
		up,
	)
}

func constructVirtualMachineImage(upgradePlan *managementv1beta1.UpgradePlan) *harvesterv1beta1.VirtualMachineImage {
	displayName := upgradePlan.Name
	if upgradePlan.Spec.Version != nil {
		displayName = fmt.Sprintf("%s-%s", upgradePlan.Name, *upgradePlan.Spec.Version)
	}
	imageName := name.SafeConcatName(upgradePlan.Name, imageComponent)

	vmImage := &harvesterv1beta1.VirtualMachineImage{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				HarvesterUpgradeImageAnnotation: "True",
			},
			Labels: map[string]string{
				HarvesterUpgradePlanLabel:      upgradePlan.Name,
				HarvesterUpgradeComponentLabel: imageComponent,
			},
			Name:      imageName,
			Namespace: harvesterSystemNamespace,
		},
		Spec: harvesterv1beta1.VirtualMachineImageSpec{
			Backend:                harvesterv1beta1.VMIBackendCDI,
			DisplayName:            displayName,
			SourceType:             harvesterv1beta1.VirtualMachineImageSourceTypeDownload,
			URL:                    upgradePlan.Status.Version.ISODownloadURL,
			Checksum:               ptr.Deref(upgradePlan.Status.Version.ISOChecksum, ""),
			Retry:                  3,
			TargetStorageClassName: longhornStaticStorageClassName,
		},
	}
	return vmImage
}
