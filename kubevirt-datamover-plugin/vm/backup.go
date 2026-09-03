// Copyright 2026 Red Hat Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/utils/volumehelper"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	podvolumeutil "github.com/vmware-tanzu/velero/pkg/util/podvolume"
	vhutil "github.com/vmware-tanzu/velero/pkg/util/volumehelper"

	kvcore "kubevirt.io/api/core/v1"

	controllercommon "github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-plugin/kubevirt-datamover-plugin/clients"
)

// apiCallTimeout bounds each individual Kubernetes API call made by this
// plugin so a stuck API server can't hang a Velero backup indefinitely.
const apiCallTimeout = 30 * time.Second

// BackupPlugin is a BackupItemAction plugin for VirtualMachine resources
// that handles kubevirt incremental backup via DataUpload CRs.
type BackupPlugin struct {
	Log               logrus.FieldLogger
	pluginPVCPodCache PluginPVCPodCache
	crClient          crclient.Client

	// dynamicClientMu guards dynamicClient. A BackupPlugin instance lives for
	// the duration of a single backup (mirrors pluginPVCPodCache's caching
	// precedent above), so the dynamic client -- expensive to bootstrap via
	// in-cluster config -- only needs to be built once instead of once per
	// DataUpload API call.
	dynamicClientMu sync.Mutex
	dynamicClient   dynamicClientInterface
}

// dataUploadGVR is the GroupVersionResource for DataUpload custom resources,
// shared by every dynamic-client call site in this file so the group/version/
// resource strings only need to be correct in one place.
var dataUploadGVR = schema.GroupVersionResource{
	Group:    "velero.io",
	Version:  "v2alpha1",
	Resource: "datauploads",
}

// dataUploadResourceClient returns a dynamic resource client scoped to the
// DataUpload GVR, bootstrapping (and caching on the plugin instance) the
// underlying dynamic client on first use.
func (p *BackupPlugin) dataUploadResourceClient() (dynamic.NamespaceableResourceInterface, error) {
	p.dynamicClientMu.Lock()
	defer p.dynamicClientMu.Unlock()

	if p.dynamicClient == nil {
		config, err := clients.GetInClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
		}
		dynamicClient, err := getDynamicClient(config)
		if err != nil {
			return nil, fmt.Errorf("failed to get dynamic client: %w", err)
		}
		p.dynamicClient = dynamicClient
	}

	return p.dynamicClient.Resource(dataUploadGVR), nil
}

type PluginPVCPodCache struct {
	pvcPodCache *podvolumeutil.PVCPodCache
}

var kubevirtCustomPolicy = map[string]any{
	"datamover": "kubevirt",
}

// NewBackupPlugin creates a new BackupPlugin instance.
func NewBackupPlugin(log logrus.FieldLogger, client crclient.Client) (*BackupPlugin, error) {
	crClient := client
	var err error
	if crClient == nil {
		crClient, err = clients.CRClient()
		if err != nil {
			return nil, fmt.Errorf("failed to get controller-runtime client: %w", err)
		}
	}

	return &BackupPlugin{Log: log, crClient: crClient}, nil
}

// Name returns the plugin name.
func (p *BackupPlugin) Name() string {
	return "kubevirt-vm-backup-plugin"
}

// AppliesTo returns a ResourceSelector that determines which resources
// this plugin applies to. This plugin handles VirtualMachine resources.
func (p *BackupPlugin) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{
			"virtualmachines.kubevirt.io",
		},
	}, nil
}

// Execute is called for each VirtualMachine resource during backup.
// It checks preconditions and creates a DataUpload CR for kubevirt datamover.
//
// Returns:
//   - Modified item (with DataUpload annotation)
//   - Additional items to backup
//   - Operation ID for async progress tracking
//   - Items to backup after operation completes
//   - Error if preconditions are not met
func (p *BackupPlugin) Execute(
	item runtime.Unstructured,
	backup *velerov1.Backup,
) (runtime.Unstructured, []velero.ResourceIdentifier, string, []velero.ResourceIdentifier, error) {
	p.Log.Info("[vm-backup] Executing VirtualMachine backup plugin for kubevirt datamover")

	if backup == nil {
		return nil, nil, "", nil, fmt.Errorf("backup object is nil")
	}

	// Convert unstructured to VirtualMachine
	vm := &kvcore.VirtualMachine{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), vm); err != nil {
		return nil, nil, "", nil, fmt.Errorf("failed to convert item to VirtualMachine: %w", err)
	}

	p.Log.Infof("[vm-backup] Processing VirtualMachine %s/%s", vm.Namespace, vm.Name)

	// Get or create the cached VolumeHelper for this backup
	vh, err := p.pluginPVCPodCache.GetOrCreateVolumeHelper(backup, p.crClient, p.Log)
	if err != nil {
		return nil, nil, "", nil, err
	}

	// Check preconditions
	eligible, reason, err := CheckPreconditions(vm, backup, p.Log, vh)
	if err != nil {
		return nil, nil, "", nil, fmt.Errorf("failed to check preconditions: %w", err)
	}

	if !eligible {
		p.Log.Infof("[vm-backup] VirtualMachine %s/%s is not eligible for kubevirt datamover: %s",
			vm.Namespace, vm.Name, reason)
		// Return item as-is without creating DataUpload
		// This allows fallback to other backup mechanisms
		return item, nil, "", nil, nil
	}

	// Generate operation ID for async tracking
	operationID := generateOperationID(backup.Name, vm.Namespace, vm.Name)
	p.Log.Infof("[vm-backup] Generated operation ID: %s", operationID)

	// Get the first PVC with skip volume policy for SourcePVC (kubevirt datamover trigger)
	sourcePVC, err := p.getFirstKubevirtPVC(vm, backup, vh)
	if err != nil {
		return nil, nil, "", nil, fmt.Errorf("failed to get source PVC: %w", err)
	}

	// Create DataUpload CR
	dataUpload, err := p.createDataUpload(vm, backup, operationID, sourcePVC)
	if err != nil {
		return nil, nil, "", nil, fmt.Errorf("failed to create DataUpload: %w", err)
	}

	p.Log.Infof("[vm-backup] Created DataUpload %s/%s for VirtualMachine %s/%s",
		dataUpload.Namespace, dataUpload.Name, vm.Namespace, vm.Name)

	// Read the operationID back off the returned object rather than trusting the
	// locally-generated value: on the idempotent-reuse path (see createDataUpload)
	// this is an existing DataUpload that could in principle have been created by
	// an older plugin build with a different ID scheme, and Progress/Cancel must be
	// given whatever ID actually matches what's stored on the object.
	returnedOperationID := operationID
	if stored := dataUpload.Annotations[controllercommon.AnnotationOperationID]; stored != "" {
		returnedOperationID = stored
	}

	// Add DataUpload annotation to VM
	if vm.Annotations == nil {
		vm.Annotations = make(map[string]string)
	}
	vm.Annotations[velerov1.DataUploadNameAnnotation] = dataUpload.Name
	vm.Annotations[controllercommon.AnnotationOperationID] = returnedOperationID
	vm.Annotations[velerov1.PVCNamespaceNameLabel] = label.GetValidName(fmt.Sprintf("%s.%s", sourcePVC.Namespace, sourcePVC.Name))

	// Convert back to unstructured
	vmMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vm)
	if err != nil {
		return nil, nil, "", nil, fmt.Errorf("failed to convert VirtualMachine to unstructured: %w", err)
	}

	// Return additional items to backup (the DataUpload itself)
	additionalItems := []velero.ResourceIdentifier{
		{
			GroupResource: schema.GroupResource{
				Group:    "velero.io",
				Resource: "datauploads",
			},
			Namespace: dataUpload.Namespace,
			Name:      dataUpload.Name,
		},
	}

	return &unstructured.Unstructured{Object: vmMap}, additionalItems, returnedOperationID, nil, nil
}

// Progress returns the progress of an async backup operation.
// It monitors the DataUpload CR status to report progress.
func (p *BackupPlugin) Progress(operationID string, backup *velerov1.Backup) (velero.OperationProgress, error) {
	p.Log.Infof("[vm-backup] Checking progress for operation %s", operationID)

	progress := velero.OperationProgress{
		Completed:      false,
		OperationUnits: "bytes",
		Description:    "Kubevirt datamover backup in progress",
		Started:        time.Now(),
		Updated:        time.Now(),
	}

	// Get the DataUpload CR to check status
	dataUpload, err := p.getDataUploadByOperationID(operationID, backup)
	if err != nil {
		p.Log.Warnf("[vm-backup] Failed to get DataUpload for operation %s: %v", operationID, err)
		progress.Err = fmt.Sprintf("failed to get DataUpload: %v", err)
		return progress, nil
	}

	if dataUpload == nil {
		p.Log.Errorf("[vm-backup] DataUpload not found for operation %s", operationID)
		progress.Completed = true
		progress.Err = fmt.Sprintf("DataUpload not found for operation %s", operationID)
		progress.Description = "DataUpload not found"
		return progress, nil
	}

	// Update progress based on DataUpload status
	switch dataUpload.Status.Phase {
	case velerov2alpha1.DataUploadPhaseNew:
		progress.Description = "DataUpload created, waiting for processing"
	case velerov2alpha1.DataUploadPhaseAccepted:
		progress.Description = "DataUpload accepted, preparing for backup"
	case velerov2alpha1.DataUploadPhasePrepared:
		progress.Description = "DataUpload prepared, starting data transfer"
	case velerov2alpha1.DataUploadPhaseInProgress:
		progress.Description = "Data transfer in progress"
		progress.NTotal = dataUpload.Status.Progress.TotalBytes
		progress.NCompleted = dataUpload.Status.Progress.BytesDone
	case velerov2alpha1.DataUploadPhaseCompleted:
		progress.Completed = true
		progress.Description = "Backup completed successfully"
		progress.NTotal = dataUpload.Status.Progress.TotalBytes
		progress.NCompleted = dataUpload.Status.Progress.TotalBytes
	case velerov2alpha1.DataUploadPhaseCanceled:
		progress.Completed = true
		progress.Err = "Backup was canceled"
		progress.Description = "Backup canceled"
	case velerov2alpha1.DataUploadPhaseCanceling:
		progress.Description = "Backup is being canceled"
	case velerov2alpha1.DataUploadPhaseFailed:
		progress.Completed = true
		progress.Err = dataUpload.Status.Message
		progress.Description = "Backup failed"
	}

	if dataUpload.Status.StartTimestamp != nil {
		progress.Started = dataUpload.Status.StartTimestamp.Time
	}
	progress.Updated = time.Now()

	p.Log.Infof("[vm-backup] Operation %s progress: phase=%s, completed=%v, bytes=%d/%d",
		operationID, dataUpload.Status.Phase, progress.Completed, progress.NCompleted, progress.NTotal)

	return progress, nil
}

// Cancel requests cancellation of an async backup operation.
func (p *BackupPlugin) Cancel(operationID string, backup *velerov1.Backup) error {
	p.Log.Infof("[vm-backup] Canceling operation %s", operationID)

	dataUpload, err := p.getDataUploadByOperationID(operationID, backup)
	if err != nil {
		return fmt.Errorf("failed to get DataUpload for cancellation: %w", err)
	}

	if dataUpload == nil {
		p.Log.Warnf("[vm-backup] DataUpload not found for operation %s, nothing to cancel", operationID)
		return nil
	}

	// Patch the cancel flag on the DataUpload
	if err := p.patchDataUploadCancel(dataUpload.Namespace, dataUpload.Name, true); err != nil {
		return fmt.Errorf("failed to update DataUpload for cancellation: %w", err)
	}

	p.Log.Infof("[vm-backup] Requested cancellation for DataUpload %s/%s", dataUpload.Namespace, dataUpload.Name)
	return nil
}

// CheckPreconditions verifies that the VirtualMachine meets all requirements
// for kubevirt datamover backup.
func CheckPreconditions(vm *kvcore.VirtualMachine, backup *velerov1.Backup, log logrus.FieldLogger, vh vhutil.VolumeHelper) (bool, string, error) {
	// Check 1: SnapshotMoveData must be true
	if backup.Spec.SnapshotMoveData == nil || !*backup.Spec.SnapshotMoveData {
		return false, "backup.Spec.SnapshotMoveData is not enabled", nil
	}

	// Check 2: VM must be running (offline backup not supported in initial release)
	// Use validation function from controller common package
	if err := controllercommon.ValidateVMIsRunning(vm); err != nil {
		return false, err.Error(), nil
	}

	// Check 3: ChangedBlockTracking must be enabled
	// Use validation function from controller common package
	if err := controllercommon.ValidateCBTEnabled(vm); err != nil {
		return false, err.Error(), nil
	}

	// Check 4: Volume policy check
	// - At least one PVC must have "custom" action with "datamover=kubevirt" parameter (triggers kubevirt datamover)
	// - No PVC can have "snapshot" action (would conflict with kubevirt datamover)
	hasKubevirtPolicy, hasConflictingPolicy, err := checkVolumePolicies(vm, backup, log, vh)
	if err != nil {
		return false, "", fmt.Errorf("failed to check volume policies: %w", err)
	}

	if !hasKubevirtPolicy {
		return false, "no PVCs with custom kubevirt volume policy found for kubevirt datamover", nil
	}

	if hasConflictingPolicy {
		return false, "VirtualMachine has PVCs with snapshot volume policy which conflicts with kubevirt datamover", nil
	}

	return true, "", nil
}

// checkVolumePolicies examines the volume policies for all PVCs associated with the VM.
// Uses Velero's volumehelper to check resource policies from the configmap.
// Returns:
//   - hasKubevirtPolicy: true if VM is eligible for kubevirt datamover
//   - hasConflictingPolicy: true if any PVC has "snapshot" action (conflicts with kubevirt datamover)
//   - error: if there was an error checking policies
func checkVolumePolicies(vm *kvcore.VirtualMachine, backup *velerov1.Backup, log logrus.FieldLogger, vh vhutil.VolumeHelper) (bool, bool, error) {
	// Get all PVCs associated with this VM using controller common function
	pvcNames := controllercommon.GetVolumesForVm(vm)
	if len(pvcNames) == 0 {
		log.Infof("[vm-backup] VirtualMachine %s/%s has no PVCs", vm.Namespace, vm.Name)
		return false, false, nil
	}

	coreClient, err := clients.CoreClient()
	if err != nil {
		return false, false, fmt.Errorf("failed to get core client: %w", err)
	}

	hasConflictingPolicy := false
	hasKubevirtPolicy := false

	for _, pvcName := range pvcNames {
		// Get the PVC
		pvc, err := coreClient.PersistentVolumeClaims(vm.Namespace).Get(context.Background(), pvcName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				// PVC doesn't exist yet (might be a DataVolume that hasn't created PVC yet)
				log.Infof("[vm-backup] PVC %s/%s not found, skipping", vm.Namespace, pvcName)
				continue
			}
			// Other errors (RBAC, timeout, etc.) should be propagated
			return false, false, fmt.Errorf("failed to get PVC %s/%s: %w", vm.Namespace, pvcName, err)
		}

		// Convert PVC to unstructured for volumehelper
		pvcMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pvc)
		if err != nil {
			return false, false, fmt.Errorf("failed to convert PVC to unstructured: %w", err)
		}
		pvcUnstructured := &unstructured.Unstructured{Object: pvcMap}

		// Use Velero's volumehelper to check if this PVC has snapshot policy
		shouldSnapshot, err := vh.ShouldPerformSnapshot(
			pvcUnstructured,
			kuberesource.PersistentVolumeClaims,
		)
		if err != nil {
			return false, false, fmt.Errorf("failed to check volume policy for PVC %s: %w", pvcName, err)
		}

		if shouldSnapshot {
			// This PVC has snapshot policy - conflicts with kubevirt datamover
			hasConflictingPolicy = true
			log.Warnf("[vm-backup] PVC %s/%s has snapshot policy which conflicts with kubevirt datamover", vm.Namespace, pvcName)
		}

		// Use Velero's volumehelper to check if this PVC has snapshot policy
		shouldUseKubevirtDm, err := vh.ShouldPerformCustomAction(
			pvcUnstructured,
			kuberesource.PersistentVolumeClaims,
			kubevirtCustomPolicy,
		)
		if err != nil {
			return false, false, fmt.Errorf("failed to check volume policy for PVC %s: %w", pvcName, err)
		}

		if shouldUseKubevirtDm {
			// This PVC has custom kubevirt policy
			hasKubevirtPolicy = true
			log.Warnf("[vm-backup] PVC %s/%s has kubevirt custom policy for kubevirt datamover", vm.Namespace, pvcName)
		}
	}

	return hasKubevirtPolicy, hasConflictingPolicy, nil
}

// getFirstKubevirtPVC returns the first PVC that doesn't have a snapshot policy.
// This PVC will be used as the SourcePVC in the DataUpload spec.
//
// Current behavior: Returns the first PVC without "snapshot" policy. This ensures we
// don't select a PVC that Velero would handle via CSI snapshot.
//
// TODO(https://github.com/migtools/kubevirt-datamover-plugin/issues/4): After upstream
// Velero changes merge, update this to return the first PVC with "custom" action type
// and kubevirt-specific parameter set. The volumehelper API will need to expose the
// actual action type (not just shouldSnapshot boolean) for this check.
func (p *BackupPlugin) getFirstKubevirtPVC(vm *kvcore.VirtualMachine, backup *velerov1.Backup, vh vhutil.VolumeHelper) (*corev1.PersistentVolumeClaim, error) {
	pvcNames := controllercommon.GetVolumesForVm(vm)
	if len(pvcNames) == 0 {
		return nil, fmt.Errorf("no PVCs found for VirtualMachine %s/%s", vm.Namespace, vm.Name)
	}

	coreClient, err := clients.CoreClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get core client: %w", err)
	}

	for _, pvcName := range pvcNames {
		pvc, err := coreClient.PersistentVolumeClaims(vm.Namespace).Get(context.Background(), pvcName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				// PVC doesn't exist yet (might be a DataVolume that hasn't created PVC yet)
				p.Log.Infof("[vm-backup] PVC %s/%s not found, skipping", vm.Namespace, pvcName)
				continue
			}
			// Other errors (RBAC, timeout, etc.) should be propagated
			return nil, fmt.Errorf("failed to get PVC %s/%s: %w", vm.Namespace, pvcName, err)
		}

		// Convert PVC to unstructured for volumehelper
		pvcMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pvc)
		if err != nil {
			return nil, fmt.Errorf("failed to convert PVC to unstructured: %w", err)
		}
		pvcUnstructured := &unstructured.Unstructured{Object: pvcMap}

		// Check if this PVC has custom kubevirt datamover policy
		shouldUseKubevirtDm, err := vh.ShouldPerformCustomAction(
			pvcUnstructured,
			kuberesource.PersistentVolumeClaims,
			kubevirtCustomPolicy,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to check volume policy for PVC %s: %w", pvcName, err)
		}

		// Return first PVC matching kubevirt dm custom policy action
		if shouldUseKubevirtDm {
			return pvc, nil
		}
	}

	return nil, fmt.Errorf("no PVC eligible for kubevirt datamover found for VirtualMachine %s/%s", vm.Namespace, vm.Name)
}

// maxOperationTimeout bounds the effective DataUpload operation timeout
// regardless of what the Backup requests: without a ceiling, a misconfigured
// ItemOperationTimeout could keep an operation -- and the resources backing
// it (upload pod, scratch PVC) -- alive far longer than any real transfer
// should reasonably take.
const maxOperationTimeout = 24 * time.Hour

// maxCreateAttempts bounds retries of the create-or-adopt loop in
// createDataUpload for the narrow double-race case: Create() reports
// AlreadyExists, but the object is already gone again by the time we
// re-fetch it (something else deleted it in between). A small bounded retry
// lets the create win on a subsequent attempt instead of erroring out on a
// transient race.
const maxCreateAttempts = 3

// createRetryBackoffUnit scales the delay between double-race create
// retries (attempt * createRetryBackoffUnit): a bare immediate retry would
// hammer the API server back-to-back for a race that, by definition, needs
// another actor's Create/Delete to land in between attempts. A var (not a
// const) so tests can shrink it to avoid paying the real delay.
var createRetryBackoffUnit = 100 * time.Millisecond

// createDataUpload creates a DataUpload CR for the kubevirt datamover.
func (p *BackupPlugin) createDataUpload(vm *kvcore.VirtualMachine, backup *velerov1.Backup, operationID string, sourcePVC *corev1.PersistentVolumeClaim) (*velerov2alpha1.DataUpload, error) {
	dataUploadName := generateDataUploadName(backup.Name, vm.Namespace, vm.Name)

	// Get the operation timeout from backup or use default (4 hours)
	// Use ItemOperationTimeout (not CSISnapshotTimeout which is only 10 minutes)
	operationTimeout := metav1.Duration{Duration: 4 * time.Hour}
	if backup.Spec.ItemOperationTimeout.Duration > 0 {
		operationTimeout = backup.Spec.ItemOperationTimeout
	}
	if operationTimeout.Duration > maxOperationTimeout {
		p.Log.Warnf("[vm-backup] ItemOperationTimeout %s exceeds the maximum of %s, capping it",
			operationTimeout.Duration, maxOperationTimeout)
		operationTimeout.Duration = maxOperationTimeout
	}

	dataUpload := &velerov2alpha1.DataUpload{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "velero.io/v2alpha1",
			Kind:       "DataUpload",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataUploadName,
			Namespace: backup.Namespace,
			Labels: map[string]string{
				velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name),
			},
			Annotations: buildDataUploadAnnotations(vm, backup, operationID),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "velero.io/v1",
					Kind:       "Backup",
					Name:       backup.Name,
					UID:        backup.UID,
					Controller: ptr.To(true),
				},
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			// SnapshotType is set to CSI because upstream Velero currently only accepts
			// CSI as a valid enum value. Once Velero adds "kubevirt" as a supported
			// SnapshotType, this should be changed to use a kubevirt-specific value.
			// The DataMover field ("kubevirt") indicates that the kubevirt-datamover-controller
			// should handle this DataUpload rather than Velero's built-in datamover.
			SnapshotType:          velerov2alpha1.SnapshotType(controllercommon.SnapshotTypeCSI),
			DataMover:             controllercommon.DataMoverKubeVirt,
			BackupStorageLocation: backup.Spec.StorageLocation,
			SourceNamespace:       vm.Namespace,
			OperationTimeout:      operationTimeout,
			Cancel:                false,
			// SourcePVC is set to the first PVC with kubevirt volume policy
			SourcePVC: sourcePVC.Name,
		},
	}

	var lastRaceErr error
	for attempt := 1; attempt <= maxCreateAttempts; attempt++ {
		// Create the DataUpload using the Velero client
		if err := p.createDataUploadResource(dataUpload); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return nil, fmt.Errorf("failed to create DataUpload resource: %w", err)
			}
			// A DataUpload with this deterministic name already exists: a prior
			// Execute() call for this same (backup, VM) must have created it, and
			// Velero re-invoked us (e.g. after a transient RPC error). Adopt it
			// instead of erroring, but only after confirming it actually targets the
			// same source PVC we were about to create it for -- a name collision
			// against something else (shouldn't happen given the hash, but cheap to
			// check) must not be silently treated as "our" operation.
			existing, getErr := p.getDataUploadByName(dataUploadName, backup.Namespace)
			if getErr != nil {
				if apierrors.IsNotFound(getErr) {
					// Double-race: something else's Create() is what triggered our
					// AlreadyExists above, but that object is already gone again by
					// the time we re-fetch it (e.g. deleted concurrently). Retry the
					// whole create instead of erroring out on a state that's likely
					// to resolve itself within a couple of attempts.
					lastRaceErr = getErr
					if attempt < maxCreateAttempts {
						time.Sleep(time.Duration(attempt) * createRetryBackoffUnit)
					}
					continue
				}
				return nil, fmt.Errorf("DataUpload %s/%s already exists but could not be re-fetched: %w", backup.Namespace, dataUploadName, getErr)
			}
			if err := p.validateAdoptableDataUpload(existing, vm, backup, sourcePVC); err != nil {
				return nil, err
			}
			p.Log.Infof("[vm-backup] DataUpload %s/%s already exists for this backup+VM, reusing it instead of creating a duplicate",
				backup.Namespace, dataUploadName)
			return existing, nil
		}

		return dataUpload, nil
	}

	return nil, fmt.Errorf("failed to create DataUpload %s/%s after %d attempts (concurrent create/delete race): %w",
		backup.Namespace, dataUploadName, maxCreateAttempts, lastRaceErr)
}

// validateAdoptableDataUpload checks whether existing (a DataUpload found
// under the deterministic name createDataUpload was about to create) may
// safely be adopted in place of creating a new one. A name collision against
// something else (shouldn't happen given the name hash, but cheap to check)
// must not be silently treated as "our" operation.
func (p *BackupPlugin) validateAdoptableDataUpload(existing *velerov2alpha1.DataUpload, vm *kvcore.VirtualMachine, backup *velerov1.Backup, sourcePVC *corev1.PersistentVolumeClaim) error {
	dataUploadName := existing.Name

	if existing.Spec.SourcePVC != sourcePVC.Name || existing.Spec.SourceNamespace != vm.Namespace {
		return fmt.Errorf("existing DataUpload %s/%s targets %s/%s, not %s/%s -- refusing to reuse it",
			backup.Namespace, dataUploadName, existing.Spec.SourceNamespace, existing.Spec.SourcePVC, vm.Namespace, sourcePVC.Name)
	}
	if !hasOwnerUID(existing.OwnerReferences, backup.UID) {
		// The name hash includes backup.Name but not backup.UID: a deleted
		// and recreated Backup with the same name (unusual, but not
		// impossible) would otherwise let this stale object from a prior
		// Backup be silently adopted as if it belonged to the current one.
		return fmt.Errorf("existing DataUpload %s/%s is not owned by Backup %s (UID %s) -- refusing to reuse it",
			backup.Namespace, dataUploadName, backup.Name, backup.UID)
	}
	if existing.Annotations[controllercommon.AnnotationVMName] != vm.Name || existing.Annotations[controllercommon.AnnotationVMNamespace] != vm.Namespace {
		// SourcePVC/SourceNamespace above identify the disk being backed
		// up, but not which VM it was resolved for; without this check a
		// same-named PVC backed up from a different VM (unlikely given the
		// name hash, but cheap to verify) could be silently adopted as if
		// it belonged to this one.
		return fmt.Errorf("existing DataUpload %s/%s has VM identity %s/%s, not %s/%s -- refusing to reuse it",
			backup.Namespace, dataUploadName, existing.Annotations[controllercommon.AnnotationVMNamespace], existing.Annotations[controllercommon.AnnotationVMName], vm.Namespace, vm.Name)
	}
	if existing.Spec.DataMover != controllercommon.DataMoverKubeVirt {
		return fmt.Errorf("existing DataUpload %s/%s uses data mover %q, not %q -- refusing to reuse it",
			backup.Namespace, dataUploadName, existing.Spec.DataMover, controllercommon.DataMoverKubeVirt)
	}
	if existing.Spec.BackupStorageLocation != backup.Spec.StorageLocation {
		return fmt.Errorf("existing DataUpload %s/%s uses backup storage location %q, not %q -- refusing to reuse it",
			backup.Namespace, dataUploadName, existing.Spec.BackupStorageLocation, backup.Spec.StorageLocation)
	}
	if existing.Annotations[controllercommon.AnnotationOperationID] == "" {
		// Without this annotation, Execute() would fall back to the locally
		// generated operationID, but getDataUploadByOperationID (used by
		// Progress/Cancel) requires an exact annotation match to that ID --
		// an existing object with none would never be found again, silently
		// stranding this operation instead of tracking it.
		return fmt.Errorf("existing DataUpload %s/%s has no %s annotation; refusing to reuse it since progress and cancellation could not track it",
			backup.Namespace, dataUploadName, controllercommon.AnnotationOperationID)
	}
	if want := controllercommon.SafeLabelValue(backup.Name); existing.Labels[velerov1.BackupNameLabel] != want {
		// getDataUploadByOperationID narrows server-side via this label
		// before its annotation re-check, so an object missing it (or with
		// a divergent value) would be unfindable even though the
		// annotation above matches.
		return fmt.Errorf("existing DataUpload %s/%s has label %s=%q, expected %q; refusing to reuse it since progress and cancellation could not find it",
			backup.Namespace, dataUploadName, velerov1.BackupNameLabel, existing.Labels[velerov1.BackupNameLabel], want)
	}
	return nil
}

// getDataUploadByName fetches a single DataUpload by name, used to adopt an
// existing one when createDataUploadResource reports AlreadyExists.
func (p *BackupPlugin) getDataUploadByName(name, namespace string) (*velerov2alpha1.DataUpload, error) {
	resourceClient, err := p.dataUploadResourceClient()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), apiCallTimeout)
	defer cancel()

	item, err := resourceClient.Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get DataUpload: %w", err)
	}

	du := &velerov2alpha1.DataUpload{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, du); err != nil {
		return nil, fmt.Errorf("failed to convert unstructured to DataUpload: %w", err)
	}
	return du, nil
}

// createDataUploadResource creates the DataUpload CR in the cluster.
func (p *BackupPlugin) createDataUploadResource(du *velerov2alpha1.DataUpload) error {
	// Create unstructured client for DataUpload
	duMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(du)
	if err != nil {
		return fmt.Errorf("failed to convert DataUpload to unstructured: %w", err)
	}

	unstructuredDU := &unstructured.Unstructured{Object: duMap}
	unstructuredDU.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "velero.io",
		Version: "v2alpha1",
		Kind:    "DataUpload",
	})

	resourceClient, err := p.dataUploadResourceClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), apiCallTimeout)
	defer cancel()

	_, err = resourceClient.Namespace(du.Namespace).Create(
		ctx,
		unstructuredDU,
		metav1.CreateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to create DataUpload: %w", err)
	}

	return nil
}

// getDataUploadByOperationID retrieves a DataUpload by its operation ID.
func (p *BackupPlugin) getDataUploadByOperationID(operationID string, backup *velerov1.Backup) (*velerov2alpha1.DataUpload, error) {
	resourceClient, err := p.dataUploadResourceClient()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), apiCallTimeout)
	defer cancel()

	// List DataUploads in the backup namespace with matching label
	list, err := resourceClient.Namespace(backup.Namespace).List(
		ctx,
		metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s", velerov1.BackupNameLabel, controllercommon.SafeLabelValue(backup.Name)),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list DataUploads: %w", err)
	}

	for _, item := range list.Items {
		annotations := item.GetAnnotations()
		if annotations != nil && annotations[controllercommon.AnnotationOperationID] == operationID {
			du := &velerov2alpha1.DataUpload{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, du); err != nil {
				return nil, fmt.Errorf("failed to convert unstructured to DataUpload: %w", err)
			}
			return du, nil
		}
	}

	return nil, nil
}

// getOrCreateVolumeHelper returns a VolumeHelper with lazy per-namespace caching.
// The VolumeHelper uses the pvcPodCache which is populated lazily as namespaces are encountered.
// Callers should use ensurePVCPodCacheForNamespace before calling methods that need
// PVC-to-Pod lookups for a specific namespace.
// Since plugin instances are unique per backup (created via newPluginManager and
// cleaned up via CleanupClients at backup completion), we can safely cache this.
func (p *PluginPVCPodCache) GetOrCreateVolumeHelper(backup *velerov1.Backup, crClient crclient.Client, log logrus.FieldLogger) (vhutil.VolumeHelper, error) {
	// Initialize the PVC-to-Pod cache if needed
	if p.pvcPodCache == nil {
		p.pvcPodCache = podvolumeutil.NewPVCPodCache()
	}

	// Return the VolumeHelper with our lazily-built cache
	// The cache will be populated incrementally as namespaces are encountered
	return p.getVolumeHelperWithCache(backup, crClient, log)
}

// getVolumeHelperWithCache creates a VolumeHelper using the pre-built PVC-to-Pod cache.
// The cache should be ensured for the relevant namespace(s) before calling this.
func (p *PluginPVCPodCache) getVolumeHelperWithCache(backup *velerov1.Backup, crClient crclient.Client, log logrus.FieldLogger) (vhutil.VolumeHelper, error) {
	// Create VolumeHelper with our lazy-built cache
	vh, err := volumehelper.NewVolumeHelperWithCache(
		*backup,
		crClient,
		log,
		p.pvcPodCache,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create VolumeHelper: %w", err)
	}
	return vh, nil
}

// cancelPatchBackoff bounds the retry attempts for updateDataUpload's cancel
// patch: Velero's own operation-timeout enforcement
// (backup_operations_controller.go) calls Cancel() at most once and discards
// its returned error, so a single transient failure here would otherwise leave
// the DataUpload (and its upload pod) running with no other mechanism to ever
// retry the cancellation.
var cancelPatchBackoff = wait.Backoff{
	Steps:    3,
	Duration: 200 * time.Millisecond,
	Factor:   2.0,
	Jitter:   0.1,
}

// cancelPatchTotalTimeout bounds the entire updateDataUpload retry loop
// (all attempts and backoff sleeps combined), not just each individual patch
// call: since Velero calls Cancel() synchronously and only once, an unbounded
// total (worst case: 3 attempts x apiCallTimeout each, plus backoff sleeps)
// would leave that single call hanging far longer than a cancellation should
// reasonably take.
const cancelPatchTotalTimeout = 45 * time.Second

// patchDataUploadCancel patches the named DataUpload's Spec.Cancel field in
// the cluster. A scoped merge patch (rather than a full Update of a
// locally-fetched object) is used deliberately: the kubevirt datamover
// controller concurrently reconciles this same DataUpload and updates its
// Status via a full object Update, so replacing the whole object from a
// possibly-stale local copy risks clobbering the controller's in-flight
// status changes. Taking just namespace/name/cancel (rather than the whole
// *velerov2alpha1.DataUpload) makes that impossible by construction, since
// there is no local copy of Status to accidentally include.
func (p *BackupPlugin) patchDataUploadCancel(namespace, name string, cancel bool) error {
	resourceClient, err := p.dataUploadResourceClient()
	if err != nil {
		return err
	}

	patch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"cancel": cancel,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to build DataUpload cancel patch: %w", err)
	}

	parentCtx, cancelParent := context.WithTimeout(context.Background(), cancelPatchTotalTimeout)
	defer cancelParent()

	retryErr := retry.OnError(cancelPatchBackoff, func(err error) bool {
		// Retrying a not-found or invalid patch can't succeed; only retry
		// errors that are plausibly transient (server hiccup, timeout, etc).
		return !apierrors.IsNotFound(err) && !apierrors.IsInvalid(err)
	}, func() error {
		ctx, cancel := context.WithTimeout(parentCtx, apiCallTimeout)
		defer cancel()
		_, patchErr := resourceClient.Namespace(namespace).Patch(
			ctx,
			name,
			types.MergePatchType,
			patch,
			metav1.PatchOptions{},
		)
		return patchErr
	})
	if retryErr != nil {
		return fmt.Errorf("failed to update DataUpload: %w", retryErr)
	}

	return nil
}

// buildDataUploadAnnotations constructs the annotation map for a DataUpload CR.
// It includes required controller annotations and propagates optional per-VM
// annotations (like backup-pvc-size) from the VM to the DataUpload.
func buildDataUploadAnnotations(vm *kvcore.VirtualMachine, backup *velerov1.Backup, operationID string) map[string]string {
	annotations := map[string]string{
		controllercommon.AnnotationVMName:      vm.Name,
		controllercommon.AnnotationVMNamespace: vm.Namespace,
		controllercommon.AnnotationOperationID: operationID,
	}

	// Propagate backup-pvc-size from VM annotation to DataUpload so the
	// controller can use it to size the temporary backup PVC per-VM.
	if vm.Annotations != nil {
		if size := vm.Annotations[controllercommon.AnnotationBackupPVCSize]; size != "" {
			annotations[controllercommon.AnnotationBackupPVCSize] = size
		}
	}

	// Handle SkipQuiesce annotation
	effectiveSkipVal := ""

	// Check VM annotation first
	if vm.Annotations != nil {
		if val, ok := vm.Annotations[controllercommon.AnnotationSkipQuiesce]; ok {
			if val == "true" || val == "false" || val == "auto" {
				effectiveSkipVal = val
			}
		}
	}

	// Fallback to Backup annotation if VM annotation is absent or invalid
	if effectiveSkipVal == "" && backup.Annotations != nil {
		if val, ok := backup.Annotations[controllercommon.AnnotationSkipQuiesce]; ok {
			if val == "true" || val == "false" || val == "auto" {
				effectiveSkipVal = val
			}
		}
	}

	// Only set on DataUpload if the effective value is "true" or "false"
	if effectiveSkipVal == "true" || effectiveSkipVal == "false" {
		annotations[controllercommon.AnnotationSkipQuiesce] = effectiveSkipVal
	}

	return annotations
}

// hasOwnerUID reports whether any of ownerReferences has the given UID.
// Used to confirm an adopted object actually belongs to the Backup being
// processed, not to a prior object of the same name left behind by a
// deleted-and-recreated Backup.
func hasOwnerUID(ownerReferences []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range ownerReferences {
		if ref.UID == uid {
			return true
		}
	}
	return false
}

// deterministicSuffix returns an 8-character hex digest of parts, joined by "/".
// Used in place of a random UUID for names/IDs that must be stable across a
// retried Execute() call for the same (backup, VM) pair: Velero may re-invoke
// a BackupItemAction after a transient error, and a random suffix would make
// each attempt mint a distinct DataUpload/operationID instead of converging
// on the same one, defeating the AlreadyExists-based idempotency check below.
func deterministicSuffix(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "/")))
	return hex.EncodeToString(sum[:4])
}

// generateOperationID creates a deterministic operation ID for tracking async
// backup operations: the same (backupName, namespace, vmName) always yields
// the same ID, so a retried Execute() call for the same item converges on the
// same DataUpload's annotation rather than one Progress/Cancel can't find.
func generateOperationID(backupName, namespace, vmName string) string {
	return fmt.Sprintf("%s-%s-%s-%s", backupName, namespace, vmName, deterministicSuffix(backupName, namespace, vmName))
}

// distributeTruncationBudget returns, for each of parts, the max byte length
// it may occupy so the sum is at most maxLen. Parts that already fit keep
// their full length; any deficit is taken evenly off the longest part(s),
// repeated until the total fits. Used instead of a fixed halfway split so the
// same logic works regardless of how many name components are involved.
func distributeTruncationBudget(maxLen int, parts []string) []int {
	budgets := make([]int, len(parts))
	for i, p := range parts {
		budgets[i] = len(p)
	}
	for {
		total := 0
		for _, b := range budgets {
			total += b
		}
		over := total - maxLen
		if over <= 0 {
			return budgets
		}
		largest := 0
		for _, b := range budgets {
			if b > largest {
				largest = b
			}
		}
		if largest == 0 {
			return budgets
		}
		for i := range budgets {
			if over <= 0 {
				break
			}
			if budgets[i] == largest {
				budgets[i]--
				over--
			}
		}
	}
}

// generateDataUploadName creates a name for the DataUpload CR.
// The name format is: du-<backupName>-<namespace>-<vmName>-<hash8>, where
// hash8 is deterministic (see deterministicSuffix) rather than random, so a
// retried Execute() call for the same (backup, namespace, VM) reproduces the
// same name and Create() surfaces AlreadyExists instead of minting a
// duplicate. namespace disambiguates same-named VMs backed up from different
// namespaces within one backup -- without it, backupName+vmName alone would
// collide for two such VMs even though they don't collide as Kubernetes
// objects.
// If the total length exceeds 253 characters (Kubernetes limit), the three
// components are truncated (see distributeTruncationBudget) while preserving
// the hash suffix.
func generateDataUploadName(backupName, namespace, vmName string) string {
	suffix := deterministicSuffix(backupName, namespace, vmName)
	// Reserve space for: "du-" (3) + "-" + "-" + "-" (3) + hash (8) = 14 chars
	const fixedLen = 14
	maxBodyLen := 253 - fixedLen

	parts := []string{backupName, namespace, vmName}
	totalBodyLen := len(backupName) + len(namespace) + len(vmName)
	if totalBodyLen > maxBodyLen {
		budgets := distributeTruncationBudget(maxBodyLen, parts)
		for i, b := range budgets {
			if b < len(parts[i]) {
				parts[i] = strings.TrimRight(parts[i][:b], "-.")
			}
		}
		backupName, namespace, vmName = parts[0], parts[1], parts[2]
	}

	return fmt.Sprintf("du-%s-%s-%s-%s", backupName, namespace, vmName, suffix)
}

// getDynamicClient returns a dynamic client for working with unstructured resources.
// This variable can be overridden for testing.
var getDynamicClient = func(config *rest.Config) (dynamicClientInterface, error) {
	return dynamic.NewForConfig(config)
}

// dynamicClientInterface defines the interface for dynamic client operations.
// This interface matches the dynamic.Interface from k8s.io/client-go/dynamic.
type dynamicClientInterface interface {
	Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface
}

// SetDynamicClientFunc allows overriding the dynamic client creation for testing.
func SetDynamicClientFunc(fn func(config *rest.Config) (dynamicClientInterface, error)) {
	getDynamicClient = fn
}
