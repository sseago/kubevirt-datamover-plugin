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
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"

	kvcore "kubevirt.io/api/core/v1"

	controllercommon "github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-plugin/kubevirt-datamover-plugin/clients"
)

const (
	testNamespace  = "test-namespace"
	testVMName     = "test-vm"
	testBackupName = "test-backup"
)

// newTestLogger creates a logger for tests that discards output.
func newTestLogger() logrus.FieldLogger {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	return logrus.NewEntry(logger)
}

// boolPtr returns a pointer to a bool value.
func boolPtr(b bool) *bool {
	return &b
}

func TestBackupPlugin_AppliesTo(t *testing.T) {
	fakeClient := getFakeClient()
	plugin, err := NewBackupPlugin(newTestLogger(), fakeClient)
	require.NoError(t, err)

	selector, err := plugin.AppliesTo()

	require.NoError(t, err)
	assert.Equal(t, velero.ResourceSelector{
		IncludedResources: []string{"virtualmachines.kubevirt.io"},
	}, selector)
}

func TestBackupPlugin_Name(t *testing.T) {
	fakeClient := getFakeClient()
	plugin, err := NewBackupPlugin(newTestLogger(), fakeClient)
	require.NoError(t, err)

	name := plugin.Name()

	assert.Equal(t, "kubevirt-vm-backup-plugin", name)
}

func TestBackupPlugin_Execute_NilBackup(t *testing.T) {
	fakeClient := getFakeClient()
	plugin, err := NewBackupPlugin(newTestLogger(), fakeClient)
	require.NoError(t, err)

	vm := createTestVM(testNamespace, testVMName, kvcore.VirtualMachineStatusRunning)
	item := vmToUnstructured(t, vm)

	_, _, _, _, err = plugin.Execute(item, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "backup object is nil")
}

func TestBackupPlugin_Execute_SnapshotMoveDataNotEnabled(t *testing.T) {
	fakeClient := getFakeClient()
	plugin, err := NewBackupPlugin(newTestLogger(), fakeClient)
	require.NoError(t, err)

	vm := createTestVM(testNamespace, testVMName, kvcore.VirtualMachineStatusRunning)
	item := vmToUnstructured(t, vm)

	backup := &velerov1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testBackupName,
			Namespace: "velero",
		},
		Spec: velerov1.BackupSpec{
			SnapshotMoveData: boolPtr(false),
		},
	}

	result, additionalItems, operationID, _, err := plugin.Execute(item, backup)

	require.NoError(t, err)
	assert.NotNil(t, result, "should return item even if not eligible")
	assert.Empty(t, additionalItems, "should not return additional items")
	assert.Empty(t, operationID, "should not return operation ID")
}

func TestBackupPlugin_Execute_VMNotRunning(t *testing.T) {
	fakeClient := getFakeClient()
	plugin, err := NewBackupPlugin(newTestLogger(), fakeClient)
	require.NoError(t, err)

	vm := createTestVM(testNamespace, testVMName, kvcore.VirtualMachineStatusStopped)
	item := vmToUnstructured(t, vm)

	backup := &velerov1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testBackupName,
			Namespace: "velero",
		},
		Spec: velerov1.BackupSpec{
			SnapshotMoveData: boolPtr(true),
		},
	}

	result, additionalItems, operationID, _, err := plugin.Execute(item, backup)

	require.NoError(t, err)
	assert.NotNil(t, result, "should return item even if not eligible")
	assert.Empty(t, additionalItems, "should not return additional items when VM not running")
	assert.Empty(t, operationID, "should not return operation ID when VM not running")
}

// TestControllerValidateVMIsRunning tests the controller's ValidateVMIsRunning function
// to ensure it performs the expected validation.
func TestControllerValidateVMIsRunning(t *testing.T) {
	testCases := []struct {
		name        string
		status      kvcore.VirtualMachinePrintableStatus
		expectError bool
	}{
		{
			name:        "Running VM",
			status:      kvcore.VirtualMachineStatusRunning,
			expectError: false,
		},
		{
			name:        "Starting VM - not allowed by controller validation",
			status:      kvcore.VirtualMachineStatusStarting,
			expectError: true, // Controller only allows Running, not Starting
		},
		{
			name:        "Stopped VM",
			status:      kvcore.VirtualMachineStatusStopped,
			expectError: true,
		},
		{
			name:        "Stopping VM",
			status:      kvcore.VirtualMachineStatusStopping,
			expectError: true,
		},
		{
			name:        "Paused VM",
			status:      kvcore.VirtualMachineStatusPaused,
			expectError: true,
		},
		{
			name:        "Migrating VM",
			status:      kvcore.VirtualMachineStatusMigrating,
			expectError: true,
		},
		{
			name:        "Provisioning VM",
			status:      kvcore.VirtualMachineStatusProvisioning,
			expectError: true,
		},
		{
			name:        "Terminating VM",
			status:      kvcore.VirtualMachineStatusTerminating,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vm := createTestVM(testNamespace, testVMName, tc.status)
			err := controllercommon.ValidateVMIsRunning(vm)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestControllerValidateCBTEnabled tests the controller's ValidateCBTEnabled function
// to ensure it performs the expected validation using vm.Status.ChangedBlockTracking.
func TestControllerValidateCBTEnabled(t *testing.T) {
	testCases := []struct {
		name        string
		vm          *kvcore.VirtualMachine
		expectError bool
	}{
		{
			name: "CBT enabled via ChangedBlockTracking status",
			vm: &kvcore.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testVMName,
					Namespace: testNamespace,
				},
				Status: kvcore.VirtualMachineStatus{
					ChangedBlockTracking: &kvcore.ChangedBlockTrackingStatus{
						State: kvcore.ChangedBlockTrackingEnabled,
					},
				},
			},
			expectError: false,
		},
		{
			name: "CBT disabled - no ChangedBlockTracking status",
			vm: &kvcore.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testVMName,
					Namespace: testNamespace,
				},
				Status: kvcore.VirtualMachineStatus{},
			},
			expectError: true,
		},
		{
			name: "CBT disabled - ChangedBlockTracking state is not Enabled",
			vm: &kvcore.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testVMName,
					Namespace: testNamespace,
				},
				Status: kvcore.VirtualMachineStatus{
					ChangedBlockTracking: &kvcore.ChangedBlockTrackingStatus{
						State: kvcore.ChangedBlockTrackingDisabled,
					},
				},
			},
			expectError: true,
		},
		{
			name:        "nil VM",
			vm:          nil,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := controllercommon.ValidateCBTEnabled(tc.vm)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestControllerGetVolumesForVm tests the controller's GetVolumesForVm function
// to ensure it properly extracts PVC names from VirtualMachine volumes.
// Full test coverage is in the controller package; this verifies integration.
func TestControllerGetVolumesForVm(t *testing.T) {
	testCases := []struct {
		name     string
		vm       *kvcore.VirtualMachine
		expected []string
	}{
		{
			name: "VM with PVC volume",
			vm: &kvcore.VirtualMachine{
				Spec: kvcore.VirtualMachineSpec{
					Template: &kvcore.VirtualMachineInstanceTemplateSpec{
						Spec: kvcore.VirtualMachineInstanceSpec{
							Volumes: []kvcore.Volume{
								{
									Name: "disk0",
									VolumeSource: kvcore.VolumeSource{
										PersistentVolumeClaim: &kvcore.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-1",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{"pvc-1"},
		},
		{
			name: "VM with DataVolume",
			vm: &kvcore.VirtualMachine{
				Spec: kvcore.VirtualMachineSpec{
					Template: &kvcore.VirtualMachineInstanceTemplateSpec{
						Spec: kvcore.VirtualMachineInstanceSpec{
							Volumes: []kvcore.Volume{
								{
									Name: "disk0",
									VolumeSource: kvcore.VolumeSource{
										DataVolume: &kvcore.DataVolumeSource{
											Name: "dv-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{"dv-1"},
		},
		{
			name: "VM with multiple volumes",
			vm: &kvcore.VirtualMachine{
				Spec: kvcore.VirtualMachineSpec{
					Template: &kvcore.VirtualMachineInstanceTemplateSpec{
						Spec: kvcore.VirtualMachineInstanceSpec{
							Volumes: []kvcore.Volume{
								{
									Name: "disk0",
									VolumeSource: kvcore.VolumeSource{
										PersistentVolumeClaim: &kvcore.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-1",
											},
										},
									},
								},
								{
									Name: "disk1",
									VolumeSource: kvcore.VolumeSource{
										DataVolume: &kvcore.DataVolumeSource{
											Name: "dv-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{"pvc-1", "dv-1"},
		},
		{
			name: "VM with no Template",
			vm: &kvcore.VirtualMachine{
				Spec: kvcore.VirtualMachineSpec{
					Template: nil,
				},
			},
			expected: []string{},
		},
		{
			name: "VM with empty volumes",
			vm: &kvcore.VirtualMachine{
				Spec: kvcore.VirtualMachineSpec{
					Template: &kvcore.VirtualMachineInstanceTemplateSpec{
						Spec: kvcore.VirtualMachineInstanceSpec{
							Volumes: []kvcore.Volume{},
						},
					},
				},
			},
			expected: []string{},
		},
		{
			name: "VM with non-PVC volumes only",
			vm: &kvcore.VirtualMachine{
				Spec: kvcore.VirtualMachineSpec{
					Template: &kvcore.VirtualMachineInstanceTemplateSpec{
						Spec: kvcore.VirtualMachineInstanceSpec{
							Volumes: []kvcore.Volume{
								{
									Name: "cloudinit",
									VolumeSource: kvcore.VolumeSource{
										CloudInitNoCloud: &kvcore.CloudInitNoCloudSource{
											UserData: "test",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{},
		},
		{
			name:     "nil VM",
			vm:       nil,
			expected: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := controllercommon.GetVolumesForVm(tc.vm)
			// Use ElementsMatch to handle nil vs empty slice difference
			assert.ElementsMatch(t, tc.expected, result)
		})
	}
}

func TestBackupPlugin_checkPreconditions(t *testing.T) {
	fakeClient := getFakeClient()
	plugin, err := NewBackupPlugin(newTestLogger(), fakeClient)
	require.NoError(t, err)

	testCases := []struct {
		name             string
		vm               *kvcore.VirtualMachine
		backup           *velerov1.Backup
		expectedEligible bool
		expectedReason   string
	}{
		{
			name: "SnapshotMoveData not enabled",
			vm:   createTestVMWithCBT(testNamespace, testVMName, kvcore.VirtualMachineStatusRunning),
			backup: &velerov1.Backup{
				Spec: velerov1.BackupSpec{
					SnapshotMoveData: boolPtr(false),
				},
			},
			expectedEligible: false,
			expectedReason:   "backup.Spec.SnapshotMoveData is not enabled",
		},
		{
			name: "SnapshotMoveData nil",
			vm:   createTestVMWithCBT(testNamespace, testVMName, kvcore.VirtualMachineStatusRunning),
			backup: &velerov1.Backup{
				Spec: velerov1.BackupSpec{
					SnapshotMoveData: nil,
				},
			},
			expectedEligible: false,
			expectedReason:   "backup.Spec.SnapshotMoveData is not enabled",
		},
		{
			name: "VM not running",
			vm:   createTestVMWithCBT(testNamespace, testVMName, kvcore.VirtualMachineStatusStopped),
			backup: &velerov1.Backup{
				Spec: velerov1.BackupSpec{
					SnapshotMoveData: boolPtr(true),
				},
			},
			expectedEligible: false,
			expectedReason:   "not running",
		},
		{
			name: "CBT not enabled",
			vm: &kvcore.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testVMName,
					Namespace: testNamespace,
				},
				Status: kvcore.VirtualMachineStatus{
					PrintableStatus: kvcore.VirtualMachineStatusRunning,
					// No ChangedBlockTracking status
				},
			},
			backup: &velerov1.Backup{
				Spec: velerov1.BackupSpec{
					SnapshotMoveData: boolPtr(true),
				},
			},
			expectedEligible: false,
			expectedReason:   "ChangedBlockTracking",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vh, err := plugin.pluginPVCPodCache.GetOrCreateVolumeHelper(tc.backup, plugin.crClient, plugin.Log)
			require.NoError(t, err)
			eligible, reason, err := CheckPreconditions(tc.vm, tc.backup, plugin.Log, vh)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedEligible, eligible)
			if !tc.expectedEligible {
				assert.Contains(t, reason, tc.expectedReason)
			}
		})
	}
}

// stubVolumeHelper is a minimal vhutil.VolumeHelper implementation used to
// test checkVolumePolicies and getFirstKubevirtPVC in isolation, without
// needing a real resource-policy ConfigMap wired through
// volumehelper.NewVolumeHelperWithCache. Responses are keyed by the PVC name
// read off the unstructured object each method is called with.
type stubVolumeHelper struct {
	shouldSnapshot    map[string]bool
	shouldSnapshotErr map[string]error
	shouldCustom      map[string]bool
	shouldCustomErr   map[string]error
}

func (s *stubVolumeHelper) ShouldPerformSnapshot(obj runtime.Unstructured, _ schema.GroupResource) (bool, error) {
	name := obj.(*unstructured.Unstructured).GetName()
	if err, ok := s.shouldSnapshotErr[name]; ok {
		return false, err
	}
	return s.shouldSnapshot[name], nil
}

func (s *stubVolumeHelper) ShouldPerformCustomAction(obj runtime.Unstructured, _ schema.GroupResource, _ map[string]any) (bool, error) {
	name := obj.(*unstructured.Unstructured).GetName()
	if err, ok := s.shouldCustomErr[name]; ok {
		return false, err
	}
	return s.shouldCustom[name], nil
}

func (s *stubVolumeHelper) ShouldPerformFSBackup(corev1.Volume, corev1.Pod) (bool, error) {
	return false, nil
}

func (s *stubVolumeHelper) GetActionParameters(runtime.Unstructured, schema.GroupResource) (bool, string, map[string]any, error) {
	return false, "", nil, nil
}

// vmWithPVCVolumes returns a running test VM whose template has one PVC
// volume per name in pvcNames (volume name == PVC name for simplicity).
func vmWithPVCVolumes(namespace, name string, pvcNames ...string) *kvcore.VirtualMachine {
	vm := createTestVM(namespace, name, kvcore.VirtualMachineStatusRunning)
	for _, pvcName := range pvcNames {
		vm.Spec.Template.Spec.Volumes = append(vm.Spec.Template.Spec.Volumes, kvcore.Volume{
			Name: pvcName,
			VolumeSource: kvcore.VolumeSource{
				PersistentVolumeClaim: &kvcore.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
					},
				},
			},
		})
	}
	return vm
}

// withFakeCoreClient installs fakeCore as the package-level core client for
// the duration of t, restoring it (to nil) via t.Cleanup.
func withFakeCoreClient(t *testing.T, fakeCore *k8sfake.Clientset) {
	t.Helper()
	clients.SetCoreClient(fakeCore.CoreV1())
	t.Cleanup(func() { clients.SetCoreClient(nil) })
}

func TestCheckVolumePolicies(t *testing.T) {
	backup := &velerov1.Backup{ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero"}}

	t.Run("no PVCs", func(t *testing.T) {
		vm := createTestVM(testNamespace, testVMName, kvcore.VirtualMachineStatusRunning)

		hasKubevirt, hasConflict, err := checkVolumePolicies(vm, backup, newTestLogger(), &stubVolumeHelper{})

		require.NoError(t, err)
		assert.False(t, hasKubevirt)
		assert.False(t, hasConflict)
	})

	t.Run("PVC not found is skipped", func(t *testing.T) {
		vm := vmWithPVCVolumes(testNamespace, testVMName, "missing-pvc")
		withFakeCoreClient(t, k8sfake.NewSimpleClientset())

		hasKubevirt, hasConflict, err := checkVolumePolicies(vm, backup, newTestLogger(), &stubVolumeHelper{})

		require.NoError(t, err)
		assert.False(t, hasKubevirt)
		assert.False(t, hasConflict)
	})

	t.Run("core client Get error propagates", func(t *testing.T) {
		vm := vmWithPVCVolumes(testNamespace, testVMName, "pvc-1")
		fakeCore := k8sfake.NewSimpleClientset()
		fakeCore.PrependReactor("get", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("simulated get failure")
		})
		withFakeCoreClient(t, fakeCore)

		_, _, err := checkVolumePolicies(vm, backup, newTestLogger(), &stubVolumeHelper{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "simulated get failure")
	})

	t.Run("snapshot policy sets hasConflictingPolicy", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: testNamespace}}
		vm := vmWithPVCVolumes(testNamespace, testVMName, "pvc-1")
		withFakeCoreClient(t, k8sfake.NewSimpleClientset(pvc))

		vh := &stubVolumeHelper{shouldSnapshot: map[string]bool{"pvc-1": true}}
		hasKubevirt, hasConflict, err := checkVolumePolicies(vm, backup, newTestLogger(), vh)

		require.NoError(t, err)
		assert.False(t, hasKubevirt)
		assert.True(t, hasConflict)
	})

	t.Run("custom kubevirt policy sets hasKubevirtPolicy", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: testNamespace}}
		vm := vmWithPVCVolumes(testNamespace, testVMName, "pvc-1")
		withFakeCoreClient(t, k8sfake.NewSimpleClientset(pvc))

		vh := &stubVolumeHelper{shouldCustom: map[string]bool{"pvc-1": true}}
		hasKubevirt, hasConflict, err := checkVolumePolicies(vm, backup, newTestLogger(), vh)

		require.NoError(t, err)
		assert.True(t, hasKubevirt)
		assert.False(t, hasConflict)
	})

	t.Run("ShouldPerformSnapshot error propagates", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: testNamespace}}
		vm := vmWithPVCVolumes(testNamespace, testVMName, "pvc-1")
		withFakeCoreClient(t, k8sfake.NewSimpleClientset(pvc))

		vh := &stubVolumeHelper{shouldSnapshotErr: map[string]error{"pvc-1": fmt.Errorf("policy check boom")}}
		_, _, err := checkVolumePolicies(vm, backup, newTestLogger(), vh)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "policy check boom")
	})

	t.Run("ShouldPerformCustomAction error propagates", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: testNamespace}}
		vm := vmWithPVCVolumes(testNamespace, testVMName, "pvc-1")
		withFakeCoreClient(t, k8sfake.NewSimpleClientset(pvc))

		vh := &stubVolumeHelper{shouldCustomErr: map[string]error{"pvc-1": fmt.Errorf("custom check boom")}}
		_, _, err := checkVolumePolicies(vm, backup, newTestLogger(), vh)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "custom check boom")
	})
}

func TestBackupPlugin_getFirstKubevirtPVC(t *testing.T) {
	backup := &velerov1.Backup{ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero"}}

	t.Run("no PVCs", func(t *testing.T) {
		plugin := &BackupPlugin{Log: newTestLogger()}
		vm := createTestVM(testNamespace, testVMName, kvcore.VirtualMachineStatusRunning)

		_, err := plugin.getFirstKubevirtPVC(vm, backup, &stubVolumeHelper{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no PVCs found")
	})

	t.Run("skips not-found PVC and returns first match", func(t *testing.T) {
		plugin := &BackupPlugin{Log: newTestLogger()}
		vm := vmWithPVCVolumes(testNamespace, testVMName, "missing-pvc", "pvc-2")
		pvc2 := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Namespace: testNamespace}}
		withFakeCoreClient(t, k8sfake.NewSimpleClientset(pvc2))

		vh := &stubVolumeHelper{shouldCustom: map[string]bool{"pvc-2": true}}
		result, err := plugin.getFirstKubevirtPVC(vm, backup, vh)

		require.NoError(t, err)
		assert.Equal(t, "pvc-2", result.Name)
	})

	t.Run("get error propagates", func(t *testing.T) {
		plugin := &BackupPlugin{Log: newTestLogger()}
		vm := vmWithPVCVolumes(testNamespace, testVMName, "pvc-1")
		fakeCore := k8sfake.NewSimpleClientset()
		fakeCore.PrependReactor("get", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("simulated get failure")
		})
		withFakeCoreClient(t, fakeCore)

		_, err := plugin.getFirstKubevirtPVC(vm, backup, &stubVolumeHelper{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "simulated get failure")
	})

	t.Run("ShouldPerformCustomAction error propagates", func(t *testing.T) {
		plugin := &BackupPlugin{Log: newTestLogger()}
		vm := vmWithPVCVolumes(testNamespace, testVMName, "pvc-1")
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: testNamespace}}
		withFakeCoreClient(t, k8sfake.NewSimpleClientset(pvc))

		vh := &stubVolumeHelper{shouldCustomErr: map[string]error{"pvc-1": fmt.Errorf("custom check boom")}}
		_, err := plugin.getFirstKubevirtPVC(vm, backup, vh)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "custom check boom")
	})

	t.Run("no PVC eligible", func(t *testing.T) {
		plugin := &BackupPlugin{Log: newTestLogger()}
		vm := vmWithPVCVolumes(testNamespace, testVMName, "pvc-1")
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: testNamespace}}
		withFakeCoreClient(t, k8sfake.NewSimpleClientset(pvc))

		_, err := plugin.getFirstKubevirtPVC(vm, backup, &stubVolumeHelper{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no PVC eligible")
	})
}

func getFakeClient() crclient.Client {
	scheme := runtime.NewScheme()
	//_ = velerov2alpha1.AddToScheme(scheme)
	_ = kvcore.AddToScheme(scheme)
	builder := fake.NewClientBuilder().
		WithScheme(scheme)
	fakeClient := builder.Build()
	return fakeClient
}

// newDataUploadDynamicClient returns a fake dynamic client seeded with objects,
// scoped to the velerov2alpha1 scheme so DataUpload GVK operations resolve.
func newDataUploadDynamicClient(t *testing.T, objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	require.NoError(t, velerov2alpha1.AddToScheme(scheme))
	return dynamicfake.NewSimpleDynamicClient(scheme, objects...)
}

// withFakeDynamicClient overrides the package-level getDynamicClient and the
// clients package's in-cluster config for the duration of t, restoring both
// via t.Cleanup. Because it mutates package-level state, tests using it must
// not run with t.Parallel().
func withFakeDynamicClient(t *testing.T, fakeDynamic *dynamicfake.FakeDynamicClient) {
	t.Helper()
	original := getDynamicClient
	SetDynamicClientFunc(func(config *rest.Config) (dynamicClientInterface, error) {
		return fakeDynamic, nil
	})
	t.Cleanup(func() { SetDynamicClientFunc(original) })

	clients.SetInClusterConfig(&rest.Config{Host: "https://fake"})
	t.Cleanup(func() { clients.SetInClusterConfig(nil) })
}

// withFastCancelBackoff overrides the package-level cancelPatchBackoff with a
// minimal backoff for the duration of t, restoring the original via
// t.Cleanup. Used by Cancel retry tests so they don't pay the real ~600ms of
// sleep the production backoff (200ms, factor 2, 3 steps) would incur.
func withFastCancelBackoff(t *testing.T) {
	t.Helper()
	original := cancelPatchBackoff
	cancelPatchBackoff = wait.Backoff{Steps: 3, Duration: time.Millisecond, Factor: 1.0}
	t.Cleanup(func() { cancelPatchBackoff = original })
}

// withFastCreateRetryBackoff overrides the package-level
// createRetryBackoffUnit with a near-zero duration for the duration of t,
// restoring the original via t.Cleanup. Used by createDataUpload retry
// tests so they don't pay the real (attempt * 100ms) sleep between
// double-race retry attempts.
func withFastCreateRetryBackoff(t *testing.T) {
	t.Helper()
	original := createRetryBackoffUnit
	createRetryBackoffUnit = time.Microsecond
	t.Cleanup(func() { createRetryBackoffUnit = original })
}

func dataUploadUnstructured(t *testing.T, du *velerov2alpha1.DataUpload) *unstructured.Unstructured {
	duMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(du)
	require.NoError(t, err)
	u := &unstructured.Unstructured{Object: duMap}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: "velero.io", Version: "v2alpha1", Kind: "DataUpload"})
	return u
}

func TestBackupPlugin_CreateDataUpload_Create(t *testing.T) {
	vm := &kvcore.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: testVMName, Namespace: testNamespace}}
	backup := &velerov1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero", UID: "backup-uid"},
		Spec:       velerov1.BackupSpec{StorageLocation: "my-bsl"},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: testNamespace}}
	const operationID = "op-create-1"

	fakeDynamic := newDataUploadDynamicClient(t)
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	result, err := plugin.createDataUpload(vm, backup, operationID, sourcePVC)

	require.NoError(t, err)
	require.NotNil(t, result)
	expectedName := generateDataUploadName(backup.Name, vm.Namespace, vm.Name)
	assert.Equal(t, expectedName, result.Name)

	gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
	created, err := fakeDynamic.Resource(gvr).Namespace(backup.Namespace).Get(context.Background(), expectedName, metav1.GetOptions{})
	require.NoError(t, err)
	createdDU := &velerov2alpha1.DataUpload{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(created.Object, createdDU))

	assert.Equal(t, controllercommon.SafeLabelValue(backup.Name), createdDU.Labels[velerov1.BackupNameLabel])
	assert.Equal(t, operationID, createdDU.Annotations[controllercommon.AnnotationOperationID])
	require.Len(t, createdDU.OwnerReferences, 1)
	assert.Equal(t, backup.Name, createdDU.OwnerReferences[0].Name)
	assert.Equal(t, backup.UID, createdDU.OwnerReferences[0].UID)
	require.NotNil(t, createdDU.OwnerReferences[0].Controller)
	assert.True(t, *createdDU.OwnerReferences[0].Controller)
	assert.Equal(t, result, createdDU, "createDataUpload must return the object it actually created")
}

func TestBackupPlugin_CreateDataUpload_CapsExcessiveOperationTimeout(t *testing.T) {
	vm := &kvcore.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: testVMName, Namespace: testNamespace}}
	backup := &velerov1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero", UID: "backup-uid"},
		Spec: velerov1.BackupSpec{
			StorageLocation:      "my-bsl",
			ItemOperationTimeout: metav1.Duration{Duration: 100 * time.Hour},
		},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: testNamespace}}

	fakeDynamic := newDataUploadDynamicClient(t)
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	result, err := plugin.createDataUpload(vm, backup, "op-cap-1", sourcePVC)

	require.NoError(t, err)
	assert.Equal(t, maxOperationTimeout, result.Spec.OperationTimeout.Duration,
		"an excessive ItemOperationTimeout must be capped rather than passed through uncapped")

	gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
	created, err := fakeDynamic.Resource(gvr).Namespace(backup.Namespace).Get(context.Background(), result.Name, metav1.GetOptions{})
	require.NoError(t, err)
	createdDU := &velerov2alpha1.DataUpload{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(created.Object, createdDU))
	assert.Equal(t, maxOperationTimeout, createdDU.Spec.OperationTimeout.Duration,
		"the persisted DataUpload must carry the capped timeout, not just the locally returned one")
}

func TestBackupPlugin_CreateDataUpload_RetriesOnDoubleRace(t *testing.T) {
	withFastCreateRetryBackoff(t)

	vm := &kvcore.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: testVMName, Namespace: testNamespace}}
	backup := &velerov1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero", UID: "backup-uid"},
		Spec:       velerov1.BackupSpec{StorageLocation: "my-bsl"},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: testNamespace}}

	fakeDynamic := newDataUploadDynamicClient(t)
	createAttempts := 0
	fakeDynamic.PrependReactor("create", "datauploads", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAttempts++
		if createAttempts == 1 {
			// Simulates the double race: some other actor's Create() is what
			// the real AlreadyExists below would report, but by the time we
			// re-fetch it, it's already gone (e.g. deleted concurrently) --
			// which in this fake is simply "the object was never actually
			// added", since this reactor intercepts the real tracker.
			gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
			name := generateDataUploadName(backup.Name, vm.Namespace, vm.Name)
			return true, nil, apierrors.NewAlreadyExists(gvr.GroupResource(), name)
		}
		// Let subsequent attempts fall through to the real tracker.
		return false, nil, nil
	})
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	result, err := plugin.createDataUpload(vm, backup, "op-race-1", sourcePVC)

	require.NoError(t, err, "createDataUpload must retry past a double create/delete race instead of erroring out")
	require.NotNil(t, result)
	assert.Equal(t, 2, createAttempts, "must retry Create() exactly once after the re-fetch reports NotFound")

	gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
	list, err := fakeDynamic.Resource(gvr).Namespace(backup.Namespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1, "the retried Create() must have persisted exactly one DataUpload")
}

func TestBackupPlugin_CreateDataUpload_ExhaustsRetriesOnPersistentDoubleRace(t *testing.T) {
	withFastCreateRetryBackoff(t)

	vm := &kvcore.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: testVMName, Namespace: testNamespace}}
	backup := &velerov1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero", UID: "backup-uid"},
		Spec:       velerov1.BackupSpec{StorageLocation: "my-bsl"},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: testNamespace}}

	fakeDynamic := newDataUploadDynamicClient(t)
	createAttempts := 0
	fakeDynamic.PrependReactor("create", "datauploads", func(action k8stesting.Action) (bool, runtime.Object, error) {
		// Every attempt hits the double race -- Create() always reports
		// AlreadyExists, and (since this reactor intercepts every real
		// Create) the object is never actually persisted, so the
		// subsequent re-fetch always reports NotFound too. This never
		// resolves, so createDataUpload must give up after
		// maxCreateAttempts rather than retrying forever.
		createAttempts++
		gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
		name := generateDataUploadName(backup.Name, vm.Namespace, vm.Name)
		return true, nil, apierrors.NewAlreadyExists(gvr.GroupResource(), name)
	})
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	_, err := plugin.createDataUpload(vm, backup, "op-race-exhaust-1", sourcePVC)

	require.Error(t, err, "createDataUpload must give up after exhausting retries on a persistent double-race")
	assert.Contains(t, err.Error(), fmt.Sprintf("after %d attempts", maxCreateAttempts))
	assert.Equal(t, maxCreateAttempts, createAttempts)
}

func TestBackupPlugin_CreateDataUpload_AlreadyExists_Reuse(t *testing.T) {
	vm := &kvcore.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: testVMName, Namespace: testNamespace}}
	backup := &velerov1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero", UID: "backup-uid"},
		Spec:       velerov1.BackupSpec{StorageLocation: "my-bsl"},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: testNamespace}}

	// Precompute the exact name/operationID createDataUpload will derive, and
	// seed the fake dynamic client with a matching DataUpload already present --
	// this simulates a prior Execute() call for this same (backup, VM) having
	// already created it (e.g. Velero retried after a transient RPC error).
	operationID := generateOperationID(backup.Name, vm.Namespace, vm.Name)
	existingName := generateDataUploadName(backup.Name, vm.Namespace, vm.Name)

	existing := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      existingName,
			Namespace: backup.Namespace,
			Labels: map[string]string{
				velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name),
			},
			Annotations: map[string]string{
				controllercommon.AnnotationVMName:      vm.Name,
				controllercommon.AnnotationVMNamespace: vm.Namespace,
				controllercommon.AnnotationOperationID: operationID,
				// Distinguishes the fetched-from-cluster object from the locally
				// built one (which never sets this annotation, since vm has no
				// AnnotationBackupPVCSize of its own), so this test actually
				// proves the adoption path returns what's in the cluster rather
				// than trivially matching locally-derived name/operationID values
				// that would match either way.
				controllercommon.AnnotationBackupPVCSize: "42Gi",
			},
			OwnerReferences: []metav1.OwnerReference{{UID: backup.UID}},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             controllercommon.DataMoverKubeVirt,
			SourcePVC:             sourcePVC.Name,
			SourceNamespace:       vm.Namespace,
			BackupStorageLocation: backup.Spec.StorageLocation,
		},
	}

	fakeDynamic := newDataUploadDynamicClient(t, dataUploadUnstructured(t, existing))
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	result, err := plugin.createDataUpload(vm, backup, operationID, sourcePVC)

	require.NoError(t, err, "createDataUpload must reuse the existing DataUpload instead of erroring on AlreadyExists")
	assert.Equal(t, existingName, result.Name)
	assert.Equal(t, operationID, result.Annotations[controllercommon.AnnotationOperationID])
	assert.Equal(t, "42Gi", result.Annotations[controllercommon.AnnotationBackupPVCSize],
		"createDataUpload must return the object fetched from the cluster, not the locally built one")

	gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
	list, err := fakeDynamic.Resource(gvr).Namespace(backup.Namespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1, "createDataUpload must not create a duplicate DataUpload for a retried call")
}

// TestBackupPlugin_CreateDataUpload_AlreadyExists_RejectionBranches covers
// every branch of createDataUpload's AlreadyExists-adoption checks that must
// reject reuse of an existing DataUpload. Each case starts from a shared
// baseline object that createDataUpload WOULD accept and mutates exactly the
// one field that should trip a single rejection branch, so a passing case
// proves that branch -- and only that branch -- is what produced the error.
func TestBackupPlugin_CreateDataUpload_AlreadyExists_RejectionBranches(t *testing.T) {
	vm := &kvcore.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: testVMName, Namespace: testNamespace}}
	backup := &velerov1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero", UID: "backup-uid"},
		Spec:       velerov1.BackupSpec{StorageLocation: "my-bsl"},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: testNamespace}}
	operationID := generateOperationID(backup.Name, vm.Namespace, vm.Name)
	existingName := generateDataUploadName(backup.Name, vm.Namespace, vm.Name)
	const existingOperationID = "existing-operation-id"

	baseline := func() *velerov2alpha1.DataUpload {
		return &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name:      existingName,
				Namespace: backup.Namespace,
				Labels: map[string]string{
					velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name),
				},
				Annotations: map[string]string{
					controllercommon.AnnotationVMName:      vm.Name,
					controllercommon.AnnotationVMNamespace: vm.Namespace,
					controllercommon.AnnotationOperationID: existingOperationID,
				},
				OwnerReferences: []metav1.OwnerReference{{UID: backup.UID}},
			},
			Spec: velerov2alpha1.DataUploadSpec{
				DataMover:             controllercommon.DataMoverKubeVirt,
				BackupStorageLocation: backup.Spec.StorageLocation,
				SourcePVC:             sourcePVC.Name,
				SourceNamespace:       vm.Namespace,
			},
		}
	}

	testCases := []struct {
		name          string
		mutate        func(du *velerov2alpha1.DataUpload)
		injectGetErr  string
		wantErrSubstr []string
	}{
		{
			// Control case: proves baseline() itself is a valid adoption
			// target, so every other case's rejection is actually caused by
			// its one mutated field rather than baseline() already being
			// invalid for some unrelated reason.
			name:   "unmutated baseline is reused without error",
			mutate: func(du *velerov2alpha1.DataUpload) {},
		},
		{
			name:          "source PVC mismatch",
			mutate:        func(du *velerov2alpha1.DataUpload) { du.Spec.SourcePVC = "some-other-pvc" },
			wantErrSubstr: []string{"refusing to reuse", "some-other-pvc"},
		},
		{
			name:          "source namespace mismatch",
			mutate:        func(du *velerov2alpha1.DataUpload) { du.Spec.SourceNamespace = "some-other-namespace" },
			wantErrSubstr: []string{"refusing to reuse", "some-other-namespace"},
		},
		{
			name: "owner UID mismatch",
			mutate: func(du *velerov2alpha1.DataUpload) {
				du.OwnerReferences = []metav1.OwnerReference{{UID: "stale-backup-uid"}}
			},
			wantErrSubstr: []string{"is not owned by Backup"},
		},
		{
			name: "VM identity mismatch",
			mutate: func(du *velerov2alpha1.DataUpload) {
				du.Annotations[controllercommon.AnnotationVMName] = "some-other-vm"
			},
			wantErrSubstr: []string{"some-other-vm", "refusing to reuse"},
		},
		{
			name: "VM namespace annotation mismatch",
			mutate: func(du *velerov2alpha1.DataUpload) {
				du.Annotations[controllercommon.AnnotationVMNamespace] = "some-other-vm-ns"
			},
			wantErrSubstr: []string{"some-other-vm-ns", "refusing to reuse"},
		},
		{
			name:          "data mover mismatch",
			mutate:        func(du *velerov2alpha1.DataUpload) { du.Spec.DataMover = "some-other-datamover" },
			wantErrSubstr: []string{"some-other-datamover"},
		},
		{
			name:          "backup storage location mismatch",
			mutate:        func(du *velerov2alpha1.DataUpload) { du.Spec.BackupStorageLocation = "some-other-bsl" },
			wantErrSubstr: []string{"some-other-bsl"},
		},
		{
			name:          "missing operationID annotation",
			mutate:        func(du *velerov2alpha1.DataUpload) { delete(du.Annotations, controllercommon.AnnotationOperationID) },
			wantErrSubstr: []string{"refusing to reuse", controllercommon.AnnotationOperationID},
		},
		{
			name:          "backup name label mismatch",
			mutate:        func(du *velerov2alpha1.DataUpload) { delete(du.Labels, velerov1.BackupNameLabel) },
			wantErrSubstr: []string{velerov1.BackupNameLabel},
		},
		{
			name: "divergent backup name label value",
			mutate: func(du *velerov2alpha1.DataUpload) {
				du.Labels[velerov1.BackupNameLabel] = "some-other-backup"
			},
			wantErrSubstr: []string{velerov1.BackupNameLabel, "some-other-backup"},
		},
		{
			name:          "re-fetch error",
			mutate:        func(du *velerov2alpha1.DataUpload) {},
			injectGetErr:  "simulated get failure",
			wantErrSubstr: []string{"could not be re-fetched"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existing := baseline()
			tc.mutate(existing)

			fakeDynamic := newDataUploadDynamicClient(t, dataUploadUnstructured(t, existing))
			if tc.injectGetErr != "" {
				fakeDynamic.PrependReactor("get", "datauploads", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("%s", tc.injectGetErr)
				})
			}
			withFakeDynamicClient(t, fakeDynamic)

			plugin := &BackupPlugin{Log: newTestLogger()}

			result, err := plugin.createDataUpload(vm, backup, operationID, sourcePVC)

			if len(tc.wantErrSubstr) == 0 {
				require.NoError(t, err, "an unmutated baseline object must be a valid adoption target")
				assert.Equal(t, existingOperationID, result.Annotations[controllercommon.AnnotationOperationID],
					"createDataUpload must return the adopted object's stored operationID so Progress and Cancel can find it")
				return
			}
			require.Error(t, err)
			for _, substr := range tc.wantErrSubstr {
				assert.Contains(t, err.Error(), substr)
			}
		})
	}
}

func TestBackupPlugin_Cancel(t *testing.T) {
	const operationID = "op-cancel-1"
	backup := &velerov1.Backup{ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero"}}

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "du-cancel",
			Namespace: backup.Namespace,
			Labels: map[string]string{
				velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name),
			},
			Annotations: map[string]string{
				controllercommon.AnnotationOperationID: operationID,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{Cancel: false, BackupStorageLocation: "my-bsl"},
		Status: velerov2alpha1.DataUploadStatus{
			Phase:   velerov2alpha1.DataUploadPhaseInProgress,
			Message: "controller-owned status",
		},
	}

	// A decoy DataUpload sharing the same backup label (as a second VM backed up
	// in the same backup would) but a different operationID: proves Cancel only
	// patches the DataUpload matching the supplied operationID and leaves the
	// decoy's Spec.Cancel untouched.
	decoy := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "du-cancel-decoy",
			Namespace: backup.Namespace,
			Labels: map[string]string{
				velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name),
			},
			Annotations: map[string]string{
				controllercommon.AnnotationOperationID: "op-cancel-decoy",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{Cancel: false},
	}

	fakeDynamic := newDataUploadDynamicClient(t, dataUploadUnstructured(t, du), dataUploadUnstructured(t, decoy))
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	err := plugin.Cancel(operationID, backup)
	require.NoError(t, err)

	gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
	updated, err := fakeDynamic.Resource(gvr).Namespace(backup.Namespace).Get(context.Background(), "du-cancel", metav1.GetOptions{})
	require.NoError(t, err)
	updatedDU := &velerov2alpha1.DataUpload{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(updated.Object, updatedDU))
	assert.True(t, updatedDU.Spec.Cancel)
	assert.Equal(t, "my-bsl", updatedDU.Spec.BackupStorageLocation,
		"Cancel must patch only spec.cancel and must not clobber other spec fields")
	assert.Equal(t, velerov2alpha1.DataUploadPhaseInProgress, updatedDU.Status.Phase,
		"Cancel must patch only spec.cancel and must not clobber the controller-owned Status")
	assert.Equal(t, "controller-owned status", updatedDU.Status.Message)

	decoyAfter, err := fakeDynamic.Resource(gvr).Namespace(backup.Namespace).Get(context.Background(), "du-cancel-decoy", metav1.GetOptions{})
	require.NoError(t, err)
	decoyDU := &velerov2alpha1.DataUpload{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(decoyAfter.Object, decoyDU))
	assert.False(t, decoyDU.Spec.Cancel, "Cancel must not touch DataUploads for other operations")
}

func TestBackupPlugin_Cancel_RetriesTransientPatchFailure(t *testing.T) {
	withFastCancelBackoff(t)

	const operationID = "op-cancel-retry-1"
	backup := &velerov1.Backup{ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero"}}

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "du-cancel-retry",
			Namespace: backup.Namespace,
			Labels: map[string]string{
				velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name),
			},
			Annotations: map[string]string{
				controllercommon.AnnotationOperationID: operationID,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{Cancel: false},
	}

	fakeDynamic := newDataUploadDynamicClient(t, dataUploadUnstructured(t, du))
	attempts := 0
	fakeDynamic.PrependReactor("patch", "datauploads", func(action k8stesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts < 3 {
			return true, nil, fmt.Errorf("simulated transient patch failure")
		}
		// Unhandled: let the request fall through to the tracker's default
		// reactor, which actually applies the patch.
		return false, nil, nil
	})
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	err := plugin.Cancel(operationID, backup)

	require.NoError(t, err, "Cancel must retry past transient patch failures rather than giving up after one attempt -- Velero's own timeout enforcement calls Cancel exactly once and discards the error, so this is the only retry path that exists")
	assert.Equal(t, 3, attempts)

	gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
	updated, err := fakeDynamic.Resource(gvr).Namespace(backup.Namespace).Get(context.Background(), "du-cancel-retry", metav1.GetOptions{})
	require.NoError(t, err)
	updatedDU := &velerov2alpha1.DataUpload{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(updated.Object, updatedDU))
	assert.True(t, updatedDU.Spec.Cancel)
}

func TestBackupPlugin_Cancel_PatchFails(t *testing.T) {
	withFastCancelBackoff(t)

	const operationID = "op-cancel-fail-1"
	backup := &velerov1.Backup{ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero"}}

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "du-cancel-fail",
			Namespace: backup.Namespace,
			Labels: map[string]string{
				velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name),
			},
			Annotations: map[string]string{
				controllercommon.AnnotationOperationID: operationID,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{Cancel: false},
	}

	fakeDynamic := newDataUploadDynamicClient(t, dataUploadUnstructured(t, du))
	attempts := 0
	fakeDynamic.PrependReactor("patch", "datauploads", func(action k8stesting.Action) (bool, runtime.Object, error) {
		attempts++
		return true, nil, fmt.Errorf("simulated patch failure")
	})
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	err := plugin.Cancel(operationID, backup)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update DataUpload for cancellation")
	assert.Equal(t, cancelPatchBackoff.Steps, attempts, "Cancel must exhaust the configured retry attempts")
}

func TestBackupPlugin_Cancel_DoesNotRetryNonRetryableError(t *testing.T) {
	withFastCancelBackoff(t)

	const operationID = "op-cancel-nonretryable-1"
	backup := &velerov1.Backup{ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero"}}

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "du-cancel-nonretryable",
			Namespace: backup.Namespace,
			Labels: map[string]string{
				velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name),
			},
			Annotations: map[string]string{
				controllercommon.AnnotationOperationID: operationID,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{Cancel: false},
	}

	fakeDynamic := newDataUploadDynamicClient(t, dataUploadUnstructured(t, du))
	attempts := 0
	fakeDynamic.PrependReactor("patch", "datauploads", func(action k8stesting.Action) (bool, runtime.Object, error) {
		attempts++
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "velero.io", Resource: "datauploads"}, "du-cancel-nonretryable")
	})
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	err := plugin.Cancel(operationID, backup)

	require.Error(t, err)
	assert.Equal(t, 1, attempts, "a NotFound patch error is not retryable and must stop after a single attempt")
}

func TestBackupPlugin_Cancel_NotFound(t *testing.T) {
	backup := &velerov1.Backup{ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero"}}

	fakeDynamic := newDataUploadDynamicClient(t)
	withFakeDynamicClient(t, fakeDynamic)

	plugin := &BackupPlugin{Log: newTestLogger()}

	err := plugin.Cancel("missing-op", backup)
	assert.NoError(t, err)
}

func TestBackupPlugin_getDataUploadByOperationID(t *testing.T) {
	backup := &velerov1.Backup{ObjectMeta: metav1.ObjectMeta{Name: testBackupName, Namespace: "velero"}}

	t.Run("finds the matching DataUpload among several sharing the backup label", func(t *testing.T) {
		const wantOperationID = "op-match"
		match := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "du-match",
				Namespace: backup.Namespace,
				Labels:    map[string]string{velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name)},
				Annotations: map[string]string{
					controllercommon.AnnotationOperationID: wantOperationID,
				},
			},
		}
		decoy := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "du-decoy",
				Namespace: backup.Namespace,
				Labels:    map[string]string{velerov1.BackupNameLabel: controllercommon.SafeLabelValue(backup.Name)},
				Annotations: map[string]string{
					controllercommon.AnnotationOperationID: "op-other",
				},
			},
		}

		fakeDynamic := newDataUploadDynamicClient(t, dataUploadUnstructured(t, match), dataUploadUnstructured(t, decoy))
		withFakeDynamicClient(t, fakeDynamic)

		plugin := &BackupPlugin{Log: newTestLogger()}

		result, err := plugin.getDataUploadByOperationID(wantOperationID, backup)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "du-match", result.Name)
	})

	t.Run("returns nil, nil when no DataUpload matches", func(t *testing.T) {
		fakeDynamic := newDataUploadDynamicClient(t)
		withFakeDynamicClient(t, fakeDynamic)

		plugin := &BackupPlugin{Log: newTestLogger()}

		result, err := plugin.getDataUploadByOperationID("no-such-op", backup)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("propagates a List error", func(t *testing.T) {
		fakeDynamic := newDataUploadDynamicClient(t)
		fakeDynamic.PrependReactor("list", "datauploads", func(action k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("simulated list failure")
		})
		withFakeDynamicClient(t, fakeDynamic)

		plugin := &BackupPlugin{Log: newTestLogger()}

		_, err := plugin.getDataUploadByOperationID("op-1", backup)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "simulated list failure")
	})
}

func TestBackupPlugin_patchDataUploadCancel(t *testing.T) {
	t.Run("patches only spec.cancel, leaving status and other spec fields untouched", func(t *testing.T) {
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "du-patch", Namespace: "velero"},
			Spec:       velerov2alpha1.DataUploadSpec{Cancel: false, BackupStorageLocation: "my-bsl"},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:   velerov2alpha1.DataUploadPhaseInProgress,
				Message: "controller-owned status",
			},
		}
		fakeDynamic := newDataUploadDynamicClient(t, dataUploadUnstructured(t, du))
		withFakeDynamicClient(t, fakeDynamic)

		plugin := &BackupPlugin{Log: newTestLogger()}

		err := plugin.patchDataUploadCancel(du.Namespace, du.Name, true)
		require.NoError(t, err)

		gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
		updated, err := fakeDynamic.Resource(gvr).Namespace(du.Namespace).Get(context.Background(), du.Name, metav1.GetOptions{})
		require.NoError(t, err)
		updatedDU := &velerov2alpha1.DataUpload{}
		require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(updated.Object, updatedDU))

		assert.True(t, updatedDU.Spec.Cancel)
		assert.Equal(t, "my-bsl", updatedDU.Spec.BackupStorageLocation)
		assert.Equal(t, velerov2alpha1.DataUploadPhaseInProgress, updatedDU.Status.Phase)
		assert.Equal(t, "controller-owned status", updatedDU.Status.Message)
	})

	t.Run("does not retry a NotFound patch", func(t *testing.T) {
		withFastCancelBackoff(t)

		fakeDynamic := newDataUploadDynamicClient(t)
		attempts := 0
		fakeDynamic.PrependReactor("patch", "datauploads", func(action k8stesting.Action) (bool, runtime.Object, error) {
			attempts++
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "velero.io", Resource: "datauploads"}, "missing-du")
		})
		withFakeDynamicClient(t, fakeDynamic)

		plugin := &BackupPlugin{Log: newTestLogger()}

		err := plugin.patchDataUploadCancel("velero", "missing-du", true)

		require.Error(t, err)
		assert.Equal(t, 1, attempts, "a NotFound patch error is not transient and must not be retried")
	})

	t.Run("retries a transient patch failure", func(t *testing.T) {
		withFastCancelBackoff(t)

		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "du-patch-retry", Namespace: "velero"},
			Spec:       velerov2alpha1.DataUploadSpec{Cancel: false},
		}
		fakeDynamic := newDataUploadDynamicClient(t, dataUploadUnstructured(t, du))
		attempts := 0
		fakeDynamic.PrependReactor("patch", "datauploads", func(action k8stesting.Action) (bool, runtime.Object, error) {
			attempts++
			if attempts < 2 {
				return true, nil, fmt.Errorf("simulated transient patch failure")
			}
			// Unhandled: let the request fall through to the tracker's default
			// reactor, which actually applies the patch.
			return false, nil, nil
		})
		withFakeDynamicClient(t, fakeDynamic)

		plugin := &BackupPlugin{Log: newTestLogger()}

		err := plugin.patchDataUploadCancel(du.Namespace, du.Name, true)

		require.NoError(t, err, "a transient patch error must be retried via cancelPatchBackoff")
		assert.Equal(t, 2, attempts)

		gvr := schema.GroupVersionResource{Group: "velero.io", Version: "v2alpha1", Resource: "datauploads"}
		updated, err := fakeDynamic.Resource(gvr).Namespace(du.Namespace).Get(context.Background(), du.Name, metav1.GetOptions{})
		require.NoError(t, err)
		updatedDU := &velerov2alpha1.DataUpload{}
		require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(updated.Object, updatedDU))
		assert.True(t, updatedDU.Spec.Cancel)
	})
}

func TestGenerateOperationID(t *testing.T) {
	operationID := generateOperationID("backup-1", "namespace-1", "vm-1")

	assert.NotEmpty(t, operationID)
	assert.Contains(t, operationID, "backup-1")
	assert.Contains(t, operationID, "namespace-1")
	assert.Contains(t, operationID, "vm-1")

	// Deterministic by design (see deterministicSuffix doc comment): a retried
	// Execute() call for the same (backup, namespace, vm) must converge on the
	// same operation ID so it can find the DataUpload it already created.
	operationID2 := generateOperationID("backup-1", "namespace-1", "vm-1")
	assert.Equal(t, operationID, operationID2, "same inputs must produce the same operation ID")

	operationID3 := generateOperationID("backup-2", "namespace-1", "vm-1")
	assert.NotEqual(t, operationID, operationID3, "different inputs must produce a different operation ID")

	operationID4 := generateOperationID("backup-1", "namespace-2", "vm-1")
	assert.NotEqual(t, operationID, operationID4, "a different namespace must produce a different operation ID")

	operationID5 := generateOperationID("backup-1", "namespace-1", "vm-2")
	assert.NotEqual(t, operationID, operationID5, "a different VM name must produce a different operation ID")
}

func TestGenerateDataUploadName(t *testing.T) {
	name := generateDataUploadName("backup-1", "namespace-1", "vm-1")

	assert.NotEmpty(t, name)
	assert.Contains(t, name, "du-")
	assert.Contains(t, name, "backup-1")
	assert.Contains(t, name, "namespace-1")
	assert.Contains(t, name, "vm-1")
	assert.LessOrEqual(t, len(name), 253)
	assert.Empty(t, validation.IsDNS1123Subdomain(name), "name must be a valid object name")

	// Same backup+VM name but a different namespace must produce a different
	// DataUpload name -- this is what prevents same-named VMs backed up from
	// different namespaces within one backup from colliding on both name and
	// operationID (see generateDataUploadName's doc comment).
	otherNSName := generateDataUploadName("backup-1", "namespace-2", "vm-1")
	assert.NotEqual(t, name, otherNSName, "different namespace must produce a different DataUpload name")

	// Test with very long names
	longBackupName := make([]byte, 200)
	for i := range longBackupName {
		longBackupName[i] = 'a'
	}
	longVMName := make([]byte, 100)
	for i := range longVMName {
		longVMName[i] = 'b'
	}

	longName := generateDataUploadName(string(longBackupName), "namespace-1", string(longVMName))
	assert.LessOrEqual(t, len(longName), 253, "generated name should not exceed 253 characters")
	assert.Empty(t, validation.IsDNS1123Subdomain(longName), "truncated name must remain a valid object name")

	// Two long backup names sharing the same truncated prefix (only the very
	// last byte differs, past where truncation would cut) must still produce
	// distinct DataUpload names -- the hash suffix is computed over the full,
	// untruncated inputs, so it's what actually guarantees uniqueness once
	// truncation collapses the visible prefix to the same bytes.
	otherLongBackupName := string(longBackupName[:199]) + "c"
	otherLongName := generateDataUploadName(otherLongBackupName, "namespace-1", string(longVMName))
	assert.NotEqual(t, longName, otherLongName, "long inputs must retain unique DataUpload names")
}

func TestBackupPlugin_Progress(t *testing.T) {
	// Skip this test if not running with proper integration setup
	// The Progress function requires kubernetes client setup
	t.Skip("Skipping Progress test - requires integration test setup with kubernetes client")
}

// Helper functions

// createTestVM creates a test VM without CBT status (for backward compatibility tests).
func createTestVM(namespace, name string, status kvcore.VirtualMachinePrintableStatus) *kvcore.VirtualMachine {
	return &kvcore.VirtualMachine{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "kubevirt.io/v1",
			Kind:       "VirtualMachine",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: kvcore.VirtualMachineSpec{
			Template: &kvcore.VirtualMachineInstanceTemplateSpec{
				Spec: kvcore.VirtualMachineInstanceSpec{
					Volumes: []kvcore.Volume{},
				},
			},
		},
		Status: kvcore.VirtualMachineStatus{
			PrintableStatus: status,
			Created:         status == kvcore.VirtualMachineStatusRunning || status == kvcore.VirtualMachineStatusStarting,
		},
	}
}

// createTestVMWithCBT creates a test VM with ChangedBlockTracking enabled.
func createTestVMWithCBT(namespace, name string, status kvcore.VirtualMachinePrintableStatus) *kvcore.VirtualMachine {
	vm := createTestVM(namespace, name, status)
	vm.Status.ChangedBlockTracking = &kvcore.ChangedBlockTrackingStatus{
		State: kvcore.ChangedBlockTrackingEnabled,
	}
	return vm
}

func vmToUnstructured(t *testing.T, vm *kvcore.VirtualMachine) runtime.Unstructured {
	vmMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vm)
	require.NoError(t, err)
	return &unstructured.Unstructured{Object: vmMap}
}

func TestBuildDataUploadAnnotations(t *testing.T) {
	operationID := "test-op-123"
	dummyBackup := &velerov1.Backup{}

	t.Run("includes required annotations", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-vm",
				Namespace: "my-ns",
			},
		}

		annotations := buildDataUploadAnnotations(vm, dummyBackup, operationID)

		assert.Equal(t, "my-vm", annotations[controllercommon.AnnotationVMName])
		assert.Equal(t, "my-ns", annotations[controllercommon.AnnotationVMNamespace])
		assert.Equal(t, operationID, annotations[controllercommon.AnnotationOperationID])
		assert.NotContains(t, annotations, "kubevirt-datamover.io/backup-pvc-size")
	})

	t.Run("propagates backup-pvc-size from VM", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-vm",
				Namespace: "my-ns",
				Annotations: map[string]string{
					"kubevirt-datamover.io/backup-pvc-size": "50Gi",
				},
			},
		}

		annotations := buildDataUploadAnnotations(vm, dummyBackup, operationID)

		assert.Equal(t, "50Gi", annotations["kubevirt-datamover.io/backup-pvc-size"])
		assert.Equal(t, "my-vm", annotations[controllercommon.AnnotationVMName])
	})

	t.Run("ignores empty backup-pvc-size annotation", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-vm",
				Namespace: "my-ns",
				Annotations: map[string]string{
					"kubevirt-datamover.io/backup-pvc-size": "",
				},
			},
		}

		annotations := buildDataUploadAnnotations(vm, dummyBackup, operationID)

		assert.NotContains(t, annotations, "kubevirt-datamover.io/backup-pvc-size")
	})

	t.Run("sets SkipQuiesce from VM when true", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "true",
				},
			},
		}
		annotations := buildDataUploadAnnotations(vm, dummyBackup, operationID)
		assert.Equal(t, "true", annotations[controllercommon.AnnotationSkipQuiesce])
	})

	t.Run("sets SkipQuiesce from VM when false", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "false",
				},
			},
		}
		backup := &velerov1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "true", // Should be ignored because VM explicitly set it to false
				},
			},
		}
		annotations := buildDataUploadAnnotations(vm, backup, operationID)
		assert.Equal(t, "false", annotations[controllercommon.AnnotationSkipQuiesce])
	})

	t.Run("does not set SkipQuiesce when VM is auto", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "auto",
				},
			},
		}
		backup := &velerov1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "true", // Should be ignored because VM explicitly set it to auto
				},
			},
		}
		annotations := buildDataUploadAnnotations(vm, backup, operationID)
		assert.NotContains(t, annotations, controllercommon.AnnotationSkipQuiesce)
	})

	t.Run("falls back to Backup when VM annotation is invalid", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "invalid-value",
				},
			},
		}
		backup := &velerov1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "true",
				},
			},
		}
		annotations := buildDataUploadAnnotations(vm, backup, operationID)
		assert.Equal(t, "true", annotations[controllercommon.AnnotationSkipQuiesce])
	})

	t.Run("sets SkipQuiesce from Backup when VM annotation is absent", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{}
		backup := &velerov1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "false",
				},
			},
		}
		annotations := buildDataUploadAnnotations(vm, backup, operationID)
		assert.Equal(t, "false", annotations[controllercommon.AnnotationSkipQuiesce])
	})

	t.Run("does not set SkipQuiesce when Backup is auto and VM is absent", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{}
		backup := &velerov1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "auto",
				},
			},
		}
		annotations := buildDataUploadAnnotations(vm, backup, operationID)
		assert.NotContains(t, annotations, controllercommon.AnnotationSkipQuiesce)
	})

	t.Run("does not set SkipQuiesce when both are invalid", func(t *testing.T) {
		vm := &kvcore.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "invalid-vm",
				},
			},
		}
		backup := &velerov1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					controllercommon.AnnotationSkipQuiesce: "invalid-backup",
				},
			},
		}
		annotations := buildDataUploadAnnotations(vm, backup, operationID)
		assert.NotContains(t, annotations, controllercommon.AnnotationSkipQuiesce)
	})
}
