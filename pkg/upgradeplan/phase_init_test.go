package upgradeplan

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	managementv1beta1 "github.com/harvester/upgrade-toolkit/api/v1beta1"
)

const testGB = 1024 * 1024 * 1024

const testCapacityBytes = 100 * testGB // 100GB

// newMockKubeletServer creates an httptest TLS server that serves kubelet
// /stats/summary and /configz endpoints with the given parameters.
// Capacity is fixed at 100GB.
func newMockKubeletServer(t *testing.T, usedBytes uint64, gcThreshold *int32) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/stats/summary", func(w http.ResponseWriter, _ *http.Request) {
		summary := kubeletStatsSummary{
			Node: kubeletNodeStats{
				Fs: &kubeletFsStats{
					AvailableBytes: ptr.To(testCapacityBytes - usedBytes),
					CapacityBytes:  ptr.To(uint64(testCapacityBytes)),
					UsedBytes:      ptr.To(usedBytes),
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(summary) //nolint:errcheck
	})
	mux.HandleFunc("/configz", func(w http.ResponseWriter, _ *http.Request) {
		resp := kubeletConfigzResponse{
			KubeletConfig: kubeletConfigPartial{
				ImageGCHighThresholdPercent: gcThreshold,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	})
	return httptest.NewTLSServer(mux)
}

// newNodeWithKubeletURL creates a Node object whose internal IP annotation and
// kubelet port point to the given test server URL.
func newNodeWithKubeletURL(t *testing.T, serverURL string) *corev1.Node {
	t.Helper()
	u, err := url.Parse(serverURL)
	require.NoError(t, err)
	host, portStr, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Annotations: map[string]string{
				RKE2InternalIPAnnotation: host,
			},
		},
		Status: corev1.NodeStatus{
			DaemonEndpoints: corev1.NodeDaemonEndpoints{
				KubeletEndpoint: corev1.DaemonEndpoint{
					Port: int32(port),
				},
			},
		},
	}
}

//nolint:staticcheck // upstream uses Endpoints; migrate to EndpointSlice later
func newKubernetesEndpoints(ips ...string) *corev1.Endpoints {
	addrs := make([]corev1.EndpointAddress, len(ips))
	for i, ip := range ips {
		addrs[i] = corev1.EndpointAddress{IP: ip}
	}
	ep := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubernetes",
			Namespace: metav1.NamespaceDefault,
		},
	}
	if len(addrs) > 0 {
		ep.Subsets = []corev1.EndpointSubset{
			{Addresses: addrs},
		}
	}
	return ep
}

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	return s
}

func newCertDialer(expiresIn time.Duration) func(string) (*x509.Certificate, error) {
	return func(_ string) (*x509.Certificate, error) {
		return &x509.Certificate{
			NotAfter: time.Now().Add(expiresIn),
		}, nil
	}
}

// --- checkNodeDiskSpace tests ---

func TestCheckNodeDiskSpace_WithinThreshold(t *testing.T) {
	// 100GB capacity, 50GB used, default 85% threshold
	// After loading 13GB: (50+13)/100 * 100 = 63% < 85%
	server := newMockKubeletServer(t, 50*testGB, nil)
	defer server.Close()

	node := newNodeWithKubeletURL(t, server.URL)
	phase := &InitPhase{
		PhaseDeps:   &PhaseDeps{Log: logr.Discard()},
		httpClient:  server.Client(),
		bearerToken: "test-token",
	}

	err := phase.checkNodeDiskSpace(node)
	assert.NoError(t, err)
}

func TestCheckNodeDiskSpace_ExceedsThreshold(t *testing.T) {
	// 100GB capacity, 80GB used, default 85% threshold
	// After loading 13GB: (80+13)/100 * 100 = 93% > 85%
	server := newMockKubeletServer(t, 80*testGB, nil)
	defer server.Close()

	node := newNodeWithKubeletURL(t, server.URL)
	phase := &InitPhase{
		PhaseDeps:   &PhaseDeps{Log: logr.Discard()},
		httpClient:  server.Client(),
		bearerToken: "test-token",
	}

	err := phase.checkNodeDiskSpace(node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "will reach")
	assert.Contains(t, err.Error(), "storage space after loading new images")
}

func TestCheckNodeDiskSpace_CustomGCThreshold(t *testing.T) {
	// 100GB capacity, 50GB used, custom 60% threshold
	// After loading 13GB: (50+13)/100 * 100 = 63% > 60%
	gcThreshold := int32(60)
	server := newMockKubeletServer(t, 50*testGB, &gcThreshold)
	defer server.Close()

	node := newNodeWithKubeletURL(t, server.URL)
	phase := &InitPhase{
		PhaseDeps:   &PhaseDeps{Log: logr.Discard()},
		httpClient:  server.Client(),
		bearerToken: "test-token",
	}

	err := phase.checkNodeDiskSpace(node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "60%")
}

func TestCheckNodeDiskSpace_MissingInternalIP(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
	}
	phase := &InitPhase{
		PhaseDeps: &PhaseDeps{Log: logr.Discard()},
	}

	err := phase.checkNodeDiskSpace(node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), RKE2InternalIPAnnotation)
}

func TestCheckNodeDiskSpace_KubeletError(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	node := newNodeWithKubeletURL(t, server.URL)
	phase := &InitPhase{
		PhaseDeps:   &PhaseDeps{Log: logr.Discard()},
		httpClient:  server.Client(),
		bearerToken: "test-token",
	}

	err := phase.checkNodeDiskSpace(node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

// --- checkDiskSpace tests ---

func TestCheckDiskSpace_SkipAnnotation(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(newTestScheme()).Build()
	phase := &InitPhase{
		PhaseDeps: &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
	}

	upgradePlan := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationSkipGCThresholdCheck: "true",
			},
		},
	}

	err := phase.checkDiskSpace(context.Background(), upgradePlan)
	assert.NoError(t, err)
}

func TestCheckDiskSpace_InvalidSkipAnnotation(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(newTestScheme()).Build()
	phase := &InitPhase{
		PhaseDeps: &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
	}

	upgradePlan := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationSkipGCThresholdCheck: "notabool",
			},
		},
	}

	err := phase.checkDiskSpace(context.Background(), upgradePlan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid value")
}

func TestCheckDiskSpace_WithNodes(t *testing.T) {
	server := newMockKubeletServer(t, 50*testGB, nil)
	defer server.Close()

	node := newNodeWithKubeletURL(t, server.URL)
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(node).
		Build()

	phase := &InitPhase{
		PhaseDeps:   &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		httpClient:  server.Client(),
		bearerToken: "test-token",
	}

	upgradePlan := &managementv1beta1.UpgradePlan{}
	err := phase.checkDiskSpace(context.Background(), upgradePlan)
	assert.NoError(t, err)
}

// --- checkCerts tests ---

func TestCheckCerts_OK(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(newKubernetesEndpoints("10.0.0.1")).
		Build()

	phase := &InitPhase{
		PhaseDeps:  &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		certDialer: newCertDialer(365 * 24 * time.Hour),
	}

	upgradePlan := &managementv1beta1.UpgradePlan{}
	err := phase.checkCerts(context.Background(), upgradePlan)
	assert.NoError(t, err)
}

func TestCheckCerts_ExpiringTooSoon(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(newKubernetesEndpoints("10.0.0.1")).
		Build()

	phase := &InitPhase{
		PhaseDeps:  &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		certDialer: newCertDialer(3 * 24 * time.Hour), // 3 days < default 7 days
	}

	upgradePlan := &managementv1beta1.UpgradePlan{}
	err := phase.checkCerts(context.Background(), upgradePlan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please rotate RKE2 certificates")
}

func TestCheckCerts_CustomThresholdPasses(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(newKubernetesEndpoints("10.0.0.1")).
		Build()

	phase := &InitPhase{
		PhaseDeps:  &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		certDialer: newCertDialer(5 * 24 * time.Hour), // 5 days
	}

	// Threshold of 3 days, cert expires in 5 days -> should pass
	upgradePlan := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationMinCertsExpirationInDay: "3",
			},
		},
	}

	err := phase.checkCerts(context.Background(), upgradePlan)
	assert.NoError(t, err)
}

func TestCheckCerts_InvalidAnnotation(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(newKubernetesEndpoints("10.0.0.1")).
		Build()

	phase := &InitPhase{
		PhaseDeps:  &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		certDialer: newCertDialer(365 * 24 * time.Hour),
	}

	upgradePlan := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationMinCertsExpirationInDay: "invalid",
			},
		},
	}

	err := phase.checkCerts(context.Background(), upgradePlan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid value")
}

func TestCheckCerts_NegativeAnnotation(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(newKubernetesEndpoints("10.0.0.1")).
		Build()

	phase := &InitPhase{
		PhaseDeps:  &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		certDialer: newCertDialer(365 * 24 * time.Hour),
	}

	upgradePlan := &managementv1beta1.UpgradePlan{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationMinCertsExpirationInDay: "-1",
			},
		},
	}

	err := phase.checkCerts(context.Background(), upgradePlan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be greater than 0")
}

func TestCheckCerts_EmptyEndpoints(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(newKubernetesEndpoints()). // no IPs
		Build()

	phase := &InitPhase{
		PhaseDeps: &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
	}

	upgradePlan := &managementv1beta1.UpgradePlan{}
	err := phase.checkCerts(context.Background(), upgradePlan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cluster IP is empty")
}

func TestCheckCerts_NoCertsReturned(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(newKubernetesEndpoints("10.0.0.1")).
		Build()

	phase := &InitPhase{
		PhaseDeps: &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		certDialer: func(_ string) (*x509.Certificate, error) {
			return nil, nil // no cert returned
		},
	}

	upgradePlan := &managementv1beta1.UpgradePlan{}
	err := phase.checkCerts(context.Background(), upgradePlan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no certificates found")
}

// --- PostRun integration test ---

func TestPostRun_AllChecksPass(t *testing.T) {
	server := newMockKubeletServer(t, 50*testGB, nil)
	defer server.Close()

	node := newNodeWithKubeletURL(t, server.URL)
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(node, newKubernetesEndpoints("10.0.0.1")).
		Build()

	phase := &InitPhase{
		PhaseDeps:   &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		httpClient:  server.Client(),
		bearerToken: "test-token",
		certDialer:  newCertDialer(365 * 24 * time.Hour),
	}

	upgradePlan := &managementv1beta1.UpgradePlan{}
	err := phase.PostRun(context.Background(), upgradePlan)
	assert.NoError(t, err)
}

func TestPostRun_DiskSpaceFailsStopsExecution(t *testing.T) {
	server := newMockKubeletServer(t, 80*testGB, nil)
	defer server.Close()

	node := newNodeWithKubeletURL(t, server.URL)
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(node).
		Build()

	certDialerCalled := false
	phase := &InitPhase{
		PhaseDeps:   &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		httpClient:  server.Client(),
		bearerToken: "test-token",
		certDialer: func(_ string) (*x509.Certificate, error) {
			certDialerCalled = true
			return nil, nil
		},
	}

	upgradePlan := &managementv1beta1.UpgradePlan{}
	err := phase.PostRun(context.Background(), upgradePlan)
	assert.NoError(t, err, "PostRun should return nil; the failure is recorded in CurrentPhase")
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, upgradePlan.Status.CurrentPhase)
	cond := upgradePlan.LookupCondition(managementv1beta1.UpgradePlanProgressing)
	require.NotNil(t, cond)
	assert.Contains(t, cond.Message, "storage space after loading new images")
	assert.False(t, certDialerCalled, "cert check should not be called when disk space fails")
}

func TestPostRun_CertFailsTerminally(t *testing.T) {
	server := newMockKubeletServer(t, 50*testGB, nil)
	defer server.Close()

	node := newNodeWithKubeletURL(t, server.URL)
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(node, newKubernetesEndpoints("10.0.0.1")).
		Build()

	phase := &InitPhase{
		PhaseDeps:   &PhaseDeps{Client: fakeClient, Log: logr.Discard()},
		httpClient:  server.Client(),
		bearerToken: "test-token",
		certDialer:  newCertDialer(3 * 24 * time.Hour), // 3 days < default 7 days
	}

	upgradePlan := &managementv1beta1.UpgradePlan{}
	err := phase.PostRun(context.Background(), upgradePlan)
	assert.NoError(t, err, "PostRun should return nil; the failure is recorded in CurrentPhase")
	assert.Equal(t, managementv1beta1.UpgradePlanPhaseFailed, upgradePlan.Status.CurrentPhase)
	cond := upgradePlan.LookupCondition(managementv1beta1.UpgradePlanProgressing)
	require.NotNil(t, cond)
	assert.Contains(t, cond.Message, "Please rotate RKE2 certificates")
}
