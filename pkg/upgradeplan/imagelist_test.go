package upgradeplan

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImagesDiff(t *testing.T) {
	testCases := []struct {
		name     string
		previous map[string]struct{}
		current  map[string]struct{}
		expected []string
	}{
		{
			name:     "both empty",
			previous: map[string]struct{}{},
			current:  map[string]struct{}{},
			expected: []string{},
		},
		{
			name: "current empty — all previous are diff",
			previous: map[string]struct{}{
				"docker.io/rancher/harvester:v1.1.0": {},
				"docker.io/rancher/fleet:v0.5.0":     {},
			},
			current:  map[string]struct{}{},
			expected: []string{"docker.io/rancher/fleet:v0.5.0", "docker.io/rancher/harvester:v1.1.0"},
		},
		{
			name:     "previous empty — no diff",
			previous: map[string]struct{}{},
			current: map[string]struct{}{
				"docker.io/rancher/harvester:v1.2.0": {},
			},
			expected: []string{},
		},
		{
			name: "overlapping sets",
			previous: map[string]struct{}{
				"docker.io/rancher/harvester:v1.1.0": {},
				"docker.io/rancher/fleet:v0.5.0":     {},
				"docker.io/longhornio/engine:v1.4.0": {},
			},
			current: map[string]struct{}{
				"docker.io/rancher/harvester:v1.2.0": {},
				"docker.io/rancher/fleet:v0.5.0":     {},
				"docker.io/longhornio/engine:v1.5.0": {},
			},
			expected: []string{
				"docker.io/longhornio/engine:v1.4.0",
				"docker.io/rancher/harvester:v1.1.0",
			},
		},
		{
			name: "identical sets — no diff",
			previous: map[string]struct{}{
				"docker.io/rancher/harvester:v1.1.0": {},
			},
			current: map[string]struct{}{
				"docker.io/rancher/harvester:v1.1.0": {},
			},
			expected: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := imagesDiff(tc.previous, tc.current)
			if len(tc.expected) == 0 {
				assert.Empty(t, result)
			} else {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestImageNameComponent(t *testing.T) {
	testCases := []struct {
		ref      string
		expected string
	}{
		{"docker.io/rancher/harvester-upgrade:v1.2.0", "harvester-upgrade"},
		{"rancher/harvester-upgrade:v1.2.0", "harvester-upgrade"},
		{"harvester-upgrade:v1.2.0", "harvester-upgrade"},
		{"harvester-upgrade", "harvester-upgrade"},
		{"docker.io/longhornio/longhorn-engine:v1.4.0", "longhorn-engine"},
		{"localhost:5000/myimage:latest", "myimage"},
		{"registry.example.com:5000/org/myimage:v1", "myimage"},
		{"docker.io/rancher/mirrored-fluent-fluent-bit:1.9.5", "mirrored-fluent-fluent-bit"},
		{"myimage@sha256:abc123", "myimage"},
		{"docker.io/org/myimage@sha256:abc123", "myimage"},
	}

	for _, tc := range testCases {
		t.Run(tc.ref, func(t *testing.T) {
			assert.Equal(t, tc.expected, imageNameComponent(tc.ref))
		})
	}
}

func TestFilterRetainedImages(t *testing.T) {
	testCases := []struct {
		name     string
		images   []string
		expected []string
	}{
		{
			name:     "empty list",
			images:   []string{},
			expected: []string{},
		},
		{
			name: "no retained images",
			images: []string{
				"docker.io/rancher/fleet:v0.5.0",
				"docker.io/rancher/rancher:v2.7.0",
			},
			expected: []string{
				"docker.io/rancher/fleet:v0.5.0",
				"docker.io/rancher/rancher:v2.7.0",
			},
		},
		{
			name: "all retained images filtered out",
			images: []string{
				"docker.io/rancher/harvester-upgrade:v1.1.0",
				"docker.io/longhornio/longhorn-engine:v1.4.0",
				"docker.io/longhornio/longhorn-instance-manager:v1.4.0",
			},
			expected: []string{},
		},
		{
			name: "mixed retained and non-retained",
			images: []string{
				"docker.io/rancher/fleet:v0.5.0",
				"docker.io/rancher/harvester-upgrade:v1.1.0",
				"docker.io/rancher/rancher:v2.7.0",
				"docker.io/longhornio/longhorn-engine:v1.4.0",
			},
			expected: []string{
				"docker.io/rancher/fleet:v0.5.0",
				"docker.io/rancher/rancher:v2.7.0",
			},
		},
		{
			name: "no false positives from substring matching",
			images: []string{
				"docker.io/rancher/my-harvester-upgrade-helper:v1.0.0",
				"docker.io/rancher/not-longhorn-engine-foo:v1.0.0",
			},
			expected: []string{
				"docker.io/rancher/my-harvester-upgrade-helper:v1.0.0",
				"docker.io/rancher/not-longhorn-engine-foo:v1.0.0",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := filterRetainedImages(tc.images)
			if len(tc.expected) == 0 {
				assert.Empty(t, result)
			} else {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestFetchImageList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/harvester-iso/bundle/harvester/images-lists-archive/v1.2.0/image_list_all.txt":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("docker.io/rancher/harvester:v1.2.0\ndocker.io/rancher/fleet:v0.6.0\n"))
		case "/harvester-iso/bundle/harvester/images-lists-archive/v1.1.0/image_list_all.txt":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("docker.io/rancher/harvester:v1.1.0\ndocker.io/rancher/fleet:v0.5.0\n"))
		case "/harvester-iso/bundle/harvester/images-lists-archive/empty/image_list_all.txt":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(""))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	baseURL := server.URL + "/harvester-iso"
	httpClient := server.Client()
	ctx := context.Background()

	t.Run("fetches and parses image list", func(t *testing.T) {
		images, err := fetchImageList(ctx, httpClient, baseURL, "v1.2.0")
		require.NoError(t, err)
		assert.Len(t, images, 2)
		assert.Contains(t, images, "docker.io/rancher/harvester:v1.2.0")
		assert.Contains(t, images, "docker.io/rancher/fleet:v0.6.0")
	})

	t.Run("empty image list", func(t *testing.T) {
		images, err := fetchImageList(ctx, httpClient, baseURL, "empty")
		require.NoError(t, err)
		assert.Empty(t, images)
	})

	t.Run("version not found returns error", func(t *testing.T) {
		_, err := fetchImageList(ctx, httpClient, baseURL, "v999.0.0")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "HTTP 404")
	})
}
