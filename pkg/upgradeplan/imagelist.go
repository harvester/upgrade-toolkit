package upgradeplan

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"k8s.io/client-go/rest"
)

// imageRetainList contains image name components that must NOT be purged during
// cleanup, even if they are absent from the new version's image list. Each
// entry is matched against the name component of a fully-qualified image
// reference (the part between the last "/" and the tag separator ":").
var imageRetainList = []string{
	"harvester-upgrade",
	"longhorn-engine",
	"longhorn-instance-manager",
	"mirrored-banzaicloud-fluentd",
	"mirrored-fluent-fluent-bit",
}

// repoBaseURL returns the HTTP base URL of the upgrade ISO repository.
// In-cluster it resolves via the Service DNS name; out-of-cluster it falls
// back to localhost (for local development).
func repoBaseURL(upgradePlanName string) string {
	if _, err := rest.InClusterConfig(); err != nil {
		return "http://localhost/harvester-iso"
	}
	return fmt.Sprintf(
		"http://%s-%s.%s/harvester-iso",
		upgradePlanName,
		repoComponent,
		harvesterSystemNamespace,
	)
}

// fetchImageList downloads the image manifest for the given version from the
// upgrade repository and returns the set of image references it contains.
func fetchImageList(
	ctx context.Context,
	httpClient *http.Client,
	baseURL, version string,
) (map[string]struct{}, error) {
	imageListURL := fmt.Sprintf(
		"%s/bundle/harvester/images-lists-archive/%s/image_list_all.txt",
		baseURL, version,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imageListURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request for %s: %w", imageListURL, err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch image list from %s: %w", imageListURL, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch image list from %s: HTTP %d", imageListURL, resp.StatusCode)
	}

	images := make(map[string]struct{})
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			images[line] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read image list from %s: %w", imageListURL, err)
	}

	return images, nil
}

// imagesDiff returns the sorted list of images present in previous but absent
// in current.
func imagesDiff(previous, current map[string]struct{}) []string {
	diff := make([]string, 0, len(previous))
	for img := range previous {
		if _, ok := current[img]; !ok {
			diff = append(diff, img)
		}
	}
	sort.Strings(diff)
	return diff
}

// filterRetainedImages removes images whose name component matches any entry
// in imageRetainList. The name component is the segment between the last "/"
// and the first ":" (or end of string if no tag).
//
// Example: "docker.io/rancher/harvester-upgrade:v1.2.0"
//
// -> name component = "harvester-upgrade"
// -> matches retain list → filtered out
func filterRetainedImages(images []string) []string {
	retained := make(map[string]struct{}, len(imageRetainList))
	for _, r := range imageRetainList {
		retained[r] = struct{}{}
	}

	result := make([]string, 0, len(images))
	for _, img := range images {
		if _, ok := retained[imageNameComponent(img)]; !ok {
			result = append(result, img)
		}
	}
	return result
}

// imageNameComponent extracts the name component from a fully-qualified image
// reference. For "registry.io/org/name:tag" it returns "name". For "name:tag"
// it returns "name". For "name" it returns "name".
func imageNameComponent(ref string) string {
	// Strip tag/digest
	name := ref
	if idx := strings.LastIndex(name, ":"); idx != -1 {
		// Ensure we don't strip a port from the registry (e.g., "localhost:5000/img:tag")
		if slashIdx := strings.LastIndex(name, "/"); slashIdx == -1 || idx > slashIdx {
			name = name[:idx]
		}
	}
	if idx := strings.LastIndex(name, "@"); idx != -1 {
		name = name[:idx]
	}
	// Extract the last path component
	if idx := strings.LastIndex(name, "/"); idx != -1 {
		name = name[idx+1:]
	}
	return name
}
