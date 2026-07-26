/*
Copyright 2026.

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

package logviewer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanForLogFiles(t *testing.T) {
	dir := t.TempDir()

	// Create nested structure with .log and non-log files
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "component-a"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "component-b", "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "component-a", "pod1.log"), []byte("line1\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "component-b", "pod2.log"), []byte("line2\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "component-b", "nested", "deep.log"), []byte("line3\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "component-a", "readme.txt"), []byte("ignore\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "root.log"), []byte("root\n"), 0o644))

	files, err := ScanForLogFiles([]string{dir})
	require.NoError(t, err)

	assert.Len(t, files, 4, "should find exactly 4 .log files")

	for _, f := range files {
		assert.True(t, strings.HasSuffix(f, ".log"), "all results should be .log files: %s", f)
		assert.True(t, filepath.IsAbs(f), "all results should be absolute paths: %s", f)
	}
}

func TestScanForLogFiles_NonExistentDir(t *testing.T) {
	files, err := ScanForLogFiles([]string{"/nonexistent/path"})
	require.NoError(t, err)
	assert.Empty(t, files)
}

func TestViewer_TailsExistingFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "pre-drain"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pre-drain", "node1.log"), []byte("hello from node1\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pre-drain", "node2.log"), []byte("hello from node2\n"), 0o644))

	var buf bytes.Buffer
	v := NewViewer(Config{
		WatchDirs:    []string{dir},
		ScanInterval: 100 * time.Millisecond,
		Output:       &buf,
	}, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())

	var runErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = v.Run(ctx)
	}()

	// Wait for output
	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "node1") && strings.Contains(buf.String(), "node2")
	}, 5*time.Second, 50*time.Millisecond)

	cancel()
	wg.Wait()
	assert.NoError(t, runErr)

	output := buf.String()
	assert.Contains(t, output, "pre-drain/node1.log: hello from node1")
	assert.Contains(t, output, "pre-drain/node2.log: hello from node2")
}

func TestViewer_DiscoversDynamicFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "component"), 0o755))

	var buf bytes.Buffer
	v := NewViewer(Config{
		WatchDirs:    []string{dir},
		ScanInterval: 200 * time.Millisecond,
		Output:       &buf,
	}, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())

	var runErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = v.Run(ctx)
	}()

	// Create file after viewer starts
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "component", "late.log"), []byte("dynamic line\n"), 0o644))

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "dynamic line")
	}, 5*time.Second, 50*time.Millisecond)

	cancel()
	wg.Wait()
	assert.NoError(t, runErr)
	assert.Contains(t, buf.String(), "component/late.log: dynamic line")
}

func TestViewer_GracefulShutdown(t *testing.T) {
	dir := t.TempDir()

	v := NewViewer(Config{
		WatchDirs:    []string{dir},
		ScanInterval: 1 * time.Second,
		Output:       &bytes.Buffer{},
	}, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- v.Run(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2 seconds after context cancellation")
	}
}

func TestViewer_RelativePathPrefix(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a", "b", "deep.log"), []byte("nested\n"), 0o644))

	var buf bytes.Buffer
	v := NewViewer(Config{
		WatchDirs:    []string{dir},
		ScanInterval: 100 * time.Millisecond,
		Output:       &buf,
	}, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = v.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "nested")
	}, 5*time.Second, 50*time.Millisecond)

	cancel()
	wg.Wait()

	// Must use relative path, not absolute
	output := buf.String()
	assert.Contains(t, output, "a/b/deep.log: nested")
	assert.NotContains(t, output, dir, "output should not contain absolute watch root path")
}
