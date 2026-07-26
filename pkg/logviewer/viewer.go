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
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

const (
	DefaultScanInterval = 5 * time.Second
	filePollDelay       = 500 * time.Millisecond
	maxLineSize         = 1024 * 1024 // 1MB
	initBufSize         = 64 * 1024   // 64KB
)

// Config holds configuration for the log viewer.
type Config struct {
	// WatchDirs are the root directories to scan for .log files.
	WatchDirs []string

	// ScanInterval controls how often new files are discovered.
	ScanInterval time.Duration

	// Output is the writer for tailed log lines. Defaults to os.Stdout.
	Output io.Writer
}

// Viewer watches directories for .log files and tails them to an output writer,
// prefixing each line with the file's relative path from its watch root.
type Viewer struct {
	config Config
	log    logr.Logger

	mu      sync.Mutex
	tailing map[string]context.CancelFunc // absolute path -> cancel

	writeMu sync.Mutex // serializes writes to config.Output
}

// NewViewer creates a Viewer.
func NewViewer(config Config, log logr.Logger) *Viewer {
	if config.Output == nil {
		config.Output = os.Stdout
	}
	if config.ScanInterval <= 0 {
		config.ScanInterval = DefaultScanInterval
	}
	return &Viewer{
		config:  config,
		log:     log.WithName("log-viewer"),
		tailing: make(map[string]context.CancelFunc),
	}
}

// Run starts watching and tailing. It blocks until ctx is cancelled.
func (v *Viewer) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	// Initial scan
	v.scan(ctx, &wg)

	ticker := time.NewTicker(v.config.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			v.mu.Lock()
			for _, cancel := range v.tailing {
				cancel()
			}
			v.mu.Unlock()
			wg.Wait()
			return nil
		case <-ticker.C:
			v.scan(ctx, &wg)
		}
	}
}

func (v *Viewer) scan(ctx context.Context, wg *sync.WaitGroup) {
	for _, dir := range v.config.WatchDirs {
		files, err := ScanForLogFiles([]string{dir})
		if err != nil {
			v.log.V(1).Info("scan error", "dir", dir, "error", err)
			continue
		}
		for _, absPath := range files {
			v.mu.Lock()
			if _, ok := v.tailing[absPath]; ok {
				v.mu.Unlock()
				continue
			}
			fileCtx, cancel := context.WithCancel(ctx)
			v.tailing[absPath] = cancel
			v.mu.Unlock()

			wg.Add(1)
			go v.tailFile(fileCtx, wg, dir, absPath)
		}
	}
}

func (v *Viewer) tailFile(ctx context.Context, wg *sync.WaitGroup, watchRoot, absPath string) {
	defer wg.Done()

	relPath, err := filepath.Rel(watchRoot, absPath)
	if err != nil {
		relPath = absPath
	}

	f, err := os.Open(absPath)
	if err != nil {
		v.log.V(1).Info("failed to open file", "path", absPath, "error", err)
		v.removeTailing(absPath)
		return
	}
	defer f.Close() //nolint:errcheck

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, initBufSize), maxLineSize)

	for {
		for scanner.Scan() {
			v.writeMu.Lock()
			_, _ = fmt.Fprintf(v.config.Output, "%s: %s\n", relPath, scanner.Text())
			v.writeMu.Unlock()
		}
		if scanner.Err() != nil {
			v.log.V(1).Info("scanner error", "path", absPath, "error", scanner.Err())
			v.removeTailing(absPath)
			return
		}

		// EOF reached, poll for more data
		select {
		case <-ctx.Done():
			return
		case <-time.After(filePollDelay):
			info, err := os.Stat(absPath)
			if err != nil {
				// File removed
				v.log.V(1).Info("file removed", "path", absPath)
				v.removeTailing(absPath)
				return
			}
			pos, _ := f.Seek(0, io.SeekCurrent)
			if info.Size() <= pos {
				continue
			}
			// File has grown, re-create scanner from current position
			scanner = bufio.NewScanner(f)
			scanner.Buffer(make([]byte, 0, initBufSize), maxLineSize)
		}
	}
}

func (v *Viewer) removeTailing(absPath string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if cancel, ok := v.tailing[absPath]; ok {
		cancel()
		delete(v.tailing, absPath)
	}
}

// ScanForLogFiles walks the given directories and returns absolute paths of all
// .log files found, including in nested subdirectories.
func ScanForLogFiles(dirs []string) ([]string, error) {
	var result []string
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // skip inaccessible entries
			}
			if !d.IsDir() && strings.HasSuffix(d.Name(), ".log") {
				abs, absErr := filepath.Abs(path)
				if absErr != nil {
					return nil
				}
				result = append(result, abs)
			}
			return nil
		})
		if err != nil {
			return result, fmt.Errorf("walking %s: %w", dir, err)
		}
	}
	return result, nil
}
