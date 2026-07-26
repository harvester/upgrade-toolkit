package logshipper

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	pb "github.com/harvester/upgrade-toolkit/pkg/logshipper/proto"
)

// Collector is a gRPC server that receives log streams and writes them to files.
type Collector struct {
	pb.UnimplementedLogCollectorServer

	logDir string
	log    logr.Logger

	mu           sync.Mutex
	trackedPods  map[string]*podTracker
	server       *grpc.Server
	bytesWritten int64
}

type podTracker struct {
	component string
	node      string
	file      *os.File
	bytes     int64
}

// NewCollector creates a new log collector that writes logs to the given directory.
func NewCollector(logDir string, log logr.Logger) *Collector {
	return &Collector{
		logDir:      logDir,
		log:         log.WithName("log-collector"),
		trackedPods: make(map[string]*podTracker),
	}
}

// StreamLogs implements the LogCollector gRPC service.
func (c *Collector) StreamLogs(stream pb.LogCollector_StreamLogsServer) error {
	var podKey string
	var tracker *podTracker

	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			c.log.V(1).Info("stream ended", "pod", podKey)
			if tracker != nil {
				if syncErr := tracker.file.Sync(); syncErr != nil {
					c.log.Error(syncErr, "failed to sync log file", "pod", podKey)
				}
			}
			var totalBytes int64
			if tracker != nil {
				totalBytes = tracker.bytes
			}
			return stream.SendAndClose(&pb.StreamLogsResponse{
				BytesReceived: totalBytes,
			})
		}
		if err != nil {
			return fmt.Errorf("receiving log entry: %w", err)
		}

		// Lazily initialize the tracker on first entry
		if tracker == nil {
			podKey = entry.PodNamespace + "/" + entry.PodName
			var initErr error
			tracker, initErr = c.getOrCreateTracker(entry)
			if initErr != nil {
				return fmt.Errorf("creating log file for pod %s: %w", podKey, initErr)
			}
			c.log.V(1).Info("started tracking pod", "pod", podKey, "component", entry.Component)
		}

		n, writeErr := tracker.file.Write(entry.Line)
		if writeErr != nil {
			return fmt.Errorf("writing log for pod %s: %w", podKey, writeErr)
		}
		tracker.bytes += int64(n)

		c.mu.Lock()
		c.bytesWritten += int64(n)
		c.mu.Unlock()
	}
}

func (c *Collector) getOrCreateTracker(entry *pb.LogEntry) (*podTracker, error) {
	key := entry.PodNamespace + "/" + entry.PodName

	c.mu.Lock()
	defer c.mu.Unlock()

	if t, exists := c.trackedPods[key]; exists {
		return t, nil
	}

	// Create directory structure: <logDir>/<component>/
	dir := filepath.Join(c.logDir, entry.Component)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating log directory %s: %w", dir, err)
	}

	logPath := filepath.Join(dir, entry.PodName+".log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening log file %s: %w", logPath, err)
	}

	t := &podTracker{
		component: entry.Component,
		node:      entry.Node,
		file:      f,
	}
	c.trackedPods[key] = t
	return t, nil
}

// Serve starts the gRPC server on the given address.
func (c *Collector) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}

	c.server = grpc.NewServer()
	pb.RegisterLogCollectorServer(c.server, c)

	c.log.Info("starting gRPC log collector", "addr", addr)
	return c.server.Serve(lis)
}

// GracefulStop stops the gRPC server gracefully.
func (c *Collector) GracefulStop() {
	if c.server != nil {
		c.server.GracefulStop()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, t := range c.trackedPods {
		if err := t.file.Close(); err != nil {
			c.log.Error(err, "failed to close log file", "pod", key)
		}
	}
}

// TotalBytesWritten returns the total bytes written across all pods.
func (c *Collector) TotalBytesWritten() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytesWritten
}
