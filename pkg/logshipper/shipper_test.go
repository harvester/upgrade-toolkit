package logshipper

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/harvester/upgrade-toolkit/pkg/logshipper/proto"
)

const bufSize = 1024 * 1024

// recordingCollector is a test gRPC server that records all received entries.
type recordingCollector struct {
	pb.UnimplementedLogCollectorServer

	mu      sync.Mutex
	entries []*pb.LogEntry
}

func (c *recordingCollector) StreamLogs(stream grpc.ClientStreamingServer[pb.LogEntry, pb.StreamLogsResponse]) error {
	var totalBytes int64
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.StreamLogsResponse{BytesReceived: totalBytes})
		}
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.entries = append(c.entries, entry)
		c.mu.Unlock()
		totalBytes += int64(len(entry.Line))
	}
}

func (c *recordingCollector) getEntries() []*pb.LogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*pb.LogEntry, len(c.entries))
	copy(out, c.entries)
	return out
}

// failAfterNCollector records entries but fails the first stream after N entries.
// Subsequent streams succeed normally.
type failAfterNCollector struct {
	pb.UnimplementedLogCollectorServer

	mu               sync.Mutex
	entries          []*pb.LogEntry
	streams          int
	failAfterEntries int
}

func (c *failAfterNCollector) StreamLogs(stream grpc.ClientStreamingServer[pb.LogEntry, pb.StreamLogsResponse]) error {
	c.mu.Lock()
	c.streams++
	streamNum := c.streams
	c.mu.Unlock()

	var count int
	var totalBytes int64
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.StreamLogsResponse{BytesReceived: totalBytes})
		}
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.entries = append(c.entries, entry)
		c.mu.Unlock()
		totalBytes += int64(len(entry.Line))
		count++

		if streamNum == 1 && count >= c.failAfterEntries {
			return fmt.Errorf("simulated stream failure")
		}
	}
}

func (c *failAfterNCollector) getEntries() []*pb.LogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*pb.LogEntry, len(c.entries))
	copy(out, c.entries)
	return out
}

func startServer(t *testing.T, srv pb.LogCollectorServer) (*bufconn.Listener, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pb.RegisterLogCollectorServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	return lis, func() { s.Stop() }
}

func shipperDialOpts(lis *bufconn.Listener) []Option {
	return []Option{WithDialOptions(
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)}
}

func writeLogFile(t *testing.T, dir string, lines []string) {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, "output.log"))
	require.NoError(t, err)
	for _, line := range lines {
		_, err := fmt.Fprintln(f, line)
		require.NoError(t, err)
	}
	require.NoError(t, f.Close())
}

func TestShipper_StreamsAllLines(t *testing.T) {
	tmpDir := t.TempDir()
	lines := []string{"line one", "line two", "line three"}
	writeLogFile(t, tmpDir, lines)

	collector := &recordingCollector{}
	lis, cleanup := startServer(t, collector)
	defer cleanup()

	shipper := NewShipper(ShipperConfig{
		LogDir:            tmpDir,
		CollectorEndpoint: "passthrough:///bufconn",
		PodName:           "test-pod",
		PodNamespace:      "test-ns",
		Component:         "test-component",
		NodeName:          "test-node",
	}, logr.Discard(), shipperDialOpts(lis)...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- shipper.Run(ctx) }()

	require.Eventually(t, func() bool {
		return len(collector.getEntries()) == len(lines)
	}, 5*time.Second, 50*time.Millisecond)

	cancel()
	err := <-done
	assert.ErrorIs(t, err, context.Canceled)

	entries := collector.getEntries()
	require.Len(t, entries, len(lines))
	for i, entry := range entries {
		assert.Equal(t, lines[i]+"\n", string(entry.Line))
		assert.Equal(t, "test-pod", entry.PodName)
		assert.Equal(t, "test-ns", entry.PodNamespace)
		assert.Equal(t, "test-component", entry.Component)
		assert.Equal(t, "test-node", entry.Node)
	}
}

func TestShipper_RetryAfterMidStreamFailure(t *testing.T) {
	tmpDir := t.TempDir()
	// Use enough lines so the scanner is still scanning when the server error propagates
	lines := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet"}
	writeLogFile(t, tmpDir, lines)

	// Server fails after receiving 2 entries on the first stream, then succeeds
	collector := &failAfterNCollector{failAfterEntries: 2}
	lis, cleanup := startServer(t, collector)
	defer cleanup()

	shipper := NewShipper(ShipperConfig{
		LogDir:            tmpDir,
		CollectorEndpoint: "passthrough:///bufconn",
		PodName:           "retry-pod",
		PodNamespace:      "retry-ns",
		Component:         "retry-comp",
		NodeName:          "retry-node",
	}, logr.Discard(), shipperDialOpts(lis)...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- shipper.Run(ctx) }()

	// Wait until all unique lines have been received
	require.Eventually(t, func() bool {
		entries := collector.getEntries()
		seen := make(map[string]bool)
		for _, e := range entries {
			seen[string(e.Line)] = true
		}
		for _, l := range lines {
			if !seen[l+"\n"] {
				return false
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond)

	cancel()
	err := <-done
	assert.ErrorIs(t, err, context.Canceled)
}

func TestShipper_RespectsContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	writeLogFile(t, tmpDir, []string{"one"})

	collector := &recordingCollector{}
	lis, cleanup := startServer(t, collector)
	defer cleanup()

	shipper := NewShipper(ShipperConfig{
		LogDir:            tmpDir,
		CollectorEndpoint: "passthrough:///bufconn",
		PodName:           "cancel-pod",
		PodNamespace:      "cancel-ns",
		Component:         "cancel-comp",
		NodeName:          "cancel-node",
	}, logr.Discard(), shipperDialOpts(lis)...)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	start := time.Now()
	err := shipper.Run(ctx)
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, 2*time.Second)
}
