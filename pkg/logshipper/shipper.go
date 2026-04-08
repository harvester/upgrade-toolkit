package logshipper

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/harvester/upgrade-toolkit/pkg/logshipper/proto"
)

const (
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 10 * time.Second
	filePollDelay  = 500 * time.Millisecond
	fileWaitTick   = 200 * time.Millisecond
	maxLineSize    = 1024 * 1024 // 1MB
	initBufSize    = 64 * 1024   // 64KB
)

// ShipperConfig holds configuration for the log shipper.
type ShipperConfig struct {
	LogDir            string
	CollectorEndpoint string
	PodName           string
	PodNamespace      string
	Component         string
	NodeName          string
}

// Option configures the Shipper.
type Option func(*Shipper)

// WithDialOptions appends gRPC dial options (useful for testing with bufconn).
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(s *Shipper) {
		s.dialOpts = append(s.dialOpts, opts...)
	}
}

// Shipper tails log files and streams them to a log collector via gRPC.
type Shipper struct {
	config   ShipperConfig
	log      logr.Logger
	dialOpts []grpc.DialOption
}

// NewShipper creates a new log shipper.
func NewShipper(config ShipperConfig, log logr.Logger, opts ...Option) *Shipper {
	s := &Shipper{
		config: config,
		log:    log.WithName("log-shipper"),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Run starts the shipper. It blocks until the context is cancelled or the
// log file reaches EOF after the main container exits.
func (s *Shipper) Run(ctx context.Context) error {
	logFile := filepath.Join(s.config.LogDir, "output.log")

	// Wait for the log file to appear (main container may not have started yet)
	s.log.Info("waiting for log file", "path", logFile)
	if err := s.waitForFile(ctx, logFile); err != nil {
		return fmt.Errorf("waiting for log file: %w", err)
	}

	// Open the file once; streamFrom seeks to the right offset on each attempt
	f, err := os.Open(logFile)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}
	defer f.Close() //nolint:errcheck // best-effort cleanup

	var offset int64
	backoff := initialBackoff

	for {
		err := s.streamFrom(ctx, f, &offset)
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}

		s.log.Info("stream failed, retrying", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// streamFrom performs a single streaming attempt starting from *offset in f.
// On success (context cancelled cleanly) it returns nil and commits the offset.
// On transient errors it returns the error without advancing *offset, so the
// caller can retry from the same position.
func (s *Shipper) streamFrom(ctx context.Context, f *os.File, offset *int64) error {
	startOffset := *offset
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seeking log file: %w", err)
	}

	opts := make([]grpc.DialOption, 0, 1+len(s.dialOpts))
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, s.dialOpts...)

	conn, err := grpc.NewClient(s.config.CollectorEndpoint, opts...)
	if err != nil {
		return fmt.Errorf("creating gRPC client: %w", err)
	}
	defer conn.Close() //nolint:errcheck // best-effort cleanup

	client := pb.NewLogCollectorClient(conn)
	stream, err := client.StreamLogs(ctx)
	if err != nil {
		return fmt.Errorf("opening stream: %w", err)
	}

	// Monitor for server-side errors asynchronously. In client-streaming,
	// the server status/error arrives as the response message. RecvMsg
	// blocks until the server sends it (normally after CloseAndRecv, or
	// immediately if the server handler returns an error).
	serverDone := make(chan error, 1)
	go func() {
		var resp pb.StreamLogsResponse
		serverDone <- stream.RecvMsg(&resp)
	}()

	// localOffset tracks how far we've sent within this stream attempt.
	// It is only committed to *offset on clean shutdown.
	localOffset := startOffset
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, initBufSize), maxLineSize)

	for {
		for scanner.Scan() {
			line := scanner.Bytes()
			lineWithNL := make([]byte, len(line)+1)
			copy(lineWithNL, line)
			lineWithNL[len(line)] = '\n'

			entry := &pb.LogEntry{
				PodName:           s.config.PodName,
				PodNamespace:      s.config.PodNamespace,
				Component:         s.config.Component,
				Node:              s.config.NodeName,
				Line:              lineWithNL,
				TimestampUnixNano: time.Now().UnixNano(),
			}
			if sendErr := stream.Send(entry); sendErr != nil {
				return fmt.Errorf("sending log entry: %w", sendErr)
			}
			localOffset += int64(len(line)) + 1
		}
		if scanner.Err() != nil {
			return fmt.Errorf("scanning log file: %w", scanner.Err())
		}

		// EOF reached. Check if the main container is still running by
		// watching for further writes. Also watch for server-side errors
		// so we can retry if the collector crashes.
		select {
		case <-ctx.Done():
			// Context cancelled (SIGTERM). Commit offset.
			*offset = localOffset
			return nil
		case sErr := <-serverDone:
			// Server closed the stream or returned an error.
			return fmt.Errorf("stream closed by server: %w", sErr)
		case <-time.After(filePollDelay):
			info, statErr := f.Stat()
			if statErr != nil {
				*offset = localOffset
				return nil
			}
			if info.Size() <= localOffset {
				// File hasn't grown. Native sidecars receive SIGTERM after
				// main container exits, so ctx.Done() will fire.
				continue
			}
			// File has grown, re-create scanner from current position
			scanner = bufio.NewScanner(f)
			scanner.Buffer(make([]byte, 0, initBufSize), maxLineSize)
		}
	}
}

func (s *Shipper) waitForFile(ctx context.Context, path string) error {
	ticker := time.NewTicker(fileWaitTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := os.Stat(path); err == nil {
				return nil
			}
		}
	}
}
