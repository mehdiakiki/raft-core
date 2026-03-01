// Command node starts a single Raft node that exposes a gRPC server.
//
// Usage:
//
//	node --id=A --addr=:50051 --peers=B=node-b:50052,C=node-c:50053 --storage-dir=/data/raft
//
// The node dials its peers over gRPC and runs the Raft state machine.
// State changes are streamed to any connected gateway via the WatchState RPC.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "github.com/mehdiakiki/raft-core/gen/raft"
	"github.com/mehdiakiki/raft-core/internal/raft"
	"github.com/mehdiakiki/raft-core/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	id := flag.String("id", "", "unique node identifier (e.g. A)")
	addr := flag.String("addr", ":50051", "gRPC listen address (e.g. :50051)")
	peersFlag := flag.String("peers", "", "comma-separated peer list: ID=host:port,…")
	storageDir := flag.String("storage-dir", "", "directory for persistent storage (if empty, uses in-memory)")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	electionTimeoutMin := flag.Duration("election-timeout-min", 0, "override minimum election timeout (e.g. 350ms)")
	electionTimeoutMax := flag.Duration("election-timeout-max", 0, "override maximum election timeout (e.g. 700ms)")
	heartbeatInterval := flag.Duration("heartbeat-interval", 0, "override leader heartbeat interval (e.g. 100ms)")
	flag.Parse()

	if *id == "" {
		fmt.Fprintln(os.Stderr, "flag --id is required")
		os.Exit(1)
	}
	if *electionTimeoutMin < 0 || *electionTimeoutMax < 0 || *heartbeatInterval < 0 {
		fmt.Fprintln(os.Stderr, "timing flags must be non-negative durations")
		os.Exit(1)
	}
	if *electionTimeoutMin > 0 && *electionTimeoutMax > 0 && *electionTimeoutMin > *electionTimeoutMax {
		fmt.Fprintln(os.Stderr, "flag --election-timeout-min cannot be greater than --election-timeout-max")
		os.Exit(1)
	}

	level, err := parseLogLevel(*logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})))

	// Initialize storage.
	var store raft.Storage
	if *storageDir != "" {
		s, err := storage.NewBoltStore(*storageDir)
		if err != nil {
			slog.Error("failed to create storage", "dir", *storageDir, "err", err)
			os.Exit(1)
		}
		store = s
		slog.Info("using persistent storage", "dir", *storageDir)
	} else {
		store = storage.NewMemoryStore()
		slog.Info("using in-memory storage")
	}

	peers, err := dialPeers(*peersFlag)
	if err != nil {
		slog.Error("failed to connect to peers", "err", err)
		os.Exit(1)
	}

	config := raft.Config{
		ID:                 *id,
		Peers:              peers,
		Storage:            store,
		ElectionTimeoutMin: *electionTimeoutMin,
		ElectionTimeoutMax: *electionTimeoutMax,
		HeartbeatInterval:  *heartbeatInterval,
	}
	logEffectiveTimingConfig(*id, config)

	node := raft.New(config)
	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, raft.NewServer(node))

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		slog.Error("failed to listen", "addr", *addr, "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("gRPC server started", "id", *id, "addr", *addr)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server error", "err", err)
		}
	}()

	go node.Run(ctx)

	<-ctx.Done()
	slog.Info("shutting down", "id", *id)
	grpcServer.GracefulStop()

	if store != nil {
		store.Close()
	}
}

func logEffectiveTimingConfig(id string, cfg raft.Config) {
	min := cfg.ElectionTimeoutMin
	if min <= 0 {
		min = raft.ElectionTimeoutMin
	}
	max := cfg.ElectionTimeoutMax
	if max <= 0 {
		max = raft.ElectionTimeoutMax
	}
	hb := cfg.HeartbeatInterval
	if hb <= 0 {
		hb = raft.HeartbeatInterval
	}

	slog.Info("raft timing configured",
		"id", id,
		"electionTimeoutMin", min,
		"electionTimeoutMax", max,
		"heartbeatInterval", hb,
	)
}

func parseLogLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("invalid --log-level %q (allowed: debug, info, warn, error)", raw)
	}
}

// dialPeers parses "--peers=B=node-b:50052,C=node-c:50053" and dials each peer.
// It retries for up to 10 seconds to allow containers to start in any order.
func dialPeers(flag string) (map[string]raft.Peer, error) {
	peers := make(map[string]raft.Peer)
	if flag == "" {
		return peers, nil
	}

	for _, pair := range strings.Split(flag, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer spec %q (expected ID=host:port)", pair)
		}
		peerID, addr := parts[0], parts[1]

		conn, err := dialWithRetry(addr, 10*time.Second)
		if err != nil {
			return nil, fmt.Errorf("could not dial peer %s at %s: %w", peerID, addr, err)
		}
		peers[peerID] = raft.NewGRPCPeer(peerID, pb.NewRaftServiceClient(conn))
		slog.Info("connected to peer", "peer", peerID, "addr", addr)
	}
	return peers, nil
}

func dialWithRetry(addr string, timeout time.Duration) (*grpc.ClientConn, error) {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err == nil {
			return conn, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		slog.Warn("peer not ready, retrying", "addr", addr)
		time.Sleep(500 * time.Millisecond)
	}
}
