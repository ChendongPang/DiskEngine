package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dataplane/internal/gateway"
	"dataplane/internal/rpc/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	var addr string
	var primary string
	var backupsStr string
	var epoch uint64
	var wq uint
	var timeout time.Duration
	var bootstrap bool

	flag.StringVar(&addr, "addr", "127.0.0.1:8000", "gateway listen address")
	flag.StringVar(&primary, "primary", "127.0.0.1:7001", "primary storagenode")
	flag.StringVar(&backupsStr, "backups", "127.0.0.1:7002,127.0.0.1:7003", "backup storagenodes")
	flag.Uint64Var(&epoch, "epoch", 1, "initial epoch")
	flag.UintVar(&wq, "wq", 2, "write quorum W (include primary)")
	flag.DurationVar(&timeout, "timeout", 2*time.Second, "rpc timeout")
	flag.BoolVar(&bootstrap, "bootstrap", true, "push initial config to nodes")
	flag.Parse()

	backups := splitNonEmpty(backupsStr)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	srv := gateway.NewServer(primary, backups, epoch, uint32(wq), timeout)
	if bootstrap {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_ = srv.Bootstrap(ctx) // best effort
		cancel()
	}

	gs := grpc.NewServer()
	pb.RegisterStorageNodeServer(gs, srv)
	reflection.Register(gs)

	log.Printf("gateway listening on %s primary=%s backups=%v epoch=%d wq=%d", addr, primary, backups, epoch, wq)

	go gs.Serve(lis)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Printf("shutdown...")
	gs.GracefulStop()
	_ = lis.Close()
}

func splitNonEmpty(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
