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
	pb "dataplane/internal/rpc/pb"

	"google.golang.org/grpc"
)

func splitNonEmpty(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
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

func main() {
	var (
		addr       string
		metaAddr   string
		numShards  int
		primary    string
		backupsStr string
		epoch      uint64
		wq         uint
		timeout    time.Duration
		bootstrap  bool
	)

	flag.StringVar(&addr, "addr", "127.0.0.1:8000", "gateway listen addr")
	flag.StringVar(&metaAddr, "meta", "127.0.0.1:9100", "meta-coordinator addr")
	flag.IntVar(&numShards, "shards", 1, "number of shards")
	flag.StringVar(&primary, "primary", "", "initial primary addr (for bootstrap)")
	flag.StringVar(&backupsStr, "backups", "", "initial backups addrs, comma-separated (for bootstrap)")
	flag.Uint64Var(&epoch, "epoch", 1, "initial epoch (for bootstrap)")
	flag.UintVar(&wq, "wq", 2, "write quorum W (include primary)")
	flag.DurationVar(&timeout, "timeout", 2*time.Second, "rpc timeout")
	flag.BoolVar(&bootstrap, "bootstrap", true, "bootstrap meta-coordinator")
	flag.Parse()

	backups := splitNonEmpty(backupsStr)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	srv := gateway.NewServer(numShards, metaAddr, timeout)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if bootstrap {
		if primary == "" {
			log.Fatalf("-primary required when -bootstrap=true")
		}
		for sh := 0; sh < numShards; sh++ {
			if err := srv.BootstrapShard(ctx, sh, epoch, primary, backups, uint32(wq)); err != nil {
				log.Fatalf("bootstrap shard %d: %v", sh, err)
			}
		}
	} else {
		if err := srv.RefreshAll(ctx); err != nil {
			log.Printf("refresh all from meta: %v", err)
		}
	}

	gs := grpc.NewServer()
	pb.RegisterStorageNodeServer(gs, srv)

	go func() {
		log.Printf("gateway listening on %s (meta=%s shards=%d)", addr, metaAddr, numShards)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("serve failed: %v", err)
		}
	}()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	<-sigC
	log.Printf("gateway shutting down")
	gs.GracefulStop()
}
