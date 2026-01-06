package main

import (
	"flag"
	"log"
	"net"

	"dataplane/internal/metacoordinator"
	pbmeta "dataplane/internal/rpc/pbmeta"

	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9100", "listen address for meta-coordinator")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	gs := grpc.NewServer()
	pbmeta.RegisterMetaCoordinatorServer(gs, metacoordinator.NewServer())

	log.Printf("meta-coordinator listening on %s", *addr)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
