package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dataplane/internal/ckv"
	"dataplane/internal/rpc/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	pb.UnimplementedStorageNodeServer
	store *ckv.Store
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutReply, error) {
	if req == nil || req.Key == "" {
		return &pb.PutReply{Ok: false, Error: "empty key"}, nil
	}
	if err := s.store.Put(req.Key, req.Value); err != nil {
		return &pb.PutReply{Ok: false, Error: err.Error()}, nil
	}
	return &pb.PutReply{Ok: true}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	if req == nil || req.Key == "" {
		return &pb.GetReply{Found: false, Error: "empty key"}, nil
	}
	v, found, err := s.store.Get(req.Key)
	if err != nil {
		return &pb.GetReply{Found: false, Error: err.Error()}, nil
	}
	if !found {
		return &pb.GetReply{Found: false}, nil
	}
	return &pb.GetReply{Found: true, Value: v}, nil
}

func (s *server) Del(ctx context.Context, req *pb.DelRequest) (*pb.DelReply, error) {
	if req == nil || req.Key == "" {
		return &pb.DelReply{Ok: false, Error: "empty key"}, nil
	}
	if err := s.store.Del(req.Key); err != nil {
		return &pb.DelReply{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DelReply{Ok: true}, nil
}

func (s *server) Ping(ctx context.Context, _ *pb.PingRequest) (*pb.PingReply, error) {
	return &pb.PingReply{Ok: true}, nil
}

func main() {
	var addr, img string
	var size uint64

	flag.StringVar(&addr, "addr", "127.0.0.1:7001", "listen address")
	flag.StringVar(&img, "img", "node1.img", "disk image")
	flag.Uint64Var(&size, "size", 64*1024*1024, "device size")
	flag.Parse()

	store, err := ckv.Open(img, size)
	if err != nil {
		log.Fatalf("kv_open failed: %v", err)
	}
	defer store.Close()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	gs := grpc.NewServer()
	pb.RegisterStorageNodeServer(gs, &server{store: store})

	// ✅ 关键：开启 gRPC reflection，grpcurl 才能 list/describe
	reflection.Register(gs)

	log.Printf("storagenode listening on %s", addr)

	go gs.Serve(lis)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Printf("shutdown...")
	done := make(chan struct{})
	go func() {
		gs.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		gs.Stop()
	}

	_ = lis.Close()
	fmt.Println("bye")
}
