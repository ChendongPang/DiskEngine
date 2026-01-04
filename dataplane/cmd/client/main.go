package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"dataplane/internal/rpc/pb"

	"google.golang.org/grpc"
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  client [flags] <command> [args...]

commands:
  ping
  put <key> <value>
  get <key>
  del <key>

flags:
`)
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8000", "gateway address")
	timeout := flag.Duration("timeout", 3*time.Second, "rpc timeout")
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
	}

	cmd := flag.Arg(0)

	conn, err := grpc.Dial(
		*addr,
		grpc.WithInsecure(), // plaintext, no TLS
		grpc.WithBlock(),
		grpc.WithTimeout(*timeout),
	)
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	cli := pb.NewStorageNodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch cmd {

	case "ping":
		resp, err := cli.Ping(ctx, &pb.PingRequest{})
		if err != nil {
			log.Fatalf("ping failed: %v", err)
		}
		fmt.Printf("ping ok=%v\n", resp.Ok)

	case "put":
		if flag.NArg() != 3 {
			usage()
		}
		key := flag.Arg(1)
		val := []byte(flag.Arg(2))

		resp, err := cli.Put(ctx, &pb.PutRequest{
			Key:   key,
			Value: val,
		})
		if err != nil {
			log.Fatalf("put failed: %v", err)
		}
		if !resp.Ok {
			log.Fatalf("put error: %s", resp.Error)
		}
		fmt.Println("put ok")

	case "get":
		if flag.NArg() != 2 {
			usage()
		}
		key := flag.Arg(1)

		resp, err := cli.Get(ctx, &pb.GetRequest{
			Key: key,
		})
		if err != nil {
			log.Fatalf("get failed: %v", err)
		}
		if !resp.Found {
			fmt.Println("not found")
			return
		}
		fmt.Printf("value=%s\n", string(resp.Value))

	case "del":
		if flag.NArg() != 2 {
			usage()
		}
		key := flag.Arg(1)

		resp, err := cli.Del(ctx, &pb.DelRequest{
			Key: key,
		})
		if err != nil {
			log.Fatalf("del failed: %v", err)
		}
		if !resp.Ok {
			log.Fatalf("del error: %s", resp.Error)
		}
		fmt.Println("del ok")

	default:
		usage()
	}
}
