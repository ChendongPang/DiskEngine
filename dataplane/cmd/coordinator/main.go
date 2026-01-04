package main

import (
    "context"
    "crypto/sha1"
    "encoding/hex"
    "flag"
    "fmt"
    "log"
    "strings"
    "sync"
    "time"

    "dataplane/internal/rpc/pb"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func pickReplicas(key string, nodes []string, rf int) []string {
    if rf >= len(nodes) {
        return append([]string{}, nodes...)
    }
    h := sha1.Sum([]byte(key))
    start := int(h[0]) % len(nodes)

    out := make([]string, 0, rf)
    for i := 0; i < rf; i++ {
        out = append(out, nodes[(start+i)%len(nodes)])
    }
    return out
}

type pool struct {
    mu    sync.Mutex
    conns map[string]*grpc.ClientConn
}

func newPool() *pool {
    return &pool{conns: make(map[string]*grpc.ClientConn)}
}

func (p *pool) get(addr string) (pb.StorageNodeClient, error) {
    p.mu.Lock()
    defer p.mu.Unlock()

    if cc, ok := p.conns[addr]; ok {
        return pb.NewStorageNodeClient(cc), nil
    }
    cc, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    p.conns[addr] = cc
    return pb.NewStorageNodeClient(cc), nil
}

func main() {
    var nodesStr string
    var rf int
    flag.StringVar(&nodesStr, "nodes", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "nodes")
    flag.IntVar(&rf, "rf", 2, "replication factor")
    flag.Parse()

    args := flag.Args()
    if len(args) < 2 {
        fmt.Println("usage: coordinator put|get|del <key> [value]")
        return
    }
    op, key := args[0], args[1]
    value := ""
    if op == "put" {
        value = args[2]
    }

    nodes := strings.Split(nodesStr, ",")
    reps := pickReplicas(key, nodes, rf)

    tidb := sha1.Sum([]byte(fmt.Sprintf("%s-%d", key, time.Now().UnixNano())))
    tid := hex.EncodeToString(tidb[:4])
    log.Printf("[tid=%s] %s %s replicas=%v", tid, op, key, reps)

    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    p := newPool()

    switch op {
    case "put":
        fanoutPut(ctx, p, reps, key, []byte(value))
    case "del":
        fanoutDel(ctx, p, reps, key)
    case "get":
        routedGet(ctx, p, reps, key)
    }
}

func fanoutPut(ctx context.Context, p *pool, reps []string, key string, val []byte) {
    var wg sync.WaitGroup
    errc := make(chan error, len(reps))

    for _, a := range reps {
        wg.Add(1)
        go func(addr string) {
            defer wg.Done()
            cli, err := p.get(addr)
            if err != nil {
                errc <- err
                return
            }
            r, err := cli.Put(ctx, &pb.PutRequest{Key: key, Value: val})
            if err != nil || !r.Ok {
                errc <- fmt.Errorf("put %s failed", addr)
            }
        }(a)
    }

    wg.Wait()
    close(errc)

    if len(errc) > 0 {
        log.Printf("PUT FAILED")
        return
    }
    log.Printf("PUT OK")
}

func fanoutDel(ctx context.Context, p *pool, reps []string, key string) {
    var wg sync.WaitGroup
    errc := make(chan error, len(reps))

    for _, a := range reps {
        wg.Add(1)
        go func(addr string) {
            defer wg.Done()
            cli, err := p.get(addr)
            if err != nil {
                errc <- err
                return
            }
            r, err := cli.Del(ctx, &pb.DelRequest{Key: key})
            if err != nil || !r.Ok {
                errc <- fmt.Errorf("del %s failed", addr)
            }
        }(a)
    }

    wg.Wait()
    close(errc)

    if len(errc) > 0 {
        log.Printf("DEL FAILED")
        return
    }
    log.Printf("DEL OK")
}

func routedGet(ctx context.Context, p *pool, reps []string, key string) {
    for _, a := range reps {
        cli, err := p.get(a)
        if err != nil {
            continue
        }
        r, err := cli.Get(ctx, &pb.GetRequest{Key: key})
        if err != nil {
            continue
        }
        if !r.Found {
            log.Printf("GET %s -> not found", a)
            return
        }
        log.Printf("GET %s -> %q", a, string(r.Value))
        return
    }
    log.Printf("GET FAILED")
}
