package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"dataplane/internal/ckv"
	"dataplane/internal/rpc/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

// ---------- idempotency cache (MVP: in-memory TTL) ----------

type idemEntry struct {
	ok   bool
	seq  uint64
	err  string
	when time.Time
}

type idemCache struct {
	mu    sync.Mutex
	data  map[string]idemEntry
	ttl   time.Duration
	limit int
}

func newIdemCache(ttl time.Duration, limit int) *idemCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if limit <= 0 {
		limit = 200000
	}
	return &idemCache{
		data:  make(map[string]idemEntry),
		ttl:   ttl,
		limit: limit,
	}
}

func (c *idemCache) get(reqID string) (idemEntry, bool) {
	if reqID == "" {
		return idemEntry{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.data[reqID]
	if !ok {
		return idemEntry{}, false
	}
	if time.Since(e.when) > c.ttl {
		delete(c.data, reqID)
		return idemEntry{}, false
	}
	return e, true
}

func (c *idemCache) put(reqID string, e idemEntry) {
	if reqID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// cheap eviction
	if len(c.data) >= c.limit {
		for k, v := range c.data {
			if time.Since(v.when) > c.ttl {
				delete(c.data, k)
				break
			}
		}
		if len(c.data) >= c.limit {
			for k := range c.data {
				delete(c.data, k)
				break
			}
		}
	}
	c.data[reqID] = e
}

// ---------- grpc conn pool (primary -> backups) ----------

type pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newPool() *pool { return &pool{conns: make(map[string]*grpc.ClientConn)} }

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

// ---------- server ----------

type server struct {
	pb.UnimplementedStorageNodeServer

	store *ckv.Store

	mu      sync.RWMutex
	role    pb.Role
	epoch   uint64
	seq     uint64
	backups []string
	wq      int

	timeout time.Duration
	pool    *pool
	idem    *idemCache
}

func (s *server) curEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epoch
}

func (s *server) isPrimary() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.role == pb.Role_ROLE_PRIMARY
}

func (s *server) isBackup() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.role == pb.Role_ROLE_BACKUP
}

func (s *server) applyEntry(e *pb.WriteEntry) error {
	switch e.Op {
	case pb.Op_OP_PUT:
		return s.store.Put(e.Key, e.Value)
	case pb.Op_OP_DEL:
		return s.store.Del(e.Key)
	default:
		return fmt.Errorf("unknown op: %v", e.Op)
	}
}

// ---------- Public API (for gateway / debug) ----------

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutReply, error) {
	// Public Put is allowed only on primary (debug). Real path: gateway->ClientWrite.
	if req == nil || req.Key == "" {
		return &pb.PutReply{Ok: false, Error: "empty key"}, nil
	}
	if !s.isPrimary() {
		return &pb.PutReply{Ok: false, Error: "not primary"}, nil
	}
	r, _ := s.ClientWrite(ctx, &pb.ClientWriteRequest{
		Entry: &pb.WriteEntry{
			Key:   req.Key,
			Value: req.Value,
			Op:    pb.Op_OP_PUT,
			Epoch: s.curEpoch(),
			ReqId: "manual",
		},
	})
	if r == nil || !r.Ok {
		msg := "put failed"
		if r != nil && r.Error != "" {
			msg = r.Error
		}
		return &pb.PutReply{Ok: false, Error: msg}, nil
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
	if !s.isPrimary() {
		return &pb.DelReply{Ok: false, Error: "not primary"}, nil
	}
	r, _ := s.ClientWrite(ctx, &pb.ClientWriteRequest{
		Entry: &pb.WriteEntry{
			Key:   req.Key,
			Op:    pb.Op_OP_DEL,
			Epoch: s.curEpoch(),
			ReqId: "manual",
		},
	})
	if r == nil || !r.Ok {
		msg := "del failed"
		if r != nil && r.Error != "" {
			msg = r.Error
		}
		return &pb.DelReply{Ok: false, Error: msg}, nil
	}
	return &pb.DelReply{Ok: true}, nil
}

func (s *server) Ping(ctx context.Context, _ *pb.PingRequest) (*pb.PingReply, error) {
	return &pb.PingReply{Ok: true}, nil
}

// ---------- Control-plane lite: Configure ----------

func (s *server) Configure(ctx context.Context, req *pb.ConfigureRequest) (*pb.ConfigureReply, error) {
	if req == nil {
		return &pb.ConfigureReply{Ok: false, Error: "nil request"}, nil
	}
	if req.Epoch == 0 {
		return &pb.ConfigureReply{Ok: false, Error: "epoch must be > 0"}, nil
	}
	if req.Role != pb.Role_ROLE_PRIMARY && req.Role != pb.Role_ROLE_BACKUP {
		return &pb.ConfigureReply{Ok: false, Error: "invalid role"}, nil
	}
	newWQ := int(req.Wq)
	if newWQ <= 0 {
		newWQ = 2
	}

	bs := make([]string, 0, len(req.Backups))
	for _, b := range req.Backups {
		b = strings.TrimSpace(b)
		if b != "" {
			bs = append(bs, b)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// epoch must be monotonic increasing (fence stale config)
	if req.Epoch < s.epoch {
		return &pb.ConfigureReply{Ok: false, Error: fmt.Sprintf("stale epoch: got=%d cur=%d", req.Epoch, s.epoch)}, nil
	}

	s.role = req.Role
	s.epoch = req.Epoch
	s.wq = newWQ
	if s.role == pb.Role_ROLE_PRIMARY {
		s.backups = bs
	} else {
		s.backups = nil
	}

	return &pb.ConfigureReply{Ok: true}, nil
}

// ---------- gateway -> primary: ClientWrite ----------

func (s *server) ClientWrite(ctx context.Context, req *pb.ClientWriteRequest) (*pb.ClientWriteReply, error) {
	if req == nil || req.Entry == nil {
		return &pb.ClientWriteReply{Ok: false, Error: "nil entry"}, nil
	}
	e := req.Entry
	if e.Key == "" {
		return &pb.ClientWriteReply{Ok: false, Error: "empty key"}, nil
	}
	if !s.isPrimary() {
		return &pb.ClientWriteReply{Ok: false, Error: "not primary"}, nil
	}

	curEpoch := s.curEpoch()
	if e.Epoch != curEpoch {
		return &pb.ClientWriteReply{Ok: false, Error: fmt.Sprintf("epoch mismatch: got=%d want=%d", e.Epoch, curEpoch)}, nil
	}

	// idempotency
	if cached, ok := s.idem.get(e.ReqId); ok {
		return &pb.ClientWriteReply{Ok: cached.ok, Error: cached.err, Seq: cached.seq}, nil
	}

	seq := atomic.AddUint64(&s.seq, 1)
	entry := &pb.WriteEntry{
		Key:   e.Key,
		Value: e.Value,
		Op:    e.Op,
		Epoch: curEpoch,
		Seq:   seq,
		ReqId: e.ReqId,
	}

	// apply locally first
	if err := s.applyEntry(entry); err != nil {
		s.idem.put(e.ReqId, idemEntry{ok: false, seq: seq, err: err.Error(), when: time.Now()})
		return &pb.ClientWriteReply{Ok: false, Error: err.Error(), Seq: seq}, nil
	}

	// replicate to backups
	s.mu.RLock()
	backups := append([]string{}, s.backups...)
	wq := s.wq
	s.mu.RUnlock()

	needBackupAcks := wq - 1
	if needBackupAcks < 0 {
		needBackupAcks = 0
	}
	if needBackupAcks == 0 || len(backups) == 0 {
		s.idem.put(e.ReqId, idemEntry{ok: true, seq: seq, when: time.Now()})
		return &pb.ClientWriteReply{Ok: true, Seq: seq}, nil
	}

	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var mu sync.Mutex
	acks := 0
	var firstErr error

	var wg sync.WaitGroup
	wg.Add(len(backups))
	for _, addr := range backups {
		a := strings.TrimSpace(addr)
		if a == "" {
			wg.Done()
			continue
		}
		go func(backupAddr string) {
			defer wg.Done()
			cli, err := s.pool.get(backupAddr)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			r, err := cli.Replicate(cctx, &pb.ReplicateRequest{Entry: entry})
			if err != nil || r == nil || !r.Ok {
				mu.Lock()
				if firstErr == nil {
					if err != nil {
						firstErr = err
					} else if r != nil && r.Error != "" {
						firstErr = fmt.Errorf(r.Error)
					} else {
						firstErr = fmt.Errorf("replicate failed: %s", backupAddr)
					}
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			acks++
			mu.Unlock()
		}(a)
	}
	wg.Wait()

	if acks >= needBackupAcks {
		s.idem.put(e.ReqId, idemEntry{ok: true, seq: seq, when: time.Now()})
		return &pb.ClientWriteReply{Ok: true, Seq: seq}, nil
	}

	if firstErr != nil {
		s.idem.put(e.ReqId, idemEntry{ok: false, seq: seq, err: firstErr.Error(), when: time.Now()})
		return &pb.ClientWriteReply{Ok: false, Error: firstErr.Error(), Seq: seq}, nil
	}

	s.idem.put(e.ReqId, idemEntry{ok: false, seq: seq, err: "write quorum not reached", when: time.Now()})
	return &pb.ClientWriteReply{Ok: false, Error: "write quorum not reached", Seq: seq}, nil
}

// ---------- primary -> backup: Replicate ----------

func (s *server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateReply, error) {
	if req == nil || req.Entry == nil {
		return &pb.ReplicateReply{Ok: false, Error: "nil entry"}, nil
	}
	e := req.Entry
	if e.Key == "" {
		return &pb.ReplicateReply{Ok: false, Error: "empty key"}, nil
	}
	if !s.isBackup() {
		return &pb.ReplicateReply{Ok: false, Error: "not backup"}, nil
	}

	curEpoch := s.curEpoch()
	if e.Epoch != curEpoch {
		return &pb.ReplicateReply{Ok: false, Error: fmt.Sprintf("epoch mismatch: got=%d want=%d", e.Epoch, curEpoch)}, nil
	}

	// idempotency
	if cached, ok := s.idem.get(e.ReqId); ok {
		if cached.ok {
			return &pb.ReplicateReply{Ok: true}, nil
		}
		return &pb.ReplicateReply{Ok: false, Error: cached.err}, nil
	}

	if err := s.applyEntry(e); err != nil {
		s.idem.put(e.ReqId, idemEntry{ok: false, seq: e.Seq, err: err.Error(), when: time.Now()})
		return &pb.ReplicateReply{Ok: false, Error: err.Error()}, nil
	}

	s.idem.put(e.ReqId, idemEntry{ok: true, seq: e.Seq, when: time.Now()})
	return &pb.ReplicateReply{Ok: true}, nil
}

func main() {
	var addr, img string
	var size uint64

	var roleStr string
	var epoch uint64
	var backupsStr string
	var wq int
	var timeout time.Duration
	var idemTTL time.Duration
	var idemLimit int

	flag.StringVar(&addr, "addr", "127.0.0.1:7001", "listen address")
	flag.StringVar(&img, "img", "node1.img", "disk image")
	flag.Uint64Var(&size, "size", 64*1024*1024, "device size")

	flag.StringVar(&roleStr, "role", "backup", "role: primary|backup")
	flag.Uint64Var(&epoch, "epoch", 1, "replica-group epoch")
	flag.StringVar(&backupsStr, "backups", "", "(primary only) backups, comma-separated")
	flag.IntVar(&wq, "wq", 2, "write quorum W (include primary)")
	flag.DurationVar(&timeout, "timeout", 2*time.Second, "rpc timeout")
	flag.DurationVar(&idemTTL, "idem_ttl", 5*time.Minute, "idempotency ttl")
	flag.IntVar(&idemLimit, "idem_limit", 200000, "idempotency limit")
	flag.Parse()

	roleStr = strings.ToLower(strings.TrimSpace(roleStr))
	role := pb.Role_ROLE_UNSPEC
	switch roleStr {
	case "primary":
		role = pb.Role_ROLE_PRIMARY
	case "backup":
		role = pb.Role_ROLE_BACKUP
	default:
		log.Fatalf("invalid role: %s", roleStr)
	}
	if epoch == 0 {
		log.Fatalf("epoch must be > 0")
	}

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

	srv := &server{
		store:   store,
		role:    role,
		epoch:   epoch,
		seq:     0,
		backups: splitNonEmpty(backupsStr),
		wq:      wq,
		timeout: timeout,
		pool:    newPool(),
		idem:    newIdemCache(idemTTL, idemLimit),
	}

	pb.RegisterStorageNodeServer(gs, srv)
	reflection.Register(gs)

	log.Printf("storagenode role=%s epoch=%d listen=%s backups=%v wq=%d", roleStr, epoch, addr, srv.backups, wq)

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
