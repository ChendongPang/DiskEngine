package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"dataplane/internal/rpc/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type connPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newConnPool() *connPool { return &connPool{conns: make(map[string]*grpc.ClientConn)} }

func (p *connPool) get(addr string) (pb.StorageNodeClient, error) {
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

// Server is a stateful router (control-plane lite) that:
// - routes writes to primary only
// - uses req_id for idempotency
// - promotes a backup on primary failure (epoch++) and pushes Configure()
type Server struct {
	pb.UnimplementedStorageNodeServer

	mu sync.RWMutex

	primary string
	backups []string
	epoch   uint64
	wq      uint32

	timeout time.Duration
	pool    *connPool

	failMu sync.Mutex
}

func NewServer(primary string, backups []string, epoch uint64, wq uint32, timeout time.Duration) *Server {
	primary = strings.TrimSpace(primary)
	bs := make([]string, 0, len(backups))
	for _, b := range backups {
		b = strings.TrimSpace(b)
		if b != "" {
			bs = append(bs, b)
		}
	}
	if epoch == 0 {
		epoch = 1
	}
	if wq == 0 {
		wq = 2
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Server{
		primary: primary,
		backups: bs,
		epoch:   epoch,
		wq:      wq,
		timeout: timeout,
		pool:    newConnPool(),
	}
}

func (s *Server) snapshot() (primary string, backups []string, epoch uint64, wq uint32) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.primary, append([]string{}, s.backups...), s.epoch, s.wq
}

// Bootstrap pushes initial config to nodes (best effort).
func (s *Server) Bootstrap(ctx context.Context) error {
	primary, backups, epoch, wq := s.snapshot()
	if primary == "" {
		return errors.New("empty primary")
	}
	if err := s.configureNode(ctx, primary, pb.Role_ROLE_PRIMARY, epoch, backups, wq); err != nil {
		return err
	}
	for _, b := range backups {
		_ = s.configureNode(ctx, b, pb.Role_ROLE_BACKUP, epoch, nil, wq)
	}
	return nil
}

// -------- public api exposed to clients --------

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutReply, error) {
	if req == nil || req.Key == "" {
		return &pb.PutReply{Ok: false, Error: "empty key"}, nil
	}
	entry := &pb.WriteEntry{
		Key:   req.Key,
		Value: req.Value,
		Op:    pb.Op_OP_PUT,
		ReqId: newReqID(),
	}
	ok, errStr := s.writeWithFailover(ctx, entry)
	if !ok {
		return &pb.PutReply{Ok: false, Error: errStr}, nil
	}
	return &pb.PutReply{Ok: true}, nil
}

func (s *Server) Del(ctx context.Context, req *pb.DelRequest) (*pb.DelReply, error) {
	if req == nil || req.Key == "" {
		return &pb.DelReply{Ok: false, Error: "empty key"}, nil
	}
	entry := &pb.WriteEntry{
		Key:   req.Key,
		Op:    pb.Op_OP_DEL,
		ReqId: newReqID(),
	}
	ok, errStr := s.writeWithFailover(ctx, entry)
	if !ok {
		return &pb.DelReply{Ok: false, Error: errStr}, nil
	}
	return &pb.DelReply{Ok: true}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	if req == nil || req.Key == "" {
		return &pb.GetReply{Found: false, Error: "empty key"}, nil
	}
	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	primary, backups, _, _ := s.snapshot()
	addrs := append([]string{primary}, backups...)
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		cli, err := s.pool.get(a)
		if err != nil {
			continue
		}
		r, err := cli.Get(cctx, &pb.GetRequest{Key: req.Key})
		if err != nil || r == nil {
			continue
		}
		if !r.Found {
			return &pb.GetReply{Found: false}, nil
		}
		return &pb.GetReply{Found: true, Value: r.Value}, nil
	}
	return &pb.GetReply{Found: false, Error: "get failed"}, nil
}

func (s *Server) Ping(ctx context.Context, _ *pb.PingRequest) (*pb.PingReply, error) {
	return &pb.PingReply{Ok: true}, nil
}

// -------- write path: primary only + failover --------

func (s *Server) writeWithFailover(ctx context.Context, entry *pb.WriteEntry) (bool, string) {
	if ok, errStr := s.writeOnce(ctx, entry); ok {
		return true, ""
	} else {
		if !s.tryFailover(ctx) {
			return false, errStr
		}
		if ok2, errStr2 := s.writeOnce(ctx, entry); ok2 {
			return true, ""
		} else {
			return false, errStr2
		}
	}
}

func (s *Server) writeOnce(ctx context.Context, entry *pb.WriteEntry) (bool, string) {
	primary, _, epoch, _ := s.snapshot()
	if primary == "" {
		return false, "no primary"
	}
	cli, err := s.pool.get(primary)
	if err != nil {
		return false, err.Error()
	}
	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	entry.Epoch = epoch
	r, err := cli.ClientWrite(cctx, &pb.ClientWriteRequest{Entry: entry})
	if err != nil {
		return false, err.Error()
	}
	if r == nil || !r.Ok {
		msg := "write failed"
		if r != nil && r.Error != "" {
			msg = r.Error
		}
		return false, msg
	}
	return true, ""
}

// Assumes single Gateway instance (MVP). Multi-gateway needs real metadata/raft.
func (s *Server) tryFailover(ctx context.Context) bool {
	s.failMu.Lock()
	defer s.failMu.Unlock()

	primary, backups, epoch, wq := s.snapshot()
	if len(backups) == 0 {
		return false
	}

	// choose first healthy backup
	var newPrimary string
	for _, b := range backups {
		b = strings.TrimSpace(b)
		if b == "" {
			continue
		}
		cli, err := s.pool.get(b)
		if err != nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, s.timeout)
		r, err := cli.Ping(cctx, &pb.PingRequest{})
		cancel()
		if err == nil && r != nil && r.Ok {
			newPrimary = b
			break
		}
	}
	if newPrimary == "" {
		return false
	}

	newEpoch := epoch + 1

	// newBackups: include old primary (best effort) + remaining backups excluding newPrimary
	newBackups := make([]string, 0, len(backups)+1)
	if strings.TrimSpace(primary) != "" && strings.TrimSpace(primary) != strings.TrimSpace(newPrimary) {
		newBackups = append(newBackups, strings.TrimSpace(primary))
	}
	for _, b := range backups {
		b = strings.TrimSpace(b)
		if b == "" || b == strings.TrimSpace(newPrimary) || b == strings.TrimSpace(primary) {
			continue
		}
		newBackups = append(newBackups, b)
	}

	// push config
	if err := s.configureNode(ctx, newPrimary, pb.Role_ROLE_PRIMARY, newEpoch, newBackups, wq); err != nil {
		return false
	}
	for _, b := range newBackups {
		_ = s.configureNode(ctx, b, pb.Role_ROLE_BACKUP, newEpoch, nil, wq)
	}

	s.mu.Lock()
	s.primary = newPrimary
	s.backups = newBackups
	s.epoch = newEpoch
	s.mu.Unlock()

	return true
}

func (s *Server) configureNode(ctx context.Context, addr string, role pb.Role, epoch uint64, backups []string, wq uint32) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return errors.New("empty addr")
	}
	cli, err := s.pool.get(addr)
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	_, err = cli.Configure(cctx, &pb.ConfigureRequest{
		Role:    role,
		Epoch:   epoch,
		Backups: backups,
		Wq:      wq,
	})
	return err
}

func newReqID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
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
