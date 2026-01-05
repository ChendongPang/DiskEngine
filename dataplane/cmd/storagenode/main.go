package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"dataplane/internal/ckv"
	"dataplane/internal/oplog"
	pb "dataplane/internal/rpc/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// -----------------------------
// Simple idempotency cache
// -----------------------------
type idemCache struct {
	mu sync.Mutex
	m  map[string]*pb.ClientWriteReply
}

func newIdem() *idemCache { return &idemCache{m: make(map[string]*pb.ClientWriteReply)} }

func (c *idemCache) get(k string) (*pb.ClientWriteReply, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[k]
	return v, ok
}

func (c *idemCache) put(k string, v *pb.ClientWriteReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[k] = v
}

// -----------------------------
// StorageNode server
// -----------------------------
type server struct {
	pb.UnimplementedStorageNodeServer

	mu   sync.Mutex
	cond *sync.Cond

	addr string

	role  pb.NodeRole
	epoch uint64

	primary string
	backups []string
	wq      uint32

	seq          uint64
	maxCommitted uint64

	// prepared entries
	prepared map[uint64]*pb.Operation

	// commit can arrive out-of-order; buffer then drain in-order.
	pendingCommit map[uint64]*pb.Operation

	oplogPath string
	ol        *oplog.Oplog
	store     *ckv.Store

	idem *idemCache

	repairCh chan struct{}
}

func newServer(addr string, store *ckv.Store, oplogPath string) (*server, error) {
	ol, err := oplog.Open(oplogPath)
	if err != nil {
		return nil, err
	}
	s := &server{
		addr:          addr,
		role:          pb.NodeRole_ROLE_UNKNOWN,
		epoch:         0,
		primary:       "",
		backups:       nil,
		wq:            0,
		seq:           0,
		maxCommitted:  0,
		prepared:      make(map[uint64]*pb.Operation),
		pendingCommit: make(map[uint64]*pb.Operation),
		oplogPath:     oplogPath,
		ol:            ol,
		store:         store,
		idem:          newIdem(),
		repairCh:      make(chan struct{}, 1),
	}
	s.cond = sync.NewCond(&s.mu)

	if err := s.replayOplog(); err != nil {
		return nil, err
	}
	go s.repairLoop()
	return s, nil
}

type replayHandler struct{ s *server }

func (s *server) replayOplog() error {
	return oplog.Replay(s.oplogPath, &replayHandler{s: s})
}

func (h *replayHandler) OnPrepare(epoch uint64, seq uint64, op *pb.Operation) error {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()

	if seq > h.s.seq {
		h.s.seq = seq
	}
	if epoch > h.s.epoch {
		h.s.epoch = epoch
	}
	h.s.prepared[seq] = op
	return nil
}

func (h *replayHandler) OnCommit(epoch uint64, seq uint64) error {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()

	if seq > h.s.maxCommitted {
		h.s.maxCommitted = seq
	}
	if seq > h.s.seq {
		h.s.seq = seq
	}
	if epoch > h.s.epoch {
		h.s.epoch = epoch
	}
	return nil
}

// -----------------------------
// RPCs
// -----------------------------
func (s *server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.PingReply{
		Ok:           true,
		Role:         s.role,
		Epoch:        s.epoch,
		MaxCommitted: s.maxCommitted,
	}, nil
}

func (s *server) Configure(ctx context.Context, req *pb.ConfigureRequest) (*pb.ConfigureReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.GetEpoch() < s.epoch {
		return &pb.ConfigureReply{Ok: false, Err: "stale epoch"}, nil
	}

	oldRole := s.role
	s.epoch = req.GetEpoch()
	s.role = req.GetRole()
	s.primary = req.GetPrimaryAddr()
	s.backups = append([]string{}, req.GetBackupAddrs()...)
	s.wq = req.GetWq()

	log.Printf("[node %s] Configure: role=%s epoch=%d primary=%s backups=%v wq=%d (oldRole=%s)",
		s.addr, s.role.String(), s.epoch, s.primary, s.backups, s.wq, oldRole.String())

	if s.role == pb.NodeRole_ROLE_PRIMARY && oldRole != pb.NodeRole_ROLE_PRIMARY {
		select {
		case s.repairCh <- struct{}{}:
		default:
		}
	}
	return &pb.ConfigureReply{Ok: true}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	v, found, err := s.store.Get(req.GetKey())
	if err != nil {
		return &pb.GetReply{Ok: false, Err: err.Error()}, nil
	}
	if !found {
		return &pb.GetReply{Ok: true, Found: false}, nil
	}
	return &pb.GetReply{Ok: true, Found: true, Value: v}, nil
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutReply, error) {
	return &pb.PutReply{Ok: false, Err: "use gateway (ClientWrite)"}, nil
}

func (s *server) Del(ctx context.Context, req *pb.DelRequest) (*pb.DelReply, error) {
	return &pb.DelReply{Ok: false, Err: "use gateway (ClientWrite)"}, nil
}

func (s *server) ClientWrite(ctx context.Context, req *pb.ClientWriteRequest) (*pb.ClientWriteReply, error) {
	if v, ok := s.idem.get(req.GetReqId()); ok {
		return v, nil
	}

	s.mu.Lock()
	if s.role != pb.NodeRole_ROLE_PRIMARY {
		s.mu.Unlock()
		r := &pb.ClientWriteReply{Ok: false, Err: "not primary"}
		s.idem.put(req.GetReqId(), r)
		return r, nil
	}
	if req.GetEpoch() != s.epoch {
		s.mu.Unlock()
		r := &pb.ClientWriteReply{Ok: false, Err: "wrong epoch"}
		s.idem.put(req.GetReqId(), r)
		return r, nil
	}

	s.seq++
	seq := s.seq
	epoch := s.epoch
	op := req.GetOp()
	backups := append([]string{}, s.backups...)
	wq := s.wq
	s.prepared[seq] = op
	s.mu.Unlock()

	// local PREPARE durable
	if err := s.ol.AppendPrepare(epoch, seq, req.GetReqId(), op); err != nil {
		r := &pb.ClientWriteReply{Ok: false, Err: "oplog prepare: " + err.Error()}
		s.idem.put(req.GetReqId(), r)
		return r, nil
	}

	// PREPARE quorum (wq-1)
	needPrepare := int(wq) - 1
	if needPrepare < 0 {
		needPrepare = 0
	}
	prepareAcks := 0
	var wg sync.WaitGroup
	var pmu sync.Mutex
	var firstPrepareErr string

	for _, b := range backups {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ok, errStr := s.replicateOnce(addr, &pb.ReplicateRequest{
				Epoch: epoch,
				Phase: pb.ReplicationPhase_PHASE_PREPARE,
				Op:    op,
				ReqId: req.GetReqId(),
				Seq:   seq,
			})
			if ok {
				pmu.Lock()
				prepareAcks++
				pmu.Unlock()
				return
			}
			pmu.Lock()
			if firstPrepareErr == "" {
				firstPrepareErr = errStr
			}
			pmu.Unlock()
		}(b)
	}
	wg.Wait()

	if prepareAcks < needPrepare {
		r := &pb.ClientWriteReply{Ok: false, Err: fmt.Sprintf("prepare quorum not reached: acks=%d need=%d err=%s", prepareAcks, needPrepare, firstPrepareErr)}
		s.idem.put(req.GetReqId(), r)
		return r, nil
	}

	// local COMMIT durable
	if err := s.ol.AppendCommit(epoch, seq, req.GetReqId()); err != nil {
		r := &pb.ClientWriteReply{Ok: false, Err: "oplog commit: " + err.Error()}
		s.idem.put(req.GetReqId(), r)
		return r, nil
	}

	// local APPLY
	if err := applyOp(s.store, op); err != nil {
		r := &pb.ClientWriteReply{Ok: false, Err: "apply: " + err.Error()}
		s.idem.put(req.GetReqId(), r)
		return r, nil
	}

	// advance local committed
	s.mu.Lock()
	if seq > s.maxCommitted {
		s.maxCommitted = seq
		s.cond.Broadcast()
	}
	s.mu.Unlock()

	// -------- NEW: COMMIT quorum ack (wq-1) --------
	needCommit := int(wq) - 1
	if needCommit < 0 {
		needCommit = 0
	}
	commitAcks := 0
	var cwg sync.WaitGroup
	var cmu sync.Mutex
	var firstCommitErr string

	for _, b := range backups {
		cwg.Add(1)
		go func(addr string) {
			defer cwg.Done()
			// commit carries full entry; backup can fill missing prepare.
			ok, errStr := s.replicateOnce(addr, &pb.ReplicateRequest{
				Epoch: epoch,
				Phase: pb.ReplicationPhase_PHASE_COMMIT,
				Op:    op,
				ReqId: req.GetReqId(),
				Seq:   seq,
			})
			if ok {
				cmu.Lock()
				commitAcks++
				cmu.Unlock()
				return
			}
			cmu.Lock()
			if firstCommitErr == "" {
				firstCommitErr = errStr
			}
			cmu.Unlock()
		}(b)
	}
	cwg.Wait()

	if commitAcks < needCommit {
		r := &pb.ClientWriteReply{Ok: false, Err: fmt.Sprintf("commit quorum not reached: acks=%d need=%d err=%s", commitAcks, needCommit, firstCommitErr)}
		s.idem.put(req.GetReqId(), r)
		return r, nil
	}

	r := &pb.ClientWriteReply{Ok: true}
	s.idem.put(req.GetReqId(), r)
	return r, nil
}

func (s *server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateReply, error) {
	s.mu.Lock()
	if s.role != pb.NodeRole_ROLE_BACKUP {
		mc := s.maxCommitted
		s.mu.Unlock()
		return &pb.ReplicateReply{Ok: false, Err: "not backup", MaxCommitted: mc}, nil
	}
	if req.GetEpoch() != s.epoch {
		mc := s.maxCommitted
		s.mu.Unlock()
		return &pb.ReplicateReply{Ok: false, Err: "wrong epoch", MaxCommitted: mc}, nil
	}
	s.mu.Unlock()

	switch req.GetPhase() {
	case pb.ReplicationPhase_PHASE_PREPARE:
		seq := req.GetSeq()
		op := req.GetOp()

		s.mu.Lock()
		_, exists := s.prepared[seq]
		mc := s.maxCommitted
		s.mu.Unlock()

		if exists || seq <= mc {
			return &pb.ReplicateReply{Ok: true, MaxCommitted: mc}, nil
		}
		if err := s.ol.AppendPrepare(req.GetEpoch(), seq, req.GetReqId(), op); err != nil {
			return &pb.ReplicateReply{Ok: false, Err: "oplog prepare: " + err.Error(), MaxCommitted: s.getMaxCommitted()}, nil
		}
		s.mu.Lock()
		s.prepared[seq] = op
		s.mu.Unlock()

		_ = s.drainCommitsInOrder(req.GetEpoch())
		return &pb.ReplicateReply{Ok: true, MaxCommitted: s.getMaxCommitted()}, nil

	case pb.ReplicationPhase_PHASE_COMMIT:
		seq := req.GetSeq()
		op := req.GetOp()

		s.mu.Lock()
		if seq <= s.maxCommitted {
			mc := s.maxCommitted
			s.mu.Unlock()
			return &pb.ReplicateReply{Ok: true, MaxCommitted: mc}, nil
		}
		s.pendingCommit[seq] = op
		s.mu.Unlock()

		_ = s.drainCommitsInOrder(req.GetEpoch())

		// IMPORTANT: only return OK after THIS seq is committed (durable+apply).
		if err := s.waitCommitted(ctx, seq); err != nil {
			return &pb.ReplicateReply{Ok: false, Err: err.Error(), MaxCommitted: s.getMaxCommitted()}, nil
		}
		return &pb.ReplicateReply{Ok: true, MaxCommitted: s.getMaxCommitted()}, nil

	default:
		return &pb.ReplicateReply{Ok: false, Err: "bad phase", MaxCommitted: s.getMaxCommitted()}, nil
	}
}

func (s *server) getMaxCommitted() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxCommitted
}

func (s *server) waitCommitted(ctx context.Context, seq uint64) error {
	deadline, hasDL := ctx.Deadline()
	for {
		s.mu.Lock()
		mc := s.maxCommitted
		s.mu.Unlock()
		if mc >= seq {
			return nil
		}
		if hasDL && time.Now().After(deadline) {
			return fmt.Errorf("commit wait timeout for seq=%d (mc=%d)", seq, mc)
		}
		time.Sleep(5 * time.Millisecond)
		_ = s.drainCommitsInOrder(0)
	}
}

func (s *server) drainCommitsInOrder(epoch uint64) error {
	for {
		s.mu.Lock()
		next := s.maxCommitted + 1
		opCommit, ok := s.pendingCommit[next]
		s.mu.Unlock()
		if !ok {
			return nil
		}

		s.mu.Lock()
		opPrep, hasPrep := s.prepared[next]
		s.mu.Unlock()
		if !hasPrep {
			if err := s.ol.AppendPrepare(epoch, next, "fill", opCommit); err != nil {
				return fmt.Errorf("fill prepare: %w", err)
			}
			s.mu.Lock()
			s.prepared[next] = opCommit
			opPrep = opCommit
			s.mu.Unlock()
		}

		if err := s.ol.AppendCommit(epoch, next, "commit"); err != nil {
			return fmt.Errorf("append commit: %w", err)
		}
		if err := applyOp(s.store, opPrep); err != nil {
			return fmt.Errorf("apply: %w", err)
		}

		s.mu.Lock()
		delete(s.pendingCommit, next)
		delete(s.prepared, next)
		s.maxCommitted = next
		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

// -----------------------------
// Primary repair loop (沿用你已有逻辑：promote 后把落后 backup 追齐)
// -----------------------------
func (s *server) repairLoop() {
	for range s.repairCh {
		time.Sleep(50 * time.Millisecond)
		s.repairBackups()
	}
}

func (s *server) repairBackups() {
	s.mu.Lock()
	if s.role != pb.NodeRole_ROLE_PRIMARY {
		s.mu.Unlock()
		return
	}
	epoch := s.epoch
	backups := append([]string{}, s.backups...)
	primaryMax := s.maxCommitted
	oplogPath := s.oplogPath
	s.mu.Unlock()

	if len(backups) == 0 {
		return
	}

	for _, b := range backups {
		mc, err := s.getBackupCommitted(b)
		if err != nil {
			log.Printf("[node %s] repair: ping backup %s err=%v", s.addr, b, err)
			continue
		}
		if mc >= primaryMax {
			continue
		}
		log.Printf("[node %s] repair: backup %s lagging mc=%d primary=%d", s.addr, b, mc, primaryMax)

		for seq := mc + 1; seq <= primaryMax; seq++ {
			op, _, err := oplog.ReadPrepareBySeq(oplogPath, seq)
			if err != nil {
				log.Printf("[node %s] repair: read prepare seq=%d err=%v", s.addr, seq, err)
				break
			}
			ok, errStr := s.replicateOnce(b, &pb.ReplicateRequest{
				Epoch: epoch,
				Phase: pb.ReplicationPhase_PHASE_COMMIT,
				Op:    op,
				ReqId: fmt.Sprintf("repair-%d", seq),
				Seq:   seq,
			})
			if !ok {
				log.Printf("[node %s] repair: send commit to %s seq=%d err=%s", s.addr, b, seq, errStr)
				break
			}
		}
	}
}

func (s *server) getBackupCommitted(addr string) (uint64, error) {
	conn, cli, err := dial(addr, 800*time.Millisecond)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	pr, err := cli.Ping(ctx, &pb.PingRequest{})
	if err != nil {
		return 0, err
	}
	if !pr.GetOk() {
		return 0, fmt.Errorf("ping not ok: %s", pr.GetErr())
	}
	return pr.GetMaxCommitted(), nil
}

// -----------------------------
// Helpers
// -----------------------------
func applyOp(store *ckv.Store, op *pb.Operation) error {
	switch op.GetType() {
	case pb.OperationType_OP_PUT:
		return store.Put(op.GetKey(), op.GetValue())
	case pb.OperationType_OP_DEL:
		return store.Del(op.GetKey())
	default:
		return fmt.Errorf("bad op type: %v", op.GetType())
	}
}

func dial(addr string, timeout time.Duration) (*grpc.ClientConn, pb.StorageNodeClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewStorageNodeClient(conn), nil
}

func (s *server) replicateOnce(addr string, req *pb.ReplicateRequest) (bool, string) {
	conn, cli, err := dial(addr, 800*time.Millisecond)
	if err != nil {
		return false, err.Error()
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := cli.Replicate(ctx, req)
	if err != nil {
		return false, err.Error()
	}
	if !r.GetOk() {
		return false, r.GetErr()
	}
	return true, ""
}

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:9000", "listen addr")
		img     = flag.String("img", "./dev.img", "disk engine image path")
		devSize = flag.Uint64("dev_size", 1<<30, "disk engine image size (bytes)")
		oplogP  = flag.String("oplog", "./oplog.log", "oplog path")
	)
	flag.Parse()

	st, err := ckv.Open(*img, *devSize)
	if err != nil {
		log.Fatalf("ckv open: %v", err)
	}
	defer st.Close()

	srv, err := newServer(*addr, st, *oplogP)
	if err != nil {
		log.Fatalf("server init: %v", err)
	}
	defer srv.ol.Close()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	gs := grpc.NewServer()
	pb.RegisterStorageNodeServer(gs, srv)

	log.Printf("storagenode listening on %s (img=%s size=%d)", *addr, *img, *devSize)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
