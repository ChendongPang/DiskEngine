package gateway

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	pb "dataplane/internal/rpc/pb"
	pbmeta "dataplane/internal/rpc/pbmeta"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ShardView struct {
	Epoch   uint64
	Primary string
	Backups []string
	WQ      uint32
}

type Server struct {
	pb.UnimplementedStorageNodeServer

	mu sync.Mutex

	shards int
	view   []ShardView

	metaAddr string

	dialTimeout time.Duration
	rpcTimeout  time.Duration
}

func NewServer(numShards int, metaAddr string, rpcTimeout time.Duration) *Server {
	return &Server{
		shards:      numShards,
		view:        make([]ShardView, numShards),
		metaAddr:    metaAddr,
		dialTimeout: 800 * time.Millisecond,
		rpcTimeout:  rpcTimeout,
	}
}

func splitNonEmptyCSV(s string) []string {
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

func (s *Server) BootstrapShard(ctx context.Context, shard int, epoch uint64, primary string, backups []string, wq uint32) error {
	// best-effort bootstrap
	{
		conn, cli, err := s.dialMeta()
		if err != nil {
			return err
		}
		defer conn.Close()

		rctx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
		defer cancel()

		_, _ = cli.Bootstrap(rctx, &pbmeta.BootstrapRequest{
			View: &pbmeta.ShardView{
				Shard:       uint32(shard),
				Epoch:       epoch,
				PrimaryAddr: primary,
				BackupAddrs: backups,
				Wq:          wq,
			},
		})
	}
	// authoritative refresh
	return s.refreshShard(ctx, shard)
}

func (s *Server) RefreshAll(ctx context.Context) error {
	for i := 0; i < s.shards; i++ {
		if err := s.refreshShard(ctx, i); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) refreshShard(ctx context.Context, shard int) error {
	conn, cli, err := s.dialMeta()
	if err != nil {
		return err
	}
	defer conn.Close()

	rctx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
	defer cancel()

	gr, err := cli.GetView(rctx, &pbmeta.GetViewRequest{Shard: uint32(shard)})
	if err != nil {
		return err
	}
	if !gr.GetOk() {
		return fmt.Errorf("meta getview: %s", gr.GetErr())
	}

	v := gr.GetView()
	s.mu.Lock()
	s.view[shard] = ShardView{
		Epoch:   v.GetEpoch(),
		Primary: v.GetPrimaryAddr(),
		Backups: append([]string{}, v.GetBackupAddrs()...),
		WQ:      v.GetWq(),
	}
	s.mu.Unlock()
	return nil
}

func (s *Server) getShard(key string) int {
	h := sha1.Sum([]byte(key))
	v := binary.LittleEndian.Uint32(h[:4])
	return int(v % uint32(s.shards))
}

func (s *Server) dial(addr string) (*grpc.ClientConn, pb.StorageNodeClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewStorageNodeClient(conn), nil
}

func (s *Server) dialMeta() (*grpc.ClientConn, pbmeta.MetaCoordinatorClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, s.metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return conn, pbmeta.NewMetaCoordinatorClient(conn), nil
}

func (s *Server) pingNode(addr string) (*pb.PingReply, error) {
	conn, cli, err := s.dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), s.rpcTimeout)
	defer cancel()
	return cli.Ping(ctx, &pb.PingRequest{})
}

func (s *Server) configureAll(shard int, epoch uint64, primary string, backups []string, wq uint32) {
	addrs := append([]string{}, backups...)
	if primary != "" {
		addrs = append(addrs, primary)
	}
	for _, addr := range addrs {
		go func(a string) {
			conn, cli, err := s.dial(a)
			if err != nil {
				log.Printf("[gw] configure dial %s err=%v", a, err)
				return
			}
			defer conn.Close()

			role := pb.NodeRole_ROLE_BACKUP
			if a == primary {
				role = pb.NodeRole_ROLE_PRIMARY
			}

			ctx, cancel := context.WithTimeout(context.Background(), s.rpcTimeout)
			defer cancel()
			_, err = cli.Configure(ctx, &pb.ConfigureRequest{
				Epoch:       epoch,
				Role:        role,
				PrimaryAddr: primary,
				BackupAddrs: backups,
				Wq:          wq,
			})
			if err != nil {
				log.Printf("[gw] configure %s err=%v", a, err)
			}
		}(addr)
	}
}

type candidate struct {
	addr         string
	maxCommitted uint64
}

func (s *Server) tryFailover(ctx context.Context, shard int, cur ShardView) (ShardView, error) {
	if len(cur.Backups) == 0 {
		return ShardView{}, fmt.Errorf("no backups")
	}

	var cands []candidate
	for _, b := range cur.Backups {
		pr, err := s.pingNode(b)
		if err != nil || !pr.GetOk() {
			continue
		}
		cands = append(cands, candidate{addr: b, maxCommitted: pr.GetMaxCommitted()})
	}
	if len(cands) == 0 {
		return ShardView{}, fmt.Errorf("no alive backups")
	}

	sort.Slice(cands, func(i, j int) bool {
		if cands[i].maxCommitted != cands[j].maxCommitted {
			return cands[i].maxCommitted > cands[j].maxCommitted
		}
		return cands[i].addr < cands[j].addr
	})
	newPrimary := cands[0].addr

	newBackups := make([]string, 0, len(cur.Backups)+1)
	for _, b := range cur.Backups {
		if b != newPrimary {
			newBackups = append(newBackups, b)
		}
	}
	if cur.Primary != "" && cur.Primary != newPrimary {
		if pr, err := s.pingNode(cur.Primary); err == nil && pr.GetOk() {
			newBackups = append(newBackups, cur.Primary)
		}
	}

	// CAS bump epoch
	conn, mcli, err := s.dialMeta()
	if err != nil {
		return ShardView{}, fmt.Errorf("dial meta: %w", err)
	}
	defer conn.Close()

	rctx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
	defer cancel()
	cas, err := mcli.CasFailover(rctx, &pbmeta.CasFailoverRequest{
		Shard:           uint32(shard),
		ExpectedEpoch:   cur.Epoch,
		NewPrimaryAddr:  newPrimary,
		NewBackupAddrs:  newBackups,
		Wq:              cur.WQ,
	})
	if err != nil {
		return ShardView{}, fmt.Errorf("meta cas: %w", err)
	}
	if !cas.GetOk() {
		if v := cas.GetView(); v != nil {
			s.mu.Lock()
			s.view[shard] = ShardView{
				Epoch:   v.GetEpoch(),
				Primary: v.GetPrimaryAddr(),
				Backups: append([]string{}, v.GetBackupAddrs()...),
				WQ:      v.GetWq(),
			}
			s.mu.Unlock()
		}
		return ShardView{}, fmt.Errorf("failover CAS failed: %s", cas.GetErr())
	}

	v := cas.GetView()
	newView := ShardView{
		Epoch:   v.GetEpoch(),
		Primary: v.GetPrimaryAddr(),
		Backups: append([]string{}, v.GetBackupAddrs()...),
		WQ:      v.GetWq(),
	}

	s.mu.Lock()
	s.view[shard] = newView
	s.mu.Unlock()

	log.Printf("[gw] shard=%d failover: primary %s -> %s epoch %d->%d",
		shard, cur.Primary, newView.Primary, cur.Epoch, newView.Epoch)

	s.configureAll(shard, newView.Epoch, newView.Primary, newView.Backups, newView.WQ)
	return newView, nil
}

// ---- StorageNode external APIs ----

func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingReply, error) {
	return &pb.PingReply{Ok: true, Role: pb.NodeRole_ROLE_UNKNOWN, Epoch: 0, MaxCommitted: 0}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	sh := s.getShard(req.GetKey())

	s.mu.Lock()
	v := s.view[sh]
	s.mu.Unlock()

	if v.Primary == "" {
		return &pb.GetReply{Ok: false, Err: "no primary"}, nil
	}
	conn, cli, err := s.dial(v.Primary)
	if err != nil {
		return &pb.GetReply{Ok: false, Err: err.Error()}, nil
	}
	defer conn.Close()

	rctx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
	defer cancel()
	gr, err := cli.Get(rctx, req)
	if err != nil {
		return &pb.GetReply{Ok: false, Err: err.Error()}, nil
	}
	return gr, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutReply, error) {
	ok, errStr := s.writeInternal(ctx, &pb.Operation{
		Type:  pb.OperationType_OP_PUT,
		Key:   req.GetKey(),
		Value: req.GetValue(),
	}, "put")
	if ok {
		return &pb.PutReply{Ok: true}, nil
	}
	return &pb.PutReply{Ok: false, Err: errStr}, nil
}

func (s *Server) Del(ctx context.Context, req *pb.DelRequest) (*pb.DelReply, error) {
	ok, errStr := s.writeInternal(ctx, &pb.Operation{
		Type: pb.OperationType_OP_DEL,
		Key:  req.GetKey(),
	}, "del")
	if ok {
		return &pb.DelReply{Ok: true}, nil
	}
	return &pb.DelReply{Ok: false, Err: errStr}, nil
}

func (s *Server) writeInternal(ctx context.Context, op *pb.Operation, kind string) (bool, string) {
	sh := s.getShard(op.GetKey())

	s.mu.Lock()
	v := s.view[sh]
	s.mu.Unlock()

	reqID := fmt.Sprintf("%s-%d-%d", kind, time.Now().UnixNano(), sh)

	// try current primary once
	if v.Primary != "" {
		if ok, errStr := s.clientWriteOnce(ctx, v.Primary, v.Epoch, reqID, op); ok {
			return true, ""
		} else {
			log.Printf("[gw] %s primary failed shard=%d primary=%s err=%s", kind, sh, v.Primary, errStr)
		}
	}

	// refresh cache (maybe someone else already failed over)
	_ = s.refreshShard(ctx, sh)

	s.mu.Lock()
	v = s.view[sh]
	s.mu.Unlock()

	if v.Primary != "" {
		if ok, errStr := s.clientWriteOnce(ctx, v.Primary, v.Epoch, reqID, op); ok {
			return true, ""
		} else {
			log.Printf("[gw] %s primary failed shard=%d primary=%s err=%s", kind, sh, v.Primary, errStr)
		}
	}

	// failover
	newV, err := s.tryFailover(ctx, sh, v)
	if err != nil {
		return false, err.Error()
	}

	if ok, errStr := s.clientWriteOnce(ctx, newV.Primary, newV.Epoch, reqID, op); ok {
		return true, ""
	} else {
		return false, errStr
	}
}

func (s *Server) clientWriteOnce(ctx context.Context, primary string, epoch uint64, reqID string, op *pb.Operation) (bool, string) {
	conn, cli, err := s.dial(primary)
	if err != nil {
		return false, err.Error()
	}
	defer conn.Close()

	rctx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
	defer cancel()
	r, err := cli.ClientWrite(rctx, &pb.ClientWriteRequest{
		Epoch: epoch,
		ReqId: reqID,
		Op:    op,
	})
	if err != nil {
		return false, err.Error()
	}
	if !r.GetOk() {
		return false, r.GetErr()
	}
	return true, ""
}
