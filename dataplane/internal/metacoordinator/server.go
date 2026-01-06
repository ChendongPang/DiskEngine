package metacoordinator

import (
	"context"
	"fmt"
	"sync"

	pbmeta "dataplane/internal/rpc/pbmeta"
)

type Server struct {
	pbmeta.UnimplementedMetaCoordinatorServer

	mu    sync.Mutex
	views map[uint32]*pbmeta.ShardView
}

func NewServer() *Server {
	return &Server{views: make(map[uint32]*pbmeta.ShardView)}
}

func cloneView(v *pbmeta.ShardView) *pbmeta.ShardView {
	if v == nil {
		return nil
	}
	out := &pbmeta.ShardView{
		Shard:       v.GetShard(),
		Epoch:       v.GetEpoch(),
		PrimaryAddr: v.GetPrimaryAddr(),
		Wq:          v.GetWq(),
	}
	out.BackupAddrs = append([]string{}, v.GetBackupAddrs()...)
	return out
}

func (s *Server) Bootstrap(ctx context.Context, req *pbmeta.BootstrapRequest) (*pbmeta.BootstrapReply, error) {
	v := req.GetView()
	if v == nil {
		return &pbmeta.BootstrapReply{Ok: false, Err: "missing view"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.views[v.GetShard()]; ok {
		return &pbmeta.BootstrapReply{Ok: false, Err: "already bootstrapped"}, nil
	}
	s.views[v.GetShard()] = cloneView(v)
	return &pbmeta.BootstrapReply{Ok: true}, nil
}

func (s *Server) GetView(ctx context.Context, req *pbmeta.GetViewRequest) (*pbmeta.GetViewReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.views[req.GetShard()]
	if !ok {
		return &pbmeta.GetViewReply{Ok: false, Err: "shard not bootstrapped"}, nil
	}
	return &pbmeta.GetViewReply{Ok: true, View: cloneView(v)}, nil
}

func (s *Server) CasFailover(ctx context.Context, req *pbmeta.CasFailoverRequest) (*pbmeta.CasFailoverReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.views[req.GetShard()]
	if !ok {
		return &pbmeta.CasFailoverReply{Ok: false, Err: "shard not bootstrapped"}, nil
	}
	if v.GetEpoch() != req.GetExpectedEpoch() {
		return &pbmeta.CasFailoverReply{
			Ok:   false,
			Err:  fmt.Sprintf("epoch mismatch: expected=%d actual=%d", req.GetExpectedEpoch(), v.GetEpoch()),
			View: cloneView(v),
		}, nil
	}

	v.Epoch = v.Epoch + 1
	v.PrimaryAddr = req.GetNewPrimaryAddr()
	v.BackupAddrs = append([]string{}, req.GetNewBackupAddrs()...)
	v.Wq = req.GetWq()

	return &pbmeta.CasFailoverReply{Ok: true, View: cloneView(v)}, nil
}
