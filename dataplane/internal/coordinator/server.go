package coordinator

import (
	"context"
	"fmt"
	"sync"

	pbcoord "dataplane/internal/rpc/pbcoord"
)

type Server struct {
	pbcoord.UnimplementedCoordinatorServer

	mu     sync.Mutex
	shards map[uint32]*pbcoord.ShardState
}

func NewServer() *Server {
	return &Server{
		shards: make(map[uint32]*pbcoord.ShardState),
	}
}

func cloneState(s *pbcoord.ShardState) *pbcoord.ShardState {
	if s == nil {
		return nil
	}
	out := &pbcoord.ShardState{
		Shard:   s.Shard,
		Epoch:   s.Epoch,
		Primary: s.Primary,
		Wq:      s.Wq,
	}
	out.Backups = append([]string{}, s.Backups...)
	return out
}

func (c *Server) Bootstrap(ctx context.Context, req *pbcoord.BootstrapRequest) (*pbcoord.BootstrapReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.shards[req.GetShard()]; ok {
		return &pbcoord.BootstrapReply{Ok: false, Err: "already bootstrapped"}, nil
	}
	c.shards[req.GetShard()] = &pbcoord.ShardState{
		Shard:   req.GetShard(),
		Epoch:   req.GetEpoch(),
		Primary: req.GetPrimary(),
		Backups: append([]string{}, req.GetBackups()...),
		Wq:      req.GetWq(),
	}
	return &pbcoord.BootstrapReply{Ok: true}, nil
}

func (c *Server) GetState(ctx context.Context, req *pbcoord.GetStateRequest) (*pbcoord.GetStateReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.shards[req.GetShard()]
	if !ok {
		return &pbcoord.GetStateReply{Ok: false, Err: "shard not bootstrapped"}, nil
	}
	return &pbcoord.GetStateReply{Ok: true, State: cloneState(st)}, nil
}

func (c *Server) CasFailover(ctx context.Context, req *pbcoord.CasFailoverRequest) (*pbcoord.CasFailoverReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.shards[req.GetShard()]
	if !ok {
		return &pbcoord.CasFailoverReply{Ok: false, Err: "shard not bootstrapped"}, nil
	}
	if st.Epoch != req.GetExpectedEpoch() {
		// CAS failed: return current state to caller
		return &pbcoord.CasFailoverReply{
			Ok:    false,
			Err:   fmt.Sprintf("epoch mismatch: expect=%d actual=%d", req.GetExpectedEpoch(), st.Epoch),
			State: cloneState(st),
		}, nil
	}

	// bump epoch atomically, set new view
	st.Epoch++
	st.Primary = req.GetNewPrimary()
	st.Backups = append([]string{}, req.GetNewBackups()...)
	st.Wq = req.GetWq()

	return &pbcoord.CasFailoverReply{Ok: true, State: cloneState(st)}, nil
}
