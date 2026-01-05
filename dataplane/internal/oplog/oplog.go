package oplog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "dataplane/internal/rpc/pb"
)

var (
	ErrNotFound = errors.New("not found")
)

type RecordType uint8

const (
	recUnknown RecordType = 0
	recPrepare RecordType = 1
	recCommit  RecordType = 2
)

// On-disk record layout (very small MVP):
// [1B type][8B epoch][8B seq][4B op_len][op_bytes]
//
// op_bytes is a simple encoding of pb.Operation:
// [1B op_type][4B key_len][key_bytes][4B val_len][val_bytes]
//
// For commit record: op_len=0 and no op_bytes.

type Oplog struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

func Open(path string) (*Oplog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// seek to end for append
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Oplog{f: f, path: path}, nil
}

func (l *Oplog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

func (l *Oplog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	return l.f.Sync()
}

func encodeOp(op *pb.Operation) ([]byte, error) {
	if op == nil {
		return nil, fmt.Errorf("nil op")
	}
	key := []byte(op.GetKey())
	val := op.GetValue()

	// Layout:
	// [1 opType][4 keyLen][key][4 valLen][val]
	out := make([]byte, 1+4+len(key)+4+len(val))
	out[0] = byte(op.GetType())
	binary.LittleEndian.PutUint32(out[1:5], uint32(len(key)))
	copy(out[5:5+len(key)], key)
	off := 5 + len(key)
	binary.LittleEndian.PutUint32(out[off:off+4], uint32(len(val)))
	copy(out[off+4:], val)
	return out, nil
}

func decodeOp(b []byte) (*pb.Operation, error) {
	if len(b) < 1+4+4 {
		return nil, fmt.Errorf("op bytes too short")
	}
	opType := pb.OperationType(b[0])
	keyLen := int(binary.LittleEndian.Uint32(b[1:5]))
	if 5+keyLen+4 > len(b) {
		return nil, fmt.Errorf("bad key len")
	}
	key := string(b[5 : 5+keyLen])
	off := 5 + keyLen
	valLen := int(binary.LittleEndian.Uint32(b[off : off+4]))
	if off+4+valLen > len(b) {
		return nil, fmt.Errorf("bad val len")
	}
	val := make([]byte, valLen)
	copy(val, b[off+4:off+4+valLen])

	return &pb.Operation{
		Type:  opType,
		Key:   key,
		Value: val,
	}, nil
}

func (l *Oplog) AppendPrepare(epoch uint64, seq uint64, reqID string, op *pb.Operation) error {
	_ = reqID // reserved for future; kept in caller
	opBytes, err := encodeOp(op)
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return fmt.Errorf("oplog closed")
	}

	hdr := make([]byte, 1+8+8+4)
	hdr[0] = byte(recPrepare)
	binary.LittleEndian.PutUint64(hdr[1:9], epoch)
	binary.LittleEndian.PutUint64(hdr[9:17], seq)
	binary.LittleEndian.PutUint32(hdr[17:21], uint32(len(opBytes)))

	if _, err := l.f.Write(hdr); err != nil {
		return err
	}
	if _, err := l.f.Write(opBytes); err != nil {
		return err
	}
	return l.f.Sync()
}

func (l *Oplog) AppendCommit(epoch uint64, seq uint64, reqID string) error {
	_ = reqID // reserved for future
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return fmt.Errorf("oplog closed")
	}

	hdr := make([]byte, 1+8+8+4)
	hdr[0] = byte(recCommit)
	binary.LittleEndian.PutUint64(hdr[1:9], epoch)
	binary.LittleEndian.PutUint64(hdr[9:17], seq)
	binary.LittleEndian.PutUint32(hdr[17:21], 0)

	if _, err := l.f.Write(hdr); err != nil {
		return err
	}
	return l.f.Sync()
}

type ReplayHandler interface {
	OnPrepare(epoch uint64, seq uint64, op *pb.Operation) error
	OnCommit(epoch uint64, seq uint64) error
}

// Replay scans from beginning; MVP O(size).
func Replay(path string, h ReplayHandler) error {
	f, err := os.Open(path)
	if err != nil {
		// If file doesn't exist yet, treat as empty.
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	for {
		hdr := make([]byte, 1+8+8+4)
		if _, err := io.ReadFull(r, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		rt := RecordType(hdr[0])
		epoch := binary.LittleEndian.Uint64(hdr[1:9])
		seq := binary.LittleEndian.Uint64(hdr[9:17])
		opLen := binary.LittleEndian.Uint32(hdr[17:21])

		switch rt {
		case recPrepare:
			opBytes := make([]byte, opLen)
			if _, err := io.ReadFull(r, opBytes); err != nil {
				return err
			}
			op, err := decodeOp(opBytes)
			if err != nil {
				return err
			}
			if err := h.OnPrepare(epoch, seq, op); err != nil {
				return err
			}
		case recCommit:
			// opLen expected 0
			if opLen != 0 {
				// skip if corrupted
				if _, err := io.CopyN(io.Discard, r, int64(opLen)); err != nil {
					return err
				}
			}
			if err := h.OnCommit(epoch, seq); err != nil {
				return err
			}
		default:
			// unknown record type: stop
			return fmt.Errorf("unknown oplog record type: %d", rt)
		}
	}
}

// ReadPrepareBySeq is used by primary repair to fetch entry by seq (MVP: full scan).
func ReadPrepareBySeq(path string, targetSeq uint64) (*pb.Operation, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	for {
		hdr := make([]byte, 1+8+8+4)
		if _, err := io.ReadFull(r, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, 0, ErrNotFound
			}
			return nil, 0, err
		}
		rt := RecordType(hdr[0])
		epoch := binary.LittleEndian.Uint64(hdr[1:9])
		seq := binary.LittleEndian.Uint64(hdr[9:17])
		opLen := binary.LittleEndian.Uint32(hdr[17:21])

		if rt == recPrepare {
			opBytes := make([]byte, opLen)
			if _, err := io.ReadFull(r, opBytes); err != nil {
				return nil, 0, err
			}
			if seq == targetSeq {
				op, err := decodeOp(opBytes)
				if err != nil {
					return nil, 0, err
				}
				return op, epoch, nil
			}
		} else {
			// commit or unknown
			if opLen != 0 {
				if _, err := io.CopyN(io.Discard, r, int64(opLen)); err != nil {
					return nil, 0, err
				}
			}
		}
	}
}
