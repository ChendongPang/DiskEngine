package ckv

import (
	"errors"
	"fmt"
	"sync"
	"syscall"
	"unsafe"
)

// Store is a thin, threadsafe wrapper over the C kv_store (disk engine + tombstone KV layer).
type Store struct {
	mu  sync.Mutex
	kv  *cKV
	img string
}

func Open(imgPath string, devSize uint64) (*Store, error) {
	if imgPath == "" {
		return nil, errors.New("empty img path")
	}
	kv, err := cKVOpen(imgPath, devSize)
	if err != nil {
		return nil, err
	}
	return &Store{kv: kv, img: imgPath}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kv == nil {
		return nil
	}
	s.kv.close()
	s.kv = nil
	return nil
}

func (s *Store) Put(key string, val []byte) error {
	if key == "" {
		return errors.New("empty key")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kv == nil {
		return errors.New("store closed")
	}
	return s.kv.put(key, val)
}

func (s *Store) Del(key string) error {
	if key == "" {
		return errors.New("empty key")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kv == nil {
		return errors.New("store closed")
	}
	return s.kv.del(key)
}

func (s *Store) Get(key string) ([]byte, bool, error) {
	if key == "" {
		return nil, false, errors.New("empty key")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kv == nil {
		return nil, false, errors.New("store closed")
	}
	return s.kv.get(key)
}

// LastBlobID is mainly for debugging / observability.
func (s *Store) LastBlobID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kv == nil {
		return 0
	}
	return s.kv.lastBlobID()
}

// errnoErr converts negative errno-style return codes (e.g., -ENOENT) into Go errors.
func errnoErr(rc int) error {
	if rc == 0 {
		return nil
	}
	if rc > 0 {
		// Unexpected (your C code returns 0 or -errno). Keep it as-is.
		return fmt.Errorf("ckv: unexpected positive rc=%d", rc)
	}
	return syscall.Errno(-rc)
}

// bytesFromC copies C memory into a Go []byte and frees it via C.free.
func bytesFromC(p unsafe.Pointer, n uintptr) []byte {
	if p == nil {
		return nil
	}
	// copy into Go memory
	b := make([]byte, n)
	copy(b, unsafe.Slice((*byte)(p), n))
	cFree(p)
	return b
}
