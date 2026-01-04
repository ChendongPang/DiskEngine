package ckv

/*
#cgo CFLAGS: -I${SRCDIR}/../../../src
#cgo LDFLAGS: -L${SRCDIR}/../../../build -lkvstore -ldiskengine

#include <stdlib.h>
#include <errno.h>
#include "kv/kv_store.h"
*/
import "C"

import (
	"unsafe"
)

type cKV struct {
	p *C.kv_store_t
}

func cKVOpen(imgPath string, devSize uint64) (*cKV, error) {
	cpath := C.CString(imgPath)
	defer C.free(unsafe.Pointer(cpath))

	var out *C.kv_store_t
	rc := int(C.kv_open((**C.kv_store_t)(unsafe.Pointer(&out)), cpath, C.uint64_t(devSize)))
	if err := errnoErr(rc); err != nil {
		return nil, err
	}
	return &cKV{p: out}, nil
}

func (k *cKV) close() {
	if k == nil || k.p == nil {
		return
	}
	C.kv_close(k.p)
	k.p = nil
}

func (k *cKV) put(key string, val []byte) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var p unsafe.Pointer
	var n C.size_t
	if len(val) > 0 {
		p = unsafe.Pointer(&val[0])
		n = C.size_t(len(val))
	} else {
		// allow empty value
		p = unsafe.Pointer(uintptr(0))
		n = 0
	}

	rc := int(C.kv_put(k.p, ckey, p, n))
	return errnoErr(rc)
}

func (k *cKV) del(key string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	rc := int(C.kv_del(k.p, ckey))
	return errnoErr(rc)
}

func (k *cKV) get(key string) ([]byte, bool, error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var outBuf unsafe.Pointer
	var outLen C.size_t
	rc := int(C.kv_get(k.p, ckey, (*unsafe.Pointer)(unsafe.Pointer(&outBuf)), (*C.size_t)(unsafe.Pointer(&outLen))))
	if rc == -int(C.ENOENT) {
		return nil, false, nil
	}
	if err := errnoErr(rc); err != nil {
		return nil, false, err
	}

	b := bytesFromC(outBuf, uintptr(outLen))
	return b, true, nil
}

func (k *cKV) lastBlobID() uint64 {
	if k == nil || k.p == nil {
		return 0
	}
	return uint64(C.kv_last_blob_id(k.p))
}

func cFree(p unsafe.Pointer) {
	C.free(p)
}
