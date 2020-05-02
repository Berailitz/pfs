package idallocator

import "sync/atomic"

type IDAllocator struct {
	next uint64
}

func (a *IDAllocator) SetNext(next uint64) {
	atomic.StoreUint64(&a.next, next)
}

func (a *IDAllocator) ReadNext() (next uint64) {
	return atomic.LoadUint64(&a.next)
}

func (a *IDAllocator) Allocate() uint64 {
	return atomic.AddUint64(&a.next, 1) - 1
}

func NewIDAllocator(first uint64) *IDAllocator {
	return &IDAllocator{first}
}
