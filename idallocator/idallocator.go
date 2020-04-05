package idallocator

import "sync/atomic"

type IDAllocator struct {
	next uint64
}

func (a *IDAllocator) Allocate() uint64 {
	return atomic.AddUint64(&a.next, 1) - 1
}

func NewIDAllocator(first uint64) *IDAllocator {
	return &IDAllocator{first}
}
