package utility

import (
	"sync"
	"sync/atomic"
)

type IterableMap struct {
	smap sync.Map
	mu   sync.RWMutex
	size int64
}

func (m *IterableMap) Len() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return atomic.LoadInt64(&m.size)
}

func (m *IterableMap) Load(key interface{}) (interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.smap.Load(key)
}

func (m *IterableMap) Store(key, value interface{}) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	atomic.AddInt64(&m.size, 1)
	m.smap.Store(key, value)
}

func (m *IterableMap) LoadOrStore(key, value interface{}) (interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.smap.LoadOrStore(key, value)
}

func (m *IterableMap) Delete(key interface{}) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	atomic.AddInt64(&m.size, -1)
	m.smap.Delete(key)
}

func (m *IterableMap) Range(f func(key, value interface{}) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.smap.Range(f)
}
