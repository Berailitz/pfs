package utility

import "sync"

type NXMap struct {
	sm   sync.Map
	lock sync.RWMutex
}

func (m *NXMap) Load(k interface{}) (interface{}, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.sm.Load(k)
}

func (m *NXMap) Store(k, v interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.sm.Store(k, v)
}

func (m *NXMap) StoreNX(k, v interface{}) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if _, ok := m.sm.Load(k); ok {
		return false
	}

	m.sm.Store(k, v)
	return true
}

func (m *NXMap) Delete(k interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.sm.Delete(k)
}

func (m *NXMap) PopX(k interface{}) (interface{}, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if v, ok := m.sm.Load(k); ok {
		m.sm.Delete(k)
		return v, true
	}

	return nil, false
}
