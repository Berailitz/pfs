package manager

import "sync"

const InitialNext = 2

type Manager interface {
	Owner(id uint64) string
	Allocate(owner uint64) uint64
	Deallocate(id uint64) bool
}

type RManager struct {
	NodeOwner sync.Map // [uint64]uint64
	Owners    sync.Map // [uint64]string
	next      uint64
}

var _ = (Manager)((*RManager)(nil))

func (m *RManager) Owner(id uint64) string {
	if ownerOut, ok := m.NodeOwner.Load(id); ok {
		if owner, ok := ownerOut.(uint64); ok {
			return m.QueryAddr(owner)
		}
	}
	return ""
}

func (m *RManager) QueryAddr(ownerID uint64) string {
	if out, ok := m.Owners.Load(ownerID); ok {
		if addr, ok := out.(string); ok {
			return addr
		}
	}
	return ""
}

func (m *RManager) Allocate(owner uint64) uint64 {
	if _, ok := m.Owners.Load(owner); ok {
		id := m.next
		m.next++
		m.NodeOwner.Store(id, owner)
		return id
	}
	return 0
}

func (m *RManager) Deallocate(id uint64) bool {
	if _, ok := m.NodeOwner.Load(id); ok {
		m.NodeOwner.Delete(id)
		return true
	}
	return false
}

func NewRManager() *RManager {
	return &RManager{
		next: InitialNext,
	}
}
