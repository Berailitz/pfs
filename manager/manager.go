package manager

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/jacobsa/fuse/fuseops"
)

const (
	RootNodeID   uint64 = fuseops.RootInodeID
	FirstOwnerID uint64 = 1
	MaxOwnerID   uint64 = 200
)

type Manager interface {
	QueryOwner(nodeID uint64) string
	Allocate(ownerID uint64) uint64
	Deallocate(nodeID uint64) bool
	RegisterOwner(addr string) uint64
	RemoveOwner(ownerID uint64) bool
	AllocateRoot(ownerID uint64) bool
}

type RManager struct {
	NodeOwner    sync.Map // [uint64]uint64
	Owners       sync.Map // [uint64]string
	OwnerCounter [MaxOwnerID]uint64
	NextNodeID   uint64
	NextOwnerID  uint64
}

var _ = (Manager)((*RManager)(nil))

func (m *RManager) QueryOwner(nodeID uint64) string {
	if ownerOut, ok := m.NodeOwner.Load(nodeID); ok {
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

func (m *RManager) Allocate(ownerID uint64) uint64 {
	if _, ok := m.Owners.Load(ownerID); ok {
		id := m.NextNodeID
		atomic.AddUint64(&m.OwnerCounter[ownerID], 1)
		m.NextNodeID++
		m.NodeOwner.Store(id, ownerID)
		return id
	}
	return 0
}

func (m *RManager) Deallocate(nodeID uint64) bool {
	if out, ok := m.NodeOwner.Load(nodeID); ok {
		if ownerID, ok := out.(uint64); ok {
			m.NodeOwner.Delete(nodeID)
			atomic.AddUint64(&m.OwnerCounter[ownerID], ^uint64(0))
			return true
		}
	}
	return false
}

// RegisterOwner return 0 if err
func (m *RManager) RegisterOwner(addr string) uint64 {
	ownerID := m.NextOwnerID
	if ownerID <= MaxOwnerID {
		m.Owners.Store(ownerID, addr)
		m.NextOwnerID++
		return ownerID
	}
	return 0
}

func (m *RManager) RemoveOwner(ownerID uint64) bool {
	if atomic.LoadUint64(&m.OwnerCounter[ownerID]) == 0 {
		m.Owners.Delete(ownerID)
		return true
	}
	return false
}

// AllocateRoot returns true if ownerID acquires the root node, false otherwise.
// Note that AllocateRoot returns false if ownID is invalid.
func (m *RManager) AllocateRoot(ownerID uint64) bool {
	if _, ok := m.Owners.Load(ownerID); ok {
		if _, loaded := m.NodeOwner.LoadOrStore(RootNodeID, ownerID); !loaded {
			log.Printf("root allocated: ownerID=%v", ownerID)
			return true
		}
	}
	return false
}

// NewRManager do not register or allocate
func NewRManager() *RManager {
	return &RManager{
		NextNodeID:  RootNodeID + 1, // since root is assigned by AllocateRoot, not allocated
		NextOwnerID: FirstOwnerID,
	}
}