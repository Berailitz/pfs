package fbackend

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	"github.com/Berailitz/pfs/idallocator"

	"github.com/jacobsa/fuse/fuseops"
)

const (
	RootNodeID      uint64 = fuseops.RootInodeID
	FirstOwnerID    uint64 = 1
	FirstProposalID uint64 = 1
	MaxOwnerID      uint64 = 200
)

type RManager struct {
	NodeOwner         sync.Map // [uint64]uint64
	Owners            sync.Map // [uint64]string
	OwnerCounter      [MaxOwnerID]uint64
	nodeAllocator     *idallocator.IDAllocator
	ownerAllocator    *idallocator.IDAllocator
	proposalAllocator *idallocator.IDAllocator

	masterAddr string

	muOwnerMapRead sync.RWMutex
	ownerMapRead   map[uint64]string
}

func (m *RManager) QueryOwner(ctx context.Context, nodeID uint64) string {
	log.Printf("query owner: nodeID=%v", nodeID)
	if ownerOut, ok := m.NodeOwner.Load(nodeID); ok {
		if owner, ok := ownerOut.(uint64); ok {
			addr := m.QueryAddr(ctx, owner)
			log.Printf("query owner success: nodeID=%v, addr=%v", nodeID, addr)
			return addr
		}
		log.Printf("query owner not node error: nodeID=%v", nodeID)
		return ""
	}
	log.Printf("query owner no node error: nodeID=%v", nodeID)
	return ""
}

func (m *RManager) QueryAddr(ctx context.Context, ownerID uint64) string {
	if out, ok := m.Owners.Load(ownerID); ok {
		if addr, ok := out.(string); ok {
			return addr
		}
	}
	return ""
}

func (m *RManager) Allocate(ctx context.Context, ownerID uint64) uint64 {
	if _, ok := m.Owners.Load(ownerID); ok {
		id := m.nodeAllocator.Allocate()
		m.NodeOwner.Store(id, ownerID)
		return id
	}
	return 0
}

func (m *RManager) Deallocate(ctx context.Context, nodeID uint64) bool {
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
func (m *RManager) RegisterOwner(ctx context.Context, addr string) uint64 {
	ownerID := m.ownerAllocator.Allocate()
	if ownerID <= MaxOwnerID {
		m.muOwnerMapRead.Lock()
		defer m.muOwnerMapRead.Unlock()

		m.Owners.Store(ownerID, addr)
		m.ownerMapRead[ownerID] = addr

		return ownerID
	}
	return 0
}

func (m *RManager) RemoveOwner(ctx context.Context, ownerID uint64) bool {
	if atomic.LoadUint64(&m.OwnerCounter[ownerID]) == 0 {
		m.muOwnerMapRead.Lock()
		defer m.muOwnerMapRead.Unlock()

		m.Owners.Delete(ownerID)
		delete(m.ownerMapRead, ownerID)

		return true
	}
	return false
}

func (m *RManager) CopyOwnerMap(ctx context.Context) map[uint64]string {
	m.muOwnerMapRead.RLock()
	defer m.muOwnerMapRead.RUnlock()

	output := make(map[uint64]string, len(m.ownerMapRead))
	for k, v := range m.ownerMapRead {
		output[k] = v
	}
	return output
}

func (m *RManager) MasterAddr() string {
	return m.masterAddr
}

// AllocateRoot returns true if ownerID acquires the root node, false otherwise.
// Note that AllocateRoot returns false if ownID is invalid.
func (m *RManager) AllocateRoot(ctx context.Context, ownerID uint64) bool {
	if _, ok := m.Owners.Load(ownerID); ok {
		if _, loaded := m.NodeOwner.LoadOrStore(RootNodeID, ownerID); !loaded {
			log.Printf("root allocated: ownerID=%v", ownerID)
			return true
		}
	}
	return false
}

func (m *RManager) SetMaster(masterAddr string) {
	m.masterAddr = masterAddr
}

// NewRManager do not register or allocate
func NewRManager() *RManager {
	return &RManager{
		nodeAllocator:     idallocator.NewIDAllocator(RootNodeID + 1), // since root is assigned by AllocateRoot, not allocated
		ownerAllocator:    idallocator.NewIDAllocator(FirstOwnerID),
		proposalAllocator: idallocator.NewIDAllocator(FirstProposalID),
		ownerMapRead:      map[uint64]string{},
	}
}
