package fbackend

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Berailitz/pfs/logger"
	"github.com/Berailitz/pfs/utility"

	"github.com/Berailitz/pfs/idallocator"

	"github.com/jacobsa/fuse/fuseops"
)

const (
	RootNodeID      uint64 = fuseops.RootInodeID
	FirstOwnerID    uint64 = 1
	FirstProposalID uint64 = 1
	MaxOwnerID      uint64 = 200
)

const (
	AddOwnerProposalType    = 1
	RemoveOwnerProposalType = 2
	AddNodeProposalType     = 3
	RemoveNodeProposalType  = 4
)

const (
	SuccessProposalState = 0
	ErrorProposalState   = 1
)

const (
	maRunnableName         = "manager"
	maRunnableLoopInterval = time.Duration(0)
)

type Proposal struct {
	ID      uint64
	Typ     int64
	OwnerID uint64
	NodeID  uint64
	Value   string
}

type RManager struct {
	utility.Runnable

	muSync sync.RWMutex // lock when syncing, rlock when using

	NodeOwner         sync.Map           // [uint64]uint64
	Owners            sync.Map           // [uint64]string
	OwnerCounter      [MaxOwnerID]uint64 // not used
	nodeAllocator     *idallocator.IDAllocator
	ownerAllocator    *idallocator.IDAllocator
	proposalAllocator *idallocator.IDAllocator

	proposalChan chan *Proposal

	masterAddr string

	muOwnerMapRead sync.RWMutex
	ownerMapRead   map[uint64]string

	muNodeMapRead sync.RWMutex
	nodeMapRead   map[uint64]uint64

	fp *FProxy
}

type ManagerErr struct {
	msg string
}

var _ = (error)((*ManagerErr)(nil))

func (m *RManager) QueryOwner(ctx context.Context, nodeID uint64) string {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

	logger.If(ctx, "query owner: nodeID=%v", nodeID)
	if ownerOut, ok := m.NodeOwner.Load(nodeID); ok {
		if owner, ok := ownerOut.(uint64); ok {
			addr := m.queryAddr(ctx, owner)
			logger.If(ctx, "query owner success: nodeID=%v, addr=%v", nodeID, addr)
			return addr
		}
		logger.Ef(ctx, "query owner not node error: nodeID=%v", nodeID)
		return ""
	}
	logger.Ef(ctx, "query owner no node error: nodeID=%v", nodeID)
	return ""
}

func (m *RManager) queryAddr(ctx context.Context, ownerID uint64) string {
	if out, ok := m.Owners.Load(ownerID); ok {
		if addr, ok := out.(string); ok {
			return addr
		}
	}
	return ""
}

func (m *RManager) doAddNode(ctx context.Context, nodeID uint64, ownerID uint64) {
	m.muNodeMapRead.Lock()
	defer m.muNodeMapRead.Unlock()

	m.NodeOwner.Store(nodeID, ownerID)
	m.nodeMapRead[nodeID] = ownerID
}

func (m *RManager) Allocate(ctx context.Context, ownerID uint64) uint64 {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

	if _, ok := m.Owners.Load(ownerID); ok {
		nodeID := m.nodeAllocator.Allocate()
		m.doAddNode(ctx, nodeID, ownerID)
		m.proposalChan <- &Proposal{
			Typ:     AddNodeProposalType,
			NodeID:  nodeID,
			OwnerID: ownerID,
		}
		return nodeID
	}
	return 0
}

func (m *RManager) doRemoveNode(ctx context.Context, nodeID uint64) bool {
	if out, ok := m.NodeOwner.Load(nodeID); ok {
		if ownerID, ok := out.(uint64); ok {
			m.muNodeMapRead.Lock()
			defer m.muNodeMapRead.Unlock()
			m.NodeOwner.Delete(nodeID)
			delete(m.nodeMapRead, nodeID)
			atomic.AddUint64(&m.OwnerCounter[ownerID], ^uint64(0))
			return true
		}
	}
	return false
}

func (m *RManager) Deallocate(ctx context.Context, nodeID uint64) bool {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

	if m.doRemoveNode(ctx, nodeID) {
		m.proposalChan <- &Proposal{
			Typ:    RemoveNodeProposalType,
			NodeID: nodeID,
		}
		return true
	}
	return false
}

func (m *RManager) doAddOwner(ctx context.Context, ownerID uint64, addr string) bool {
	if addr == "" {
		logger.Ef(ctx, "invalid empty addr error")
		return false
	}

	m.muOwnerMapRead.Lock()
	defer m.muOwnerMapRead.Unlock()

	m.Owners.Store(ownerID, addr)
	m.ownerMapRead[ownerID] = addr
	return true
}

// RegisterOwner return 0 if err
func (m *RManager) RegisterOwner(ctx context.Context, addr string) uint64 {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

	ownerID := m.ownerAllocator.Allocate()
	if ownerID <= MaxOwnerID {
		if !m.doAddOwner(ctx, ownerID, addr) {
			return 0
		}
		m.proposalChan <- &Proposal{
			Typ:     AddOwnerProposalType,
			OwnerID: ownerID,
			Value:   addr,
		}

		return ownerID
	}
	return 0
}

func (m *RManager) doRemoveOwner(ctx context.Context, ownerID uint64) {
	m.muOwnerMapRead.Lock()
	defer m.muOwnerMapRead.Unlock()
	m.Owners.Delete(ownerID)
	delete(m.ownerMapRead, ownerID)
}

func (m *RManager) RemoveOwner(ctx context.Context, ownerID uint64) bool {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

	if atomic.LoadUint64(&m.OwnerCounter[ownerID]) == 0 {
		m.doRemoveOwner(ctx, ownerID)
		m.proposalChan <- &Proposal{
			Typ:     RemoveOwnerProposalType,
			OwnerID: ownerID,
		}

		return true
	}
	return false
}

func (m *RManager) CopyOwnerMap(ctx context.Context) map[uint64]string {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

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
// No need to broadcast for 1. root owner is the first owner and there is no one to answer
// 2. root owner will be automatically chose after recovery
func (m *RManager) AllocateRoot(ctx context.Context, ownerID uint64) bool {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

	if _, ok := m.Owners.Load(ownerID); ok {
		if _, loaded := m.NodeOwner.LoadOrStore(RootNodeID, ownerID); !loaded {
			logger.If(ctx, "root allocated: ownerID=%v", ownerID)
			return true
		}
	}
	return false
}

func (m *RManager) AnswerProposal(ctx context.Context, addr string, proposal *Proposal) (state int64, err error) {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

	switch proposal.Typ {
	case AddOwnerProposalType:
		if !m.doAddOwner(ctx, proposal.OwnerID, proposal.Value) {
			err = &ManagerErr{fmt.Sprintf("invalid proposal value: proposal=%+v", proposal)}
			logger.If(ctx, err.Error())
			return ErrorProposalState, err
		}
		m.ownerAllocator.SetNext(proposal.OwnerID + 1)
	case RemoveOwnerProposalType:
		m.doRemoveOwner(ctx, proposal.OwnerID)
	case AddNodeProposalType:
		m.doAddNode(ctx, proposal.NodeID, proposal.OwnerID)
		m.nodeAllocator.SetNext(proposal.NodeID + 1)
	case RemoveNodeProposalType:
		m.doRemoveNode(ctx, proposal.NodeID)
	default:
		err = &ManagerErr{fmt.Sprintf("invalid proposal type: proposal=%+v", proposal)}
		logger.If(ctx, err.Error())
		return ErrorProposalState, err
	}
	m.proposalAllocator.SetNext(proposal.ID + 1)
	return 0, nil
}

func (m *RManager) Run(ctx context.Context) (err error) {
	for {
		select {
		case <-m.Runnable.ToStop:
			logger.If(ctx, "runnable is quitting: name=%v", m.Runnable.Name)
			return nil
		case proposal := <-m.proposalChan:
			m.broadcastProposal(ctx, proposal)
		}
	}
}

func (m *RManager) broadcastProposal(ctx context.Context, proposal *Proposal) {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

	proposal.ID = m.proposalAllocator.Allocate()

	m.muOwnerMapRead.RLock()
	defer m.muOwnerMapRead.RUnlock()

	for _, addr := range m.ownerMapRead {
		if addr != m.MasterAddr() {
			// TODO handle state and err
			_, err := m.fp.SendProposal(ctx, addr, proposal)
			if err != nil {
				logger.If(ctx, "rpc proposal error: addr=%v, proposal=%+v, err=%+v",
					addr, proposal, err)
			}
		}
	}
}

func (m *RManager) SetMaster(masterAddr string) {
	m.masterAddr = masterAddr
}

func (m *RManager) SetFP(fp *FProxy) {
	m.fp = fp
}

// NewRManager do not register or allocate
func NewRManager(ctx context.Context) *RManager {
	ma := &RManager{
		nodeAllocator:     idallocator.NewIDAllocator(RootNodeID + 1), // since root is assigned by AllocateRoot, not allocated
		ownerAllocator:    idallocator.NewIDAllocator(FirstOwnerID),
		proposalAllocator: idallocator.NewIDAllocator(FirstProposalID),
		ownerMapRead:      make(map[uint64]string),
		nodeMapRead:       make(map[uint64]uint64),
		proposalChan:      make(chan *Proposal),
	}
	ma.InitRunnable(ctx, maRunnableName, maRunnableLoopInterval, nil, ma.Run)
	return ma
}

func (e *ManagerErr) Error() string {
	return fmt.Sprintf("manager error: %v", e.msg)
}
