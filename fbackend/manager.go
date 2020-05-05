package fbackend

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v2"

	pb "github.com/Berailitz/pfs/remotetree"

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

const (
	wdRunnableName         = "watchdog"
	wdRunnableLoopInterval = 10 * time.Second
	tofUpdateRatio         = 0.8
)

const (
	newElectionIDByIncr = 0
)

var (
	NodeNotExistErr    = &ManagerErr{"node not exist"}
	OwnerNotExistErr   = &ManagerErr{"owner not exist"}
	RootAllocatedError = &ManagerErr{"root already allocated error"}
	InvalidAddrErr     = &ManagerErr{"invalid empty addr error"}
	OutOfOwnerIDErr    = &ManagerErr{"out of owner ID error"}

	InternalInvalidOwnerIDErr = &ManagerErr{"owner ID not uint64 error"}

	NotFollowingStateErr = &ManagerErr{"not following state error"}
	NotLeadingStateErr   = &ManagerErr{"not leading state error"}

	NoRouteErr = &ManagerErr{"no route"}
)

type Proposal struct {
	ID      uint64
	Typ     int64
	OwnerID uint64
	NodeID  uint64
	Value   string
}

type Vote struct {
	Voter      string
	ElectionID int64
	ProposalID int64
	Nominee    string
}

const (
	LookingState   = 0
	SyncingState   = 1
	LeadingState   = 2
	FollowingState = 3
)

type RouteRule struct {
	next string
	tof  int64
}

type RManager struct {
	broadcastRunner utility.Runnable

	muSync sync.RWMutex // lock when syncing, rlock when using

	NodeOwner         sync.Map // [uint64]uint64
	Owners            sync.Map // [uint64]string
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

	localAddr string
	_state    int64

	watchDogRunner utility.Runnable

	remoteTofMaps sync.Map // map[string]map[string]int64, inner map is readonly

	realTofMap   sync.Map         // map[string]int64
	tofMapRead   map[string]int64 // map[string]int64
	muTofMapRead sync.RWMutex

	routeMap       sync.Map // map[string]*RouteRule
	routeMapRead   map[string]*RouteRule
	muRouteMapRead sync.RWMutex

	staticTofCfgFile string

	nominee string

	backupsOwnerMaps []*sync.Map // map[uint64]string

	muElectionID sync.RWMutex
	electionID   int64
}

type ManagerErr struct {
	msg string
}

var _ = (error)((*ManagerErr)(nil))

func (m *RManager) QueryOwner(ctx context.Context, nodeID uint64) (string, error) {
	m.muSync.RLock()
	defer m.muSync.RUnlock()

	logger.If(ctx, "query owner: nodeID=%v", nodeID)
	if ownerOut, ok := m.NodeOwner.Load(nodeID); ok {
		if owner, ok := ownerOut.(uint64); ok {
			addr := m.queryAddr(ctx, owner)
			logger.If(ctx, "query owner success: nodeID=%v, addr=%v", nodeID, addr)
			return addr, nil
		}
		logger.Ef(ctx, "query owner not node error: nodeID=%v", nodeID)
		return "", InternalInvalidOwnerIDErr
	}
	logger.Ef(ctx, "query owner no node error: nodeID=%v", nodeID)
	return "", OwnerNotExistErr
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

func (m *RManager) Allocate(ctx context.Context, ownerID uint64) (uint64, error) {
	if err := m.ErrIfNotLeadingState(ctx); err != nil {
		return 0, err
	}

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
		return nodeID, nil
	}
	return 0, OwnerNotExistErr
}

func (m *RManager) doRemoveNode(ctx context.Context, nodeID uint64) error {
	if out, ok := m.NodeOwner.Load(nodeID); ok {
		if _, ok := out.(uint64); ok {
			m.muNodeMapRead.Lock()
			defer m.muNodeMapRead.Unlock()
			m.NodeOwner.Delete(nodeID)
			delete(m.nodeMapRead, nodeID)
			return nil
		}
	}
	return NodeNotExistErr
}

func (m *RManager) Deallocate(ctx context.Context, nodeID uint64) (err error) {
	if err := m.ErrIfNotLeadingState(ctx); err != nil {
		return err
	}

	m.muSync.RLock()
	defer m.muSync.RUnlock()

	if err = m.doRemoveNode(ctx, nodeID); err != nil {
		return err
	}

	m.proposalChan <- &Proposal{
		Typ:    RemoveNodeProposalType,
		NodeID: nodeID,
	}
	return nil
}

func (m *RManager) doAddOwner(ctx context.Context, ownerID uint64, addr string) error {
	if addr == "" {
		logger.Ef(ctx, "invalid empty addr error", "ownerID", ownerID, "addr", addr)
		return InvalidAddrErr
	}

	m.muOwnerMapRead.Lock()
	defer m.muOwnerMapRead.Unlock()

	m.Owners.Store(ownerID, addr)
	m.ownerMapRead[ownerID] = addr
	return nil
}

// RegisterOwner return 0 if err
func (m *RManager) RegisterOwner(ctx context.Context, addr string) (uint64, error) {
	if err := m.ErrIfNotLeadingState(ctx); err != nil {
		return 0, err
	}

	m.muSync.RLock()
	defer m.muSync.RUnlock()

	ownerID := m.ownerAllocator.Allocate()
	if ownerID <= MaxOwnerID {
		err := m.doAddOwner(ctx, ownerID, addr)
		if err != nil {
			return 0, err
		}
		m.saveDefaultDirectRoute(ctx, addr)
		m.proposalChan <- &Proposal{
			Typ:     AddOwnerProposalType,
			OwnerID: ownerID,
			Value:   addr,
		}

		return ownerID, nil
	}
	return 0, OutOfOwnerIDErr
}

func (m *RManager) doRemoveOwner(ctx context.Context, ownerID uint64) error {
	m.muOwnerMapRead.Lock()
	defer m.muOwnerMapRead.Unlock()
	if _, ok := m.Owners.Load(ownerID); ok {
		m.Owners.Delete(ownerID)
		delete(m.ownerMapRead, ownerID)
		return nil
	}
	return OwnerNotExistErr
}

func (m *RManager) RemoveOwner(ctx context.Context, ownerID uint64) (err error) {
	if err := m.ErrIfNotLeadingState(ctx); err != nil {
		return err
	}

	m.muSync.RLock()
	defer m.muSync.RUnlock()

	if err = m.doRemoveOwner(ctx, ownerID); err != nil {
		return err
	}

	m.proposalChan <- &Proposal{
		Typ:     RemoveOwnerProposalType,
		OwnerID: ownerID,
	}
	return nil
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
func (m *RManager) AllocateRoot(ctx context.Context, ownerID uint64) error {
	if err := m.ErrIfNotLeadingState(ctx); err != nil {
		return err
	}

	m.muSync.RLock()
	defer m.muSync.RUnlock()

	if _, ok := m.Owners.Load(ownerID); ok {
		if _, loaded := m.NodeOwner.LoadOrStore(RootNodeID, ownerID); !loaded {
			logger.If(ctx, "root allocated: ownerID=%v", ownerID)
			return nil
		}
		return RootAllocatedError
	}
	return OwnerNotExistErr
}

func (m *RManager) loadManager(ctx context.Context, pbm *pb.Manager) {
	m.muSync.Lock()
	defer m.muSync.Unlock()

	m.Owners = sync.Map{}
	m.ownerMapRead = make(map[uint64]string)
	if pbm.Owners != nil {
		m.ownerMapRead = pbm.Owners
		for k, v := range pbm.Owners {
			m.Owners.Store(k, v)
		}
	}

	m.NodeOwner = sync.Map{}
	m.nodeMapRead = make(map[uint64]uint64)
	if pbm.Nodes != nil {
		m.nodeMapRead = m.nodeMapRead
		for k, v := range pbm.Nodes {
			m.NodeOwner.Store(k, v)
		}
	}

	m.ownerAllocator.SetNext(pbm.NextOwner)
	m.nodeAllocator.SetNext(pbm.NextNode)
	m.proposalAllocator.SetNext(pbm.NextProposal)

	m.masterAddr = pbm.MasterAddr
}

func (m *RManager) fetchManager(ctx context.Context) {
	defer utility.RecoverWithStack(ctx, nil)

	if m.fp == nil {
		logger.E(ctx, "no fp, cannot fetch")
		return
	}

	logger.W(ctx, "will fetch manager")
	pbm, err := m.fp.CopyManager(ctx)
	if err != nil {
		logger.E(ctx, "fetch manager error", "err", err)
		return
	}
	m.loadManager(ctx, pbm)
}

func (m *RManager) AnswerProposal(ctx context.Context, addr string, proposal *Proposal) (state int64, err error) {
	if err := m.ErrIfNotFollowingState(ctx); err != nil {
		return ErrorProposalState, err
	}

	m.muSync.RLock()
	defer m.muSync.RUnlock()

	switch proposal.Typ {
	case AddOwnerProposalType:
		if err = m.doAddOwner(ctx, proposal.OwnerID, proposal.Value); err == nil {
			m.ownerAllocator.SetNext(proposal.OwnerID + 1)
		}
	case RemoveOwnerProposalType:
		err = m.doRemoveOwner(ctx, proposal.OwnerID)
	case AddNodeProposalType:
		m.doAddNode(ctx, proposal.NodeID, proposal.OwnerID)
		m.nodeAllocator.SetNext(proposal.NodeID + 1)
	case RemoveNodeProposalType:
		err = m.doRemoveNode(ctx, proposal.NodeID)
	default:
		err = &ManagerErr{fmt.Sprintf("invalid proposal type: proposal=%+v", proposal)}
	}

	if err != nil {
		logger.E(ctx, "answer proposal error", "proposal", proposal, "err", err)
		return ErrorProposalState, err
	}

	if m.proposalAllocator.ReadNext() != proposal.ID {
		logger.W(ctx, "answer proposal need fetch", "proposal", proposal)
		go m.fetchManager(utility.WithoutCancel(ctx))
	}

	m.proposalAllocator.SetNext(proposal.ID + 1)
	return 0, nil
}

func (m *RManager) runBroadcast(ctx context.Context) (err error) {
	for {
		select {
		case <-m.broadcastRunner.ToStop:
			logger.If(ctx, "runnable is quitting: name=%v", m.broadcastRunner.Name)
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

func (m *RManager) SetState(ctx context.Context, state int64) {
	atomic.StoreInt64(&m._state, state)
}

func (m *RManager) State(ctx context.Context) int64 {
	return atomic.LoadInt64(&m._state)
}

func (m *RManager) enterNewElection(ctx context.Context, newElectionID int64) {
	m.muElectionID.Lock()
	defer m.muElectionID.Unlock()

	if newElectionID == newElectionIDByIncr {
		if m.State(ctx) == LookingState {
			logger.E(ctx, "incr election ID on looking state err")
			return
		}
		m.SetState(ctx, LookingState)
		newElectionID = m.electionID + 1
	}

	m.electionID = newElectionID
	// TODO: invalidate old votes, re-send votes
}

func (m *RManager) ElectionID(ctx context.Context) int64 {
	m.muElectionID.Lock()
	defer m.muElectionID.Unlock()
	return m.electionID
}

func (m *RManager) ErrIfNotLeadingState(ctx context.Context) error {
	if m.State(ctx) != LeadingState {
		return NotLeadingStateErr
	}
	return nil
}

func (m *RManager) ErrIfNotFollowingState(ctx context.Context) error {
	if m.State(ctx) != FollowingState {
		return NotFollowingStateErr
	}
	return nil
}

func (m *RManager) CopyManager(ctx context.Context) (*pb.Manager, error) {
	if err := m.ErrIfNotLeadingState(ctx); err != nil {
		return nil, err
	}

	m.muSync.Lock()
	defer m.muSync.Unlock()

	copiedNodes := make(map[uint64]uint64)
	copiedOwners := make(map[uint64]string)
	func() {
		for k, v := range m.nodeMapRead {
			copiedNodes[k] = v
		}
	}()
	func() {
		for k, v := range m.ownerMapRead {
			copiedOwners[k] = v
		}
	}()
	return &pb.Manager{
		Err:          nil,
		Owners:       copiedOwners,
		Nodes:        copiedNodes,
		NextOwner:    m.ownerAllocator.ReadNext(),
		NextNode:     m.nodeAllocator.ReadNext(),
		NextProposal: m.proposalAllocator.ReadNext(),
		MasterAddr:   m.MasterAddr(),
	}, nil
}

func (m *RManager) Route(addr string) (string, error) {
	if out, ok := m.routeMap.Load(addr); ok {
		if route, ok := out.(*RouteRule); ok {
			return route.next, nil
		}
	}
	return "", NoRouteErr
}

func (m *RManager) LogicTof(addr string) (int64, bool) {
	if out, ok := m.routeMap.Load(addr); ok {
		return out.(*RouteRule).tof, true
	}
	return 0, false
}

func (m *RManager) saveRealTof(ctx context.Context, addr string, tof int64) {
	m.realTofMap.Store(addr, tof)
	func() {
		m.muTofMapRead.Lock()
		defer m.muTofMapRead.Unlock()
		m.tofMapRead[addr] = tof
	}()

	if _, ok := m.routeMap.Load(addr); !ok {
		m.saveRoute(ctx, addr, addr, tof)
	}
}

func (m *RManager) realTof(addr string) (int64, bool) {
	if distance, ok := m.realTofMap.Load(addr); ok {
		return distance.(int64), true
	}
	return 0, false
}

func (m *RManager) CopyTofMap(ctx context.Context) (copied map[string]int64) {
	m.muTofMapRead.RLock()
	defer m.muTofMapRead.RUnlock()
	copied = make(map[string]int64, len(m.tofMapRead))
	for k, v := range m.tofMapRead {
		copied[k] = v
	}
	return copied
}

func (m *RManager) smoothTof(ctx context.Context, addr string, rawTof int64) (smoothTof int64) {
	smoothTof = rawTof
	if oldTof, ok := m.realTof(addr); ok {
		smoothTof = int64(float64(oldTof)*(1-tofUpdateRatio) + float64(rawTof)*tofUpdateRatio)
	}
	return smoothTof
}

func (m *RManager) deleteRoute(ctx context.Context, addr string) {
	m.routeMap.Delete(addr)
	m.muRouteMapRead.Lock()
	defer m.muRouteMapRead.Unlock()
	delete(m.routeMapRead, addr)
}

func (m *RManager) saveDefaultDirectRoute(ctx context.Context, addr string) {
	m.saveRoute(ctx, addr, addr, math.MaxInt64)
}

func (m *RManager) saveRoute(ctx context.Context, addr string, next string, tof int64) {
	rule := &RouteRule{
		next: addr,
		tof:  tof,
	}
	m.muRouteMapRead.Lock()
	defer m.muRouteMapRead.Unlock()
	m.saveRouteWithoutLock(ctx, addr, rule)
}

func (m *RManager) saveRouteWithoutLock(ctx context.Context, addr string, rule *RouteRule) {
	m.routeMap.Store(addr, rule)
	m.routeMapRead[addr] = rule
}

// do not need lock
func (m *RManager) findRoute(ctx context.Context, dst string) *RouteRule {
	if tof, ok := m.realTof(dst); ok {
		m.saveRoute(ctx, dst, dst, tof)
		return &RouteRule{
			next: dst,
			tof:  tof,
		}
	}

	shortestRule := &RouteRule{
		next: dst,
		tof:  math.MaxInt64,
	}
	for transitAddr, transitRule := range m.routeMapRead {
		out, ok := m.remoteTofMaps.Load(transitAddr)
		if !ok {
			logger.W(ctx, "non remoteTofMap but has tofMap", "transitAddr", transitAddr)
			continue
		}

		remoteTofMap, ok := out.(map[string]int64)
		if !ok {
			logger.E(ctx, "remoteTofMap not map error",
				"transitAddr", transitAddr, "remoteTofMap", remoteTofMap)
			continue
		}

		remoteTof, ok := remoteTofMap[dst]
		if !ok {
			logger.W(ctx, "remote has offline owner", "transitAddr", transitAddr, "dst", dst)
			continue
		}

		if totalTof := remoteTof + transitRule.tof; totalTof < shortestRule.tof {
			shortestRule.tof = totalTof
		}
	}

	if shortestRule.tof == math.MaxInt64 {
		return nil
	}

	return shortestRule
}

func (m *RManager) turnIntoElectionState(ctx context.Context) {
	if m.State(ctx) == LookingState {
		logger.W(ctx, "already in election")
		return
	}

	logger.W(ctx, "start election")
	m.enterNewElection(ctx, newElectionIDByIncr)
}

func (m *RManager) updateOldTransit(ctx context.Context, transitAddr string, transitTof int64, remoteTofMap map[string]int64) {
	m.muRouteMapRead.Lock()
	defer m.muRouteMapRead.Unlock()
	for dst, rule := range m.routeMapRead {
		if rule.next == transitAddr {
			if remoteTof, ok := remoteTofMap[dst]; ok {
				rule.tof = remoteTof + transitTof
				m.saveRouteWithoutLock(ctx, dst, rule)
				continue
			}

			if rule = m.findRoute(ctx, dst); rule != nil {
				m.saveRouteWithoutLock(ctx, dst, rule)
			}

			logger.E(ctx, "owner offline", "dst", dst)
			m.deleteRoute(ctx, dst)
			if dst == m.MasterAddr() {
				m.turnIntoElectionState(ctx)
			}
		}
	}
}

func (m *RManager) addNewTransit(ctx context.Context, transitAddr string, transitTof int64, remoteTofMap map[string]int64) {
	for dst, remoteTof := range remoteTofMap {
		if out, ok := m.routeMap.Load(dst); ok {
			if rule, ok := out.(*RouteRule); ok {
				totalTof := transitTof + remoteTof
				if rule.tof > totalTof {
					m.saveRoute(ctx, dst, transitAddr, totalTof)
					logger.If(ctx, "save new rule: dst=%v, transit=%v, transitTof=%v, totalTof=%v, oldRule=%+v",
						dst, transitAddr, transitTof, totalTof, *rule)
				}
				continue
			}
			logger.Ef(ctx, "non-rule error: dst=%v, out=%+v", dst, out)
		}
	}
}

func (m *RManager) loadStaticTof(ctx context.Context) (staticTofMap map[string]int64) {
	staticTofMap = make(map[string]int64)
	if len(m.staticTofCfgFile) > 0 {
		bytes, err := ioutil.ReadFile(m.staticTofCfgFile)
		if err != nil {
			logger.Ef(ctx, "load static tof read file error: path=%v, err=%+v", m.staticTofCfgFile, err)
			return
		}
		if err := yaml.Unmarshal(bytes, staticTofMap); err != nil {
			logger.Ef(ctx, "load static tof unmarshal error: path=%v, err=%+v", m.staticTofCfgFile, err)
			return
		}
	}
	return
}

func (m *RManager) randomOtherOwner(ctx context.Context) (addr string) {
	m.muTofMapRead.RLock()
	defer m.muTofMapRead.RUnlock()

	for addr, _ := range m.tofMapRead {
		if addr != m.localAddr {
			return addr
		}
	}
	return ""
}

func (m *RManager) getBackupAddrs(ctx context.Context, nodeID uint64) (addrs []string) {
	for i, backupOwnerMap := range m.backupsOwnerMaps {
		if addr := m.randomOtherOwner(ctx); addr != "" && !utility.InStringSlice(addr, addrs) {
			if out, loaded := backupOwnerMap.LoadOrStore(nodeID, addr); loaded {
				addr = out.(string)
			}
			logger.If(ctx, "get backup addrs: i=%v, addr=%v", i, addr)
			addrs = append(addrs, addr)
		} else {
			break
		}
	}
	return addrs
}

func (m *RManager) isMasterAlive(ctx context.Context) (ok bool) {
	_, err := m.fp.Measure(ctx, m.MasterAddr())
	return err == nil
}

func (m *RManager) AcceptVote(ctx context.Context, addr string, vote *Vote) (masterAddr string, err error) {
	if m.State(ctx) != LookingState {
		if m.isMasterAlive(ctx) {
			return m.MasterAddr(), nil
		}

		m.turnIntoElectionState(ctx)
		return "", nil
	}

	if vote.ElectionID > m.ElectionID(ctx) {
		m.enterNewElection(ctx, vote.ElectionID)
	}

	// TODO: handle current election vote
	return "", nil
}

func (m *RManager) AnswerGossip(ctx context.Context, addr string) (tofMap map[string]int64, nominee string, err error) {
	return m.CopyTofMap(ctx), m.nominee, nil
}

func (m *RManager) runWatchDogLoop(ctx context.Context) (err error) {
	logger.I(ctx, "updating tof map")
	owners, err := m.fp.GetOwnerMap(ctx)
	if err != nil {
		logger.E(ctx, "get owners error", "err", err)
	}

	staticTofMap := m.loadStaticTof(ctx)
	nomineeMap := make(map[string]int64, len(owners))

	for _, addr := range owners {
		rawTof, ok := staticTofMap[addr]
		if ok {
			logger.I(ctx, "use static tof", "addr", addr, "rawTof", rawTof)
		} else {
			rawTof, err = m.fp.Measure(ctx, addr)
			if err != nil {
				logger.E(ctx, "ping error", "addr", addr, "rawTof", rawTof, "err", err)
				continue
			}
			logger.I(ctx, "ping success", "addr", addr, "rawTof", rawTof)
		}
		smoothTof := m.smoothTof(ctx, addr, rawTof)
		m.saveRealTof(ctx, addr, smoothTof)

		if addr == m.localAddr {
			continue
		}

		m.remoteTofMaps.Delete(addr)
		remoteTofMap, nominee, err := m.fp.Gossip(ctx, addr)
		if err != nil {
			logger.E(ctx, "gossip error", "addr", addr, "err", err)
			continue
		}
		logger.I(ctx, "gossip success", "addr", addr, "remoteTofMap", remoteTofMap)
		m.remoteTofMaps.Store(addr, remoteTofMap)

		m.addNewTransit(ctx, addr, smoothTof, remoteTofMap)
		m.updateOldTransit(ctx, addr, smoothTof, remoteTofMap)

		if nominee != "" {
			if counter, ok := nomineeMap[nominee]; ok {
				nomineeMap[nominee] = counter + 1
			} else {
				nomineeMap[nominee] = 1
			}
		}
	}

	for nominee, poll := range nomineeMap {
		if poll<<1 > int64(len(owners)) {
			m.SetMaster(nominee)
		}
	}
	return nil
}

func (m *RManager) Start(ctx context.Context) {
	m.broadcastRunner.Start(ctx)
	m.watchDogRunner.Start(ctx)
}

func (m *RManager) Stop(ctx context.Context) {
	m.broadcastRunner.Stop(ctx)
	m.watchDogRunner.Stop(ctx)
}

// NewRManager do not register or allocate
func NewRManager(ctx context.Context, localAddr string, masterAddr string, staticTofCfgFile string, backupSize int) *RManager {
	ma := &RManager{
		nodeAllocator:     idallocator.NewIDAllocator(RootNodeID + 1), // since root is assigned by AllocateRoot, not allocated
		ownerAllocator:    idallocator.NewIDAllocator(FirstOwnerID),
		proposalAllocator: idallocator.NewIDAllocator(FirstProposalID),
		ownerMapRead:      make(map[uint64]string),
		nodeMapRead:       make(map[uint64]uint64),
		proposalChan:      make(chan *Proposal),
		localAddr:         localAddr,
		masterAddr:        masterAddr,
		tofMapRead:        make(map[string]int64),
		routeMapRead:      make(map[string]*RouteRule),
		staticTofCfgFile:  staticTofCfgFile,
		backupsOwnerMaps:  make([]*sync.Map, backupSize),
	}
	switch masterAddr {
	case localAddr:
		ma.SetState(ctx, LeadingState)
	case "":
		ma.SetState(ctx, LookingState)
	default:
		ma.SetState(ctx, FollowingState)
		ma.saveDefaultDirectRoute(ctx, masterAddr)
	}
	ma.saveDefaultDirectRoute(ctx, localAddr)
	ma.broadcastRunner.InitRunnable(ctx, maRunnableName, maRunnableLoopInterval, nil, ma.runBroadcast)
	ma.watchDogRunner.InitRunnable(ctx, wdRunnableName, wdRunnableLoopInterval, ma.runWatchDogLoop, nil)
	return ma
}

func (e *ManagerErr) Error() string {
	return fmt.Sprintf("manager error: %v", e.msg)
}
