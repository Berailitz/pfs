package fbackend

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/im7mortal/kmutex"

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
	AddOwnerProposalType       = 1
	RemoveOwnerProposalType    = 2
	AddNodeProposalType        = 3
	RemoveNodeProposalType     = 4
	SetBackupAddrsProposalType = 5
	UpdateOwnerIDProposalType  = 6
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
	voteRunnableName         = "voter"
	voteRunnableLoopInterval = time.Duration(0)
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
	ID      uint64   `json:"id"`
	Typ     int64    `json:"typ"`
	OwnerID uint64   `json:"owner_id"`
	NodeID  uint64   `json:"node_id"`
	Strs    []string `json:"strs"`
	Value   string   `json:"value"`
}

const (
	EmptyProposalID = 0
)

type Vote struct {
	Voter      string `json:"voter"`
	ElectionID int64  `json:"election_id"`
	ProposalID uint64 `json:"proposal_id"`
	Nominee    string `json:"nominee"`
}

const (
	LookingState   = 0
	SyncingState   = 1
	LeadingState   = 2
	FollowingState = 3
)

type RouteRule struct {
	next string `json:"next"`
	tof  int64  `json:"tof"`
}

const (
	ballotTimeToLive = time.Second * 30
	minBallotBoxSize = 2
)

type Ballot struct {
	Nominee  string    `json:"nominee"`
	Deadline time.Time `json:"deadline"`
}

type ManagerSummary struct {
	pb.Manager
	LocalAddr       string               `json:"local_addr"`
	State           int64                `json:"state"`
	TofMap          map[string]int64     `json:"tof_map"`
	RouteMap        map[string]RouteRule `json:"route_map"`
	BackupOwnerMaps map[uint64][]string  `json:"backups_owner_maps"`
	ElectionID      int64                `json:"election_id"`
	Ballots         map[string]Ballot    `json:"ballots"`
	NominateMap     map[string]int64     `json:"nominate_map"`
	Vote            Vote                 `json:"vote"`
}

type RManager struct {
	broadcastRunner utility.Runnable
	voteRunner      utility.Runnable

	muSync sync.RWMutex // lock when syncing, rlock when using

	NodeOwner         utility.IterableMap // [uint64]uint64
	Owners            utility.IterableMap // [uint64]string
	nodeAllocator     *idallocator.IDAllocator
	ownerAllocator    *idallocator.IDAllocator
	proposalAllocator *idallocator.IDAllocator

	proposalChan chan *Proposal

	masterAddr atomic.Value

	fp *FProxy

	localAddr string
	_state    int64

	watchDogRunner utility.Runnable

	remoteTofMaps utility.IterableMap // map[string]map[string]int64, inner map is readonly

	realTofMap utility.IterableMap // map[string]int64

	routeMap utility.IterableMap // map[string]*RouteRule

	staticTofCfgFile string

	backupSize       int
	backupOwnerMap   utility.IterableMap // map[uint64][]string
	muBackupOwnerMap *kmutex.Kmutex

	muElectionID sync.RWMutex
	electionID   int64

	// muBallots protects ballots and nomineeMap
	muBallots  sync.RWMutex
	ballots    map[string]*Ballot
	nomineeMap map[string]int64

	muVote sync.RWMutex
	vote   Vote

	voteChan chan *Vote
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

func (m *RManager) queryOwnerID(ctx context.Context, addr string) (ownerID uint64) {
	m.Owners.Range(func(key, value interface{}) bool {
		if value.(string) == addr {
			ownerID = key.(uint64)
			return false
		}

		logger.I(ctx, "owner found by addr", "addr", addr, "ownerID", ownerID)
		return true
	})

	logger.E(ctx, "owner not found by addr", "addr", addr)
	return 0
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
	m.NodeOwner.Store(nodeID, ownerID)
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
			m.NodeOwner.Delete(nodeID)
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

	m.Owners.Store(ownerID, addr)
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
	if _, ok := m.Owners.Load(ownerID); ok {
		m.Owners.Delete(ownerID)
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

	output := make(map[uint64]string)
	m.Owners.Range(func(key, value interface{}) bool {
		output[key.(uint64)] = value.(string)
		return true
	})
	return output
}

func (m *RManager) MasterAddr() string {
	return m.masterAddr.Load().(string)
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

	m.Owners = utility.IterableMap{}
	if pbm.Owners != nil {
		for k, v := range pbm.Owners {
			m.Owners.Store(k, v)
		}
	}

	m.NodeOwner = utility.IterableMap{}
	if pbm.Nodes != nil {
		for k, v := range pbm.Nodes {
			m.NodeOwner.Store(k, v)
		}
	}

	m.ownerAllocator.SetNext(pbm.NextOwner)
	m.nodeAllocator.SetNext(pbm.NextNode)
	m.proposalAllocator.SetNext(pbm.NextProposal)

	m.SetMaster(ctx, pbm.MasterAddr)
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
	if proposal.Typ != SetBackupAddrsProposalType {
		if err := m.ErrIfNotFollowingState(ctx); err != nil {
			logger.E(ctx, "not in following state", "proposal", *proposal)
			return ErrorProposalState, err
		}
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
	case SetBackupAddrsProposalType:
		err = m.setBackupAddrs(ctx, proposal.NodeID, proposal.Strs)
	case UpdateOwnerIDProposalType:
		m.doAddNode(ctx, proposal.NodeID, proposal.OwnerID)
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
	currentOwnerMap := m.CopyOwnerMap(ctx)
	proposal.ID = m.proposalAllocator.Allocate()

	for _, addr := range currentOwnerMap {
		if addr == m.localAddr {
			continue
		}

		if addr != m.MasterAddr() || proposal.Typ == SetBackupAddrsProposalType {
			// TODO handle state and err
			logger.I(ctx, "rpc proposal",
				"addr", addr, "proposal", *proposal)
			_, err := m.fp.SendProposal(ctx, addr, proposal)
			if err != nil {
				logger.If(ctx, "rpc proposal error: addr=%v, proposal=%+v, err=%+v",
					addr, proposal, err)
			}
		}
	}
}

func (m *RManager) SetMaster(ctx context.Context, masterAddr string) {
	m.masterAddr.Store(masterAddr)
}

func (m *RManager) SetFP(fp *FProxy) {
	m.fp = fp
}

func (m *RManager) SetState(ctx context.Context, state int64) {
	logger.W(ctx, "set state", "state", state)
	atomic.StoreInt64(&m._state, state)
}

func (m *RManager) State(ctx context.Context) int64 {
	return atomic.LoadInt64(&m._state)
}

func (m *RManager) CopyVote(ctx context.Context) *Vote {
	m.muVote.RLock()
	defer m.muVote.RUnlock()
	copiedVote := m.vote
	return &copiedVote
}

func (m *RManager) SetVote(ctx context.Context, nominee string, proposalID uint64) {
	m.muVote.Lock()
	defer m.muVote.Unlock()
	m.vote.ElectionID = m.electionID
	if proposalID != EmptyProposalID {
		m.vote.ProposalID = proposalID
	} else {
		m.vote.ProposalID = m.proposalAllocator.ReadNext()
	}
	m.vote.Nominee = nominee
}

func (m *RManager) clearBallots(ctx context.Context) {
	m.muBallots.Lock()
	defer m.muBallots.Unlock()

	m.ballots = make(map[string]*Ballot)
	m.nomineeMap = make(map[string]int64)
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
	m.clearBallots(ctx)

	m.SetVote(ctx, m.localAddr, EmptyProposalID)
	m.voteChan <- m.CopyVote(ctx)
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
		m.NodeOwner.Range(func(key, value interface{}) bool {
			copiedNodes[key.(uint64)] = value.(uint64)
			return true
		})
	}()
	func() {
		m.Owners.Range(func(key, value interface{}) bool {
			copiedOwners[key.(uint64)] = value.(string)
			return true
		})
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
	copied = make(map[string]int64, m.realTofMap.Len())
	m.realTofMap.Range(func(key, value interface{}) bool {
		copied[key.(string)] = value.(int64)
		return true
	})
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
}

func (m *RManager) saveDefaultDirectRoute(ctx context.Context, addr string) {
	m.saveRoute(ctx, addr, addr, math.MaxInt64)
}

func (m *RManager) saveRoute(ctx context.Context, addr string, next string, tof int64) {
	rule := &RouteRule{
		next: addr,
		tof:  tof,
	}
	m.saveRouteWithoutLock(ctx, addr, rule)
}

func (m *RManager) saveRouteWithoutLock(ctx context.Context, addr string, rule *RouteRule) {
	m.routeMap.Store(addr, rule)
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
	m.routeMap.Range(func(key, value interface{}) bool {
		transitAddr := key.(string)
		transitRule := value.(*RouteRule)
		out, ok := m.remoteTofMaps.Load(transitAddr)
		if !ok {
			logger.W(ctx, "non remoteTofMap but has tofMap", "transitAddr", transitAddr)
			return true
		}

		remoteTofMap, ok := out.(map[string]int64)
		if !ok {
			logger.E(ctx, "remoteTofMap not map error",
				"transitAddr", transitAddr, "remoteTofMap", remoteTofMap)
			return true
		}

		remoteTof, ok := remoteTofMap[dst]
		if !ok {
			logger.W(ctx, "remote has offline owner", "transitAddr", transitAddr, "dst", dst)
			return true
		}

		if totalTof := remoteTof + transitRule.tof; totalTof < shortestRule.tof {
			shortestRule.tof = totalTof
		}
		return true
	})

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
	m.routeMap.Range(func(key, value interface{}) bool {
		dst := key.(string)
		rule := value.(*RouteRule)
		if rule.next == transitAddr {
			if remoteTof, ok := remoteTofMap[dst]; ok {
				rule.tof = remoteTof + transitTof
				m.saveRouteWithoutLock(ctx, dst, rule)
				return true
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
		return true
	})
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

func (m *RManager) randomBackupOwners(ctx context.Context, addrs []string, size int) []string {
	m.realTofMap.Range(func(key, value interface{}) bool {
		if len(addrs) >= size {
			return false
		}

		if skey := key.(string); skey != m.localAddr && !utility.InStringSlice(skey, addrs) {
			addrs = append(addrs, skey)
		}
		return true
	})
	return addrs
}

func (m *RManager) setBackupAddrs(ctx context.Context, nodeID uint64, addrs []string) error {
	m.backupOwnerMap.Store(nodeID, addrs)
	return nil
}

func (m *RManager) getBackupAddrs(ctx context.Context, nodeID uint64) (addrs []string) {
	m.muBackupOwnerMap.Lock(nodeID)
	defer m.muBackupOwnerMap.Unlock(nodeID)

	if out, ok := m.backupOwnerMap.Load(nodeID); ok {
		addrs = out.([]string)
	}

	addrs = m.randomBackupOwners(ctx, addrs, m.backupSize)
	m.proposalChan <- &Proposal{
		Typ:    SetBackupAddrsProposalType,
		NodeID: nodeID,
		Strs:   addrs,
	}
	logger.I(ctx, "get backup addrs", "nodeID", nodeID, "addrs", addrs)
	m.backupOwnerMap.Store(nodeID, addrs)
	return addrs
}

func (m *RManager) isMasterAlive(ctx context.Context) (ok bool) {
	_, err := m.fp.Measure(ctx, m.MasterAddr())
	return err == nil
}

func (m *RManager) recountVotesWithoutLock(ctx context.Context) {
	logger.W(ctx, "recounting votes")
	m.nomineeMap = make(map[string]int64)
	for _, ballot := range m.ballots {
		m.addVoteWithoutLock(ctx, ballot.Nominee, 1)
	}
}

func (m *RManager) addVoteWithoutLock(ctx context.Context, nominee string, addition int64) (isNewNominee bool) {
	if oldPoll, ok := m.nomineeMap[nominee]; ok {
		newPoll := oldPoll + addition
		if newPoll < 0 {
			logger.E(ctx, "poll reached minus", "nominee", nominee, "addition", addition)
			newPoll = 0
		}
		m.nomineeMap[nominee] = newPoll
		return false
	}
	return true
}

func (m *RManager) saveVote(ctx context.Context, vote *Vote) {
	m.muBallots.Lock()
	defer m.muBallots.Unlock()

	if ballot, ok := m.ballots[vote.Voter]; ok {
		logger.I(ctx, "revoke vote", "vote", vote)
		if m.addVoteWithoutLock(ctx, ballot.Nominee, -1) {
			logger.E(ctx, "old nominee not exist error")
			m.recountVotesWithoutLock(ctx)
		}
	}

	logger.I(ctx, "save new vote", "vote", vote)
	m.ballots[vote.Voter] = &Ballot{
		Nominee:  vote.Nominee,
		Deadline: time.Now().Add(ballotTimeToLive),
	}
	m.addVoteWithoutLock(ctx, vote.Nominee, 1)
}

func (m *RManager) sweepOldBallots(ctx context.Context) {
	logger.I(ctx, "sweep old ballots")

	m.muBallots.Lock()
	defer m.muBallots.Unlock()

	now := time.Now()
	for voter, ballot := range m.ballots {
		if ballot.Deadline.Before(now) {
			logger.W(ctx, "sweep old ballot", "ballot", ballot, "voter", voter)
			delete(m.ballots, voter)
			if m.addVoteWithoutLock(ctx, ballot.Nominee, -1) {
				logger.E(ctx, "old ballot with non-exists nominee", "ballot", ballot, "voter", voter)
			}
		}
	}
}

func (m *RManager) replaceUnreachableNodes(ctx context.Context) {
	m.muSync.Lock()
	defer m.muSync.Unlock()

	m.NodeOwner.Range(func(key, value interface{}) bool {
		nodeID := key.(uint64)
		oldOwnerID := value.(uint64)
		oldOwnerAddr := m.queryAddr(ctx, oldOwnerID)
		if _, err := m.Route(oldOwnerAddr); err != NoRouteErr {
			return true
		}

		backupAddrs := m.getBackupAddrs(ctx, nodeID)
		// TODO: compare versions
		for _, backupAddr := range backupAddrs {
			if _, err := m.Route(backupAddr); err != NoRouteErr {
				backupOwnerID := m.queryOwnerID(ctx, backupAddr)
				if backupOwnerID == 0 {
					continue
				}

				if err := m.fp.MakeRegular(ctx, backupAddr, nodeID); err != nil {
					logger.E(ctx, "replace make regular error",
						"backupAddr", backupAddr, "backupOwnerID", backupOwnerID, "nodeID", nodeID, "err", err)
					continue
				}

				m.doAddNode(ctx, nodeID, backupOwnerID)
				m.proposalChan <- &Proposal{
					Typ:     UpdateOwnerIDProposalType,
					OwnerID: backupOwnerID,
					NodeID:  nodeID,
				}
				return true
			}
		}

		logger.E(ctx, "node lost", "nodeID", nodeID, "oldOwnerID", oldOwnerID, "oldOwnerAddr", oldOwnerAddr)
		_ = m.doRemoveNode(ctx, nodeID)
		m.proposalChan <- &Proposal{
			Typ:    RemoveNodeProposalType,
			NodeID: nodeID,
		}
		return true
	})
}

func (m *RManager) syncWithMaster(ctx context.Context) {
	m.SetState(ctx, SyncingState)
	if m.MasterAddr() == m.localAddr {
		m.SetState(ctx, LeadingState)
		m.replaceUnreachableNodes(ctx)
	} else {
		m.SetState(ctx, FollowingState)
		m.fetchManager(ctx)
	}
}

func (m *RManager) canExitElection(ctx context.Context) bool {
	newMasterAddr := m.doElectionReachAgreement(ctx)
	if newMasterAddr != "" {
		logger.W(ctx, "exit election", "newMasterAddr", newMasterAddr)
		m.SetMaster(ctx, newMasterAddr)
		m.syncWithMaster(ctx)
		return true
	}

	logger.I(ctx, "still in election")
	return false
}

func (m *RManager) doElectionReachAgreement(ctx context.Context) (newMasterAddr string) {
	mapSize := len(m.CopyOwnerMap(ctx))

	if m.State(ctx) != LookingState {
		logger.E(ctx, "not in election error")
		if masterAddr := m.MasterAddr(); masterAddr != "" {
			logger.W(ctx, "process as in agreement", "masterAddr", masterAddr)
			return masterAddr
		}

		logger.E(ctx, "not in election and no masterAddr error")
		return ""
	}

	m.muBallots.Lock()
	defer m.muBallots.Unlock()
	if len(m.nomineeMap) == 1 && len(m.ballots) >= minBallotBoxSize && len(m.ballots)<<1 > mapSize {
		for nominee, _ := range m.nomineeMap {
			return nominee
		}
	}

	return ""
}

func (m *RManager) AcceptVote(ctx context.Context, addr string, vote *Vote) (masterAddr string, err error) {
	if m.State(ctx) != LookingState {
		if m.isMasterAlive(ctx) {
			return m.MasterAddr(), nil
		}

		m.turnIntoElectionState(ctx)
		return "", nil
	}

	if vote.ElectionID < m.ElectionID(ctx) {
		remoteMasterAddr, err := m.fp.Vote(ctx, vote.Voter, m.CopyVote(ctx))
		if err != nil {
			logger.E(ctx, "send vote to elderly remote error", "addr", addr, "vote", vote)
		}
		if remoteMasterAddr != "" {
			logger.W(ctx, "elderly remote has non-empty masterAddr")
		}

		return "", nil
	}

	if vote.ElectionID > m.ElectionID(ctx) {
		m.enterNewElection(ctx, vote.ElectionID)
	}

	m.saveVote(ctx, vote)

	if vote.ProposalID > m.CopyVote(ctx).ProposalID {
		m.SetVote(ctx, vote.Nominee, vote.ProposalID)
	}

	if vote.Nominee < m.CopyVote(ctx).Nominee {
		m.SetVote(ctx, vote.Nominee, EmptyProposalID)
	}

	m.canExitElection(ctx)
	return "", nil
}

func (m *RManager) AnswerGossip(ctx context.Context, addr string) (tofMap map[string]int64, err error) {
	return m.CopyTofMap(ctx), nil
}

func (m *RManager) runWatchDogLoop(ctx context.Context) (err error) {
	m.sweepOldBallots(ctx)

	logger.I(ctx, "updating tof map")
	owners, err := m.fp.GetOwnerMap(ctx)
	if err != nil {
		logger.E(ctx, "get owners error", "err", err)
	}

	staticTofMap := m.loadStaticTof(ctx)
	//nomineeMap := make(map[string]int64, len(owners))

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
		remoteTofMap, err := m.fp.Gossip(ctx, addr)
		if err != nil {
			logger.E(ctx, "gossip error", "addr", addr, "err", err)
			continue
		}
		logger.I(ctx, "gossip success", "addr", addr, "remoteTofMap", remoteTofMap)
		m.remoteTofMaps.Store(addr, remoteTofMap)

		m.addNewTransit(ctx, addr, smoothTof, remoteTofMap)
		m.updateOldTransit(ctx, addr, smoothTof, remoteTofMap)

		//if nominee != "" {
		//	if counter, ok := nomineeMap[nominee]; ok {
		//		nomineeMap[nominee] = counter + 1
		//	} else {
		//		nomineeMap[nominee] = 1
		//	}
		//}
	}

	//for nominee, poll := range nomineeMap {
	//	if poll<<1 > int64(len(owners)) {
	//		m.SetMaster(ctx, nominee)
	//	}
	//}
	if m.State(ctx) == LookingState {
		m.canExitElection(ctx)
	}
	return nil
}

func (m *RManager) useRemoteMasterAddr(ctx context.Context, remoteMasterAddr string, voter string) {
	if _, err := m.fp.Measure(ctx, remoteMasterAddr); err == nil {
		m.saveDefaultDirectRoute(ctx, remoteMasterAddr)
	} else {
		m.saveRoute(ctx, remoteMasterAddr, voter, math.MaxInt64)
	}
	m.SetMaster(ctx, remoteMasterAddr)
}

func (m *RManager) broadcastVote(ctx context.Context, vote *Vote) {
	for _, addr := range m.CopyOwnerMap(ctx) {
		if addr != m.localAddr {
			remoteMasterAddr, err := m.fp.Vote(ctx, addr, vote)
			if err != nil {
				logger.If(ctx, "rpc vote error: addr=%v, vote=%+v, err=%+v",
					addr, vote, err)
			}

			if remoteMasterAddr != "" {
				m.useRemoteMasterAddr(ctx, remoteMasterAddr, addr)
			}
		}
	}
}

func (m *RManager) runVoter(ctx context.Context) (err error) {
	for {
		select {
		case <-m.voteRunner.ToStop:
			logger.If(ctx, "runnable is quitting: name=%v", m.broadcastRunner.Name)
			return nil
		case vote := <-m.voteChan:
			m.broadcastVote(ctx, vote)
		}
	}
}

func (m *RManager) CopyRouteMap(ctx context.Context) map[string]RouteRule {
	r := make(map[string]RouteRule)
	m.routeMap.Range(func(key, value interface{}) bool {
		r[key.(string)] = *value.(*RouteRule)
		return true
	})
	return r
}

func (m *RManager) CopyBackupOwnerMap(ctx context.Context) map[uint64][]string {
	r := make(map[uint64][]string)
	m.backupOwnerMap.Range(func(key, value interface{}) bool {
		r[key.(uint64)] = value.([]string)
		return true
	})
	return r
}

func (m *RManager) CopyBallots(ctx context.Context) map[string]Ballot {
	m.muBallots.RLock()
	defer m.muBallots.RUnlock()

	r := make(map[string]Ballot)
	for k, v := range m.ballots {
		r[k] = *v
	}
	return r
}

func (m *RManager) CopyNomineeMap(ctx context.Context) map[string]int64 {
	m.muBallots.RLock()
	defer m.muBallots.RUnlock()

	r := make(map[string]int64)
	for k, v := range m.nomineeMap {
		r[k] = v
	}
	return r
}

func (m *RManager) Summary(ctx context.Context) *ManagerSummary {
	pbm, err := m.CopyManager(ctx)
	if err != nil {
		return nil
	}

	return &ManagerSummary{
		Manager:         *pbm,
		LocalAddr:       m.localAddr,
		State:           m.State(ctx),
		TofMap:          m.CopyTofMap(ctx),
		RouteMap:        m.CopyRouteMap(ctx),
		BackupOwnerMaps: m.CopyBackupOwnerMap(ctx),
		ElectionID:      m.ElectionID(ctx),
		Ballots:         m.CopyBallots(ctx),
		NominateMap:     m.CopyNomineeMap(ctx),
		Vote:            *m.CopyVote(ctx),
	}
}

func (m *RManager) Start(ctx context.Context) {
	m.broadcastRunner.Start(ctx)
	m.voteRunner.Start(ctx)
	m.watchDogRunner.Start(ctx)
}

func (m *RManager) Stop(ctx context.Context) {
	m.broadcastRunner.Stop(ctx)
	m.voteRunner.Stop(ctx)
	m.watchDogRunner.Stop(ctx)
}

// NewRManager do not register or allocate
func NewRManager(ctx context.Context, localAddr string, masterAddr string, staticTofCfgFile string, backupSize int) *RManager {
	ma := &RManager{
		nodeAllocator:     idallocator.NewIDAllocator(RootNodeID + 1), // since root is assigned by AllocateRoot, not allocated
		ownerAllocator:    idallocator.NewIDAllocator(FirstOwnerID),
		proposalAllocator: idallocator.NewIDAllocator(FirstProposalID),
		proposalChan:      make(chan *Proposal),
		localAddr:         localAddr,
		staticTofCfgFile:  staticTofCfgFile,
		backupSize:        backupSize,
		muBackupOwnerMap:  kmutex.New(),
		voteChan:          make(chan *Vote),
		vote: Vote{
			Voter:      localAddr,
			ElectionID: 0,
			ProposalID: 0,
			Nominee:    "",
		},
	}
	ma.masterAddr.Store(masterAddr)
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
	ma.voteRunner.InitRunnable(ctx, voteRunnableName, voteRunnableLoopInterval, nil, ma.runVoter)
	ma.watchDogRunner.InitRunnable(ctx, wdRunnableName, wdRunnableLoopInterval, ma.runWatchDogLoop, nil)
	return ma
}

func (e *ManagerErr) Error() string {
	return fmt.Sprintf("manager error: %v", e.msg)
}
