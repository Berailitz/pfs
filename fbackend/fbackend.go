package fbackend

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/Berailitz/pfs/logger"

	"github.com/Berailitz/pfs/utility"

	"bazil.org/fuse"

	"github.com/Berailitz/pfs/idallocator"

	"github.com/Berailitz/pfs/rnode"

	"golang.org/x/sys/unix"
)

type FBackEnd struct {
	uid uint32
	gid uint32

	mu sync.RWMutex

	nodes          utility.IterableMap // [uint64]*rnode.RNode
	redundantNodes utility.IterableMap // [uint64]*rnode.RNode
	localID        uint64

	fp *FProxy

	handleAllocator *idallocator.IDAllocator
	handleMap       utility.IterableMap // map[uint64]uint64

	ma *RManager
}

type FBackEndErr struct {
	msg string
}

var _ = (error)((*FBackEndErr)(nil))

type FBSummary struct {
	Nodes       map[uint64]int64  `json:"nodes"`        // map[nodeID]NVersion
	BackupNodes map[uint64]int64  `json:"backup_nodes"` // map[nodeID]NVersion
	Handles     map[uint64]uint64 `json:"handles"`
}

type internalSetInodeAttributesParam struct {
	Size  *uint64
	Mode  *os.FileMode
	Mtime *time.Time
}

type SetInodeAttributesParam struct {
	Size     uint64
	Mode     os.FileMode
	Mtime    time.Time
	HasSize  bool
	HasMode  bool
	HasMtime bool
}

func NewFBackEnd(
	uid uint32,
	gid uint32,
	allocator *idallocator.IDAllocator,
	ma *RManager) *FBackEnd {
	return &FBackEnd{
		uid:             uid,
		gid:             gid,
		handleAllocator: allocator,
		ma:              ma,
	}
}

func (fb *FBackEnd) SetFP(ctx context.Context, fp *FProxy) {
	if fp == nil {
		logger.Pf(ctx, "set nil fp error")
	}

	fb.fp = fp
	// Set up the root rnode
	fb.registerSelf(ctx, fp.localAddr)
	if err := fb.MakeRoot(ctx); err != nil {
		logger.W(ctx, "make root error, use remote root", "err", err)
	}
}

func (fb *FBackEnd) registerSelf(ctx context.Context, addr string) {
	if fb.localID > 0 {
		logger.Pf(ctx, "duplicate register error: addr=%v", addr)
	}

	localID, err := fb.fp.RegisterOwner(ctx, addr)
	if err != nil {
		logger.P(ctx, "register self error", "addr", addr, "err", err)
	}

	fb.localID = localID
	logger.If(ctx, "register success: addr=%v, localID=%v", addr, localID)
}

func (fb *FBackEnd) doLoadNodeForX(ctx context.Context, id uint64, isRead bool,
	localOnly bool) (node *rnode.RNode, lockID int64, err error) {
	node, err = fb.LoadLocalNode(ctx, id)

	if err != nil && !localOnly {
		logger.I(ctx, "load node load local", "id", id, "isRead", isRead, "err", err)
		node, err = fb.fp.LoadRemoteNode(ctx, id, isRead)
		if err != nil {
			logger.Ef(ctx, "load node load remote error: id=%v, isRead=%v, err=%+v", id, isRead, err)
			return nil, 0, err
		}
	}

	if err != nil {
		logger.Ef(ctx, "load node error: id=%v, isRead=%v, err=%+v", id, isRead, err)
		return nil, 0, err
	}

	if isRead {
		lockID, err = node.RLock(ctx)
	} else {
		lockID, err = node.Lock(ctx)
	}
	if err != nil {
		logger.Ef(ctx, "load node lock error: id=%v, isRead=%v, err=%+v", id, isRead, err)
		return nil, 0, err
	}
	return node, lockID, nil
}

func (fb *FBackEnd) LoadLocalNode(ctx context.Context, id uint64) (*rnode.RNode, error) {
	if out, exist := fb.nodes.Load(id); exist {
		if node, ok := out.(*rnode.RNode); ok {
			return node, nil
		}
		return nil, &FBackEndErr{msg: fmt.Sprintf("load local node non-node error: id=%v", id)}
	}
	return nil, &FBackEndErr{msg: fmt.Sprintf("load local node not exist error: id=%v", id)}
}

func (fb *FBackEnd) LoadLocalNodeForRead(ctx context.Context, id uint64) (node *rnode.RNode, lockID int64, err error) {
	return fb.doLoadNodeForX(ctx, id, true, true)
}

func (fb *FBackEnd) LoadNodeForRead(ctx context.Context, id uint64) (node *rnode.RNode, lockID int64, err error) {
	return fb.doLoadNodeForX(ctx, id, true, false)
}

func (fb *FBackEnd) LoadLocalNodeForWrite(ctx context.Context, id uint64) (node *rnode.RNode, lockID int64, err error) {
	return fb.doLoadNodeForX(ctx, id, false, true)
}

func (fb *FBackEnd) LoadNodeForWrite(ctx context.Context, id uint64) (node *rnode.RNode, lockID int64, err error) {
	return fb.doLoadNodeForX(ctx, id, false, false)
}

func (fb *FBackEnd) pushBackupNode(ctx context.Context, copiedNode rnode.RNode) {
	defer utility.RecoverWithStack(ctx, nil)

	for i, addr := range fb.ma.getBackupAddrs(ctx, copiedNode.ID()) {
		logger.If(ctx, "push backup node: i=%v, node=%v, addr=%v", i, copiedNode, addr)
		if err := fb.fp.PushNode(ctx, addr, &copiedNode); err != nil {
			logger.If(ctx, "push backup node err: i=%v, node=%v, addr=%v, err=%+v", i, copiedNode, addr, err)
		}
	}
}

func (fb *FBackEnd) doUnlockRemoteNode(ctx context.Context, node *rnode.RNode, isRead bool) (err error) {
	id := node.ID()

	if isRead {
		err = fb.fp.RUnlockNode(ctx, id, node.RemoteLockID())
	} else {
		err = fb.fp.UnlockNode(ctx, node, node.RemoteLockID())
	}
	if err != nil {
		logger.Ef(ctx, "unlock remote node error: id=%v, isRead=%v, err=%+v", id, isRead, err)
		return err
	}

	return nil
}

func (fb *FBackEnd) doUnlockNode(ctx context.Context, node *rnode.RNode, lockID int64, isRead bool) error {
	id := node.ID()
	logger.If(ctx, "unlock node: id=%v, isRead=%v", id, isRead)
	if isRead {
		node.RUnlock(ctx, lockID)
	} else {
		node.IncrVersion()
		if fb.IsLocal(ctx, id) {
			go fb.pushBackupNode(ctx, *node)
		}
		node.Unlock(ctx, lockID)
	}
	if fb.IsLocal(ctx, id) {
		logger.If(ctx, "unlock local node success: id=%v, isRead=%v", id, isRead)
		return nil
	}

	return fb.doUnlockRemoteNode(ctx, node, isRead)
}

func (fb *FBackEnd) UnlockNode(ctx context.Context, node *rnode.RNode, lockID int64) error {
	return fb.doUnlockNode(ctx, node, lockID, false)
}

func (fb *FBackEnd) RUnlockNode(ctx context.Context, node *rnode.RNode, lockID int64) error {
	return fb.doUnlockNode(ctx, node, lockID, true)
}

func (fb *FBackEnd) MakeRegular(
	ctx context.Context,
	nodeID uint64) error {
	backupNode, ok := fb.redundantNodes.Load(nodeID)
	if !ok {
		logger.E(ctx, "make regular backup node not exist error", "nodeID", nodeID)
		return &FBackEndErr{fmt.Sprintf("backup node not exist: nodeID=%v", nodeID)}
	}

	if err := fb.storeNode(ctx, nodeID, backupNode.(*rnode.RNode)); err != nil {
		logger.E(ctx, "make regular store backup node error", "nodeID", nodeID)
		return err
	}

	fb.redundantNodes.Delete(nodeID)
	logger.I(ctx, "make regular success", "nodeID", nodeID)
	return nil
}

func (fb *FBackEnd) UpdateNode(ctx context.Context, node *rnode.RNode) error {
	localNode, err := fb.LoadLocalNode(ctx, node.ID())
	if err != nil {
		logger.Ef(ctx, "update node load error: id=%v, err=%v", node.ID(), err)
		return err
	}

	oldLock := localNode.NLock
	*localNode = *node
	localNode.NLock = oldLock
	logger.If(ctx, "update node success: id=%v, err=%v", node.ID(), err)
	return nil
}

func (fb *FBackEnd) IsLocal(ctx context.Context, id uint64) bool {
	if _, err := fb.LoadLocalNode(ctx, id); err == nil {
		return true
	}
	return false
}

func (fb *FBackEnd) storeNode(ctx context.Context, id uint64, node *rnode.RNode) error {
	logger.If(ctx, "store node: id=%v", id)
	if _, loaded := fb.nodes.LoadOrStore(id, node); loaded {
		logger.Ef(ctx, "store node overwrite error: id=%v", id)
	} else {
		logger.If(ctx, "store node success: id=%v", id)
	}
	return nil
}

func (fb *FBackEnd) deleteNode(ctx context.Context, id uint64) error {
	logger.If(ctx, "delete node: id=%v", id)
	if !fb.IsLocal(ctx, id) {
		return &FBackEndErr{fmt.Sprintf("delete node not exist error: id=%v", id)}
	}
	fb.nodes.Delete(id)
	logger.If(ctx, "delete node success: id=%v", id)
	return nil
}

func (fb *FBackEnd) InsertRoot(ctx context.Context) {
	rootAttrs := fuse.Attr{
		Valid:     0,
		Inode:     RootNodeID,
		Size:      0,
		Blocks:    0,
		Atime:     time.Time{},
		Mtime:     time.Time{},
		Ctime:     time.Time{},
		Crtime:    time.Time{},
		Mode:      0666 | os.ModeDir,
		Nlink:     1,
		Uid:       fb.uid,
		Gid:       fb.gid,
		Rdev:      0,
		Flags:     0,
		BlockSize: 0,
	}
	if err := fb.storeNode(ctx, RootNodeID, rnode.NewRNode(ctx, rootAttrs, RootNodeID)); err != nil {
		logger.P(ctx, "insert root store node error")
	}
	logger.If(ctx, "insert root success")
}

// MakeRoot should only be called at new
func (fb *FBackEnd) MakeRoot(ctx context.Context) error {
	err := fb.fp.AllocateRoot(ctx, fb.localID)
	if err != nil {
		logger.W(ctx, "make root allocate root error", "err", err)
		return err
	}

	fb.InsertRoot(ctx)
	return nil
}

func (fb *FBackEnd) LoadHandle(
	ctx context.Context,
	h uint64) (uint64, error) {
	if out, ok := fb.handleMap.Load(h); ok {
		if node, ok := out.(uint64); ok {
			return node, nil
		}
		return 0, &FBackEndErr{fmt.Sprintf("load handle value not node id error: handle=%v", h)}
	}
	return 0, &FBackEndErr{fmt.Sprintf("load handle not found error: handle=%v", h)}
}

func (fb *FBackEnd) AllocateHandle(
	ctx context.Context,
	node uint64) (uint64, error) {
	if !fb.IsLocal(ctx, node) {
		return 0, &FBackEndErr{fmt.Sprintf("allocate handle node not local error: node=%v", node)}
	}
	handle := fb.handleAllocator.Allocate()
	fb.handleMap.Store(handle, node)
	return handle, nil
}

func (fb *FBackEnd) ReleaseHandle(
	ctx context.Context,
	h uint64) error {
	logger.If(ctx, "fb release: h=%v", h)
	if _, err := fb.LoadHandle(ctx, h); err == nil {
		fb.handleMap.Delete(h)
		logger.If(ctx, "fb release success: h=%v", h)
		return nil
	}
	return &FBackEndErr{fmt.Sprintf("release not found error: handle=%v", h)}
}

func (fb *FBackEnd) lock() {
	fb.mu.Lock()
}

func (fb *FBackEnd) unlock() {
	fb.mu.Unlock()
}

// Allocate a new rnode.RNode, assigning it an ID that is not in use.
//
// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) allocateInode(
	ctx context.Context,
	attrs fuse.Attr) (*rnode.RNode, error) {
	// Create the rnode.RNode.
	id, err := fb.fp.Allocate(ctx, fb.localID)
	if err != nil {
		logger.E(ctx, "allocate inode error", "err", err)
		return nil, err
	}

	node := rnode.NewRNode(ctx, attrs, id)
	if err := fb.storeNode(ctx, id, node); err != nil {
		logger.Ef(ctx, "allocate inode store error: id=%v, err=%v", id, err)
		return nil, err
	}
	return node, nil

}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) deallocateInode(ctx context.Context, id uint64) error {
	logger.If(ctx, "deallocate: id=%v", id)
	if err := fb.fp.Deallocate(ctx, id); err != nil {
		logger.Ef(ctx, "deallocate master deallocate error: id=%v, err=%+v", id, err)
		return err
	}
	if err := fb.deleteNode(ctx, id); err != nil {
		return err
	}
	logger.If(ctx, "deallocate success: id=%v", id)
	return nil
}

func (fb *FBackEnd) AttachChild(
	ctx context.Context,
	parentID uint64,
	childID uint64,
	name string,
	dt fuse.DirentType,
	doOpen bool) (hid uint64, err error) {
	logger.If(ctx, "attach child: parentID=%v, childID=%v, name=%v, dt=%v, doOpen=%v",
		parentID, childID, name, dt, doOpen)

	parent, lockID, err := fb.LoadNodeForWrite(ctx, parentID)
	if err != nil {
		logger.If(ctx, "sadd child load parent err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, parent, lockID); uerr != nil {
			logger.Ef(ctx, "unlock node error: id=%v, err=%+v", parent.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(name)
	if exists {
		return 0, syscall.EEXIST
	}

	// Add an entry in the parent.
	parent.AddChild(childID, name, dt)

	if !doOpen {
		return 0, nil
	}

	handle, err := fb.AllocateHandle(ctx, childID)

	if err != nil {
		logger.If(ctx, "fb add open child allocate handle err: parentID=%v, childID=%v, err=%v",
			parentID, childID, err.Error())
		return 0, err
	}

	logger.If(ctx, "fb add open child success: parent=%v, name=%v, childID=%v, dt=%v, handle=%v",
		parentID, name, childID, dt, handle)
	return handle, nil
}

func (fb *FBackEnd) LookUpInode(
	ctx context.Context,
	parentID uint64,
	name string) (_ uint64, _ fuse.Attr, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb look up inode: parent=%v, name=%v", parentID, name)
	// Grab the parent directory.
	parent, pLockID, err := fb.LoadNodeForRead(ctx, parentID)
	if err != nil {
		logger.If(ctx, "look up inode load prarent err: err=%v", err.Error())
		return 0, fuse.Attr{}, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, parent, pLockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", parent.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Does the directory have an entry with the given name?
	childID, _, ok := parent.LookUpChild(name)
	if !ok {
		err = syscall.ENOENT
		logger.W(ctx, "fb look up inode child not exists error", "parentID", parentID, "name", name, "err", err)
		return 0, fuse.Attr{}, err
	}

	// Grab the child.
	child, cLockID, err := fb.LoadNodeForRead(ctx, childID)
	if err != nil {
		logger.If(ctx, "look up inode load child err: err=%v", err.Error())
		return 0, fuse.Attr{}, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, child, cLockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", child.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	attr := child.Attrs()
	logger.If(ctx, "fb look up inode success: parent=%v, name=%v, attr=%+v", parentID, name, attr)
	return childID, attr, nil
}

func (fb *FBackEnd) GetInodeAttributes(
	ctx context.Context,
	id uint64) (_ fuse.Attr, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb get inode attr: id=%v", id)
	// Grab the rnode.RNode.
	node, lockID, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "get node attr err: err=%v", err.Error())
		return fuse.Attr{}, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Fill in the response.
	attr := node.Attrs()
	logger.If(ctx, "fb get inode attr success: id=%v, attr=%+v", id, attr)
	return attr, nil
}

func (fb *FBackEnd) SetInodeAttributes(
	ctx context.Context,
	id uint64,
	param SetInodeAttributesParam) (_ fuse.Attr, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb set inode attr: id=%v, param=%+v",
		id, param)
	// Grab the rnode.RNode.
	node, lockID, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "set node attr err: err=%v", err.Error())
		return fuse.Attr{}, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "lock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Handle the request.
	internalParam := internalSetInodeAttributesParam{}
	if param.HasMtime {
		internalParam.Mtime = &param.Mtime
	}
	if param.HasMode {
		internalParam.Mode = &param.Mode
	}
	if param.HasSize {
		internalParam.Size = &param.Size
	}
	node.SetAttributes(internalParam.Size, internalParam.Mode, internalParam.Mtime)

	// Fill in the response.
	logger.If(ctx, "fb set inode attr success: id=%v, param=%+v",
		id, param)
	return node.Attrs(), nil
}

func (fb *FBackEnd) MkDir(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (_ uint64, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb mkdir: parent=%v, name=%v, mode=%v",
		parentID, name, mode)

	// Set up attributes from the child.
	childAttrs := fuse.Attr{
		Nlink: 1,
		Mode:  mode,
		Uid:   fb.uid,
		Gid:   fb.gid,
	}

	// Allocate a child.
	child, err := fb.allocateInode(ctx, childAttrs)
	if err != nil {
		logger.E(ctx, "mkdir allocate inode error", "childAttrs", childAttrs, "err", err)
		return 0, err
	}
	defer func() {
		if err != nil {
			if derr := fb.deallocateInode(ctx, child.ID()); derr != nil {
				logger.Ef(ctx, "mkdir deallocate node error", "childID", child.ID(), "derr", derr)
			}
		}
	}()

	_, err = fb.fp.AttachChild(ctx, parentID, child.ID(), name, fuse.DT_Dir, false)

	logger.If(ctx, "fb mkdir result: parent=%v, name=%v, mode=%v, childID=%v, err=%+v",
		parentID, name, mode, child.ID(), err)
	return child.ID(), err
}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) CreateNode(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (_ uint64, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb create node: parent=%v, name=%v, mode=%v",
		parentID, name, mode)

	// Set up attributes for the child.
	now := time.Now()
	childAttrs := fuse.Attr{
		Nlink:  1,
		Mode:   mode,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fb.uid,
		Gid:    fb.gid,
	}

	// Allocate a child.
	child, err := fb.allocateInode(ctx, childAttrs)
	if err != nil {
		logger.E(ctx, "create node allocate inode error", "childAttrs", childAttrs, "err", err)
		return 0, err
	}
	defer func() {
		if err != nil {
			if derr := fb.deallocateInode(ctx, child.ID()); derr != nil {
				logger.Ef(ctx, "create node deallocate node error: childID=%v, derr=%+V", child.ID(), derr)
			}
		}
	}()

	_, err = fb.fp.AttachChild(ctx, parentID, child.ID(), name, fuse.DT_File, false)

	logger.If(ctx, "fb create node result: parent=%v, name=%v, mode=%v, childID=%v, err=%+v",
		parentID, name, mode, child.ID(), err)
	return child.ID(), err
}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) CreateFile(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode,
	flags uint32) (_ uint64, _ uint64, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb create file: parent=%v, name=%v, mode=%v, flags=%v",
		parentID, name, mode, flags)

	// Set up attributes for the child.
	now := time.Now()
	childAttrs := fuse.Attr{
		Nlink:  1,
		Mode:   mode,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fb.uid,
		Gid:    fb.gid,
	}

	// Allocate a child.
	child, err := fb.allocateInode(ctx, childAttrs)
	if err != nil {
		logger.E(ctx, "create file allocate inode error", "childAttrs", childAttrs, "err", err)
		return 0, 0, err
	}
	defer func() {
		if err != nil {
			if derr := fb.deallocateInode(ctx, child.ID()); derr != nil {
				logger.Ef(ctx, "create file deallocate node error: childID=%v, derr=%+V", child.ID(), derr)
			}
		}
	}()

	if _, err = fb.fp.AttachChild(ctx, parentID, child.ID(), name, fuse.DT_File, false); err != nil {
		logger.E(ctx, "create file attach child err",
			"parentID", parentID, "childID", child.ID(), "err", err.Error())
		return 0, 0, err
	}

	handleID, err := fb.AllocateHandle(ctx, child.ID())
	if err != nil {
		logger.E(ctx, "create file allocate handle err: id=%v, err=%v", child.ID(), err.Error())
		return 0, 0, err
	}

	logger.If(ctx, "fb create file result: parent=%v, name=%v, mode=%v, childID=%v, handleID=%v, err=%+v",
		parentID, name, mode, child.ID(), handleID, err)
	return child.ID(), handleID, err
}

func (fb *FBackEnd) CreateSymlink(
	ctx context.Context,
	parentID uint64,
	name string,
	target string) (_ uint64, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb create symlink: parent=%v, name=%v, target=%v",
		parentID, name, target)

	// Set up attributes from the child.
	now := time.Now()
	childAttrs := fuse.Attr{
		Nlink:  1,
		Mode:   0444 | os.ModeSymlink,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fb.uid,
		Gid:    fb.gid,
	}

	// Allocate a child.
	child, err := fb.allocateInode(ctx, childAttrs)
	if err != nil {
		logger.E(ctx, "create symlink allocate inode error", "childAttrs", childAttrs, "err", err)
		return 0, err
	}
	defer func() {
		if err != nil {
			if derr := fb.deallocateInode(ctx, child.ID()); derr != nil {
				logger.Ef(ctx, "create file deallocate node error: childID=%v, derr=%+V", child.ID(), derr)
			}
		}
	}()
	child.SetTarget(target)

	_, err = fb.fp.AttachChild(ctx, parentID, child.ID(), name, fuse.DT_Link, false)

	logger.If(ctx, "fb create symlink result: parent=%v, name=%v, target=%v, err=%+v",
		parentID, name, target, err)
	return child.ID(), nil
}

func (fb *FBackEnd) CreateLink(
	ctx context.Context,
	parentID uint64,
	name string,
	targetID uint64) (_ uint64, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb create link: parent=%v, name=%v, target=%v",
		parentID, name, targetID)
	// Grab the parent, which we will update shortly.
	parent, lockID, err := fb.LoadNodeForWrite(ctx, parentID)
	if err != nil {
		logger.If(ctx, "create link load parent err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, parent, lockID); uerr != nil {
			logger.Ef(ctx, "lock node error: id=%v, err=%+v", parent.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(name)
	if exists {
		return 0, fuse.EEXIST
	}

	// Get the target rnode.RNode to be linked
	target, lockID, err := fb.LoadNodeForWrite(ctx, targetID)
	if err != nil {
		logger.If(ctx, "create link load target err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, target, lockID); uerr != nil {
			logger.Ef(ctx, "lock node error: id=%v, err=%+v", target.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Update the attributes
	now := time.Now()
	target.SetCtime(now)
	target.IncrNlink()

	// Add an entry in the parent.
	parent.AddChild(targetID, name, fuse.DT_File)

	// Return the response.
	logger.If(ctx, "fb create link success: parent=%v, name=%v, target=%v",
		parentID, name, targetID)
	return targetID, nil
}

func (fb *FBackEnd) Rename(
	ctx context.Context,
	oldParent uint64,
	oldName string,
	newParent uint64,
	newName string) (err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb rename: oldParent=%v, olaName=%v, newParent=%v, newName=%v",
		oldParent, oldName, newParent, newName)
	// Ask the old parent for the child's rnode.RNode ID and type.
	oldParentNode, oldParentLockID, err := fb.LoadNodeForWrite(ctx, oldParent)
	if err != nil {
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, oldParentNode, oldParentLockID); uerr != nil {
			logger.Ef(ctx, "lock node error: id=%v, err=%+v", oldParentNode.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()
	childID, childType, ok := oldParentNode.LookUpChild(oldName)

	if !ok {
		err = syscall.ENOENT
		return
	}

	// If the new name exists already in the new parent, make sure it's not a
	// non-empty directory, then delete it.
	newParentNode := oldParentNode
	if newParent != oldParent {
		var newParentLockID int64
		newParentNode, newParentLockID, err = fb.LoadNodeForWrite(ctx, newParent)
		if err != nil {
			return err
		}
		defer func() {
			if uerr := fb.UnlockNode(ctx, newParentNode, newParentLockID); uerr != nil {
				logger.Ef(ctx, "lock node error: id=%v, err=%+v", newParentNode.ID(), uerr)
				if err != nil {
					logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
				}
				err = uerr
			}
		}()
	}

	existingID, _, ok := newParentNode.LookUpChild(newName)
	if ok {
		var existing *rnode.RNode
		var existingLockID int64
		existing, existingLockID, err = fb.LoadNodeForRead(ctx, existingID)
		if err != nil {
			return err
		}
		defer func() {
			if uerr := fb.RUnlockNode(ctx, existing, existingLockID); uerr != nil {
				logger.Ef(ctx, "rlock node error: id=%v, err=%+v", existing.ID(), uerr)
				if err != nil {
					logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
				}
				err = uerr
			}
		}()

		if existing.IsDir() {
			err = fuse.ToErrno(&FBackEndErr{fmt.Sprintf("rename target is dir error: newName=%v", newName)})
			return err
		}

		newParentNode.RemoveChild(newName)
	}

	// Link the new name.
	newParentNode.AddChild(
		childID,
		newName,
		childType)

	// Finally, remove the old name from the old parent.
	oldParentNode.RemoveChild(oldName)

	logger.If(ctx, "fb rename success: oldParent=%v, olaName=%v, newParent=%v, newName=%v",
		oldParent, oldName, newParent, newName)
	return
}

func (fb *FBackEnd) DetachChild(
	ctx context.Context,
	parent uint64,
	name string) (err error) {
	logger.If(ctx, "fb detach child: parent=%v, name=%v", parent, name)
	// Grab the parent, which we will update shortly.
	parentNode, lockID, err := fb.LoadNodeForWrite(ctx, uint64(parent))
	if err != nil {
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, parentNode, lockID); uerr != nil {
			logger.Ef(ctx, "lock node error: id=%v, err=%+v", parentNode.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Find the child within the parent.
	childID, _, ok := parentNode.LookUpChild(name)
	if !ok {
		err = syscall.ENOENT
		logger.Ef(ctx, "fb detach no child error: parent=%v, name=%v, childID=%v", parent, name, childID)
		return
	}

	// Remove the entry within the parent.
	parentNode.RemoveChild(name)

	logger.If(ctx, "fb detach child success: parent=%v, name=%v, childID=%v", parent, name, childID)
	return
}

func (fb *FBackEnd) Unlink(
	ctx context.Context,
	parent uint64,
	name string,
	childID uint64) (err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb unlink: parent=%v, name=%v", parent, name)

	// Grab the child.
	child, lockID, err := fb.LoadNodeForWrite(ctx, childID)
	if err != nil {
		return err
	}
	defer func() {
		if fb.IsLocal(ctx, childID) {
			if uerr := fb.UnlockNode(ctx, child, lockID); uerr != nil {
				logger.Ef(ctx, "lock node error: id=%v, err=%+v", child.ID(), uerr)
				if err != nil {
					logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
				}
				err = uerr
			}
		}
	}()

	err = fb.fp.DetachChild(ctx, parent, name)
	if err != nil {
		logger.Ef(ctx, "fb unlink detach error: parent=%v, name=%v, err=%+v", parent, name, err)
		return err
	}

	// Mark the child as unlinked.
	child.DecrNlink()
	if child.IsLost() {
		if err := fb.deallocateInode(ctx, childID); err != nil {
			return err
		}
	}

	logger.If(ctx, "fb unlink success: parent=%v, name=%v", parent, name)
	return
}

func (fb *FBackEnd) Open(
	ctx context.Context,
	id uint64,
	flags uint32) (handle uint64, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb open: id=%v, flags=%v", id, flags)
	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// rnode.RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	node, lockID, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "open dir err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	//if !node.IsDir() {
	//	err := &FBackEndErr{msg: fmt.Sprintf("open dir non-dir error: id=%v", id)}
	//	logger.If(ctx, err.Error())
	//	return 0, err
	//}

	handle, err = fb.AllocateHandle(ctx, id)
	if err != nil {
		logger.If(ctx, "open allocate handle err: id=%v, err=%v", id, err.Error())
		return 0, err
	}

	logger.If(ctx, "open dir allocate handle success: id=%v, handle=%v", id, handle)
	return handle, nil
}

func (fb *FBackEnd) ReadDir(
	ctx context.Context,
	id uint64) ([]fuse.Dirent, error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb readdir: id=%v", id)
	// Grab the directory.
	node, lockID, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "read dir err: err=%v", err.Error())
		return nil, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	dirents := node.ReadDir()

	logger.If(ctx, "fb readdir success: id=%v, dirents=%+v", id, dirents)
	return dirents, nil
}

func (fb *FBackEnd) ReleaseDirHandle(
	ctx context.Context,
	handle uint64) error {
	logger.If(ctx, "release dir: handle=%v", handle)
	if err := fb.ReleaseHandle(ctx, handle); err != nil {
		logger.If(ctx, "release dir error: handle=%v, err=%+v",
			handle, err)
		return err
	}

	logger.If(ctx, "release dir success: handle=%v", handle)
	return nil
}

func (fb *FBackEnd) OpenFile(
	ctx context.Context,
	id uint64,
	flags uint32) (handle uint64, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb openfile: id=%v, flags=%v", id, flags)
	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// rnode.RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	node, lockID, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "open file err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	if !node.IsFile() {
		err := &FBackEndErr{msg: fmt.Sprintf("open file non-file error: id=%v", id)}
		logger.If(ctx, err.Error())
		return 0, err
	}

	handle, err = fb.AllocateHandle(ctx, id)
	if err != nil {
		logger.If(ctx, "open file allocate handle err: id=%v, err=%v", id, err.Error())
		return 0, err
	}

	logger.If(ctx, "open file allocate handle success: id=%v, handle=%v", id, handle)
	return handle, nil
}

func (fb *FBackEnd) ReadFile(
	ctx context.Context,
	id uint64,
	length uint64,
	offset uint64) (bytesRead uint64, buf []byte, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb readfile: id=%v, length=%v, offset=%v", id, length, offset)
	// Find the rnode.RNode in question.
	node, lockID, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "read file err: err=%v", err.Error())
		return
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Serve the request.
	buf = make([]byte, length)
	bytesReadI, err := node.ReadAt(buf, int64(offset))
	bytesRead = uint64(bytesReadI)

	// Don't return EOF errors; we just indicate EOF to fuse using a short read.
	if err == io.EOF {
		logger.If(ctx, "readfile meets EOF, return nil: id=%v, length=%v, bytesRead=%v, offset=%v",
			id, length, bytesRead, offset)
		err = nil
	}

	if err != nil {
		logger.Ef(ctx, "readfile error: id=%v, length=%v, offset=%v, err=%+v", id, length, offset, err)
	}
	logger.If(ctx, "fb readfile success: id=%v, length=%v, offset=%v, bytesRead=%v", id, length, offset, bytesRead)
	return
}

func (fb *FBackEnd) WriteFile(
	ctx context.Context,
	id uint64,
	offset uint64,
	data []byte) (_ uint64, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "write file: id=%v, offset=%v, data=%v", id, offset, data)
	// Find the rnode.RNode in question.
	node, lockID, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "write file err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "lock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Serve the request.
	bytesWrite, err := node.WriteAt(data, int64(offset))

	if err == nil {
		logger.If(ctx, "write file: id=%v, offset=%v, bytesWrite=%v", id, offset, bytesWrite)
	} else {
		logger.If(ctx, "writefile error: id=%v, offset=%v, bytesWrite=%v, data=%v, err=%+v",
			id, offset, bytesWrite, data, err)
	}

	logger.If(ctx, "write file success: id=%v, offset=%v, bytesWrite=%v, data=%v", id, offset, bytesWrite, data)
	return uint64(bytesWrite), err
}

func (fb *FBackEnd) ReleaseFileHandle(
	ctx context.Context,
	handle uint64) error {
	logger.If(ctx, "release file: handle=%v", handle)
	if err := fb.ReleaseHandle(ctx, handle); err != nil {
		logger.If(ctx, "release file error: handle=%v, err=%+v",
			handle, err)
		return err
	}

	logger.If(ctx, "release file success: handle=%v", handle)
	return nil
}

func (fb *FBackEnd) ReadSymlink(
	ctx context.Context,
	id uint64) (target string, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fp read symlink: id=%v", id)
	// Find the rnode.RNode in question.
	node, lockID, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "read symlink err: err=%v", err.Error())
		return
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	// Serve the request.
	target = node.Target()

	logger.If(ctx, "fp read symlink success: id=%v", id)
	return
}

func (fb *FBackEnd) GetXattr(ctx context.Context,
	id uint64,
	name string,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb get xattr: id=%v, name=%v, length=%v",
		id, name, length)
	node, lockID, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "get xattr err: err=%v", err.Error())
		return
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	if value, ok := node.Xattrs()[name]; ok {
		bytesRead = uint64(len(value))
		dst = make([]byte, length)
		if len(dst) >= len(value) {
			copy(dst, value)
		} else if len(dst) != 0 {
			err = syscall.ERANGE
			return
		}
	} else {
		err = syscall.ENODATA
		return
	}

	logger.If(ctx, "fb get xattr success: id=%v, name=%v, length=%v",
		id, name, length)
	return
}

func (fb *FBackEnd) ListXattr(ctx context.Context,
	id uint64,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb list xattr: id=%v, length=%v",
		id, length)
	node, lockID, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "list xattr err: err=%v", err.Error())
		return
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "rlock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "runlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	dst = make([]byte, length)
	dstLeft := dst[:]
	for key := range node.Xattrs() {
		keyLen := len(key) + 1

		if len(dstLeft) >= keyLen {
			copy(dstLeft, key)
			dstLeft = dstLeft[keyLen:]
		} else if len(dst) != 0 {
			err = syscall.ERANGE
			return
		}
		bytesRead += uint64(keyLen)
	}

	logger.If(ctx, "fb list xattr success: id=%v, length=%v",
		id, length)
	return
}

func (fb *FBackEnd) RemoveXattr(ctx context.Context,
	id uint64,
	name string) (err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb rm xattr: id=%v, name=%v", id, name)
	node, lockID, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "remove xattr err: err=%v", err.Error())
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "lock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	if _, ok := node.Xattrs()[name]; ok {
		xattrs := node.Xattrs()
		delete(xattrs, name)
		node.SetXattrs(xattrs)
	} else {
		return syscall.ENOENT
	}

	logger.If(ctx, "fb rm xattr success: id=%v, name=%v", id, name)
	return nil
}

func (fb *FBackEnd) SetXattr(ctx context.Context,
	id uint64,
	name string,
	flags uint32,
	value []byte) (err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb set xattr: id=%v, name=%v, flag=%v, value=%X", id, name, flags, value)
	node, lockID, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "fb set xattr load node error: id=%v, name=%v, flag=%v, value=%X",
			id, name, flags, value, err)
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "lock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	_, hasAttr := node.Xattrs()[name]

	switch flags {
	case unix.XATTR_CREATE:
		if hasAttr {
			return syscall.EEXIST
		}
	case unix.XATTR_REPLACE:
		if !hasAttr {
			return syscall.ENOENT
		}
	}

	localValue := make([]byte, len(value))
	copy(localValue, value)
	xattrs := node.Xattrs()
	xattrs[name] = localValue
	node.SetXattrs(xattrs)

	logger.If(ctx, "fb set xattr success: id=%v, name=%v, flag=%v, value=%X",
		id, name, flags, value)
	return nil
}

func (fb *FBackEnd) Fallocate(ctx context.Context,
	id uint64,
	mode uint32,
	length uint64) (err error) {
	fb.lock()
	defer fb.unlock()

	logger.If(ctx, "fb fallocate: id=%v, mode=%v, len=%v", id, mode, length)
	node, lockID, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "fallocate err: err=%v", err.Error())
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node, lockID); uerr != nil {
			logger.Ef(ctx, "lock node error: id=%v, err=%+v", node.ID(), uerr)
			if err != nil {
				logger.Ef(ctx, "unlock node error overwrite method error: err=%+v", err)
			}
			err = uerr
		}
	}()

	err = node.Fallocate(mode, length, length)
	if err != nil {
		err := &FBackEndErr{msg: fmt.Sprintf("node fallocate error: err=%+v", err)}
		logger.If(ctx, err.Error())
		return err
	}

	logger.If(ctx, "fb fallocate success: id=%v, mode=%v, len=%v", id, mode, length)
	return nil
}

func (fb *FBackEnd) SaveRedundantNode(ctx context.Context, node *rnode.RNode) error {
	fb.redundantNodes.Store(node.ID(), node)
	return nil
}

func (fb *FBackEnd) CopyNodes(ctx context.Context) map[uint64]int64 {
	r := make(map[uint64]int64)
	fb.nodes.RangeWithLock(func(key, value interface{}) bool {
		r[key.(uint64)] = value.(*rnode.RNode).Version()
		return true
	})
	return r
}

func (fb *FBackEnd) CopyBackupNodes(ctx context.Context) map[uint64]int64 {
	r := make(map[uint64]int64)
	fb.redundantNodes.RangeWithLock(func(key, value interface{}) bool {
		r[key.(uint64)] = value.(*rnode.RNode).Version()
		return true
	})
	return r
}

func (fb *FBackEnd) CopyHandles(ctx context.Context) map[uint64]uint64 {
	r := make(map[uint64]uint64)
	fb.handleMap.RangeWithLock(func(key, value interface{}) bool {
		r[key.(uint64)] = value.(uint64)
		return true
	})
	return r
}

func (fb *FBackEnd) Summary(ctx context.Context) *FBSummary {
	return &FBSummary{
		Nodes:       fb.CopyNodes(ctx),
		BackupNodes: fb.CopyBackupNodes(ctx),
		Handles:     fb.CopyHandles(ctx),
	}
}

func (e *FBackEndErr) Error() string {
	return fmt.Sprintf("fbackend error: %v", e.msg)
}
