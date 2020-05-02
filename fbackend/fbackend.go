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

	nodes          sync.Map // [uint64]*rnode.RNode
	redundantNodes sync.Map // [uint64]*rnode.RNode
	nodesRead      map[uint64]*rnode.RNode
	muNodesRead    sync.RWMutex
	localID        uint64

	fp *FProxy

	handleAllocator *idallocator.IDAllocator
	handleMap       sync.Map // map[uint64]uint64

	wd *WatchDog
}

type FBackEndErr struct {
	msg string
}

var _ = (error)((*FBackEndErr)(nil))

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
	wd *WatchDog) *FBackEnd {
	return &FBackEnd{
		uid:             uid,
		gid:             gid,
		handleAllocator: allocator,
		nodesRead:       make(map[uint64]*rnode.RNode),
		wd:              wd,
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

	localID := fb.fp.RegisterOwner(ctx, addr)
	if localID > 0 {
		fb.localID = localID
		logger.If(ctx, "register success: addr=%v, localID=%v", addr, localID)
		return
	}

	logger.Pf(ctx, "register error: addr=%v", addr)
}

func (fb *FBackEnd) doLoadNodeForX(ctx context.Context, id uint64, isRead bool, localOnly bool) (*rnode.RNode, error) {
	node, err := fb.LoadLocalNode(ctx, id)

	if err != nil && !localOnly {
		logger.Ef(ctx, "load node load local error: id=%v, isRead=%v, err=%+v", id, isRead, err)
		node, err = fb.LoadRemoteNode(ctx, id, isRead)
		if err != nil {
			logger.Ef(ctx, "load node load remote error: id=%v, isRead=%v, err=%+v", id, isRead, err)
			return nil, err
		}
	}

	if err != nil {
		logger.Ef(ctx, "load node error: id=%v, isRead=%v, err=%+v", id, isRead, err)
		return nil, err
	}

	if isRead {
		err = node.RLock(ctx)
	} else {
		err = node.Lock(ctx)
	}
	if err != nil {
		logger.Ef(ctx, "load node lock error: id=%v, isRead=%v, err=%+v", id, isRead, err)
		return nil, err
	}
	return node, nil
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

func (fb *FBackEnd) LoadRemoteNode(ctx context.Context, id uint64, isRead bool) (*rnode.RNode, error) {
	node, err := fb.fp.LoadNode(ctx, id, isRead)
	if err != nil {
		logger.If(ctx, "rpc fetch node error: id=%v, err=%+v",
			id, err)
		return nil, &FBackEndErr{msg: fmt.Sprintf("load node rpc error: id=%v, isRead=%v, err=%+v", id, isRead, err)}
	}
	return node, nil
}

func (fb *FBackEnd) LoadLocalNodeForRead(ctx context.Context, id uint64) (node *rnode.RNode, err error) {
	return fb.doLoadNodeForX(ctx, id, true, true)
}

func (fb *FBackEnd) LoadNodeForRead(ctx context.Context, id uint64) (node *rnode.RNode, err error) {
	return fb.doLoadNodeForX(ctx, id, true, false)
}

func (fb *FBackEnd) LoadLocalNodeForWrite(ctx context.Context, id uint64) (node *rnode.RNode, err error) {
	return fb.doLoadNodeForX(ctx, id, false, true)
}

func (fb *FBackEnd) LoadNodeForWrite(ctx context.Context, id uint64) (node *rnode.RNode, err error) {
	return fb.doLoadNodeForX(ctx, id, false, false)
}

func (fb *FBackEnd) pushBackupNode(ctx context.Context, copiedNode rnode.RNode) {
	defer utility.RecoverWithStack(ctx, nil)

	for i, addr := range fb.wd.getBackupAddrs(ctx, copiedNode.ID()) {
		logger.If(ctx, "push backup node: i=%v, node=%v, addr=%v", i, copiedNode, addr)
		if err := fb.fp.PushNode(ctx, addr, &copiedNode); err != nil {
			logger.If(ctx, "push backup node err: i=%v, node=%v, addr=%v, err=%+v", i, copiedNode, addr, err)
		}
	}
}

func (fb *FBackEnd) doUnlockRemoteNode(ctx context.Context, node *rnode.RNode, isRead bool) (err error) {
	id := node.ID()

	if isRead {
		err = fb.fp.RUnlockNode(ctx, id)
	} else {
		err = fb.fp.UnlockNode(ctx, node)
	}
	if err != nil {
		logger.Ef(ctx, "unlock remote node error: id=%v, isRead=%v, err=%+v", id, isRead, err)
		return err
	}

	return nil
}

func (fb *FBackEnd) doUnlockNode(ctx context.Context, node *rnode.RNode, isRead bool) error {
	id := node.ID()
	logger.If(ctx, "unlock node: id=%v, isRead=%v", id, isRead)
	if isRead {
		node.RUnlock(ctx)
	} else {
		node.IncrVersion()
		if fb.IsLocal(ctx, id) {
			go fb.pushBackupNode(ctx, *node)
		}
		node.Unlock(ctx)
	}
	if fb.IsLocal(ctx, id) {
		logger.If(ctx, "unlock local node success: id=%v, isRead=%v", id, isRead)
		return nil
	}

	return fb.doUnlockRemoteNode(ctx, node, isRead)
}

func (fb *FBackEnd) UnlockNode(ctx context.Context, node *rnode.RNode) error {
	return fb.doUnlockNode(ctx, node, false)
}

func (fb *FBackEnd) RUnlockNode(ctx context.Context, node *rnode.RNode) error {
	return fb.doUnlockNode(ctx, node, true)
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
	fb.muNodesRead.Lock()
	defer fb.muNodesRead.Unlock()
	if _, loaded := fb.nodes.LoadOrStore(id, node); loaded {
		logger.Ef(ctx, "store node overwrite error: id=%v", id)
	} else {
		logger.If(ctx, "store node success: id=%v", id)
	}
	fb.nodesRead[id] = node
	return nil
}

func (fb *FBackEnd) deleteNode(ctx context.Context, id uint64) error {
	logger.If(ctx, "delete node: id=%v", id)
	fb.muNodesRead.Lock()
	defer fb.muNodesRead.Unlock()
	if !fb.IsLocal(ctx, id) {
		return &FBackEndErr{fmt.Sprintf("delete node not exist error: id=%v", id)}
	}
	fb.nodes.Delete(id)
	delete(fb.nodesRead, id)
	logger.If(ctx, "delete node success: id=%v", id)
	return nil
}

// MakeRoot should only be called at new
func (fb *FBackEnd) MakeRoot(ctx context.Context) error {
	if ok := fb.fp.AllocateRoot(ctx, fb.localID); !ok {
		logger.W(ctx, "make root allocate root error")
		return &FBackEndErr{"make root allocate root error"}
	}
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
	if err := fb.storeNode(ctx, RootNodeID, rnode.NewRNode(rootAttrs, RootNodeID)); err != nil {
		logger.Ef(ctx, "make root store node error")
		return err
	}
	logger.If(ctx, "make root success")
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
	attrs fuse.Attr) (uint64, *rnode.RNode) {
	// Create the rnode.RNode.
	id := fb.fp.Allocate(ctx, fb.localID)
	if id > 0 {
		node := rnode.NewRNode(attrs, id)
		if err := fb.storeNode(ctx, id, node); err != nil {
			logger.Ef(ctx, "allocate inode store error: id=%v, err=%v", id, err)
			return 0, nil
		}
		return id, node
	}

	return 0, nil
}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) deallocateInode(ctx context.Context, id uint64) error {
	logger.If(ctx, "deallocate: id=%v", id)
	if ok := fb.fp.Deallocate(ctx, id); !ok {
		err := &FBackEndErr{fmt.Sprintf("deallocate no node error: nodeID=%v", id)}
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

	parent, err := fb.LoadNodeForWrite(ctx, parentID)
	if err != nil {
		logger.If(ctx, "sadd child load parent err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, parent); uerr != nil {
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
	parent, err := fb.LoadNodeForRead(ctx, parentID)
	if err != nil {
		logger.If(ctx, "look up inode load prarent err: err=%v", err.Error())
		return 0, fuse.Attr{}, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, parent); uerr != nil {
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
	child, err := fb.LoadNodeForRead(ctx, childID)
	if err != nil {
		logger.If(ctx, "look up inode load child err: err=%v", err.Error())
		return 0, fuse.Attr{}, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, child); uerr != nil {
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
	node, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "get node attr err: err=%v", err.Error())
		return fuse.Attr{}, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "set node attr err: err=%v", err.Error())
		return fuse.Attr{}, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node); uerr != nil {
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
	childID, _ := fb.allocateInode(ctx, childAttrs)
	defer func() {
		if err != nil {
			if derr := fb.deallocateInode(ctx, childID); derr != nil {
				logger.Ef(ctx, "mkdir deallocate node error: childID=%v, derr=%+V", childID, derr)
			}
		}
	}()

	_, err = fb.fp.AttachChild(ctx, parentID, childID, name, fuse.DT_Dir, false)

	logger.If(ctx, "fb mkdir result: parent=%v, name=%v, mode=%v, childID=%v, err=%+v",
		parentID, name, mode, childID, err)
	return childID, err
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
	childID, _ := fb.allocateInode(ctx, childAttrs)
	defer func() {
		if err != nil {
			if derr := fb.deallocateInode(ctx, childID); derr != nil {
				logger.Ef(ctx, "create node deallocate node error: childID=%v, derr=%+V", childID, derr)
			}
		}
	}()

	_, err = fb.fp.AttachChild(ctx, parentID, childID, name, fuse.DT_File, false)

	logger.If(ctx, "fb create node result: parent=%v, name=%v, mode=%v, childID=%v, err=%+v",
		parentID, name, mode, childID, err)
	return childID, err
}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) CreateFile(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode,
	flags uint32) (childID uint64, handleID uint64, err error) {
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
	childID, _ = fb.allocateInode(ctx, childAttrs)
	defer func() {
		if err != nil {
			if derr := fb.deallocateInode(ctx, childID); derr != nil {
				logger.Ef(ctx, "create file deallocate node error: childID=%v, derr=%+V", childID, derr)
			}
		}
	}()

	handleID, err = fb.fp.AttachChild(ctx, parentID, childID, name, fuse.DT_File, true)

	logger.If(ctx, "fb create file result: parent=%v, name=%v, mode=%v, childID=%v, handleID=%v, err=%+v",
		parentID, name, mode, childID, handleID, err)
	return childID, handleID, err
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
	childID, child := fb.allocateInode(ctx, childAttrs)
	defer func() {
		if err != nil {
			if derr := fb.deallocateInode(ctx, childID); derr != nil {
				logger.Ef(ctx, "create file deallocate node error: childID=%v, derr=%+V", childID, derr)
			}
		}
	}()
	child.SetTarget(target)

	_, err = fb.fp.AttachChild(ctx, parentID, childID, name, fuse.DT_Link, false)

	logger.If(ctx, "fb create symlink result: parent=%v, name=%v, target=%v, err=%+v",
		parentID, name, target, err)
	return childID, nil
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
	parent, err := fb.LoadNodeForWrite(ctx, parentID)
	if err != nil {
		logger.If(ctx, "create link load parent err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, parent); uerr != nil {
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
	target, err := fb.LoadNodeForWrite(ctx, targetID)
	if err != nil {
		logger.If(ctx, "create link load target err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, target); uerr != nil {
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
	oldParentNode, err := fb.LoadNodeForWrite(ctx, oldParent)
	if err != nil {
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, oldParentNode); uerr != nil {
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
		newParentNode, err = fb.LoadNodeForWrite(ctx, newParent)
		if err != nil {
			return err
		}
		defer func() {
			if uerr := fb.UnlockNode(ctx, newParentNode); uerr != nil {
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
		existing, err = fb.LoadNodeForRead(ctx, existingID)
		if err != nil {
			return err
		}
		defer func() {
			if uerr := fb.RUnlockNode(ctx, existing); uerr != nil {
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
	parentNode, err := fb.LoadNodeForWrite(ctx, uint64(parent))
	if err != nil {
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, parentNode); uerr != nil {
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
	child, err := fb.LoadNodeForWrite(ctx, childID)
	if err != nil {
		return err
	}
	defer func() {
		if fb.IsLocal(ctx, childID) {
			if uerr := fb.UnlockNode(ctx, child); uerr != nil {
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
	node, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "open dir err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "read dir err: err=%v", err.Error())
		return nil, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "open file err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "read file err: err=%v", err.Error())
		return
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "write file err: err=%v", err.Error())
		return 0, err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "read symlink err: err=%v", err.Error())
		return
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "get xattr err: err=%v", err.Error())
		return
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForRead(ctx, id)
	if err != nil {
		logger.If(ctx, "list xattr err: err=%v", err.Error())
		return
	}
	defer func() {
		if uerr := fb.RUnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "remove xattr err: err=%v", err.Error())
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "fb set xattr load node error: id=%v, name=%v, flag=%v, value=%X",
			id, name, flags, value, err)
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node); uerr != nil {
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
	node, err := fb.LoadNodeForWrite(ctx, id)
	if err != nil {
		logger.If(ctx, "fallocate err: err=%v", err.Error())
		return err
	}
	defer func() {
		if uerr := fb.UnlockNode(ctx, node); uerr != nil {
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

func (e *FBackEndErr) Error() string {
	return fmt.Sprintf("fbackend error: %v", e.msg)
}
