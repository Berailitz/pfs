// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fbackend

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/Berailitz/pfs/remotetree"

	"github.com/Berailitz/pfs/gclipool"

	"github.com/Berailitz/pfs/utility"

	"google.golang.org/grpc"

	"github.com/Berailitz/pfs/rnode"

	"github.com/Berailitz/pfs/rclient"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"golang.org/x/sys/unix"
)

const initialHandle = 1

type FBackEnd struct {
	fuseutil.NotImplementedFileSystem
	// The UID and GID that every rnode.RNode receives.
	uid uint32
	gid uint32

	/////////////////////////
	// Mutable state
	/////////////////////////
	mu sync.RWMutex

	nodes sync.Map // [uint64]*rnode.RNode
	mcli  *rclient.RClient
	pool  *gclipool.GCliPool

	nextHandle uint64
	handleMap  sync.Map // map[uint64]uint64
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

// Create a file system that stores data and metadata in memory.
//
// The supplied UID/GID pair will own the root rnode.RNode. This file system does no
// permissions checking, and should therefore be mounted with the
// default_permissions option.
func NewFBackEnd(
	uid uint32,
	gid uint32,
	masterAddr string,
	localAddr string,
	gopts []grpc.DialOption) *FBackEnd {
	gcli, err := utility.BuildGCli(masterAddr, gopts)
	if err != nil {
		log.Fatalf("new rcli fial error: master=%v, opts=%+v, err=%+V",
			masterAddr, gopts, err)
		return nil
	}

	mcli := rclient.NewRClient(gcli)
	if mcli == nil {
		log.Fatalf("nil mcli error")
	}

	mcli.RegisterSelf(localAddr)
	fb := &FBackEnd{
		uid:        uid,
		gid:        gid,
		mcli:       mcli,
		pool:       gclipool.NewGCliPool(gopts, localAddr),
		nextHandle: initialHandle,
	}

	// Set up the root rnode.RNode.
	if err := fb.MakeRoot(); err != nil {
		log.Printf("make root error: err=%+v", err)
		return nil
	}

	return fb
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////
func (fb *FBackEnd) LoadNodeForWrite(id uint64) (*rnode.RNode, error) {
	if out, exist := fb.nodes.Load(id); exist {
		if node, ok := out.(*rnode.RNode); ok {
			return node, nil
		}
	}
	log.Printf("load node error: id=%v", id)
	return nil, &FBackEndErr{msg: fmt.Sprintf("load local node for write not exist error: id=%v", id)}
}

func (fb *FBackEnd) LoadLocalNodeForRead(id uint64) (*rnode.RNode, error) {
	if out, exist := fb.nodes.Load(id); exist {
		if node, ok := out.(*rnode.RNode); ok {
			return node, nil
		}
		return nil, &FBackEndErr{msg: fmt.Sprintf("load local node non-node error: id=%v", id)}
	}
	return nil, &FBackEndErr{msg: fmt.Sprintf("load local node not exist error: id=%v", id)}
}

func (fb *FBackEnd) LoadNodeForRead(id uint64) (*rnode.RNode, error) {
	if node, err := fb.LoadLocalNodeForRead(id); err == nil {
		return node, nil
	} else {
		addr := fb.mcli.QueryOwner(id)
		gcli := fb.pool.Load(addr)
		if gcli == nil {
			return nil, &FBackEndErr{msg: fmt.Sprintf("load node for read no gcli error: id=%v", id)}
		}

		ctx := context.Background()
		node, err := gcli.FetchNode(ctx, &pb.UInt64ID{
			Id: id,
		})
		if err != nil {
			log.Printf("rpc fetch node error: id=%v, err=%+v",
				id, err)
			return nil, &FBackEndErr{msg: fmt.Sprintf("load node for read rpc error: id=%v, err=%+v", id, err)}
		}

		return utility.FromPbNode(node), nil
	}
}

func (fb *FBackEnd) storeNode(id uint64, node *rnode.RNode) error {
	log.Printf("store node: id=%v", id)
	if _, loaded := fb.nodes.LoadOrStore(id, node); loaded {
		return &FBackEndErr{fmt.Sprintf("store node overwrite error: id=%v", id)}
	}
	log.Printf("store node success: id=%v", id)
	return nil
}

func (fb *FBackEnd) deleteNode(id uint64) error {
	log.Printf("delete node: id=%v", id)
	if _, ok := fb.nodes.Load(id); !ok {
		return &FBackEndErr{fmt.Sprintf("delete node not exist error: id=%v", id)}
	}
	fb.nodes.Delete(id)
	log.Printf("delete node success: id=%v", id)
	return nil
}

// MakeRoot should only be called at new
func (fb *FBackEnd) MakeRoot() error {
	if ok := fb.mcli.AllocateRoot(); !ok {
		log.Printf("make root allocate root error")
		return &FBackEndErr{"make root allocate root error"}
	}
	rootAttrs := fuseops.InodeAttributes{
		Mode: 0700 | os.ModeDir,
		Uid:  fb.uid,
		Gid:  fb.gid,
	}
	if err := fb.storeNode(uint64(fuseops.RootInodeID), rnode.NewRNode(rootAttrs, fuseops.RootInodeID)); err != nil {
		log.Printf("make root store node error")
		return err
	}
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
	handle := atomic.AddUint64(&fb.nextHandle, 1) - 1
	fb.handleMap.Store(handle, node)
	return handle, nil
}

func (fb *FBackEnd) ReleaseHandle(
	ctx context.Context,
	h uint64) error {
	if _, err := fb.LoadHandle(ctx, h); err == nil {
		fb.handleMap.Delete(h)
		return nil
	}
	return &FBackEndErr{fmt.Sprintf("release handle not found error: handle=%v", h)}
}

func (fb *FBackEnd) lock() {
	fb.mu.Lock()
}

func (fb *FBackEnd) unlock() {
	fb.mu.Unlock()
}

// Find the given rnode.RNode. Panic if it doesn't exist.
//
// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) mustLoadInodeForWrite(id uint64) *rnode.RNode {
	// TODO: remove mustLoadInodeForWrite
	if node, err := fb.LoadNodeForWrite(id); err == nil {
		return node
	}
	log.Fatalf("Unknown rnode.RNode: %v", id)
	return nil
}

// Allocate a new rnode.RNode, assigning it an ID that is not in use.
//
// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) allocateInode(
	attrs fuseops.InodeAttributes) (uint64, *rnode.RNode) {
	// Create the rnode.RNode.
	id := fb.mcli.Allocate()
	if id > 0 {
		node := rnode.NewRNode(attrs, id)
		if err := fb.storeNode(id, node); err != nil {
			log.Printf("allocate inode store error: id=%v, err=%v", id, err)
			return 0, nil
		}
		return id, node
	}

	return 0, nil
}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) deallocateInode(id uint64) {
	log.Printf("deallocate: id=%v", id)
	if ok := fb.mcli.Deallocate(id); !ok {
		log.Printf("deallocate master deallocate error: id=%v", id)
	}
	if err := fb.deleteNode(id); err != nil {
		log.Printf("deallocate node delete error: id=%v, err=%+v", id, err)
	}
	log.Printf("deallocate success: id=%v", id)
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (fb *FBackEnd) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	return nil
}

func (fb *FBackEnd) LookUpInode(
	ctx context.Context,
	parentID uint64,
	name string) (uint64, fuseops.InodeAttributes, error) {
	fb.lock()
	defer fb.unlock()

	// Grab the parent directory.
	parent, err := fb.LoadNodeForRead(parentID)
	if err != nil {
		log.Printf("look up inode load prarent err: err=%v", err.Error())
		return 0, fuseops.InodeAttributes{}, err
	}

	// Does the directory have an entry with the given name?
	childID, _, ok := parent.LookUpChild(name)
	if !ok {
		return 0, fuseops.InodeAttributes{}, fuse.ENOENT
	}

	// Grab the child.
	child, err := fb.LoadNodeForRead(childID)
	if err != nil {
		log.Printf("look up inode load child err: err=%v", err.Error())
		return 0, fuseops.InodeAttributes{}, err
	}

	return childID, child.Attrs(), nil
}

func (fb *FBackEnd) GetInodeAttributes(
	ctx context.Context,
	id uint64) (fuseops.InodeAttributes, error) {
	fb.lock()
	defer fb.unlock()

	// Grab the rnode.RNode.
	node, err := fb.LoadNodeForRead(id)
	if err != nil {
		log.Printf("get node attr err: err=%v", err.Error())
		return fuseops.InodeAttributes{}, err
	}

	// Fill in the response.
	return node.Attrs(), nil
}

func (fb *FBackEnd) SetInodeAttributes(
	ctx context.Context,
	id uint64,
	param SetInodeAttributesParam) (fuseops.InodeAttributes, error) {
	fb.lock()
	defer fb.unlock()

	// Grab the rnode.RNode.
	node, err := fb.LoadNodeForWrite(id)
	if err != nil {
		log.Printf("set node attr err: err=%v", err.Error())
		return fuseops.InodeAttributes{}, err
	}

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
	return node.Attrs(), nil
}

func (fb *FBackEnd) MkDir(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (uint64, fuseops.InodeAttributes, error) {
	fb.lock()
	defer fb.unlock()

	// Grab the parent, which we will update shortly.
	parent, err := fb.LoadNodeForWrite(parentID)
	if err != nil {
		log.Printf("mkdir err: err=%v", err.Error())
		return 0, fuseops.InodeAttributes{}, err
	}

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(name)
	if exists {
		return 0, fuseops.InodeAttributes{}, fuse.EEXIST
	}

	// Set up attributes from the child.
	childAttrs := fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  mode,
		Uid:   fb.uid,
		Gid:   fb.gid,
	}

	// Allocate a child.
	childID, child := fb.allocateInode(childAttrs)

	// Add an entry in the parent.
	parent.AddChild(childID, name, fuseutil.DT_Directory)

	// Fill in the response.
	return childID, child.Attrs(), nil
}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) CreateNode(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (fuseops.ChildInodeEntry, error) {
	fb.lock()
	defer fb.unlock()

	// Grab the parent, which we will update shortly.
	parent, err := fb.LoadNodeForWrite(parentID)
	if err != nil {
		log.Printf("create node err: err=%v", err.Error())
		return fuseops.ChildInodeEntry{}, err
	}

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(name)
	if exists {
		return fuseops.ChildInodeEntry{}, fuse.EEXIST
	}

	// Set up attributes for the child.
	now := time.Now()
	childAttrs := fuseops.InodeAttributes{
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
	childID, child := fb.allocateInode(childAttrs)

	// Add an entry in the parent.
	parent.AddChild(childID, name, fuseutil.DT_File)

	// Fill in the response entry.
	var entry fuseops.ChildInodeEntry
	entry.Child = fuseops.InodeID(childID)
	entry.Attributes = child.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	entry.EntryExpiration = entry.AttributesExpiration

	return entry, nil
}

func (fb *FBackEnd) CreateSymlink(
	ctx context.Context,
	parentID uint64,
	name string,
	target string) (uint64, fuseops.InodeAttributes, error) {
	fb.lock()
	defer fb.unlock()

	// Grab the parent, which we will update shortly.
	parent, err := fb.LoadNodeForWrite(parentID)
	if err != nil {
		log.Printf("create symlink err: err=%v", err.Error())
		return 0, fuseops.InodeAttributes{}, err
	}

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(name)
	if exists {
		return 0, fuseops.InodeAttributes{}, fuse.EEXIST
	}

	// Set up attributes from the child.
	now := time.Now()
	childAttrs := fuseops.InodeAttributes{
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
	childID, child := fb.allocateInode(childAttrs)

	// Set up its target.
	child.SetTarget(target)

	// Add an entry in the parent.
	parent.AddChild(childID, name, fuseutil.DT_Link)

	// Fill in the response entry.
	return childID, child.Attrs(), nil
}

func (fb *FBackEnd) CreateLink(
	ctx context.Context,
	parentID uint64,
	name string,
	targetID uint64) (fuseops.InodeAttributes, error) {
	fb.lock()
	defer fb.unlock()

	// Grab the parent, which we will update shortly.
	parent, err := fb.LoadNodeForWrite(parentID)
	if err != nil {
		log.Printf("create link load parent err: err=%v", err.Error())
		return fuseops.InodeAttributes{}, err
	}

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(name)
	if exists {
		return fuseops.InodeAttributes{}, fuse.EEXIST
	}

	// Get the target rnode.RNode to be linked
	target, err := fb.LoadNodeForWrite(targetID)
	if err != nil {
		log.Printf("create link load target err: err=%v", err.Error())
		return fuseops.InodeAttributes{}, err
	}

	// Update the attributes
	now := time.Now()
	attrs := target.Attrs()
	attrs.Nlink++
	attrs.Ctime = now
	target.SetAttrs(attrs)

	// Add an entry in the parent.
	parent.AddChild(targetID, name, fuseutil.DT_File)

	// Return the response.
	return target.Attrs(), nil
}

func (fb *FBackEnd) Rename(
	ctx context.Context,
	op fuseops.RenameOp) (err error) {
	fb.lock()
	defer fb.unlock()
	defer func() {
		if rout := recover(); rout != nil {
			if derr, ok := rout.(error); ok {
				log.Printf("rename error: err=%+v", err)
				err = derr
			}
			err = &FBackEndErr{msg: fmt.Sprintf("rename error: %#v", rout)}
			log.Printf(err.Error())
		}
	}()

	// Ask the old parent for the child's rnode.RNode ID and type.
	oldParent := fb.mustLoadInodeForWrite(uint64(op.OldParent))
	childID, childType, ok := oldParent.LookUpChild(op.OldName)

	if !ok {
		err = fuse.ENOENT
		return
	}

	// If the new name exists already in the new parent, make sure it's not a
	// non-empty directory, then delete it.
	newParent := fb.mustLoadInodeForWrite(uint64(op.NewParent))
	existingID, _, ok := newParent.LookUpChild(op.NewName)
	if ok {
		existing := fb.mustLoadInodeForWrite(existingID)

		var buf [4096]byte
		if existing.IsDir() && existing.ReadDir(buf[:], 0) > 0 {
			err = fuse.ENOTEMPTY
			return
		}

		newParent.RemoveChild(op.NewName)
	}

	// Link the new name.
	newParent.AddChild(
		childID,
		op.NewName,
		childType)

	// Finally, remove the old name from the old parent.
	oldParent.RemoveChild(op.OldName)

	return
}

func (fb *FBackEnd) RmDir(
	ctx context.Context,
	op fuseops.RmDirOp) (err error) {
	fb.lock()
	defer fb.unlock()
	defer func() {
		if rout := recover(); rout != nil {
			if derr, ok := rout.(error); ok {
				log.Printf("rmdir error: err=%+v", err)
				err = derr
			}
			err = &FBackEndErr{msg: fmt.Sprintf("rmdir error: %#v", rout)}
			log.Printf(err.Error())
		}
	}()

	// Grab the parent, which we will update shortly.
	parent := fb.mustLoadInodeForWrite(uint64(op.Parent))

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		err = fuse.ENOENT
		return
	}

	// Grab the child.
	child := fb.mustLoadInodeForWrite(childID)

	// Make sure the child is empty.
	if child.Len() != 0 {
		err = fuse.ENOTEMPTY
		return
	}

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	attrs := child.Attrs()
	attrs.Nlink--
	child.SetAttrs(attrs)

	return
}

func (fb *FBackEnd) Unlink(
	ctx context.Context,
	op fuseops.UnlinkOp) (err error) {
	fb.lock()
	defer fb.unlock()
	defer func() {
		if rout := recover(); rout != nil {
			if derr, ok := rout.(error); ok {
				log.Printf("unlink error: err=%+v", err)
				err = derr
			}
			err = &FBackEndErr{msg: fmt.Sprintf("unlink error: %#v", rout)}
			log.Printf(err.Error())
		}
	}()

	// Grab the parent, which we will update shortly.
	parent := fb.mustLoadInodeForWrite(uint64(op.Parent))

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		err = fuse.ENOENT
		return
	}

	// Grab the child.
	child := fb.mustLoadInodeForWrite(childID)

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	attrs := child.Attrs()
	attrs.Nlink--
	child.SetAttrs(attrs)

	return
}

func (fb *FBackEnd) OpenDir(
	ctx context.Context,
	id uint64) (handle uint64, err error) {
	fb.lock()
	defer fb.unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// rnode.RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	node, err := fb.LoadNodeForRead(id)
	if err != nil {
		log.Printf("open dir err: err=%v", err.Error())
		return 0, err
	}

	if !node.IsDir() {
		err := &FBackEndErr{msg: fmt.Sprintf("open dir non-dir error: id=%v", id)}
		log.Printf(err.Error())
		return 0, err
	}

	handle, err = fb.AllocateHandle(ctx, id)
	if err != nil {
		log.Printf("open dir allocate handle err: id=%v, err=%v", id, err.Error())
		return 0, err
	}

	log.Printf("open dir allocate handle success: id=%v, handle=%v", id, handle)
	return handle, nil
}

func (fb *FBackEnd) ReadDir(
	ctx context.Context,
	id uint64,
	length uint64,
	offset uint64) (bytesRead uint64, buf []byte, err error) {
	fb.lock()
	defer fb.unlock()

	// Grab the directory.
	node, err := fb.LoadNodeForRead(id)
	if err != nil {
		log.Printf("read dir err: err=%v", err.Error())
		return
	}

	buf = make([]byte, length)
	// Serve the request.
	bytesRead = uint64(node.ReadDir(buf, int(offset)))

	return
}

func (fb *FBackEnd) ReleaseDirHandle(
	ctx context.Context,
	handle uint64) error {
	log.Printf("release dir: handle=%v", handle)
	if err := fb.ReleaseHandle(ctx, handle); err != nil {
		log.Printf("release dir error: handle=%v, err=%+v",
			handle, err)
		return err
	}

	log.Printf("release dir success: handle=%v", handle)
	return nil
}

func (fb *FBackEnd) OpenFile(
	ctx context.Context,
	id uint64) (handle uint64, err error) {
	fb.lock()
	defer fb.unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// rnode.RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	node, err := fb.LoadNodeForRead(id)
	if err != nil {
		log.Printf("open file err: err=%v", err.Error())
		return 0, err
	}

	if !node.IsFile() {
		err := &FBackEndErr{msg: fmt.Sprintf("open file non-file error: id=%v", id)}
		log.Printf(err.Error())
		return 0, err
	}

	handle, err = fb.AllocateHandle(ctx, id)
	if err != nil {
		log.Printf("open file allocate handle err: id=%v, err=%v", id, err.Error())
		return 0, err
	}

	log.Printf("open file allocate handle success: id=%v, handle=%v", id, handle)
	return handle, nil
}

func (fb *FBackEnd) ReadFile(
	ctx context.Context,
	id uint64,
	length uint64,
	offset uint64) (bytesRead uint64, buf []byte, err error) {
	fb.lock()
	defer fb.unlock()

	// Find the rnode.RNode in question.
	node, err := fb.LoadNodeForRead(id)
	if err != nil {
		log.Printf("read file err: err=%v", err.Error())
		return
	}

	// Serve the request.
	buf = make([]byte, length)
	bytesReadI, err := node.ReadAt(buf, int64(offset))

	// Don't return EOF errors; we just indicate EOF to fuse using a short read.
	if err == io.EOF {
		log.Printf("readfile meets EOF, return nil: id=%v, length=%v, offset=%v", id, length, offset)
		err = nil
	}

	if err != nil {
		log.Printf("readfile error: id=%v, length=%v, offset=%v, err=%+v", id, length, offset, err)
	}
	bytesRead = uint64(bytesReadI)
	return
}

func (fb *FBackEnd) WriteFile(
	ctx context.Context,
	id uint64,
	offset uint64,
	data []byte) (uint64, error) {
	fb.lock()
	defer fb.unlock()

	// Find the rnode.RNode in question.
	node, err := fb.LoadNodeForWrite(id)
	if err != nil {
		log.Printf("write file err: err=%v", err.Error())
		return 0, err
	}

	// Serve the request.
	bytesWrite, err := node.WriteAt(data, int64(offset))

	if err == nil {
		log.Printf("write file: id=%v, offset=%v, bytesWrite=%v", id, offset, bytesWrite)
	} else {
		log.Printf("writefile error: id=%v, offset=%v, bytesWrite=%v, data=%v, err=%+v",
			id, offset, bytesWrite, data, err)
	}

	return uint64(bytesWrite), err
}

func (fb *FBackEnd) ReleaseFileHandle(
	ctx context.Context,
	handle uint64) error {
	log.Printf("release file: handle=%v", handle)
	if err := fb.ReleaseHandle(ctx, handle); err != nil {
		log.Printf("release file error: handle=%v, err=%+v",
			handle, err)
		return err
	}

	log.Printf("release file success: handle=%v", handle)
	return nil
}

func (fb *FBackEnd) ReadSymlink(
	ctx context.Context,
	id uint64) (target string, err error) {
	fb.lock()
	defer fb.unlock()

	// Find the rnode.RNode in question.
	node, err := fb.LoadNodeForRead(id)
	if err != nil {
		log.Printf("read symlink err: err=%v", err.Error())
		return
	}

	// Serve the request.
	target = node.Target()

	return
}

func (fb *FBackEnd) GetXattr(ctx context.Context,
	id uint64,
	name string,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	fb.lock()
	defer fb.unlock()

	node, err := fb.LoadNodeForRead(id)
	if err != nil {
		log.Printf("get xattr err: err=%v", err.Error())
		return
	}

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
		err = fuse.ENOATTR
		return
	}

	return
}

func (fb *FBackEnd) ListXattr(ctx context.Context,
	id uint64,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	fb.lock()
	defer fb.unlock()

	node, err := fb.LoadNodeForRead(id)
	if err != nil {
		log.Printf("list xattr err: err=%v", err.Error())
		return
	}

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

	return
}

func (fb *FBackEnd) RemoveXattr(ctx context.Context,
	id uint64,
	name string) error {
	fb.lock()
	defer fb.unlock()

	node, err := fb.LoadNodeForWrite(id)
	if err != nil {
		log.Printf("remove xattr err: err=%v", err.Error())
		return err
	}

	if _, ok := node.Xattrs()[name]; ok {
		xattrs := node.Xattrs()
		delete(xattrs, name)
		node.SetXattrs(xattrs)
	} else {
		return fuse.ENOATTR
	}
	return nil
}

func (fb *FBackEnd) SetXattr(ctx context.Context,
	op fuseops.SetXattrOp) error {
	fb.lock()
	defer fb.unlock()

	node, err := fb.LoadNodeForWrite(uint64(op.Inode))
	if err != nil {
		log.Printf("set xattr err: err=%v", err.Error())
		return err
	}

	_, hasAttr := node.Xattrs()[op.Name]

	switch op.Flags {
	case unix.XATTR_CREATE:
		if hasAttr {
			return fuse.EEXIST
		}
	case unix.XATTR_REPLACE:
		if !hasAttr {
			return fuse.ENOATTR
		}
	}

	value := make([]byte, len(op.Value))
	copy(value, op.Value)
	xattrs := node.Xattrs()
	xattrs[op.Name] = value
	node.SetXattrs(xattrs)
	return nil
}

func (fb *FBackEnd) Fallocate(ctx context.Context,
	id uint64,
	mode uint32,
	length uint64) error {
	fb.lock()
	defer fb.unlock()

	node, err := fb.LoadNodeForWrite(id)
	if err != nil {
		log.Printf("fallocate err: err=%v", err.Error())
		return err
	}

	err = node.Fallocate(mode, length, length)
	if err != nil {
		err := &FBackEndErr{msg: fmt.Sprintf("node fallocate error: err=%+v", err)}
		log.Printf(err.Error())
		return err
	}
	return nil
}

func (fb *FBackEnd) IsLocal(ctx context.Context, id uint64) bool {
	_, err := fb.LoadNodeForRead(id)
	return err == nil
}

func (e *FBackEndErr) Error() string {
	return fmt.Sprintf("fbackend error: %v", e.msg)
}
