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
		uid:  uid,
		gid:  gid,
		mcli: mcli,
		pool: gclipool.NewGCliPool(gopts),
	}

	// Set up the root rnode.RNode.
	fb.MakeRoot()

	return fb
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////
func (fb *FBackEnd) LoadNode(id uint64) (*rnode.RNode, bool) {
	if out, exist := fb.nodes.Load(id); exist {
		if node, ok := out.(*rnode.RNode); ok {
			return node, true
		}
	}
	log.Printf("load node error: id=%v", id)
	return nil, false
}

func (fb *FBackEnd) LoadNodeForRead(id uint64) (*rnode.RNode, bool) {
	if out, exist := fb.nodes.Load(id); exist {
		if node, ok := out.(*rnode.RNode); ok {
			return node, true
		}
	} else {
		addr := fb.mcli.QueryOwner(id)
		gcli := fb.pool.Load(addr)
		if gcli == nil {
			return nil, false
		}

		ctx := context.Background()
		node, err := gcli.FetchNode(ctx, &pb.NodeId{
			Id: id,
		})
		if err != nil {
			log.Printf("rpc fetch node error: id=%v, err=%+v",
				id, err)
			return nil, false
		}

		return utility.FromPbNode(node), true
	}
	log.Printf("load node error: id=%v", id)
	return nil, false
}

func (fb *FBackEnd) storeNode(id uint64, node *rnode.RNode) {
	fb.nodes.Store(id, node)
}

func (fb *FBackEnd) deleteNode(id uint64) {
	fb.nodes.Delete(id)
}

// MakeRoot should only be called at new
func (fb *FBackEnd) MakeRoot() {
	ok := fb.mcli.AllocateRoot()
	if ok {
		rootAttrs := fuseops.InodeAttributes{
			Mode: 0700 | os.ModeDir,
			Uid:  fb.uid,
			Gid:  fb.gid,
		}
		fb.storeNode(uint64(fuseops.RootInodeID), rnode.NewRNode(rootAttrs, fuseops.RootInodeID))
	}
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
func (fb *FBackEnd) mustLoadInode(id uint64) *rnode.RNode {
	// TODO: lock remote rnode.RNode
	if node, ok := fb.LoadNode(id); ok {
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
		fb.storeNode(id, node)
		return id, node
	}

	return 0, nil
}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) deallocateInode(id uint64) {
	ok := fb.mcli.Deallocate(id)
	if ok {
		fb.deleteNode(id)
	}
	log.Fatalf("deallocate error")
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
	parent, ok := fb.LoadNodeForRead(parentID)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("look up node parent not found error: id=%v", parentID)}
		log.Printf(err.Error())
		return 0, fuseops.InodeAttributes{}, err
	}

	// Does the directory have an entry with the given name?
	childID, _, ok := parent.LookUpChild(name)
	if !ok {
		return 0, fuseops.InodeAttributes{}, fuse.ENOENT
	}

	// Grab the child.
	child, ok := fb.LoadNodeForRead(childID)
	// TODO: handle remote child node, by GetInodeAttributes
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("look up node child not found error: id=%v", childID)}
		log.Printf(err.Error())
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
	node, ok := fb.LoadNodeForRead(id)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("get node attr not found error: id=%v", id)}
		log.Printf(err.Error())
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
	node, ok := fb.LoadNode(id)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("set node attr not found error: id=%v", id)}
		log.Printf(err.Error())
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
	parent, ok := fb.LoadNode(parentID)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("mkdir parent not found error: parentID=%v, name=%v",
			parentID, name)}
		log.Printf(err.Error())
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
	parent, ok := fb.LoadNode(parentID)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("do create file parent not found error: parentID=%v, name=%v",
			parentID, name)}
		log.Printf(err.Error())
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
	parent, ok := fb.LoadNode(parentID)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("create symlink parent not found error: parentID=%v, name=%v",
			parentID, name)}
		log.Printf(err.Error())
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
	parent, ok := fb.LoadNode(parentID)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("create link parent not found error: parentID=%v, name=%v",
			parentID, name)}
		log.Printf(err.Error())
		return fuseops.InodeAttributes{}, err
	}

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(name)
	if exists {
		return fuseops.InodeAttributes{}, fuse.EEXIST
	}

	// Get the target rnode.RNode to be linked
	target, ok := fb.LoadNode(targetID)
	if !ok {
		err := &FBackEndErr{
			msg: fmt.Sprintf("create link target not found error: parentID=%v, name=%v, targetID=%v",
				parentID, name, targetID),
		}
		log.Printf(err.Error())
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
	oldParent := fb.mustLoadInode(uint64(op.OldParent))
	childID, childType, ok := oldParent.LookUpChild(op.OldName)

	if !ok {
		err = fuse.ENOENT
		return
	}

	// If the new name exists already in the new parent, make sure it's not a
	// non-empty directory, then delete it.
	newParent := fb.mustLoadInode(uint64(op.NewParent))
	existingID, _, ok := newParent.LookUpChild(op.NewName)
	if ok {
		existing := fb.mustLoadInode(existingID)

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
	parent := fb.mustLoadInode(uint64(op.Parent))

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		err = fuse.ENOENT
		return
	}

	// Grab the child.
	child := fb.mustLoadInode(childID)

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
	parent := fb.mustLoadInode(uint64(op.Parent))

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		err = fuse.ENOENT
		return
	}

	// Grab the child.
	child := fb.mustLoadInode(childID)

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
	id uint64) error {
	fb.lock()
	defer fb.unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// rnode.RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	node, ok := fb.LoadNodeForRead(id)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("open dir not found error: id=%v", id)}
		log.Printf(err.Error())
		return err
	}

	if !node.IsDir() {
		err := &FBackEndErr{msg: fmt.Sprintf("open dir non-dir error: id=%v", id)}
		log.Printf(err.Error())
		return err
	}

	return nil
}

func (fb *FBackEnd) ReadDir(
	ctx context.Context,
	id uint64,
	length uint64,
	offset uint64) (bytesRead uint64, buf []byte, err error) {
	fb.lock()
	defer fb.unlock()

	// Grab the directory.
	node, ok := fb.LoadNodeForRead(id)
	if !ok {
		err = &FBackEndErr{msg: fmt.Sprintf("read dir not found error: id=%v", id)}
		log.Printf(err.Error())
		return
	}

	buf = make([]byte, length)
	// Serve the request.
	bytesRead = uint64(node.ReadDir(buf, int(offset)))

	return
}

func (fb *FBackEnd) OpenFile(
	ctx context.Context,
	id uint64) error {
	fb.lock()
	defer fb.unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// rnode.RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	node, ok := fb.LoadNodeForRead(id)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("open file not found error: id=%v", id)}
		log.Printf(err.Error())
		return err
	}

	if !node.IsFile() {
		err := &FBackEndErr{msg: fmt.Sprintf("open file non-file error: id=%v", id)}
		log.Printf(err.Error())
		return err
	}

	return nil
}

func (fb *FBackEnd) ReadFile(
	ctx context.Context,
	id uint64,
	length uint64,
	offset uint64) (bytesRead uint64, buf []byte, err error) {
	fb.lock()
	defer fb.unlock()

	// Find the rnode.RNode in question.
	node, ok := fb.LoadNodeForRead(id)
	if !ok {
		err = &FBackEndErr{msg: fmt.Sprintf("read file not found error: id=%v", id)}
		log.Printf(err.Error())
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
	node, ok := fb.LoadNode(id)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("write file not found error: id=%v", id)}
		log.Printf(err.Error())
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

func (fb *FBackEnd) ReadSymlink(
	ctx context.Context,
	id uint64) (target string, err error) {
	fb.lock()
	defer fb.unlock()

	// Find the rnode.RNode in question.
	node, ok := fb.LoadNodeForRead(id)
	if !ok {
		err = &FBackEndErr{msg: fmt.Sprintf("read symlink not found error: id=%v", id)}
		log.Printf(err.Error())
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

	node, ok := fb.LoadNodeForRead(id)
	if !ok {
		err = &FBackEndErr{msg: fmt.Sprintf("get xattr not found error: id=%v", id)}
		log.Printf(err.Error())
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

	node, ok := fb.LoadNodeForRead(id)
	if !ok {
		err = &FBackEndErr{msg: fmt.Sprintf("get xattr not found error: id=%v", id)}
		log.Printf(err.Error())
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

	node, ok := fb.LoadNode(id)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("get xattr not found error: id=%v", id)}
		log.Printf(err.Error())
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

	node, hasNode := fb.LoadNode(uint64(op.Inode))
	if !hasNode {
		err := &FBackEndErr{msg: fmt.Sprintf("set xattr not found error: id=%v", op.Inode)}
		log.Printf(err.Error())
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

	node, ok := fb.LoadNode(id)
	if !ok {
		err := &FBackEndErr{msg: fmt.Sprintf("fallocate not found error: id=%v", id)}
		log.Printf(err.Error())
		return err
	}

	err := node.Fallocate(mode, length, length)
	if err != nil {
		err := &FBackEndErr{msg: fmt.Sprintf("node fallocate error: err=%+v", err)}
		log.Printf(err.Error())
		return err
	}
	return nil
}

func (fb *FBackEnd) IsLocal(ctx context.Context, id uint64) bool {
	_, ok := fb.LoadNode(id)
	return ok
}

func (e *FBackEndErr) Error() string {
	return fmt.Sprintf("fbackend error: %v", e.msg)
}
