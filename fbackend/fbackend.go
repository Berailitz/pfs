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
	"io"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

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

	nodes sync.Map // [fuseops.InodeID]*rnode.RNode
	rcli  *rclient.RClient
}

// Create a file system that stores data and metadata in memory.
//
// The supplied UID/GID pair will own the root rnode.RNode. This file system does no
// permissions checking, and should therefore be mounted with the
// default_permissions option.
func NewFBackEnd(
	uid uint32,
	gid uint32,
	rCliCfg rclient.RCliCfg) *FBackEnd {
	// Set up the basic struct.
	fb := &FBackEnd{
		uid:  uid,
		gid:  gid,
		rcli: rclient.NewRClient(rCliCfg),
	}

	// Set up the root rnode.RNode.
	fb.MakeRoot()

	return fb
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func (fb *FBackEnd) LoadNode(id fuseops.InodeID) (*rnode.RNode, bool) {
	if out, exist := fb.nodes.Load(id); exist {
		if node, ok := out.(*rnode.RNode); ok {
			return node, true
		}
	}
	return nil, false
}

func (fb *FBackEnd) StoreNode(id fuseops.InodeID, node *rnode.RNode) {
	fb.nodes.Store(id, node)
}

func (fb *FBackEnd) DeleteNode(id fuseops.InodeID) {
	fb.nodes.Delete(id)
}

// MakeRoot should only be called at new
func (fb *FBackEnd) MakeRoot() {
	ok := fb.rcli.AllocateRoot(fuseops.RootInodeID)
	if ok {
		rootAttrs := fuseops.InodeAttributes{
			Mode: 0700 | os.ModeDir,
			Uid:  fb.uid,
			Gid:  fb.gid,
		}
		fb.StoreNode(fuseops.InodeID(fuseops.RootInodeID), rnode.NewRNode(rootAttrs, fuseops.RootInodeID))
	}
}

func (fb *FBackEnd) Lock() {
	fb.mu.Lock()
}

func (fb *FBackEnd) Unlock() {
	fb.mu.Unlock()
}

// Find the given rnode.RNode. Panic if it doesn't exist.
//
// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) MustLoadInode(id fuseops.InodeID) *rnode.RNode {
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
	attrs fuseops.InodeAttributes) (fuseops.InodeID, *rnode.RNode) {
	// Create the rnode.RNode.
	id := fb.rcli.Allocate()
	if id > 0 {
		node := rnode.NewRNode(attrs, id)
		fb.StoreNode(id, node)
		return id, node
	}

	return 0, nil
}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) deallocateInode(id fuseops.InodeID) {
	ok := fb.rcli.Deallocate(id)
	if ok {
		fb.DeleteNode(id)
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
	op *fuseops.LookUpInodeOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Grab the parent directory.
	node := fb.MustLoadInode(op.Parent)

	// Does the directory have an entry with the given name?
	childID, _, ok := node.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := fb.MustLoadInode(childID)

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fb *FBackEnd) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Grab the rnode.RNode.
	node := fb.MustLoadInode(op.Inode)

	// Fill in the response.
	op.Attributes = node.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return nil
}

func (fb *FBackEnd) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	var err error
	if op.Size != nil && op.Handle == nil && *op.Size != 0 {
		// require that truncate to non-zero has to be ftruncate()
		// but allow open(O_TRUNC)
		err = syscall.EBADF
	}

	// Grab the rnode.RNode.
	node := fb.MustLoadInode(op.Inode)

	// Handle the request.
	node.SetAttributes(op.Size, op.Mode, op.Mtime)

	// Fill in the response.
	op.Attributes = node.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return err
}

func (fb *FBackEnd) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fb.MustLoadInode(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		return fuse.EEXIST
	}

	// Set up attributes from the child.
	childAttrs := fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  op.Mode,
		Uid:   fb.uid,
		Gid:   fb.gid,
	}

	// Allocate a child.
	childID, child := fb.allocateInode(childAttrs)

	// Add an entry in the parent.
	parent.AddChild(childID, op.Name, fuseutil.DT_Directory)

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fb *FBackEnd) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	var err error
	op.Entry, err = fb.DoCreateFile(op.Parent, op.Name, op.Mode)
	return err
}

// LOCKS_REQUIRED(fb.mu)
func (fb *FBackEnd) DoCreateFile(
	parentID fuseops.InodeID,
	name string,
	mode os.FileMode) (fuseops.ChildInodeEntry, error) {
	// Grab the parent, which we will update shortly.
	parent := fb.MustLoadInode(parentID)

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
	entry.Child = childID
	entry.Attributes = child.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	entry.EntryExpiration = entry.AttributesExpiration

	return entry, nil
}

func (fb *FBackEnd) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	var err error
	op.Entry, err = fb.DoCreateFile(op.Parent, op.Name, op.Mode)
	return err
}

func (fb *FBackEnd) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fb.MustLoadInode(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		return fuse.EEXIST
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
	child.SetTarget(op.Target)

	// Add an entry in the parent.
	parent.AddChild(childID, op.Name, fuseutil.DT_Link)

	// Fill in the response entry.
	op.Entry.Child = childID
	op.Entry.Attributes = child.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fb *FBackEnd) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fb.MustLoadInode(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		return fuse.EEXIST
	}

	// Get the target rnode.RNode to be linked
	target := fb.MustLoadInode(op.Target)

	// Update the attributes
	now := time.Now()
	attrs := target.Attrs()
	attrs.Nlink++
	attrs.Ctime = now
	target.SetAttrs(attrs)

	// Add an entry in the parent.
	parent.AddChild(op.Target, op.Name, fuseutil.DT_File)

	// Return the response.
	op.Entry.Child = op.Target
	op.Entry.Attributes = target.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fb *FBackEnd) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Ask the old parent for the child's rnode.RNode ID and type.
	oldParent := fb.MustLoadInode(op.OldParent)
	childID, childType, ok := oldParent.LookUpChild(op.OldName)

	if !ok {
		return fuse.ENOENT
	}

	// If the new name exists already in the new parent, make sure it's not a
	// non-empty directory, then delete it.
	newParent := fb.MustLoadInode(op.NewParent)
	existingID, _, ok := newParent.LookUpChild(op.NewName)
	if ok {
		existing := fb.MustLoadInode(existingID)

		var buf [4096]byte
		if existing.IsDir() && existing.ReadDir(buf[:], 0) > 0 {
			return fuse.ENOTEMPTY
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

	return nil
}

func (fb *FBackEnd) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fb.MustLoadInode(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := fb.MustLoadInode(childID)

	// Make sure the child is empty.
	if child.Len() != 0 {
		return fuse.ENOTEMPTY
	}

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	attrs := child.Attrs()
	attrs.Nlink--
	child.SetAttrs(attrs)

	return nil
}

func (fb *FBackEnd) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fb.MustLoadInode(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := fb.MustLoadInode(childID)

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	attrs := child.Attrs()
	attrs.Nlink--
	child.SetAttrs(attrs)

	return nil
}

func (fb *FBackEnd) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// rnode.RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	node := fb.MustLoadInode(op.Inode)

	if !node.IsDir() {
		panic("Found non-dir.")
	}

	return nil
}

func (fb *FBackEnd) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Grab the directory.
	node := fb.MustLoadInode(op.Inode)

	// Serve the request.
	op.BytesRead = node.ReadDir(op.Dst, int(op.Offset))

	return nil
}

func (fb *FBackEnd) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// rnode.RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	node := fb.MustLoadInode(op.Inode)

	if !node.IsFile() {
		panic("Found non-file.")
	}

	return nil
}

func (fb *FBackEnd) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Find the rnode.RNode in question.
	node := fb.MustLoadInode(op.Inode)

	// Serve the request.
	var err error
	op.BytesRead, err = node.ReadAt(op.Dst, op.Offset)

	// Don't return EOF errors; we just indicate EOF to fuse using a short read.
	if err == io.EOF {
		return nil
	}

	return err
}

func (fb *FBackEnd) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Find the rnode.RNode in question.
	node := fb.MustLoadInode(op.Inode)

	// Serve the request.
	_, err := node.WriteAt(op.Data, op.Offset)

	return err
}

func (fb *FBackEnd) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Find the rnode.RNode in question.
	node := fb.MustLoadInode(op.Inode)

	// Serve the request.
	op.Target = node.Target()

	return nil
}

func (fb *FBackEnd) GetXattr(ctx context.Context,
	op *fuseops.GetXattrOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	node := fb.MustLoadInode(op.Inode)
	if value, ok := node.Xattrs()[op.Name]; ok {
		op.BytesRead = len(value)
		if len(op.Dst) >= len(value) {
			copy(op.Dst, value)
		} else if len(op.Dst) != 0 {
			return syscall.ERANGE
		}
	} else {
		return fuse.ENOATTR
	}

	return nil
}

func (fb *FBackEnd) ListXattr(ctx context.Context,
	op *fuseops.ListXattrOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()

	node := fb.MustLoadInode(op.Inode)

	dst := op.Dst[:]
	for key := range node.Xattrs() {
		keyLen := len(key) + 1

		if len(dst) >= keyLen {
			copy(dst, key)
			dst = dst[keyLen:]
		} else if len(op.Dst) != 0 {
			return syscall.ERANGE
		}
		op.BytesRead += keyLen
	}

	return nil
}

func (fb *FBackEnd) RemoveXattr(ctx context.Context,
	op *fuseops.RemoveXattrOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	node := fb.MustLoadInode(op.Inode)

	if _, ok := node.Xattrs()[op.Name]; ok {
		xattrs := node.Xattrs()
		delete(xattrs, op.Name)
		node.SetXattrs(xattrs)
	} else {
		return fuse.ENOATTR
	}
	return nil
}

func (fb *FBackEnd) SetXattr(ctx context.Context,
	op *fuseops.SetXattrOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	node := fb.MustLoadInode(op.Inode)

	_, ok := node.Xattrs()[op.Name]

	switch op.Flags {
	case unix.XATTR_CREATE:
		if ok {
			return fuse.EEXIST
		}
	case unix.XATTR_REPLACE:
		if !ok {
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
	op *fuseops.FallocateOp) error {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	node := fb.MustLoadInode(op.Inode)
	err := node.Fallocate(op.Mode, op.Length, op.Length)
	if err != nil {
		log.Fatalf("node fallocate error: err=%+v", err)
	}
	return nil
}
