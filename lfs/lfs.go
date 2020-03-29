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

package lfs

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/Berailitz/pfs/rnode"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/syncutil"
	"golang.org/x/sys/unix"
)

type LFS struct {
	fuseutil.NotImplementedFileSystem

	// The UID and GID that every RNode receives.
	uid uint32
	gid uint32

	/////////////////////////
	// Mutable state
	/////////////////////////

	mu syncutil.InvariantMutex

	// The collection of live inodes, indexed by ID. IDs of free inodes that may
	// be re-used have nil entries. No ID less than fuseops.RootInodeID is ever
	// used.
	//
	// All inodes are protected by the file system mutex.
	//
	// INVARIANT: For each RNode in, in.CheckInvariants() does not panic.
	// INVARIANT: len(inodes) > fuseops.RootInodeID
	// INVARIANT: For all i < fuseops.RootInodeID, inodes[i] == nil
	// INVARIANT: inodes[fuseops.RootInodeID] != nil
	// INVARIANT: inodes[fuseops.RootInodeID].IsDir()
	inodes []*rnode.RNode // GUARDED_BY(mu)

	// A list of RNode IDs within inodes available for reuse, not including the
	// reserved IDs less than fuseops.RootInodeID.
	//
	// INVARIANT: This is all and only indices i of 'inodes' such that i >
	// fuseops.RootInodeID and inodes[i] == nil
	freeInodes []fuseops.InodeID // GUARDED_BY(mu)
}

// Create a file system that stores data and metadata in memory.
//
// The supplied UID/GID pair will own the root RNode. This file system does no
// permissions checking, and should therefore be mounted with the
// default_permissions option.
func NewLFS(
	uid uint32,
	gid uint32) *LFS {
	// Set up the basic struct.
	lfs := &LFS{
		inodes: make([]*rnode.RNode, fuseops.RootInodeID+1),
		uid:    uid,
		gid:    gid,
	}

	// Set up the root RNode.
	rootAttrs := fuseops.InodeAttributes{
		Mode: 0700 | os.ModeDir,
		Uid:  uid,
		Gid:  gid,
	}

	lfs.inodes[fuseops.RootInodeID] = rnode.NewRNode(rootAttrs, fuseops.RootInodeID)

	// Set up invariant checking.
	lfs.mu = syncutil.NewInvariantMutex(lfs.checkInvariants)

	return lfs
}

func NewLFSServer(lfs *LFS) fuse.Server {
	if lfs == nil {
		return nil
	}
	return fuseutil.NewFileSystemServer(lfs)
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func (lfs *LFS) Lock() {
	lfs.mu.Lock()
}

func (lfs *LFS) Unlock() {
	lfs.mu.Unlock()
}

func (lfs *LFS) checkInvariants() {
	// Check reserved inodes.
	for i := 0; i < fuseops.RootInodeID; i++ {
		if lfs.inodes[i] != nil {
			panic(fmt.Sprintf("Non-nil RNode for ID: %v", i))
		}
	}

	// Check the root RNode.
	if !lfs.inodes[fuseops.RootInodeID].IsDir() {
		panic("Expected root to be a directory.")
	}

	// Build our own list of free IDs.
	freeIDsEncountered := make(map[fuseops.InodeID]struct{})
	for i := fuseops.RootInodeID + 1; i < len(lfs.inodes); i++ {
		RNode := lfs.inodes[i]
		if RNode == nil {
			freeIDsEncountered[fuseops.InodeID(i)] = struct{}{}
			continue
		}
	}

	// Check lfs.freeInodes.
	if len(lfs.freeInodes) != len(freeIDsEncountered) {
		panic(
			fmt.Sprintf(
				"Length mismatch: %v vs. %v",
				len(lfs.freeInodes),
				len(freeIDsEncountered)))
	}

	for _, id := range lfs.freeInodes {
		if _, ok := freeIDsEncountered[id]; !ok {
			panic(fmt.Sprintf("Unexected free RNode ID: %v", id))
		}
	}

	// INVARIANT: For each RNode in, in.CheckInvariants() does not panic.
	for _, in := range lfs.inodes {
		in.CheckInvariants()
	}
}

// Find the given RNode. Panic if it doesn't exist.
//
// LOCKS_REQUIRED(lfs.mu)
func (lfs *LFS) GetInodeOrDie(id fuseops.InodeID) *rnode.RNode {
	// TODO: lock remote RNode
	RNode := lfs.inodes[id]
	if RNode == nil {
		panic(fmt.Sprintf("Unknown RNode: %v", id))
	}

	return RNode
}

// Find the given RNode. Return nil if it doesn't exist.
//
// LOCKS_REQUIRED(lfs.mu)
func (lfs *LFS) GetInode(id fuseops.InodeID) *rnode.RNode {
	// TODO: lock remote RNode
	RNode := lfs.inodes[id]
	if RNode == nil {
		fmt.Printf("Unknown RNode: %v", id)
		return nil
	}

	return RNode
}

// Allocate a new RNode, assigning it an ID that is not in use.
//
// LOCKS_REQUIRED(lfs.mu)
func (lfs *LFS) allocateInode(
	attrs fuseops.InodeAttributes) (id fuseops.InodeID, RNode *rnode.RNode) {
	// Create the RNode.

	// Re-use a free ID if possible. Otherwise mint a new one.
	numFree := len(lfs.freeInodes)
	if numFree != 0 {
		id = lfs.freeInodes[numFree-1]
		RNode = rnode.NewRNode(attrs, id)
		lfs.freeInodes = lfs.freeInodes[:numFree-1]
		lfs.inodes[id] = RNode
	} else {
		id = fuseops.InodeID(len(lfs.inodes))
		RNode = rnode.NewRNode(attrs, id)
		lfs.inodes = append(lfs.inodes, RNode)
	}

	return id, RNode
}

// LOCKS_REQUIRED(lfs.mu)
func (lfs *LFS) deallocateInode(id fuseops.InodeID) {
	lfs.freeInodes = append(lfs.freeInodes, id)
	lfs.inodes[id] = nil
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (lfs *LFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	return nil
}

func (lfs *LFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {
	log.Printf("look up inode: parent=%v, name=%v", op.Parent, op.Name)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Grab the parent directory.
	RNode := lfs.GetInodeOrDie(op.Parent)

	// Does the directory have an entry with the given name?
	childID, _, ok := RNode.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := lfs.GetInodeOrDie(childID)

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	log.Printf("look up inode success: parent=%v, name=%v", op.Parent, op.Name)
	return nil
}

func (lfs *LFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	log.Printf("get inode attr: id=%v", op.Inode)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Grab the RNode.
	RNode := lfs.GetInodeOrDie(op.Inode)

	// Fill in the response.
	op.Attributes = RNode.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	log.Printf("get inode attr success: id=%v", op.Inode)
	return nil
}

func (lfs *LFS) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) error {
	log.Printf("set inode attr: id=%v, size=%v, mode=%v, mtime=%v",
		op.Inode, op.Size, op.Mode, op.Mtime)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	var err error
	if op.Size != nil && op.Handle == nil && *op.Size != 0 {
		// require that truncate to non-zero has to be ftruncate()
		// but allow open(O_TRUNC)
		err = syscall.EBADF
	}

	// Grab the RNode.
	RNode := lfs.GetInodeOrDie(op.Inode)

	// Handle the request.
	RNode.SetAttributes(op.Size, op.Mode, op.Mtime)

	// Fill in the response.
	op.Attributes = RNode.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	log.Printf("set inode attr success: id=%v, size=%v, mode=%v, mtime=%v",
		op.Inode, op.Size, op.Mode, op.Mtime)
	return err
}

func (lfs *LFS) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) error {
	log.Printf("mkdir: parent=%v, name=%v, mode=%v",
		op.Parent, op.Name, op.Mode)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := lfs.GetInodeOrDie(op.Parent)

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
		Uid:   lfs.uid,
		Gid:   lfs.gid,
	}

	// Allocate a child.
	childID, child := lfs.allocateInode(childAttrs)

	// Add an entry in the parent.
	parent.AddChild(childID, op.Name, fuseutil.DT_Directory)

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	log.Printf("mkdir success: parent=%v, name=%v, mode=%v",
		op.Parent, op.Name, op.Mode)
	return nil
}

func (lfs *LFS) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) error {
	log.Printf("mknode: parent=%v, name=%v, mode=%v",
		op.Parent, op.Name, op.Mode)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	var err error
	op.Entry, err = lfs.DoCreateFile(op.Parent, op.Name, op.Mode)
	log.Printf("mknode success: parent=%v, name=%v, mode=%v",
		op.Parent, op.Name, op.Mode)
	return err
}

// LOCKS_REQUIRED(lfs.mu)
func (lfs *LFS) DoCreateFile(
	parentID fuseops.InodeID,
	name string,
	mode os.FileMode) (fuseops.ChildInodeEntry, error) {
	// Grab the parent, which we will update shortly.
	parent := lfs.GetInodeOrDie(parentID)

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
		Uid:    lfs.uid,
		Gid:    lfs.gid,
	}

	// Allocate a child.
	childID, child := lfs.allocateInode(childAttrs)

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

func (lfs *LFS) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) error {
	log.Printf("create file: parent=%v, name=%v, mode=%v",
		op.Parent, op.Name, op.Mode)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	var err error
	op.Entry, err = lfs.DoCreateFile(op.Parent, op.Name, op.Mode)
	log.Printf("create file success: op=%#v", op)
	return err
}

func (lfs *LFS) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) error {
	log.Printf("create symlink: parent=%v, name=%v, target=%v",
		op.Parent, op.Name, op.Target)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := lfs.GetInodeOrDie(op.Parent)

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
		Uid:    lfs.uid,
		Gid:    lfs.gid,
	}

	// Allocate a child.
	childID, child := lfs.allocateInode(childAttrs)

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

	log.Printf("create symlink success: parent=%v, name=%v, target=%v",
		op.Parent, op.Name, op.Target)
	return nil
}

func (lfs *LFS) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) error {
	log.Printf("create link: parent=%v, name=%v, target=%v",
		op.Parent, op.Name, op.Target)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := lfs.GetInodeOrDie(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		return fuse.EEXIST
	}

	// Get the target RNode to be linked
	target := lfs.GetInodeOrDie(op.Target)

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

	log.Printf("create link success: parent=%v, name=%v, target=%v",
		op.Parent, op.Name, op.Target)
	return nil
}

func (lfs *LFS) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) error {
	log.Printf("rename: op=%#v", op)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Ask the old parent for the child's RNode ID and type.
	oldParent := lfs.GetInodeOrDie(op.OldParent)
	childID, childType, ok := oldParent.LookUpChild(op.OldName)

	if !ok {
		return fuse.ENOENT
	}

	// If the new name exists already in the new parent, make sure it's not a
	// non-empty directory, then delete it.
	newParent := lfs.GetInodeOrDie(op.NewParent)
	existingID, _, ok := newParent.LookUpChild(op.NewName)
	if ok {
		existing := lfs.GetInodeOrDie(existingID)

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

	log.Printf("rename success: op=%#v", op)
	return nil
}

func (lfs *LFS) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) error {
	log.Printf("rmdir: op=%#v", op)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := lfs.GetInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := lfs.GetInodeOrDie(childID)

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

	log.Printf("rmdir success: op=%#v", op)
	return nil
}

func (lfs *LFS) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	log.Printf("unlink: op=%#v", op)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := lfs.GetInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := lfs.GetInodeOrDie(childID)

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	attrs := child.Attrs()
	attrs.Nlink--
	child.SetAttrs(attrs)

	log.Printf("unlink success: op=%#v", op)
	return nil
}

func (lfs *LFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	log.Printf("opendir: id=%v", op.Inode)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	RNode := lfs.GetInodeOrDie(op.Inode)

	if !RNode.IsDir() {
		panic("Found non-dir.")
	}

	log.Printf("opendir success: id=%v", op.Inode)
	return nil
}

func (lfs *LFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	log.Printf("readdir: id=%v, len=%v, offset=%v",
		op.Inode, len(op.Dst), op.Offset)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Grab the directory.
	RNode := lfs.GetInodeOrDie(op.Inode)

	// Serve the request.
	op.BytesRead = RNode.ReadDir(op.Dst, int(op.Offset))

	log.Printf("readdir success: id=%v, len=%v, offset=%v, bytesRead=%v, dst=%X",
		op.Inode, len(op.Dst), op.Offset, op.BytesRead, op.Dst[:op.BytesRead])
	return nil
}

func (lfs *LFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	log.Printf("openfile: id=%v", op.Inode)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	RNode := lfs.GetInodeOrDie(op.Inode)

	if !RNode.IsFile() {
		panic("Found non-file.")
	}

	log.Printf("openfile success: id=%v", op.Inode)
	return nil
}

func (lfs *LFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	log.Printf("readfile success: id=%v, offset=%v", op.Inode, op.Offset)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Find the RNode in question.
	RNode := lfs.GetInodeOrDie(op.Inode)

	// Serve the request.
	var err error
	op.BytesRead, err = RNode.ReadAt(op.Dst, op.Offset)

	// Don't return EOF errors; we just indicate EOF to fuse using a short read.
	if err == io.EOF {
		return nil
	}

	log.Printf("readfile success: id=%v, offset=%v, bytesread=%v, dst=%X",
		op.Inode, op.Offset, op.BytesRead, op.Dst[:op.BytesRead])
	return err
}

func (lfs *LFS) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	log.Printf("write file: id=%v, offset=%v", op.Inode, op.Offset)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Find the RNode in question.
	RNode := lfs.GetInodeOrDie(op.Inode)

	// Serve the request.
	bytesWrite, err := RNode.WriteAt(op.Data, op.Offset)

	log.Printf("write file success: id=%v, offset=%v, bytesWrite=%v", op.Inode, op.Offset, bytesWrite)
	return err
}

func (lfs *LFS) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) error {
	log.Printf("read symlink: id=%v", op.Inode)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Find the RNode in question.
	RNode := lfs.GetInodeOrDie(op.Inode)

	// Serve the request.
	op.Target = RNode.Target()

	log.Printf("read symlink success: id=%v, target=%v", op.Inode, op.Target)
	return nil
}

func (lfs *LFS) GetXattr(ctx context.Context,
	op *fuseops.GetXattrOp) error {
	log.Printf("get xattr: id=%v, name=%v, length=%v",
		op.Inode, op.Name, len(op.Dst))
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	RNode := lfs.GetInodeOrDie(op.Inode)
	if value, ok := RNode.Xattrs()[op.Name]; ok {
		op.BytesRead = len(value)
		if len(op.Dst) >= len(value) {
			copy(op.Dst, value)
		} else if len(op.Dst) != 0 {
			return syscall.ERANGE
		}
	} else {
		return fuse.ENOATTR
	}

	log.Printf("get xattr success: id=%v, name=%v, length=%v, bytesRead=%v",
		op.Inode, op.Name, len(op.Dst), op.BytesRead)
	return nil
}

func (lfs *LFS) ListXattr(ctx context.Context,
	op *fuseops.ListXattrOp) error {
	log.Printf("list xattr: id=%v, length=%v",
		op.Inode, len(op.Dst))
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	RNode := lfs.GetInodeOrDie(op.Inode)

	dst := op.Dst[:]
	for key := range RNode.Xattrs() {
		keyLen := len(key) + 1

		if len(dst) >= keyLen {
			copy(dst, key)
			dst = dst[keyLen:]
		} else if len(op.Dst) != 0 {
			return syscall.ERANGE
		}
		op.BytesRead += keyLen
	}

	log.Printf("list xattr success: id=%v, length=%v, bytesRead=%v",
		op.Inode, len(op.Dst), op.BytesRead)
	return nil
}

func (lfs *LFS) RemoveXattr(ctx context.Context,
	op *fuseops.RemoveXattrOp) error {
	log.Printf("rm xattr: id=%v, name=%v", op.Inode, op.Name)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()
	RNode := lfs.GetInodeOrDie(op.Inode)

	if _, ok := RNode.Xattrs()[op.Name]; ok {
		xattrs := RNode.Xattrs()
		delete(xattrs, op.Name)
		RNode.SetXattrs(xattrs)
	} else {
		return fuse.ENOATTR
	}
	log.Printf("rm xattr success: id=%v, name=%v", op.Inode, op.Name)
	return nil
}

func (lfs *LFS) SetXattr(ctx context.Context,
	op *fuseops.SetXattrOp) error {
	log.Printf("set xattr: op=%#v", op)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()
	RNode := lfs.GetInodeOrDie(op.Inode)

	_, ok := RNode.Xattrs()[op.Name]

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
	xattrs := RNode.Xattrs()
	xattrs[op.Name] = value
	RNode.SetXattrs(xattrs)
	log.Printf("set xattr success: op=%#v", op)
	return nil
}

func (lfs *LFS) Fallocate(ctx context.Context,
	op *fuseops.FallocateOp) error {
	log.Printf("fallocate: op=%#v", op)
	lfs.mu.Lock()
	defer lfs.mu.Unlock()
	RNode := lfs.GetInodeOrDie(op.Inode)
	RNode.Fallocate(op.Mode, op.Length, op.Length)
	log.Printf("fallocate success: op=%#v", op)
	return nil
}
