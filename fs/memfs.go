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

package fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/syncutil"
	"golang.org/x/sys/unix"
)

type MemFS struct {
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
	// INVARIANT: inodes[fuseops.RootInodeID].isDir()
	inodes []*RNode // GUARDED_BY(mu)

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
func NewMemFS(
	uid uint32,
	gid uint32) *MemFS {
	// Set up the basic struct.
	fs := &MemFS{
		inodes: make([]*RNode, fuseops.RootInodeID+1),
		uid:    uid,
		gid:    gid,
	}

	// Set up the root RNode.
	rootAttrs := fuseops.InodeAttributes{
		Mode: 0700 | os.ModeDir,
		Uid:  uid,
		Gid:  gid,
	}

	fs.inodes[fuseops.RootInodeID] = newInode(rootAttrs, fuseops.RootInodeID)

	// Set up invariant checking.
	fs.mu = syncutil.NewInvariantMutex(fs.checkInvariants)

	return fs
}

func NewFServer(fs *MemFS) fuse.Server {
	if fs == nil {
		return nil
	}
	return fuseutil.NewFileSystemServer(fs)
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func (fs *MemFS) Lock() {
	fs.mu.Lock()
}

func (fs *MemFS) Unlock() {
	fs.mu.Unlock()
}

func (fs *MemFS) checkInvariants() {
	// Check reserved inodes.
	for i := 0; i < fuseops.RootInodeID; i++ {
		if fs.inodes[i] != nil {
			panic(fmt.Sprintf("Non-nil RNode for ID: %v", i))
		}
	}

	// Check the root RNode.
	if !fs.inodes[fuseops.RootInodeID].isDir() {
		panic("Expected root to be a directory.")
	}

	// Build our own list of free IDs.
	freeIDsEncountered := make(map[fuseops.InodeID]struct{})
	for i := fuseops.RootInodeID + 1; i < len(fs.inodes); i++ {
		RNode := fs.inodes[i]
		if RNode == nil {
			freeIDsEncountered[fuseops.InodeID(i)] = struct{}{}
			continue
		}
	}

	// Check fs.freeInodes.
	if len(fs.freeInodes) != len(freeIDsEncountered) {
		panic(
			fmt.Sprintf(
				"Length mismatch: %v vs. %v",
				len(fs.freeInodes),
				len(freeIDsEncountered)))
	}

	for _, id := range fs.freeInodes {
		if _, ok := freeIDsEncountered[id]; !ok {
			panic(fmt.Sprintf("Unexected free RNode ID: %v", id))
		}
	}

	// INVARIANT: For each RNode in, in.CheckInvariants() does not panic.
	for _, in := range fs.inodes {
		in.CheckInvariants()
	}
}

// Find the given RNode. Panic if it doesn't exist.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *MemFS) GetInodeOrDie(id fuseops.InodeID) *RNode {
	// TODO: lock remote RNode
	RNode := fs.inodes[id]
	if RNode == nil {
		panic(fmt.Sprintf("Unknown RNode: %v", id))
	}

	return RNode
}

// Find the given RNode. Return nil if it doesn't exist.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *MemFS) GetInode(id fuseops.InodeID) *RNode {
	// TODO: lock remote RNode
	RNode := fs.inodes[id]
	if RNode == nil {
		fmt.Printf("Unknown RNode: %v", id)
		return nil
	}

	return RNode
}

// Allocate a new RNode, assigning it an ID that is not in use.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *MemFS) allocateInode(
	attrs fuseops.InodeAttributes) (id fuseops.InodeID, RNode *RNode) {
	// Create the RNode.

	// Re-use a free ID if possible. Otherwise mint a new one.
	numFree := len(fs.freeInodes)
	if numFree != 0 {
		id = fs.freeInodes[numFree-1]
		RNode = newInode(attrs, id)
		fs.freeInodes = fs.freeInodes[:numFree-1]
		fs.inodes[id] = RNode
	} else {
		id = fuseops.InodeID(len(fs.inodes))
		RNode = newInode(attrs, id)
		fs.inodes = append(fs.inodes, RNode)
	}

	return id, RNode
}

// LOCKS_REQUIRED(fs.mu)
func (fs *MemFS) deallocateInode(id fuseops.InodeID) {
	fs.freeInodes = append(fs.freeInodes, id)
	fs.inodes[id] = nil
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (fs *MemFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	return nil
}

func (fs *MemFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent directory.
	RNode := fs.GetInodeOrDie(op.Parent)

	// Does the directory have an entry with the given name?
	childID, _, ok := RNode.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := fs.GetInodeOrDie(childID)

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fs *MemFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the RNode.
	RNode := fs.GetInodeOrDie(op.Inode)

	// Fill in the response.
	op.Attributes = RNode.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return nil
}

func (fs *MemFS) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var err error
	if op.Size != nil && op.Handle == nil && *op.Size != 0 {
		// require that truncate to non-zero has to be ftruncate()
		// but allow open(O_TRUNC)
		err = syscall.EBADF
	}

	// Grab the RNode.
	RNode := fs.GetInodeOrDie(op.Inode)

	// Handle the request.
	RNode.SetAttributes(op.Size, op.Mode, op.Mtime)

	// Fill in the response.
	op.Attributes = RNode.Attrs()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return err
}

func (fs *MemFS) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.GetInodeOrDie(op.Parent)

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
		Uid:   fs.uid,
		Gid:   fs.gid,
	}

	// Allocate a child.
	childID, child := fs.allocateInode(childAttrs)

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

func (fs *MemFS) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var err error
	op.Entry, err = fs.DoCreateFile(op.Parent, op.Name, op.Mode)
	return err
}

// LOCKS_REQUIRED(fs.mu)
func (fs *MemFS) DoCreateFile(
	parentID fuseops.InodeID,
	name string,
	mode os.FileMode) (fuseops.ChildInodeEntry, error) {
	// Grab the parent, which we will update shortly.
	parent := fs.GetInodeOrDie(parentID)

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
		Uid:    fs.uid,
		Gid:    fs.gid,
	}

	// Allocate a child.
	childID, child := fs.allocateInode(childAttrs)

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

func (fs *MemFS) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var err error
	op.Entry, err = fs.DoCreateFile(op.Parent, op.Name, op.Mode)
	return err
}

func (fs *MemFS) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.GetInodeOrDie(op.Parent)

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
		Uid:    fs.uid,
		Gid:    fs.gid,
	}

	// Allocate a child.
	childID, child := fs.allocateInode(childAttrs)

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

func (fs *MemFS) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.GetInodeOrDie(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		return fuse.EEXIST
	}

	// Get the target RNode to be linked
	target := fs.GetInodeOrDie(op.Target)

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

func (fs *MemFS) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ask the old parent for the child's RNode ID and type.
	oldParent := fs.GetInodeOrDie(op.OldParent)
	childID, childType, ok := oldParent.LookUpChild(op.OldName)

	if !ok {
		return fuse.ENOENT
	}

	// If the new name exists already in the new parent, make sure it's not a
	// non-empty directory, then delete it.
	newParent := fs.GetInodeOrDie(op.NewParent)
	existingID, _, ok := newParent.LookUpChild(op.NewName)
	if ok {
		existing := fs.GetInodeOrDie(existingID)

		var buf [4096]byte
		if existing.isDir() && existing.ReadDir(buf[:], 0) > 0 {
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

func (fs *MemFS) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.GetInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := fs.GetInodeOrDie(childID)

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

func (fs *MemFS) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.GetInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := fs.GetInodeOrDie(childID)

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	attrs := child.Attrs()
	attrs.Nlink--
	child.SetAttrs(attrs)

	return nil
}

func (fs *MemFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	RNode := fs.GetInodeOrDie(op.Inode)

	if !RNode.isDir() {
		panic("Found non-dir.")
	}

	return nil
}

func (fs *MemFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the directory.
	RNode := fs.GetInodeOrDie(op.Inode)

	// Serve the request.
	op.BytesRead = RNode.ReadDir(op.Dst, int(op.Offset))

	return nil
}

func (fs *MemFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// RNode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	RNode := fs.GetInodeOrDie(op.Inode)

	if !RNode.isFile() {
		panic("Found non-file.")
	}

	return nil
}

func (fs *MemFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the RNode in question.
	RNode := fs.GetInodeOrDie(op.Inode)

	// Serve the request.
	var err error
	op.BytesRead, err = RNode.ReadAt(op.Dst, op.Offset)

	// Don't return EOF errors; we just indicate EOF to fuse using a short read.
	if err == io.EOF {
		return nil
	}

	return err
}

func (fs *MemFS) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the RNode in question.
	RNode := fs.GetInodeOrDie(op.Inode)

	// Serve the request.
	_, err := RNode.WriteAt(op.Data, op.Offset)

	return err
}

func (fs *MemFS) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the RNode in question.
	RNode := fs.GetInodeOrDie(op.Inode)

	// Serve the request.
	op.Target = RNode.Target()

	return nil
}

func (fs *MemFS) GetXattr(ctx context.Context,
	op *fuseops.GetXattrOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	RNode := fs.GetInodeOrDie(op.Inode)
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

	return nil
}

func (fs *MemFS) ListXattr(ctx context.Context,
	op *fuseops.ListXattrOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	RNode := fs.GetInodeOrDie(op.Inode)

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

	return nil
}

func (fs *MemFS) RemoveXattr(ctx context.Context,
	op *fuseops.RemoveXattrOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	RNode := fs.GetInodeOrDie(op.Inode)

	if _, ok := RNode.Xattrs()[op.Name]; ok {
		xattrs := RNode.Xattrs()
		delete(xattrs, op.Name)
		RNode.SetXattrs(xattrs)
	} else {
		return fuse.ENOATTR
	}
	return nil
}

func (fs *MemFS) SetXattr(ctx context.Context,
	op *fuseops.SetXattrOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	RNode := fs.GetInodeOrDie(op.Inode)

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
	return nil
}

func (fs *MemFS) Fallocate(ctx context.Context,
	op *fuseops.FallocateOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	RNode := fs.GetInodeOrDie(op.Inode)
	RNode.Fallocate(op.Mode, op.Length, op.Length)
	return nil
}
