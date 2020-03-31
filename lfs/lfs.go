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
	"log"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Berailitz/pfs/fproxy"

	"github.com/Berailitz/pfs/fbackend"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

const initialHandle = 1

type LFS struct {
	fuseutil.NotImplementedFileSystem

	// The UID and GID that every RNode receives.
	uid uint32
	gid uint32

	fp *fproxy.FProxy

	nextHandle uint64
	handleMap  sync.Map // map[fuseops.HandleID]uint64
}

type LFSErr struct {
	msg string
}

var _ = (error)((*LFSErr)(nil))

// Create a file system that stores data and metadata in memory.
//
// The supplied UID/GID pair will own the root RNode. This file system does no
// permissions checking, and should therefore be mounted with the
// default_permissions option.
func NewLFS(
	uid uint32,
	gid uint32,
	fp *fproxy.FProxy) *LFS {
	if fp == nil {
		log.Fatalf("nil fbackend error")
	}
	return &LFS{
		uid:        uid,
		gid:        gid,
		fp:         fp,
		nextHandle: initialHandle,
	}
}

func NewLFSServer(lfs *LFS) fuse.Server {
	if lfs == nil {
		return nil
	}
	return fuseutil.NewFileSystemServer(lfs)
}

func (lfs *LFS) AllocateHandle(
	ctx context.Context,
	node uint64) (uint64, error) {
	if !lfs.fp.IsLocalNode(ctx, node) {
		return 0, &LFSErr{fmt.Sprintf("allocate handle node not local error: node=%v", node)}
	}
	handle := atomic.AddUint64(&lfs.nextHandle, 1) - 1
	lfs.handleMap.Store(handle, node)
	return handle, nil
}

func (lfs *LFS) LoadHandle(
	ctx context.Context,
	h fuseops.HandleID) (uint64, error) {
	if out, ok := lfs.handleMap.Load(h); ok {
		if node, ok := out.(uint64); ok {
			return node, nil
		}
		return 0, &LFSErr{fmt.Sprintf("load handle value not node id error: handle=%v", h)}
	}
	return 0, &LFSErr{fmt.Sprintf("load handle not found error: handle=%v", h)}
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
	childID, attrs, err := lfs.fp.LookUpInode(ctx, uint64(op.Parent), op.Name)
	if err != nil {
		log.Printf("look up inode error: parent=%v, name=%v, err=%+v", op.Parent, op.Name, err)
		return err
	}

	// Fill in the response.
	op.Entry.Child = fuseops.InodeID(childID)
	op.Entry.Attributes = attrs

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
	// Grab the RNode.
	attr, err := lfs.fp.GetInodeAttributes(ctx, uint64(op.Inode))
	if err != nil {
		log.Printf("get inode attr error: id=%v, err=%+v", op.Inode, err)
		return err
	}

	// Fill in the response.
	op.Attributes = attr

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
	if op.Size != nil && op.Handle == nil && *op.Size != 0 {
		// require that truncate to non-zero has to be ftruncate()
		// but allow open(O_TRUNC)
		return syscall.EBADF
	}

	// Grab the RNode.
	param := fbackend.SetInodeAttributesParam{}
	if op.Size != nil {
		param.HasSize = true
		param.Size = *op.Size
	}
	if op.Mtime != nil {
		param.HasMtime = true
		param.Mtime = *op.Mtime
	}
	if op.Mode != nil {
		param.HasMode = true
		param.Mode = *op.Mode
	}
	attr, err := lfs.fp.SetInodeAttributes(ctx, uint64(op.Inode), param)
	if err != nil {
		log.Printf("set inode attr error: id=%v, size=%v, mode=%v, mtime=%v, err=%+v",
			op.Inode, op.Size, op.Mode, op.Mtime, err)
		return err
	}

	// Fill in the response.
	op.Attributes = attr

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

	// Grab the parent, which we will update shortly.
	childID, attr, err := lfs.fp.MkDir(ctx, uint64(op.Parent), op.Name, op.Mode)
	if err != nil {
		log.Printf("mkdir error: parent=%v, name=%v, mode=%v, err=%+v",
			op.Parent, op.Name, op.Mode, err)
		return err
	}

	// Fill in the response.
	op.Entry.Child = fuseops.InodeID(childID)
	op.Entry.Attributes = attr

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
	entry, err := lfs.fp.CreateNode(ctx, uint64(op.Parent), op.Name, op.Mode)
	if err != nil {
		log.Printf("mknode error: parent=%v, name=%v, mode=%v, err=%+v",
			op.Parent, op.Name, op.Mode, err)
		return err
	}

	op.Entry = entry
	log.Printf("mknode success: parent=%v, name=%v, mode=%v",
		op.Parent, op.Name, op.Mode)
	return err
}

func (lfs *LFS) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) error {
	entry, err := lfs.fp.CreateNode(ctx, uint64(op.Parent), op.Name, op.Mode)
	log.Printf("create file: parent=%v, name=%v, mode=%v",
		op.Parent, op.Name, op.Mode)
	if err != nil {
		log.Printf("create file error: parent=%v, name=%v, mode=%v, err=%+v",
			op.Parent, op.Name, op.Mode, err)
		return err
	}

	op.Entry = entry
	log.Printf("create file success: op=%#v", op)
	return err
}

func (lfs *LFS) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) error {
	log.Printf("create symlink: parent=%v, name=%v, target=%v",
		op.Parent, op.Name, op.Target)
	// Grab the parent, which we will update shortly.
	childID, attr, err := lfs.fp.CreateSymlink(ctx, uint64(op.Parent), op.Name, op.Target)
	if err != nil {
		log.Printf("create symlink error: parent=%v, name=%v, target=%v, err=%+v",
			op.Parent, op.Name, op.Target, err)
		return err
	}

	// Fill in the response entry.
	op.Entry.Child = fuseops.InodeID(childID)
	op.Entry.Attributes = attr

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
	// Grab the parent, which we will update shortly.
	attr, err := lfs.fp.CreateLink(ctx, uint64(op.Parent), op.Name, uint64(op.Target))
	if err != nil {
		log.Printf("create link error: parent=%v, name=%v, target=%v, err=%+v",
			op.Parent, op.Name, op.Target, err)
		return err
	}

	// Return the response.
	op.Entry.Child = op.Target
	op.Entry.Attributes = attr

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
	err := lfs.fp.Rename(ctx, *op)
	if err != nil {
		log.Printf("rename error: op=%#v, err=%+v", op, err)
		return err
	}

	log.Printf("rename success: op=%#v", op)
	return nil
}

func (lfs *LFS) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) error {
	log.Printf("rmdir: op=%#v", op)

	err := lfs.fp.RmDir(ctx, *op)
	if err != nil {
		log.Printf("rmdir error: op=%#v, err=%+v", op, err)
		return err
	}
	log.Printf("rmdir success: op=%#v", op)
	return nil
}

func (lfs *LFS) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	log.Printf("unlink: op=%#v", op)
	err := lfs.fp.Unlink(ctx, *op)
	if err != nil {
		log.Printf("unlink error: op=%#v, err=%+v", op, err)
		return err
	}
	log.Printf("unlink success: op=%#v", op)
	return nil
}

func (lfs *LFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	log.Printf("opendir: id=%v", op.Inode)
	node := uint64(op.Inode)
	if err := lfs.fp.OpenDir(ctx, node); err != nil {
		log.Printf("opendir error: id=%v, err=%+v", op.Inode, err)
		return err
	}

	handle, err := lfs.AllocateHandle(ctx, node)
	if handle == 0 {
		log.Printf("opendir error: node=%v, err=%+v", node, err)
		return err
	}
	op.Handle = fuseops.HandleID(handle)

	log.Printf("opendir success: op=%+v", op)
	return nil
}

func (lfs *LFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	log.Printf("readdir: id=%v, len=%v, offset=%v",
		op.Inode, len(op.Dst), op.Offset)
	// Grab the directory.
	bytesRead, dst, err := lfs.fp.ReadDir(ctx, uint64(op.Inode), uint64(len(op.Dst)), uint64(op.Offset))
	if err != nil {
		log.Printf("readdir error: id=%v, len=%v, offset=%v, err=%+v",
			op.Inode, len(op.Dst), op.Offset, err)
		return err
	}

	// Serve the request.
	op.BytesRead = int(bytesRead)
	copy(op.Dst, dst)
	log.Printf("readdir success: id=%v, len=%v, offset=%v, bytesRead=%v, dst=%X",
		op.Inode, len(op.Dst), op.Offset, op.BytesRead, op.Dst[:op.BytesRead])
	return nil
}

func (lfs *LFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	log.Printf("openfile: id=%v", op.Inode)
	node := uint64(op.Inode)
	if err := lfs.fp.OpenFile(ctx, node); err != nil {
		log.Printf("openfile error: op=%#v, err=%+v", op, err)
		return err
	}

	handle, err := lfs.AllocateHandle(ctx, node)
	if handle == 0 {
		log.Printf("openfile error: node=%v, err=%+v", node, err)
		return err
	}
	op.Handle = fuseops.HandleID(handle)

	log.Printf("openfile success: op=%+v", op)
	return nil
}

func (lfs *LFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	log.Printf("readfile success: id=%v, offset=%v", op.Inode, op.Offset)
	bytesRead, dst, err := lfs.fp.ReadFile(ctx, uint64(op.Inode), uint64(len(op.Dst)), uint64(op.Offset))

	if err != nil {
		log.Printf("readfile error: op=%#v, bytesRead=%v, err=%+v", op, bytesRead, err)
		return err
	}

	// Serve the request.
	op.BytesRead = int(bytesRead)
	copy(op.Dst, dst)

	log.Printf("readfile success: id=%v, offset=%v, bytesread=%v, dst=%X",
		op.Inode, op.Offset, op.BytesRead, op.Dst[:op.BytesRead])
	return nil
}

func (lfs *LFS) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	log.Printf("write file: id=%v, offset=%v", op.Inode, op.Offset)
	bytesWrite, err := lfs.fp.WriteFile(ctx, uint64(op.Inode), uint64(op.Offset), op.Data)

	if err != nil {
		log.Printf("writefile error: id=%v, offset=%v, bytesWrite=%v, data=%v, err=%+v",
			op.Inode, op.Offset, bytesWrite, op.Data, err)
		return err
	}

	log.Printf("write file success: id=%v, offset=%v, bytesWrite=%v", op.Inode, op.Offset, bytesWrite)
	return err
}

func (lfs *LFS) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) error {
	log.Printf("read symlink: id=%v", op.Inode)
	target, err := lfs.fp.ReadSymlink(ctx, uint64(op.Inode))

	if err != nil {
		log.Printf("read symlink error: id=%v, err=%+v", op.Inode, err)
		return err
	}

	// Serve the request.
	op.Target = target

	log.Printf("read symlink success: id=%v, target=%v", op.Inode, target)
	return nil
}

func (lfs *LFS) GetXattr(ctx context.Context,
	op *fuseops.GetXattrOp) error {
	log.Printf("get xattr: id=%v, name=%v, length=%v",
		op.Inode, op.Name, len(op.Dst))
	bytesRead, dst, err := lfs.fp.GetXattr(ctx, uint64(op.Inode), op.Name, uint64(len(op.Dst)))

	if err != nil {
		log.Printf("get xattr error: id=%v, name=%v, length=%v, bytesRead=%v, err=%+v",
			op.Inode, op.Name, len(op.Dst), bytesRead, err)
		return err
	}

	op.BytesRead = int(bytesRead)
	copy(op.Dst, dst)

	log.Printf("get xattr success: id=%v, name=%v, length=%v, bytesRead=%v",
		op.Inode, op.Name, len(op.Dst), bytesRead)
	return nil
}

func (lfs *LFS) ListXattr(ctx context.Context,
	op *fuseops.ListXattrOp) error {
	log.Printf("list xattr: id=%v, length=%v",
		op.Inode, len(op.Dst))
	bytesRead, dst, err := lfs.fp.ListXattr(ctx, uint64(op.Inode), uint64(len(op.Dst)))

	if err != nil {
		log.Printf("list xattr error: id=%v, length=%v, bytesRead=%v, err=%+v",
			op.Inode, len(op.Dst), bytesRead, err)
		return err
	}

	copy(op.Dst, dst)
	op.BytesRead = int(bytesRead)
	log.Printf("list xattr success: id=%v, length=%v, bytesRead=%v",
		op.Inode, len(op.Dst), bytesRead)
	return nil
}

func (lfs *LFS) RemoveXattr(ctx context.Context,
	op *fuseops.RemoveXattrOp) error {
	log.Printf("rm xattr: id=%v, name=%v", op.Inode, op.Name)
	err := lfs.fp.RemoveXattr(ctx, uint64(op.Inode), op.Name)

	if err != nil {
		log.Printf("rm xattr error: id=%v, name=%v, err=%+v",
			op.Inode, op.Name, err)
		return err
	}

	log.Printf("rm xattr success: id=%v, name=%v", op.Inode, op.Name)
	return nil
}

func (lfs *LFS) SetXattr(ctx context.Context,
	op *fuseops.SetXattrOp) error {
	log.Printf("set xattr: op=%#v", op)
	err := lfs.fp.SetXattr(ctx, *op)

	if err != nil {
		log.Printf("set xattr error: op=%#v, err=%+v",
			op, err)
		return err
	}

	log.Printf("set xattr success: op=%#v", op)
	return nil
}

func (lfs *LFS) Fallocate(ctx context.Context,
	op *fuseops.FallocateOp) error {
	log.Printf("fallocate: op=%#v", op)
	err := lfs.fp.Fallocate(ctx, uint64(op.Inode), op.Mode, op.Length)
	if err != nil {
		log.Printf("fallocate error: id=%v, mode=%v, length=%v, err=%+v",
			op.Inode, op.Mode, op.Length, err)
		return err
	}

	log.Printf("fallocate success: op=%#v", op)
	return nil
}

func (e *LFSErr) Error() string {
	return fmt.Sprintf("lfs error: %v", e.msg)
}
