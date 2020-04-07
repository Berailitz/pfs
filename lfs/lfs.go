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
	"fmt"
	"log"

	"github.com/Berailitz/pfs/manager"

	"bazil.org/fuse"

	"bazil.org/fuse/fs"

	"github.com/Berailitz/pfs/fproxy"
)

type LFS struct {
	root *LNode
	c    *fuse.Conn
	fp   *fproxy.FProxy
	svr  *fs.Server
}

var _ = (fs.FS)((*LFS)(nil))

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
	fp *fproxy.FProxy) *LFS {
	if fp == nil {
		log.Fatalf("nil fbackend error")
	}
	return &LFS{
		fp: fp,
	}
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (lfs *LFS) Root() (fs.Node, error) {
	return lfs.root, nil
}

// Serve blocks until umount or error
func (lfs *LFS) Mount(dir, fsName, fsType, volumeName string) (err error) {
	lfs.c, err = fuse.Mount(dir, fuse.FSName(fsName), fuse.Subtype(fsType), fuse.VolumeName(volumeName))
	lfs.svr = fs.New(lfs.c, &fs.Config{
		Debug: fuseDebug,
	})
	lfs.root = NewLNode(manager.RootNodeID, lfs.fp, lfs.svr)
	return err
}

// Serve blocks until umount or error
func (lfs *LFS) Serve() error {
	if lfs.svr == nil || lfs.root == nil {
		return &LFSErr{"lfs serve nil svr or root error"}
	}
	return lfs.svr.Serve(lfs)
}

func (lfs *LFS) Umount() error {
	return lfs.c.Close()
}

func (e *LFSErr) Error() string {
	return fmt.Sprintf("lfs error: %v", e.msg)
}

func fuseDebug(msg interface{}) {
	log.Printf("DEBUG: msg=%s\n", msg)
}
