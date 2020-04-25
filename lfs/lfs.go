package lfs

import (
	"fmt"
	"log"

	"github.com/Berailitz/pfs/fbackend"
	"github.com/Berailitz/pfs/manager"

	"bazil.org/fuse"

	"bazil.org/fuse/fs"
)

type LFS struct {
	root *LNode
	c    *fuse.Conn
	fp   *fbackend.FProxy
	svr  *fs.Server
}

var _ = (fs.FS)((*LFS)(nil))

type LFSErr struct {
	msg string
}

var _ = (error)((*LFSErr)(nil))

func NewLFS(
	fp *fbackend.FProxy) *LFS {
	if fp == nil {
		log.Fatalf("nil fbackend error")
	}
	return &LFS{
		fp: fp,
	}
}

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
