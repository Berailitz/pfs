package lfs

import (
	"context"
	"fmt"

	"github.com/Berailitz/pfs/logger"

	"github.com/Berailitz/pfs/fbackend"

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
	ctx context.Context,
	fp *fbackend.FProxy) *LFS {
	if fp == nil {
		logger.Pf(ctx, "nil fbackend error")
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
		Debug:       fuseDebug,
		WithContext: lfs.buildRequestCtx,
	})
	lfs.root = NewLNode(fbackend.RootNodeID, lfs.fp, lfs.svr)
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

func (lfs *LFS) buildRequestCtx(ctx context.Context, req fuse.Request) context.Context {
	ctx = lfs.fp.MakeRequestCtx(ctx)
	logger.I(ctx, "lfs start", "request", req)
	return ctx
}

func (e *LFSErr) Error() string {
	return fmt.Sprintf("lfs error: %v", e.msg)
}

func fuseDebug(msg interface{}) {
	//logger.If(ctx, "DEBUG: msg=%s\n", msg)
}
