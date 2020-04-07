package lfs

import (
	"context"
	"log"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/Berailitz/pfs/fproxy"
)

const (
	readAllLength = 65535
)

type LHandle struct {
	id   uint64
	node uint64
	fp   *fproxy.FProxy
}

var _ = (fs.HandleFlusher)((*LHandle)(nil))
var _ = (fs.HandleReadAller)((*LHandle)(nil))

var _ = (fs.HandleReadDirAller)((*LHandle)(nil)) // TODO: handle ReadDirAll
var _ = (fs.HandleReader)((*LHandle)(nil))
var _ = (fs.HandleWriter)((*LHandle)(nil))
var _ = (fs.HandleReleaser)((*LHandle)(nil))

func NewLHandle(id uint64, node uint64, fp *fproxy.FProxy) *LHandle {
	return &LHandle{
		id:   id,
		node: node,
		fp:   fp,
	}
}

func (lh *LHandle) CheckRequest(ctx context.Context, r fuse.Request, hid uint64) {
	if uint64(r.Hdr().Node) != lh.node || hid != lh.id {
		log.Printf("mismatch request: requestNode=%v, realNode=%v, requestHandle=%v, readHandle=%v",
			r.Hdr().Node, lh.node, hid, lh.id)
	}
}

func (lh *LHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	lh.CheckRequest(ctx, req, uint64(req.Handle))
	// TODO: imply flush
	return nil
}

func (lh *LHandle) ReadAll(ctx context.Context) ([]byte, error) {
	_, buf, err := lh.fp.ReadFile(ctx, lh.node, readAllLength, 0)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (lh *LHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return lh.fp.ReadDirAll(ctx, lh.node)
}

func (lh *LHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	lh.CheckRequest(ctx, req, uint64(req.Handle))
	_, resp.Data, err = lh.fp.ReadFile(ctx, lh.node, uint64(req.Size), uint64(req.Offset))
	return
}

func (lh *LHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	lh.CheckRequest(ctx, req, uint64(req.Handle))
	byteswrite, err := lh.fp.WriteFile(ctx, lh.node, uint64(req.Offset), req.Data)
	resp.Size = int(byteswrite)
	return err
}

func (lh *LHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	lh.CheckRequest(ctx, req, uint64(req.Handle))
	return lh.fp.ReleaseHandle(ctx, lh.id)
}
