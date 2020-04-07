package lfs

import (
	"context"
	"fmt"
	"log"
	"syscall"

	"github.com/Berailitz/pfs/fbackend"

	"github.com/Berailitz/pfs/fproxy"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type LNode struct {
	id uint64
	fp *fproxy.FProxy
}

var _ = (fs.Node)((*LNode)(nil))
var _ = (fs.NodeGetattrer)((*LNode)(nil))
var _ = (fs.NodeSetattrer)((*LNode)(nil))
var _ = (fs.NodeSymlinker)((*LNode)(nil))
var _ = (fs.NodeReadlinker)((*LNode)(nil))
var _ = (fs.NodeLinker)((*LNode)(nil))
var _ = (fs.NodeRemover)((*LNode)(nil))
var _ = (fs.NodeAccesser)((*LNode)(nil))

//var _ = (fs.NodeStringLookuper)((*LNode)(nil))
var _ = (fs.NodeRequestLookuper)((*LNode)(nil))
var _ = (fs.NodeMkdirer)((*LNode)(nil))
var _ = (fs.NodeOpener)((*LNode)(nil))
var _ = (fs.NodeCreater)((*LNode)(nil))
var _ = (fs.NodeForgetter)((*LNode)(nil))
var _ = (fs.NodeRenamer)((*LNode)(nil))
var _ = (fs.NodeMknoder)((*LNode)(nil))
var _ = (fs.NodeGetxattrer)((*LNode)(nil))
var _ = (fs.NodeListxattrer)((*LNode)(nil))
var _ = (fs.NodeSetxattrer)((*LNode)(nil))
var _ = (fs.NodeRemovexattrer)((*LNode)(nil))

func NewLNode(id uint64, fp *fproxy.FProxy) *LNode {
	return &LNode{
		id: id,
		fp: fp,
	}
}

func (ln *LNode) New(id uint64) *LNode {
	return NewLNode(id, ln.fp)
}

func (ln *LNode) NewOtherHandle(otherNode, hid uint64) *LHandle {
	return NewLHandle(hid, otherNode, ln.fp)
}

func (ln *LNode) NewHandle(hid uint64) *LHandle {
	return NewLHandle(hid, ln.id, ln.fp)
}

func (ln *LNode) CheckRequest(ctx context.Context, r fuse.Request) {
	if uint64(r.Hdr().Node) != ln.id {
		log.Printf("mismatch request: h.Node=%v, ln.id=%v", r.Hdr().Node, ln.id)
	}
}

func (ln *LNode) Attr(ctx context.Context, attr *fuse.Attr) (err error) {
	*attr, err = ln.fp.GetInodeAttributes(ctx, ln.id)
	return
}

func (ln *LNode) Getattr(ctx context.Context, req *fuse.GetattrRequest, resp *fuse.GetattrResponse) (err error) {
	ln.CheckRequest(ctx, req)
	return ln.Attr(ctx, &resp.Attr)
}

func (ln *LNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) (err error) {
	ln.CheckRequest(ctx, req)
	attr, err := ln.fp.SetInodeAttributes(ctx, ln.id, fbackend.SetInodeAttributesParam{
		Size:     req.Size,
		Mode:     req.Mode,
		Mtime:    req.Mtime,
		HasSize:  req.Valid.Size(),
		HasMode:  req.Valid.Mode(),
		HasMtime: req.Valid.Mtime(),
	})
	resp.Attr = attr
	return
}

func (ln *LNode) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	ln.CheckRequest(ctx, req)
	id, err := ln.fp.CreateSymlink(ctx, ln.id, req.NewName, req.Target)
	if err != nil {
		return nil, err
	}
	return ln.New(id), nil
}

func (ln *LNode) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	ln.CheckRequest(ctx, req)
	return ln.fp.ReadSymlink(ctx, ln.id)
}

func (ln *LNode) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	ln.CheckRequest(ctx, req)
	id, err := ln.fp.CreateLink(ctx, ln.id, req.NewName, uint64(req.OldNode))
	if err != nil {
		return nil, err
	}
	return ln.New(id), nil
}

func (ln *LNode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	ln.CheckRequest(ctx, req)
	return ln.fp.Unlink(ctx, ln.id, req.Name)
}

func (ln *LNode) Access(ctx context.Context, req *fuse.AccessRequest) error {
	ln.CheckRequest(ctx, req)
	return nil
}

//func (ln *LNode) Lookup(ctx context.Context, name string) (Node, error) {
//	nodeI, _, err := ln.fp.LookUpInode(ctx, ln.id, name)
//	return ln.New(nodeI), err
//}

func (ln *LNode) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	ln.CheckRequest(ctx, req)
	id, attr, err := ln.fp.LookUpInode(ctx, ln.id, req.Name)
	resp.Node = fuse.NodeID(id)
	resp.Attr = attr
	if err != nil {
		return nil, err
	}
	return ln.New(id), nil
}

func (ln *LNode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	ln.CheckRequest(ctx, req)
	id, err := ln.fp.MkDir(ctx, ln.id, req.Name, req.Mode)
	if err != nil {
		return nil, err
	}
	return ln.New(id), nil
}

func (ln *LNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	ln.CheckRequest(ctx, req)
	hid, err := ln.fp.Open(ctx, ln.id, uint32(req.Flags))
	if err != nil {
		return nil, err
	}
	return ln.NewHandle(hid), nil
}

func (ln *LNode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	ln.CheckRequest(ctx, req)
	nid, hid, err := ln.fp.CreateFile(ctx, ln.id, req.Name, req.Mode, uint32(req.Flags))
	if err != nil {
		return nil, nil, err
	}
	return ln.New(nid), ln.NewOtherHandle(nid, hid), err
}

func (ln *LNode) Forget() {
	//TODO: imply forget
}

func (ln *LNode) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	ln.CheckRequest(ctx, req)
	if newDirLNode, ok := newDir.(*LNode); ok {
		return ln.fp.Rename(ctx, ln.id, req.OldName, newDirLNode.id, req.NewName)
	}
	return syscall.ENOENT
}

func (ln *LNode) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {
	ln.CheckRequest(ctx, req)
	id, err := ln.fp.CreateNode(ctx, ln.id, req.Name, req.Mode)
	return ln.New(id), err
}

func (ln *LNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	ln.CheckRequest(ctx, req)
	// TODO: handle offset
	_, dst, err := ln.fp.GetXattr(ctx, ln.id, req.Name, uint64(req.Size))
	resp.Xattr = dst
	return err
}

func (ln *LNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	ln.CheckRequest(ctx, req)
	// TODO: handle position
	_, dst, err := ln.fp.ListXattr(ctx, ln.id, uint64(req.Size))
	resp.Xattr = dst
	return err
}

func (ln *LNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	ln.CheckRequest(ctx, req)
	return ln.fp.SetXattr(ctx, ln.id, req.Name, req.Flags, req.Xattr)
}

func (ln *LNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	ln.CheckRequest(ctx, req)
	return ln.fp.RemoveXattr(ctx, ln.id, req.Name)
}

func (ln *LNode) String() string {
	return fmt.Sprintf("LN(0x%x)", ln.id)
}
