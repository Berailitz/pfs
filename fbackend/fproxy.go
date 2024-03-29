package fbackend

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/Berailitz/pfs/logger"

	"bazil.org/fuse"
	"github.com/Berailitz/pfs/idallocator"

	"github.com/Berailitz/pfs/rnode"

	pb "github.com/Berailitz/pfs/remotetree"

	"github.com/Berailitz/pfs/utility"
)

const (
	initialHandle = 1
)

const (
	firstLogID = 1
)

type remoteHandle struct {
	handle uint64
	addr   string
}

type FProxy struct {
	fb        *FBackEnd
	ma        *RManager
	pool      *GCliPool
	localAddr string

	allcator        *idallocator.IDAllocator
	remoteHandleMap utility.IterableMap // [uint64]remoteHandle

	requestIDAllocator *idallocator.IDAllocator
}

type FPErr struct {
	msg string
}

var _ = (error)((*FPErr)(nil))

func NewFProxy(
	ctx context.Context,
	uid uint32,
	gid uint32,
	localAddr string,
	ma *RManager) *FProxy {
	allcator := idallocator.NewIDAllocator(initialHandle)
	fb := NewFBackEnd(uid, gid, allcator, ma)
	if fb == nil {
		logger.P(ctx, "new fp nil fb error: uid=%v, gid=%v, localAddr=%v",
			uid, gid, localAddr)
	}

	fp := &FProxy{
		fb:                 fb,
		ma:                 ma,
		pool:               NewGCliPool(localAddr, ma),
		allcator:           allcator,
		localAddr:          localAddr,
		requestIDAllocator: idallocator.NewIDAllocator(firstLogID),
	}
	fb.SetFP(ctx, fp)
	return fp
}

func (fp *FProxy) Measure(ctx context.Context, addr string) (tof int64, err error) {
	departure := time.Now().UnixNano()
	offset, err := fp.Ping(ctx, addr, true, true)
	arrival := time.Now().UnixNano()
	return arrival - departure + offset, err
}

func (fp *FProxy) Ping(ctx context.Context, addr string, disableCache bool, disableRoute bool) (offset int64, err error) {
	if addr == fp.localAddr {
		return 0, nil
	}

	if !disableCache {
		if tof, ok := fp.ma.LogicTof(addr); ok {
			return tof, nil
		}
	}

	var gcli pb.RemoteTreeClient
	if disableRoute {
		gcli, err = fp.pool.LoadWithoutRoute(ctx, addr)
	} else {
		gcli, err = fp.pool.Load(ctx, addr)
	}
	if err != nil {
		return 0, err
	}

	reply, err := gcli.Ping(ctx, &pb.PingRequest{
		Addr:      addr,
		Departure: time.Now().UnixNano(),
		Src:       fp.localAddr,
	})
	if err != nil {
		logger.E(ctx, "fp ping error", "addr", addr, "err", err)
		return 0, err
	}
	return reply.Offset, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) Vote(ctx context.Context, addr string, vote *Vote) (masterAddr string, err error) {
	if addr == fp.localAddr {
		return fp.ma.AcceptVote(ctx, addr, vote)
	}

	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return "", err
	}

	reply, err := gcli.Vote(ctx, &pb.VoteRequest{
		Addr:       addr,
		Voter:      vote.Voter,
		ElectionID: vote.ElectionID,
		ProposalID: vote.ProposalID,
		Nominee:    vote.Nominee,
	})
	if err == nil {
		err = rnode.FromPbErr(reply.Err)
	}
	if err != nil {
		logger.E(ctx, "fp vote error", "addr", addr, "err", err)
		return "", err
	}
	return reply.MasterAddr, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) MakeRegular(ctx context.Context, addr string, nodeID uint64) (err error) {
	if addr == fp.localAddr {
		return fp.fb.MakeRegular(ctx, nodeID)
	}

	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	perr, err := gcli.MakeRegular(ctx, &pb.UInt64IDAddr{
		Addr: addr,
		Id:   nodeID,
	})
	if err != nil {
		logger.E(ctx, "fp make regular error", "addr", addr, "err", err)
		return err
	}
	return rnode.FromPbErr(perr)
}

func (fp *FProxy) Gossip(ctx context.Context, addr string) (_ map[string]int64, err error) {
	if addr == fp.localAddr {
		return fp.ma.AnswerGossip(ctx, addr)
	}

	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return nil, err
	}

	reply, err := gcli.Gossip(ctx, &pb.GossipRequest{
		Addr: addr,
	})
	if err != nil {
		logger.E(ctx, "fp gossip error", "addr", addr, "err", err)
		return nil, err
	}
	return reply.TofMap, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) CopyManager(ctx context.Context) (*pb.Manager, error) {
	addr := fp.ma.MasterAddr()
	if addr == fp.localAddr {
		return fp.ma.CopyManager(ctx)
	}

	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return nil, err
	}

	reply, err := gcli.CopyManager(ctx, &pb.EmptyMsg{})
	if err != nil {
		logger.E(ctx, "fp copy manager error", "addr", addr, "err", err)
		return nil, err
	}
	return reply, nil
}

func (fp *FProxy) GetOwnerMap(ctx context.Context) (map[uint64]string, error) {
	addr := fp.ma.MasterAddr()
	if addr == fp.localAddr {
		return fp.ma.CopyOwnerMap(ctx), nil
	}

	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return nil, err
	}

	reply, err := gcli.GetOwnerMap(ctx, &pb.EmptyMsg{})
	if err != nil {
		logger.E(ctx, "fp get owner map error", "addr", addr, "err", err)
		return nil, err
	}
	return reply.Map, nil
}

func (fp *FProxy) LoadRemoteNode(ctx context.Context, id uint64, isRead bool) (*rnode.RNode, error) {
	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return nil, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return nil, err
	}

	reply, err := gcli.FetchNode(ctx, &pb.NodeIsReadRequest{
		Id:     id,
		IsRead: isRead,
	})
	if err == nil {
		err = rnode.FromPbErr(reply.Err)
	}
	if err != nil {
		logger.E(ctx, "rpc load inode error", "id", id, "err", err)
		return nil, err
	}
	node := rnode.FromPbNode(ctx, reply.Node)
	node.SetRemoteLockID(reply.LockID)
	logger.I(ctx, "rpc load inode success", "id", id, "lockID", node.RemoteLockID())
	return node, nil
}

func (fp *FProxy) LoadNode(ctx context.Context, id uint64, isRead bool) (node *rnode.RNode, lockID int64, err error) {
	if isRead {
		return fp.fb.LoadNodeForRead(ctx, id)
	} else {
		return fp.fb.LoadNodeForWrite(ctx, id)
	}
}

func (fp *FProxy) RUnlockNode(ctx context.Context, nodeID uint64, lockID int64) error {
	if node, err := fp.fb.LoadLocalNode(ctx, nodeID); err == nil {
		return fp.fb.RUnlockNode(ctx, node, lockID)
	}

	addr, qaErr := fp.QueryOwner(ctx, nodeID)
	if qaErr != nil {
		return qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	perr, err := gcli.RUnlockNode(ctx, &pb.RUnlockNodeRequest{
		NodeID: nodeID,
		LockID: lockID,
	})
	if err != nil {
		logger.E(ctx, "rpc runlock node error", "nodeID", nodeID, "err", err)
		return err
	}
	return rnode.FromPbErr(perr)
}

func (fp *FProxy) UnlockNode(ctx context.Context, node *rnode.RNode, lockID int64) error {
	id := node.ID()
	if localNode, err := fp.fb.LoadLocalNode(ctx, id); err != nil {
		if err := fp.fb.UpdateNode(ctx, node); err != nil {
			logger.E(ctx, "unlock node update node error", "id", id, "err", err)
			return err
		}
		return fp.fb.UnlockNode(ctx, localNode, lockID)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	perr, err := gcli.UnlockNode(ctx, &pb.UnlockNodeRequest{
		Node:   rnode.ToPbNode(node),
		LockID: lockID,
	})
	if err != nil {
		logger.E(ctx, "rpc unlock node error", "id", id, "err", err)
		return err
	}
	return rnode.FromPbErr(perr)
}

func (fp *FProxy) LookUpInode(
	ctx context.Context,
	parentID uint64,
	name string) (uint64, fuse.Attr, error) {
	logger.I(ctx, "fp look up inode", "parentID", parentID, "name", name)
	if fp.IsLocalNode(ctx, parentID) {
		logger.I(ctx, "fp local look up inode", "parent", parentID, "name", name)
		return fp.fb.LookUpInode(ctx, parentID, name)
	}

	addr, qaErr := fp.QueryOwner(ctx, parentID)
	if qaErr != nil {
		return 0, fuse.Attr{}, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return 0, fuse.Attr{}, err
	}

	reply, err := gcli.LookUpInode(ctx, &pb.LookUpInodeRequest{
		ParentID: parentID,
		Name:     name,
	})
	if err != nil {
		logger.E(ctx, "rpc look up inode error", "parent", parentID, "name", name, "err", err)
		return 0, fuse.Attr{}, err
	}
	logger.I(ctx, "rpc look up inode success", "parentID", parentID, "name", name)
	return reply.Id, rnode.FromPbAttr(*reply.Attr), rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) GetInodeAttributes(
	ctx context.Context,
	id uint64) (fuse.Attr, error) {
	logger.I(ctx, "fp get inode attr", "id", id)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.GetInodeAttributes(ctx, id)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return fuse.Attr{}, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return fuse.Attr{}, err
	}

	reply, err := gcli.GetInodeAttributes(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		logger.E(ctx, "rpc get inode attr error", "id", id, "err", err)
		return fuse.Attr{}, err
	}
	return rnode.FromPbAttr(*reply.Attr), rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) SetInodeAttributes(
	ctx context.Context,
	id uint64,
	param SetInodeAttributesParam) (fuse.Attr, error) {
	logger.I(ctx, "fp set inode attr", "id", id, "param", param)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.SetInodeAttributes(ctx, id, param)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return fuse.Attr{}, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return fuse.Attr{}, err
	}

	reply, err := gcli.SetInodeAttributes(ctx, &pb.SetInodeAttributesRequest{
		Id:       id,
		HasSize:  param.HasSize,
		Size:     param.Size,
		HasMode:  param.HasMode,
		Mode:     uint32(param.Mode),
		HasMtime: param.HasMtime,
		Mtime:    param.Mtime.Unix(),
	})
	if err != nil {
		logger.E(ctx, "rpc set inode attr error", "id", id, "err", err)
		return fuse.Attr{}, err
	}
	return rnode.FromPbAttr(*reply.Attr), rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) MkDir(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (uint64, error) {
	return fp.fb.MkDir(ctx, parentID, name, mode)
}

// LOCKS_REQUIRED(fp.mu)
func (fp *FProxy) CreateNode(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (uint64, error) {
	return fp.fb.CreateNode(ctx, parentID, name, mode)
}

// LOCKS_REQUIRED(fp.mu)
func (fp *FProxy) CreateFile(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode,
	flags uint32) (uint64, uint64, error) {
	return fp.fb.CreateFile(ctx, parentID, name, mode, flags)
}

func (fp *FProxy) AttachChild(
	ctx context.Context,
	parentID uint64,
	childID uint64,
	name string,
	dt fuse.DirentType,
	doOpen bool) (hid uint64, err error) {
	if fp.IsLocalNode(ctx, parentID) {
		return fp.fb.AttachChild(ctx, parentID, childID, name, dt, doOpen)
	}

	addr, qaErr := fp.QueryOwner(ctx, parentID)
	if qaErr != nil {
		return 0, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return 0, err
	}

	reply, err := gcli.AttachChild(ctx, &pb.AttachChildRequest{
		ParentID: parentID,
		ChildID:  childID,
		Name:     name,
		Dt:       uint32(dt),
		DoOpen:   doOpen,
	})
	if err != nil {
		logger.E(ctx, "rpc attach child error",
			"parentID", parentID, "cid", childID, "name", name, "dt", dt, "err", err)
		return 0, err
	}
	return reply.Num, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) CreateSymlink(
	ctx context.Context,
	parentID uint64,
	name string,
	target string) (uint64, error) {
	return fp.fb.CreateSymlink(ctx, parentID, name, target)
}

func (fp *FProxy) CreateLink(
	ctx context.Context,
	parentID uint64,
	name string,
	targetID uint64) (uint64, error) {
	logger.I(ctx, "fp create link", "parent",
		parentID, "name", name, "targetID", targetID)
	if fp.IsLocalNode(ctx, parentID) {
		return fp.fb.CreateLink(ctx, parentID, name, targetID)
	}

	// TODO: Parent owner start and acquire child
	addr, qaErr := fp.QueryOwner(ctx, parentID)
	if qaErr != nil {
		return 0, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return 0, err
	}

	reply, err := gcli.CreateLink(ctx, &pb.CreateLinkRequest{
		Id:       parentID,
		Name:     name,
		TargetID: targetID,
	})
	if err != nil {
		logger.E(ctx, "rpc create link error: ", "parentID",
			parentID, "name", name, "targetID", targetID, "err", err)
		return 0, err
	}
	return reply.Num, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) Rename(
	ctx context.Context,
	oldParent uint64,
	oldName string,
	newParent uint64,
	newName string) (err error) {
	logger.I(ctx, "fp rename",
		"oldParent", oldParent, "oldName", oldName, "newParent", newParent, "newName", newName)
	if fp.IsLocalNode(ctx, newParent) && fp.IsLocalNode(ctx, oldParent) &&
		fp.isChildLocal(ctx, oldParent, oldName) {
		return fp.fb.Rename(ctx, oldParent, oldName, newParent, newName)
	}

	// TODO: NewParent owner start and acquire child, OldParent owner rm OldChild, rm node, NewParent add child
	addr, qaErr := fp.QueryOwner(ctx, newParent)
	if qaErr != nil {
		return qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	perr, err := gcli.Rename(ctx, &pb.RenameRequest{
		OldParent: oldParent,
		OldName:   oldName,
		NewParent: newParent,
		NewName:   newName,
	})
	if err != nil {
		logger.E(ctx, "rpc fp rename",
			"oldParent", oldParent, "oldName", oldName, "newParent", newParent, "newName", newName, "err", err)
		return err
	}
	return rnode.FromPbErr(perr)
}

func (fp *FProxy) DetachChild(
	ctx context.Context,
	parent uint64,
	name string) (err error) {
	logger.I(ctx, "fp DetachChild", "parent", parent, "name", name)
	if fp.IsLocalNode(ctx, parent) {
		return fp.fb.DetachChild(ctx, parent, name)
	}

	addr, qaErr := fp.QueryOwner(ctx, parent)
	if qaErr != nil {
		return qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	perr, err := gcli.DetachChild(ctx, &pb.UnlinkRequest{
		Parent: parent,
		Name:   name,
	})
	if err != nil {
		logger.E(ctx, "rpc fp DetachChild error", "parent", parent, "name", name, "err", err)
		return err
	}
	return rnode.FromPbErr(perr)
}

func (fp *FProxy) Unlink(
	ctx context.Context,
	parent uint64,
	name string) (err error) {
	logger.I(ctx, "fp unlink", "parent", parent, "name", name)
	childID, _, err := fp.LookUpInode(ctx, parent, name)
	if err != nil {
		logger.E(ctx, "rpc fp unlink no child error", "parent", parent, "name", name, "err", err)
		err = syscall.ENOENT
		return err
	}

	if fp.IsLocalNode(ctx, childID) {
		return fp.fb.Unlink(ctx, parent, name, childID)
	}

	addr, qaErr := fp.QueryOwner(ctx, childID)
	if qaErr != nil {
		return qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	perr, err := gcli.Unlink(ctx, &pb.UnlinkRequest{
		Parent: parent,
		Name:   name,
	})
	if err != nil {
		logger.E(ctx, "rpc fp unlink no child error", "parent", parent, "name", name, "err", err)
		return err
	}
	return rnode.FromPbErr(perr)
}

func (fp *FProxy) Open(
	ctx context.Context,
	id uint64,
	flags uint32) (handle uint64, err error) {
	logger.I(ctx, "fp opendir", "id", id, "flags", flags)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.Open(ctx, id, flags)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return 0, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return 0, err
	}

	reply, err := gcli.Open(ctx, &pb.OpenXRequest{
		Id:    id,
		Flags: flags,
	})
	if err != nil {
		logger.E(ctx, "rpc opendir error", "id", id, "flags", flags, "err", err)
		return 0, err
	}

	err = rnode.FromPbErr(reply.Err)
	remoteHandle := reply.Num
	if err != nil || remoteHandle <= 0 {
		logger.E(ctx, "rpc opendir error", "id", id, "flags", flags, "err", err)
	}
	localHandle := fp.allcator.Allocate()
	fp.StoreRemoteHandle(ctx, localHandle, remoteHandle, addr)
	return localHandle, nil
}

func (fp *FProxy) ReadDirAll(
	ctx context.Context,
	id uint64) ([]fuse.Dirent, error) {
	logger.I(ctx, "fp readdir", "id", id)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ReadDir(ctx, id)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return nil, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return nil, err
	}

	reply, err := gcli.ReadDir(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		logger.E(ctx, "rpc opendir error",
			"id", id, "err", err)
		return nil, err
	}
	return rnode.FromPbDirents(reply.Dirents), rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) ReleaseHandle(
	ctx context.Context,
	h uint64) error {
	logger.I(ctx, "release handle", "hid", h)
	if rh := fp.LoadRemoteHandle(ctx, h); rh != nil {
		logger.I(ctx, "rpc release remote handle", "hid", h)
		addr := rh.addr
		gcli, err := fp.pool.Load(ctx, addr)
		if err != nil {
			return err
		}

		perr, err := gcli.ReleaseHandle(ctx, &pb.UInt64ID{
			Id: rh.handle,
		})
		if err != nil {
			logger.E(ctx, "rpc release remote handle error",
				"localHID", h, "remoeHID", rh.handle, "err", err)
			return err
		}
		if err := rnode.FromPbErr(perr); err != nil {
			logger.E(ctx, "rpc release remote handle remote error",
				"localHID", h, "remoeHID", rh.handle, "err", err)
			return err
		}
		if err := fp.ReleaseRemoteHandle(ctx, h); err != nil {
			logger.E(ctx, "rpc release remote handle release map error",
				"localHID", h, "remoeHID", rh.handle, "err", err)
			return err
		}
		logger.I(ctx, "rpc release remote handle success",
			"localHID", h, "remoeHID", rh.handle)
		return nil
	}

	logger.I(ctx, "release remote handle success",
		"localHID", h)
	return fp.fb.ReleaseHandle(ctx, h)
}

func (fp *FProxy) ReadFile(
	ctx context.Context,
	id uint64,
	length uint64,
	offset uint64) (bytesRead uint64, buf []byte, err error) {
	logger.I(ctx, "fp readfile", "id", id, "len", length, "off", offset)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ReadFile(ctx, id, length, offset)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return 0, nil, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return 0, nil, err
	}

	reply, err := gcli.ReadFile(ctx, &pb.ReadXRequest{
		Id:     id,
		Length: length,
		Offset: offset,
	})
	if err != nil {
		logger.E(ctx, "fp readfile error", "id", id, "len", length, "off", offset, "err", err)
		return 0, nil, err
	}
	return reply.BytesRead, reply.Buf, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) WriteFile(
	ctx context.Context,
	id uint64,
	offset uint64,
	data []byte) (uint64, error) {
	logger.I(ctx, "write file", "id", id, "off", offset, "data", data)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.WriteFile(ctx, id, offset, data)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return 0, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return 0, err
	}

	reply, err := gcli.WriteFile(ctx, &pb.WriteXRequest{
		Id:     id,
		Offset: offset,
		Data:   data,
	})
	if err != nil {
		logger.E(ctx, "rpc write file", "id", id, "off", offset, "data", data, "err", err)
		return 0, err
	}
	return reply.Num, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) ReadSymlink(
	ctx context.Context,
	id uint64) (target string, err error) {
	logger.I(ctx, "fp read symlink", "id", id)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ReadSymlink(ctx, id)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return "", qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return "", err
	}

	reply, err := gcli.ReadSymlink(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		logger.E(ctx, "fp read symlink", "id", id, "err", err)
		return "", err
	}
	return "", rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) GetXattr(ctx context.Context,
	id uint64,
	name string,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	logger.I(ctx, "fp get xattr",
		"id", id, "name", name, "len", length)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.GetXattr(ctx, id, name, length)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return 0, nil, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return 0, nil, err
	}

	reply, err := gcli.GetXattr(ctx, &pb.GetXattrRequest{
		Id:     id,
		Length: length,
		Name:   name,
	})
	if err != nil {
		logger.E(ctx, "rpc fp get xattr error",
			"id", id, "name", name, "len", length, "err", err)
		return 0, nil, err
	}
	return 0, nil, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) ListXattr(ctx context.Context,
	id uint64,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	logger.I(ctx, "fp list xattr",
		"id", id, "len", length)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ListXattr(ctx, id, length)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return 0, nil, qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return 0, nil, err
	}

	reply, err := gcli.ListXattr(ctx, &pb.ListXattrRequest{
		Id:     id,
		Length: length,
	})
	if err != nil {
		logger.E(ctx, "rpc fp list xattr error",
			"id", id, "len", length, "err", err)
		return 0, nil, err
	}
	return 0, nil, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) RemoveXattr(ctx context.Context, id uint64, name string) error {
	logger.I(ctx, "fp rm xattr", "id", id, "name", name)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.RemoveXattr(ctx, id, name)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	reply, err := gcli.RemoveXattr(ctx, &pb.RemoveXattrRequest{
		Id:   id,
		Name: name,
	})
	if err != nil {
		logger.E(ctx, "rpc fp rm xattr error", "id", id, "name", name, "err", err)
		return err
	}
	return rnode.FromPbErr(reply)
}

func (fp *FProxy) SetXattr(ctx context.Context, id uint64, name string, flags uint32, value []byte) error {
	logger.I(ctx, "fp set xattr", "id", id, "name", name, "flag", flags, "value", value)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.SetXattr(ctx, id, name, flags, value)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	reply, err := gcli.SetXattr(ctx, &pb.SetXattrRequest{
		Id:    id,
		Name:  name,
		Value: value,
		Flag:  flags,
	})
	if err != nil {
		logger.E(ctx, "fp set xattr error", "id", id, "name", name, "flag", flags, "value", value, "err", err)
		return err
	}
	return rnode.FromPbErr(reply)
}

func (fp *FProxy) Fallocate(ctx context.Context,
	id uint64,
	mode uint32,
	length uint64) error {
	logger.I(ctx, "fp fallocate", "id", id, "mode", mode, "len", length)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.Fallocate(ctx, id, mode, length)
	}

	addr, qaErr := fp.QueryOwner(ctx, id)
	if qaErr != nil {
		return qaErr
	}
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	reply, err := gcli.Fallocate(ctx, &pb.FallocateRequest{
		Id:     id,
		Mode:   mode,
		Length: length,
	})
	if err != nil {
		logger.E(ctx, "fp fallocate error", "id", id, "mode", mode, "len", length, "err", err)
		return err
	}
	return rnode.FromPbErr(reply)
}

func (fp *FProxy) IsLocalNode(ctx context.Context, id uint64) bool {
	return fp.fb.IsLocal(ctx, id)
}

func (fp *FProxy) ReleaseRemoteHandle(ctx context.Context, remoteHandleID uint64) error {
	if rh := fp.LoadRemoteHandle(ctx, remoteHandleID); rh != nil {
		fp.remoteHandleMap.Delete(remoteHandleID)
		return nil
	}
	return &FPErr{fmt.Sprintf("release remote handle no handle err: remoteHandleID=%v", remoteHandleID)}
}

func (fp *FProxy) StoreRemoteHandle(ctx context.Context, localHandleID uint64, remoteHandleID uint64, addr string) {
	fp.remoteHandleMap.Store(localHandleID, remoteHandle{
		handle: remoteHandleID,
		addr:   addr,
	})
}

func (fp *FProxy) LoadRemoteHandle(ctx context.Context, id uint64) *remoteHandle {
	if out, ok := fp.remoteHandleMap.Load(id); ok {
		if rh, ok := out.(remoteHandle); ok {
			return &rh
		}
	}
	return nil
}

func (fp *FProxy) isChildLocal(ctx context.Context, parentId uint64, name string) bool {
	_, _, err := fp.fb.LookUpInode(ctx, parentId, name)
	return err == nil
}

func (fp *FProxy) QueryOwner(ctx context.Context, nodeID uint64) (string, error) {
	if fp.localAddr == fp.ma.MasterAddr() {
		return fp.ma.QueryOwner(ctx, nodeID)
	}

	logger.I(ctx, "query owner", "nodeId", nodeID)
	gcli, err := fp.pool.Load(ctx, fp.ma.MasterAddr())
	if err != nil {
		return "", err
	}

	reply, err := gcli.QueryOwner(ctx, &pb.UInt64ID{
		Id: nodeID,
	})
	if err == nil {
		err = rnode.FromPbErr(reply.Err)
	}
	if err != nil {
		logger.E(ctx, "query owner error", "nodeID", nodeID, "err", err)
		return "", err
	}
	logger.I(ctx, "query owner success", "nodeID", nodeID, "addr", reply.Addr)
	return reply.Addr, nil
}

func (fp *FProxy) Allocate(ctx context.Context, ownerID uint64) (uint64, error) {
	if fp.localAddr == fp.ma.MasterAddr() {
		return fp.ma.Allocate(ctx, ownerID)
	}

	logger.I(ctx, "allocate node")
	gcli, err := fp.pool.Load(ctx, fp.ma.MasterAddr())
	if err != nil {
		return 0, err
	}

	reply, err := gcli.Allocate(ctx, &pb.OwnerId{
		Id: ownerID,
	})
	if err == nil {
		err = rnode.FromPbErr(reply.Err)
	}
	if err != nil {
		logger.E(ctx, "allocate error", "ownerID", ownerID, "err", err)
		return 0, err
	}
	logger.I(ctx, "allocate node success", "nodeID", reply.Id)
	return reply.Id, err
}

func (fp *FProxy) Deallocate(ctx context.Context, nodeID uint64) error {
	if fp.localAddr == fp.ma.MasterAddr() {
		return fp.ma.Deallocate(ctx, nodeID)
	}

	logger.I(ctx, "deallocate", "nodeID", nodeID)
	gcli, err := fp.pool.Load(ctx, fp.ma.MasterAddr())
	if err != nil {
		return err
	}

	perr, err := gcli.Deallocate(ctx, &pb.UInt64ID{
		Id: nodeID,
	})
	if err == nil {
		err = rnode.FromPbErr(perr)
	}
	if err != nil {
		logger.E(ctx, "deallocate error", "nodeID", nodeID, "err", err)
		return err
	}
	logger.I(ctx, "deallocate success", "nodeID", nodeID)
	return nil
}

func (fp *FProxy) RegisterOwner(ctx context.Context, addr string) (uint64, error) {
	if fp.localAddr == fp.ma.MasterAddr() {
		return fp.ma.RegisterOwner(ctx, addr)
	}

	logger.I(ctx, "register owner", "addr", addr)
	gcli, err := fp.pool.Load(ctx, fp.ma.MasterAddr())
	if err != nil {
		return 0, err
	}

	out, err := gcli.RegisterOwner(ctx, &pb.Addr{
		Addr: addr,
	})
	if err == nil {
		err = rnode.FromPbErr(out.Err)
	}
	if err != nil {
		logger.E(ctx, "register owner error", "adrr", addr, "err", err)
		return 0, err
	}
	logger.I(ctx, "register owner success", "addr", addr)
	return out.Id, nil
}

func (fp *FProxy) RemoveOwner(ctx context.Context, ownerID uint64) error {
	if fp.localAddr == fp.ma.MasterAddr() {
		return fp.ma.RemoveOwner(ctx, ownerID)
	}

	logger.I(ctx, "remove owner", "ownerID", ownerID)
	gcli, err := fp.pool.Load(ctx, fp.ma.MasterAddr())
	if err != nil {
		return err
	}

	perr, err := gcli.RemoveOwner(ctx, &pb.OwnerId{
		Id: ownerID,
	})
	if err == nil {
		err = rnode.FromPbErr(perr)
	}
	if err != nil {
		logger.E(ctx, "remove owner error", "ownerID", ownerID, "err", err)
		return err
	}
	logger.I(ctx, "remove owner success", "ownerID", ownerID)
	return nil
}

func (fp *FProxy) AllocateRoot(ctx context.Context, ownerID uint64) error {
	if fp.localAddr == fp.ma.MasterAddr() {
		return fp.ma.AllocateRoot(ctx, ownerID)
	}

	gcli, err := fp.pool.Load(ctx, fp.ma.MasterAddr())
	if err != nil {
		return err
	}

	logger.I(ctx, "allocate root", "ownerID", ownerID)
	perr, err := gcli.AllocateRoot(ctx, &pb.OwnerId{
		Id: ownerID,
	})
	if err != nil {
		logger.E(ctx, "allocate root error", "ownerID", ownerID, "err", err)
		return err
	}
	logger.I(ctx, "allocate root finished", "ownerID", ownerID, "perr", perr)
	return rnode.FromPbErr(perr)
}

func (fp *FProxy) AnswerProposal(ctx context.Context, addr string, proposal *Proposal) (state int64, err error) {
	if addr == fp.localAddr {
		return fp.ma.AnswerProposal(ctx, addr, proposal)
	}
	return fp.SendProposal(ctx, addr, proposal)
}

func (fp *FProxy) SendProposal(ctx context.Context, addr string, proposal *Proposal) (state int64, err error) {
	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return 0, err
	}

	reply, err := gcli.Propose(ctx, &pb.ProposeRequest{
		Addr:        addr,
		ProposeID:   proposal.ID,
		ProposeType: proposal.Typ,
		OwnerID:     proposal.OwnerID,
		NodeID:      proposal.NodeID,
		Strs:        proposal.Strs,
		Value:       proposal.Value,
	})
	if err != nil {
		logger.E(ctx, "rpc proposal error",
			"addr", addr, "proposal", proposal, "err", err)
		return 0, err
	}
	return reply.State, rnode.FromPbErr(reply.Err)
}

func (fp *FProxy) PushNode(ctx context.Context, addr string, node *rnode.RNode) error {
	if addr == fp.localAddr {
		return fp.fb.SaveRedundantNode(ctx, node)
	}

	gcli, err := fp.pool.Load(ctx, addr)
	if err != nil {
		return err
	}

	reply, err := gcli.PushNode(ctx, &pb.PushNodeRequest{
		Addr: addr,
		Node: rnode.ToPbNode(node),
	})
	if err != nil {
		logger.E(ctx, "rpc push node error",
			"addr", addr, "node", node, "err", err)
		return err
	}
	return rnode.FromPbErr(reply)
}

func (fp *FProxy) MakeRequestCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, logger.ContextRequestIDKey, strconv.FormatUint(fp.requestIDAllocator.Allocate(), 10))
}

func (e *FPErr) Error() string {
	return fmt.Sprintf("fproxy error: %v", e.msg)
}
