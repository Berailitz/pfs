package fbackend

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/Berailitz/pfs/manager"

	"bazil.org/fuse"
	"github.com/Berailitz/pfs/idallocator"

	"github.com/Berailitz/pfs/rnode"

	"github.com/Berailitz/pfs/gclipool"

	pb "github.com/Berailitz/pfs/remotetree"

	"github.com/Berailitz/pfs/utility"
	"google.golang.org/grpc"

	"github.com/Berailitz/pfs/rclient"
)

const initialHandle = 1

type remoteHandle struct {
	handle uint64
	addr   string
}

type FProxy struct {
	fb        *FBackEnd
	ma        *manager.RManager
	wd        *WatchDog
	pcli      *rclient.RClient
	pool      *gclipool.GCliPool
	localAddr string

	allcator        *idallocator.IDAllocator
	remoteHandleMap sync.Map // [uint64]remoteHandle
}

type FPErr struct {
	msg string
}

var _ = (error)((*FPErr)(nil))

func NewFProxy(
	uid uint32,
	gid uint32,
	masterAddr string,
	localAddr string,
	gopts []grpc.DialOption,
	ma *manager.RManager) *FProxy {
	gcli, err := utility.BuildGCli(masterAddr, gopts)
	if err != nil {
		log.Fatalf("new gcli fial error: master=%v, opts=%#v, err=%+v",
			masterAddr, gopts, err)
		return nil
	}

	pcli := rclient.NewRClient(gcli)
	if pcli == nil {
		log.Fatalf("nil pcli error")
	}
	localID := pcli.RegisterSelf(localAddr)

	allcator := idallocator.NewIDAllocator(initialHandle)
	fb := NewFBackEnd(uid, gid, masterAddr, localAddr, gopts, allcator, localID)
	if fb == nil {
		log.Fatalf("new fp nil fb error: uid=%v, gid=%v, masterAddr=%v, localAddr=%v, gopts=%+v",
			uid, gid, masterAddr, localAddr, gopts)
	}
	// Set up the root rnode.RNode.

	wd := NewWatchDog()

	fp := &FProxy{
		fb:        fb,
		ma:        ma,
		wd:        wd,
		pcli:      pcli,
		pool:      gclipool.NewGCliPool(gopts, localAddr),
		allcator:  allcator,
		localAddr: localAddr,
	}
	fb.SetFP(fp)
	wd.SetFP(fp)
	return fp
}

func (fp *FProxy) Run(ctx context.Context) error {
	return fp.wd.Run(ctx)
}

func (fp *FProxy) Stop(ctx context.Context) {
	fp.wd.Stop(ctx)
}

func (fp *FProxy) ProxyMeasure(ctx context.Context, addr string, disableCache bool) (tof int64, err error) {
	departure := time.Now().UnixNano()
	offset, err := fp.ProxyPing(ctx, addr, disableCache)
	arrival := time.Now().UnixNano()
	return arrival - departure + offset, err
}

func (fp *FProxy) ProxyPing(ctx context.Context, addr string, disableCache bool) (offset int64, err error) {
	if addr == fp.localAddr {
		return 0, nil
	}

	if !disableCache {
		if tof, ok := fp.wd.Tof(addr); ok {
			return tof, nil
		}
	}

	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return 0, err
	}

	reply, err := gcli.Ping(ctx, &pb.PingRequest{
		Addr:      addr,
		Departure: time.Now().UnixNano(),
	})
	if err != nil {
		log.Printf("fp ping error: addr=%v, err=%+v", addr, err)
		return 0, err
	}
	return reply.Offset, utility.FromPbErr(reply.Err)
}

func (fp *FProxy) GetOwnerMap(ctx context.Context) (map[uint64]string, error) {
	mid, addr := fp.ma.QueryMaster()
	if mid == fp.pcli.ID() {
		return fp.ma.CopyOwnerMap(), nil
	}

	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return nil, err
	}

	reply, err := gcli.GetOwnerMap(ctx, &pb.EmptyMsg{})
	if err != nil {
		log.Printf("fp get owner map error: addr=%v, err=%+v", addr, err)
		return nil, err
	}
	return reply.Map, nil
}

func (fp *FProxy) LoadNode(ctx context.Context, id uint64, isRead bool) (*rnode.RNode, error) {
	if isRead {
		return fp.fb.LoadNodeForRead(ctx, id)
	} else {
		return fp.fb.LoadNodeForWrite(ctx, id)
	}
}

func (fp *FProxy) RUnlockNode(ctx context.Context, id uint64) error {
	if node, err := fp.fb.LoadLocalNode(ctx, id); err != nil {
		return fp.fb.RUnlockNode(ctx, node)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return err
	}

	perr, err := gcli.RUnlockNode(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		log.Printf("rpc runlock node error: id=%v, err=%+V", id, err)
		return err
	}
	return utility.FromPbErr(perr)
}

func (fp *FProxy) UnlockNode(ctx context.Context, node *rnode.RNode) error {
	id := node.ID()
	if localNode, err := fp.fb.LoadLocalNode(ctx, id); err != nil {
		if err := fp.fb.UpdateNode(ctx, node); err != nil {
			log.Printf("unlock node update node error: id=%v, err=%+V", id, err)
			return err
		}
		return fp.fb.UnlockNode(ctx, localNode)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return err
	}

	perr, err := gcli.UnlockNode(ctx, utility.ToPbNode(node))
	if err != nil {
		log.Printf("rpc unlock node error: id=%v, err=%+V", id, err)
		return err
	}
	return utility.FromPbErr(perr)
}

func (fp *FProxy) LookUpInode(
	ctx context.Context,
	parentID uint64,
	name string) (uint64, fuse.Attr, error) {
	log.Printf("fp look up inode: parent=%v, name=%v", parentID, name)
	if fp.IsLocalNode(ctx, parentID) {
		log.Printf("fp local look up inode: parent=%v, name=%v", parentID, name)
		return fp.fb.LookUpInode(ctx, parentID, name)
	}

	addr := fp.pcli.QueryOwner(parentID)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return 0, fuse.Attr{}, err
	}

	reply, err := gcli.LookUpInode(ctx, &pb.LookUpInodeRequest{
		ParentID: parentID,
		Name:     name,
	})
	if err != nil {
		log.Printf("rpc look up inode error: parentID=%v, name=%v, err=%+v", parentID, name, err)
		return 0, fuse.Attr{}, err
	}
	log.Printf("rpc look up inode success: parentID=%v, name=%v", parentID, name)
	return reply.Id, utility.FromPbAttr(*reply.Attr), utility.FromPbErr(reply.Err)
}

func (fp *FProxy) GetInodeAttributes(
	ctx context.Context,
	id uint64) (fuse.Attr, error) {
	log.Printf("fp get inode attr: id=%v", id)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.GetInodeAttributes(ctx, id)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return fuse.Attr{}, err
	}

	reply, err := gcli.GetInodeAttributes(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		log.Printf("rpc get inode attr error: id=%v, err=%+v", id, err)
		return fuse.Attr{}, err
	}
	return utility.FromPbAttr(*reply.Attr), utility.FromPbErr(reply.Err)
}

func (fp *FProxy) SetInodeAttributes(
	ctx context.Context,
	id uint64,
	param SetInodeAttributesParam) (fuse.Attr, error) {
	log.Printf("fp set inode attr: id=%v, param=%+v",
		id, param)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.SetInodeAttributes(ctx, id, param)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
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
		log.Printf("rpc set inode attr error: id=%v, err=%+v", id, err)
		return fuse.Attr{}, err
	}
	return utility.FromPbAttr(*reply.Attr), utility.FromPbErr(reply.Err)
}

func (fp *FProxy) MkDir(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (uint64, error) {
	return fp.fb.MkDir(ctx, parentID, name, mode)
}

//func (fp *FProxy) MkDir(
//	ctx context.Context,
//	parentID uint64,
//	name string,
//	mode os.FileMode) (uint64, error) {
//	log.Printf("fp mkdir: parent=%v, name=%v, mode=%v",
//		parentID, name, mode)
//	if fp.IsLocalNode(ctx, parentID) {
//		return fp.fb.MkDir(ctx, parentID, name, mode)
//	}
//
//	addr := fp.pcli.QueryOwner(parentID)
//	gcli, err := fp.pool.Load(addr)
//	if err != nil {
//		return 0, err
//	}
//
//	reply, err := gcli.MkDir(ctx, &pb.MkDirRequest{
//		Id:   parentID,
//		Name: name,
//		Mode: uint32(mode),
//	})
//	if err != nil {
//		log.Printf("rpc mkdir error: parentID=%v, name=%v, err=%+v", parentID, name, err)
//		return 0, err
//	}
//	return reply.Id, utility.FromPbErr(reply.Err)
//}

// LOCKS_REQUIRED(fp.mu)
func (fp *FProxy) CreateNode(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (uint64, error) {
	return fp.fb.CreateNode(ctx, parentID, name, mode)
}

//// LOCKS_REQUIRED(fp.mu)
//func (fp *FProxy) CreateNode(
//	ctx context.Context,
//	parentID uint64,
//	name string,
//	mode os.FileMode) (uint64, error) {
//	log.Printf("fp create node: parent=%v, name=%v, mode=%v",
//		parentID, name, mode)
//	if fp.IsLocalNode(ctx, parentID) {
//		return fp.fb.CreateNode(ctx, parentID, name, mode)
//	}
//
//	addr := fp.pcli.QueryOwner(parentID)
//	gcli, err := fp.pool.Load(addr)
//	if err != nil {
//		return 0, err
//	}
//
//	reply, err := gcli.CreateNode(ctx, &pb.CreateNodeRequest{
//		Id:   parentID,
//		Name: name,
//		Mode: uint32(mode),
//	})
//	if err != nil {
//		log.Printf("rpc look up inode error: parentID=%v, name=%v, err=%+v", parentID, name, err)
//		return 0, err
//	}
//	return reply.Num, utility.FromPbErr(reply.Err)
//}

// LOCKS_REQUIRED(fp.mu)
func (fp *FProxy) CreateFile(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode,
	flags uint32) (uint64, uint64, error) {
	return fp.fb.CreateFile(ctx, parentID, name, mode, flags)
}

//// LOCKS_REQUIRED(fp.mu)
//func (fp *FProxy) CreateFile(
//	ctx context.Context,
//	parentID uint64,
//	name string,
//	mode os.FileMode,
//	flags uint32) (uint64, uint64, error) {
//	log.Printf("fp create file: parent=%v, name=%v, mode=%v, flags=%v",
//		parentID, name, mode, flags)
//	if fp.IsLocalNode(ctx, parentID) {
//		return fp.fb.CreateFile(ctx, parentID, name, mode, flags)
//	}
//
//	addr := fp.pcli.QueryOwner(parentID)
//	gcli, err := fp.pool.Load(addr)
//	if err != nil {
//		return 0, 0, err
//	}
//
//	reply, err := gcli.CreateFile(ctx, &pb.CreateFileRequest{
//		Id:    parentID,
//		Name:  name,
//		Mode:  uint32(mode),
//		Flags: flags,
//	})
//	if err != nil {
//		log.Printf("rpc look up inode error: parentID=%v, name=%v, err=%+v", parentID, name, err)
//		return 0, 0, err
//	}
//
//	err = utility.FromPbErr(reply.Err)
//	remoteHandle := reply.Handle
//	if err != nil || remoteHandle <= 0 {
//		log.Printf("rpc create file error: id=%v, remoteHandle=%v, perr=%+v",
//			remoteHandle, remoteHandle, err)
//	}
//	localHandle := fp.allcator.Allocate()
//	fp.StoreRemoteHandle(ctx, localHandle, remoteHandle, addr)
//	log.Printf("fp create remote file success: parent=%v, name=%v, mode=%v, flags=%v, remoteHandle=%v, localHandle=%v",
//		parentID, name, mode, flags, remoteHandle, localHandle)
//	return reply.Id, localHandle, nil
//}

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

	addr := fp.pcli.QueryOwner(parentID)
	gcli, err := fp.pool.Load(addr)
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
		log.Printf("rpc attach child error: parentID=%v, childID=%v name=%v, dt=%v, err=%+v",
			parentID, childID, name, dt, err)
		return 0, err
	}
	return reply.Num, utility.FromPbErr(reply.Err)
}

func (fp *FProxy) CreateSymlink(
	ctx context.Context,
	parentID uint64,
	name string,
	target string) (uint64, error) {
	return fp.fb.CreateSymlink(ctx, parentID, name, target)
}

//func (fp *FProxy) CreateSymlink(
//	ctx context.Context,
//	parentID uint64,
//	name string,
//	target string) (uint64, error) {
//	log.Printf("fp create symlink: parent=%v, name=%v, target=%v",
//		parentID, name, target)
//	if fp.IsLocalNode(ctx, parentID) {
//		return fp.fb.CreateSymlink(ctx, parentID, name, target)
//	}
//
//	addr := fp.pcli.QueryOwner(parentID)
//	gcli, err := fp.pool.Load(addr)
//	if err != nil {
//		return 0, err
//	}
//
//	reply, err := gcli.CreateSymlink(ctx, &pb.CreateSymlinkRequest{
//		Id:     parentID,
//		Name:   name,
//		Target: target,
//	})
//	if err != nil {
//		log.Printf("rpc create symlink error: parentID=%v, name=%v, target=%v, err=%+v",
//			parentID, name, target, err)
//		return 0, err
//	}
//	return reply.Num, utility.FromPbErr(reply.Err)
//}

func (fp *FProxy) CreateLink(
	ctx context.Context,
	parentID uint64,
	name string,
	targetID uint64) (uint64, error) {
	log.Printf("fp create link: parent=%v, name=%v, target=%v",
		parentID, name, targetID)
	if fp.IsLocalNode(ctx, parentID) {
		return fp.fb.CreateLink(ctx, parentID, name, targetID)
	}

	// TODO: Parent owner start and acquire child
	addr := fp.pcli.QueryOwner(parentID)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return 0, err
	}

	reply, err := gcli.CreateLink(ctx, &pb.CreateLinkRequest{
		Id:       parentID,
		Name:     name,
		TargetID: targetID,
	})
	if err != nil {
		log.Printf("rpc create link error: parentID=%v, name=%v, targetID=%v, err=%+v",
			parentID, name, targetID, err)
		return 0, err
	}
	return reply.Num, utility.FromPbErr(reply.Err)
}

func (fp *FProxy) Rename(
	ctx context.Context,
	oldParent uint64,
	oldName string,
	newParent uint64,
	newName string) (err error) {
	log.Printf("fp rename: oldParent=%v, oldName=%v, newParent=%v, newName=%v",
		oldParent, oldName, newParent, newName)
	if fp.IsLocalNode(ctx, newParent) && fp.IsLocalNode(ctx, oldParent) &&
		fp.isChildLocal(ctx, oldParent, oldName) {
		return fp.fb.Rename(ctx, oldParent, oldName, newParent, newName)
	}

	// TODO: NewParent owner start and acquire child, OldParent owner rm OldChild, rm node, NewParent add child
	addr := fp.pcli.QueryOwner(newParent)
	gcli, err := fp.pool.Load(addr)
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
		log.Printf("rpc fp rename: oldParent=%v, oldName=%v, newParent=%v, newName=%v, err=%+v",
			oldParent, oldName, newParent, newName, err)
		return err
	}
	return utility.FromPbErr(perr)
}

func (fp *FProxy) DetachChild(
	ctx context.Context,
	parent uint64,
	name string) (err error) {
	log.Printf("fp DetachChild: parent=%v, name=%v", parent, name)
	if fp.IsLocalNode(ctx, parent) {
		return fp.fb.DetachChild(ctx, parent, name)
	}

	addr := fp.pcli.QueryOwner(parent)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return err
	}

	perr, err := gcli.DetachChild(ctx, &pb.UnlinkRequest{
		Parent: parent,
		Name:   name,
	})
	if err != nil {
		log.Printf("rpc fp DetachChild error: parent=%v, name=%v, err=%+v", parent, name, err)
		return err
	}
	return utility.FromPbErr(perr)
}

func (fp *FProxy) Unlink(
	ctx context.Context,
	parent uint64,
	name string) (err error) {
	log.Printf("fp unlink: parent=%v, name=%v", parent, name)
	childID, _, err := fp.LookUpInode(ctx, parent, name)
	if err != nil {
		log.Printf("rpc fp unlink no child error: parent=%v, name=%v, err=%+v", parent, name, err)
		err = syscall.ENOENT
		return err
	}

	if fp.IsLocalNode(ctx, childID) {
		return fp.fb.Unlink(ctx, parent, name, childID)
	}

	addr := fp.pcli.QueryOwner(childID)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return err
	}

	perr, err := gcli.Unlink(ctx, &pb.UnlinkRequest{
		Parent: parent,
		Name:   name,
	})
	if err != nil {
		log.Printf("rpc fp unlink error: parent=%v, name=%v, childID=%v, err=%+v",
			parent, name, childID, err)
		return err
	}
	return utility.FromPbErr(perr)
}

func (fp *FProxy) Open(
	ctx context.Context,
	id uint64,
	flags uint32) (handle uint64, err error) {
	log.Printf("fp opendir: id=%v, flags=%v", id, flags)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.Open(ctx, id, flags)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return 0, err
	}

	reply, err := gcli.Open(ctx, &pb.OpenXRequest{
		Id:    id,
		Flags: flags,
	})
	if err != nil {
		log.Printf("rpc opendir error: id=%v, err=%+v",
			id, err)
		return 0, err
	}

	err = utility.FromPbErr(reply.Err)
	remoteHandle := reply.Num
	if err != nil || remoteHandle <= 0 {
		log.Printf("rpc opendir error: id=%v, remoteHandle=%v, perr=%+v",
			id, remoteHandle, err)
	}
	localHandle := fp.allcator.Allocate()
	fp.StoreRemoteHandle(ctx, localHandle, remoteHandle, addr)
	return localHandle, nil
}

func (fp *FProxy) ReadDirAll(
	ctx context.Context,
	id uint64) ([]fuse.Dirent, error) {
	log.Printf("fp readdir: id=%v", id)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ReadDir(ctx, id)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return nil, err
	}

	reply, err := gcli.ReadDir(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		log.Printf("rpc opendir error: id=%v, err=%+v",
			id, err)
		return nil, err
	}
	return utility.FromPbDirents(reply.Dirents), utility.FromPbErr(reply.Err)
}

func (fp *FProxy) ReleaseHandle(
	ctx context.Context,
	h uint64) error {
	log.Printf("release handle: handleID=%v", h)
	if rh := fp.LoadRemoteHandle(ctx, h); rh != nil {
		log.Printf("rpc release remote handle: handleID=%v", h)
		addr := rh.addr
		gcli, err := fp.pool.Load(addr)
		if err != nil {
			return err
		}

		perr, err := gcli.ReleaseHandle(ctx, &pb.UInt64ID{
			Id: rh.handle,
		})
		if err != nil {
			log.Printf("rpc release remote handle error: local id=%v, remote id=%v, err=%+v",
				h, rh.handle, err)
			return err
		}
		if err := utility.FromPbErr(perr); err != nil {
			log.Printf("rpc release remote handle remote error: local id=%v, remote id=%v, err=%+v",
				h, rh.handle, err)
			return err
		}
		if err := fp.ReleaseRemoteHandle(ctx, h); err != nil {
			log.Printf("rpc release remote handle release map error: local id=%v, remote id=%v, err=%+v",
				h, rh.handle, err)
			return err
		}
		log.Printf("rpc release remote handle success: local id=%v, remote id=%v",
			h, rh.handle)
		return nil
	}

	log.Printf("release local handle: handleID=%v", h)
	return fp.fb.ReleaseHandle(ctx, h)
}

func (fp *FProxy) ReadFile(
	ctx context.Context,
	id uint64,
	length uint64,
	offset uint64) (bytesRead uint64, buf []byte, err error) {
	log.Printf("fp readfile: id=%v, length=%v, offset=%v", id, length, offset)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ReadFile(ctx, id, length, offset)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return 0, nil, err
	}

	reply, err := gcli.ReadFile(ctx, &pb.ReadXRequest{
		Id:     id,
		Length: length,
		Offset: offset,
	})
	if err != nil {
		log.Printf("rpc readfile error: id=%v, err=%+v",
			id, err)
		return 0, nil, err
	}
	return reply.BytesRead, reply.Buf, utility.FromPbErr(reply.Err)
}

func (fp *FProxy) WriteFile(
	ctx context.Context,
	id uint64,
	offset uint64,
	data []byte) (uint64, error) {
	log.Printf("write file: id=%v, offset=%v, data=%v", id, offset, data)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.WriteFile(ctx, id, offset, data)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return 0, err
	}

	reply, err := gcli.WriteFile(ctx, &pb.WriteXRequest{
		Id:     id,
		Offset: offset,
		Data:   data,
	})
	if err != nil {
		log.Printf("rpc write file error: id=%v, err=%+v",
			id, err)
		return 0, err
	}
	return reply.Num, utility.FromPbErr(reply.Err)
}

func (fp *FProxy) ReadSymlink(
	ctx context.Context,
	id uint64) (target string, err error) {
	log.Printf("fp read symlink: id=%v", id)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ReadSymlink(ctx, id)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return "", err
	}

	reply, err := gcli.ReadSymlink(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		log.Printf("rpc read symlink error: id=%v, err=%+v",
			id, err)
		return "", err
	}
	return "", utility.FromPbErr(reply.Err)
}

func (fp *FProxy) GetXattr(ctx context.Context,
	id uint64,
	name string,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	log.Printf("fp get xattr: id=%v, name=%v, length=%v",
		id, name, length)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.GetXattr(ctx, id, name, length)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return 0, nil, err
	}

	reply, err := gcli.GetXattr(ctx, &pb.GetXattrRequest{
		Id:     id,
		Length: length,
		Name:   name,
	})
	if err != nil {
		log.Printf("rpc getxattr error: id=%v, err=%+v",
			id, err)
		return 0, nil, err
	}
	return 0, nil, utility.FromPbErr(reply.Err)
}

func (fp *FProxy) ListXattr(ctx context.Context,
	id uint64,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	log.Printf("fp list xattr: id=%v, length=%v",
		id, length)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ListXattr(ctx, id, length)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return 0, nil, err
	}

	reply, err := gcli.ListXattr(ctx, &pb.ListXattrRequest{
		Id:     id,
		Length: length,
	})
	if err != nil {
		log.Printf("rpc listxattr error: id=%v, err=%+v",
			id, err)
		return 0, nil, err
	}
	return 0, nil, utility.FromPbErr(reply.Err)
}

func (fp *FProxy) RemoveXattr(ctx context.Context, id uint64, name string) error {
	log.Printf("fp rm xattr: id=%v, name=%v", id, name)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.RemoveXattr(ctx, id, name)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return err
	}

	reply, err := gcli.RemoveXattr(ctx, &pb.RemoveXattrRequest{
		Id:   id,
		Name: name,
	})
	if err != nil {
		log.Printf("rpc listxattr error: id=%v, err=%+v",
			id, err)
		return err
	}
	return utility.FromPbErr(reply)
}

func (fp *FProxy) SetXattr(ctx context.Context, id uint64, name string, flags uint32, value []byte) error {
	log.Printf("fp set xattr: id=%v, name=%v, flag=%v, value=%X", id, name, flags, value)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.SetXattr(ctx, id, name, flags, value)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
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
		log.Printf("rpc fp set xattr err: id=%v, name=%v, flag=%v, value=%X",
			id, name, flags, value, err)
		return err
	}
	return utility.FromPbErr(reply)
}

func (fp *FProxy) Fallocate(ctx context.Context,
	id uint64,
	mode uint32,
	length uint64) error {
	log.Printf("fp fallocate: id=%v, mode=%v, len=%v", id, mode, length)
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.Fallocate(ctx, id, mode, length)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli, err := fp.pool.Load(addr)
	if err != nil {
		return err
	}

	reply, err := gcli.Fallocate(ctx, &pb.FallocateRequest{
		Id:     id,
		Mode:   mode,
		Length: length,
	})
	if err != nil {
		log.Printf("rpc fallocate error: id=%v, mode=%v, length=%v, err=%+v",
			id, mode, length, err)
		return err
	}
	return utility.FromPbErr(reply)
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

func (fp *FProxy) QueryOwner(nodeID uint64) string {
	return fp.ma.QueryOwner(nodeID)
}

func (fp *FProxy) QueryAddr(nodeID uint64) string {
	return fp.ma.QueryAddr(nodeID)
}

func (fp *FProxy) Allocate(ownerID uint64) uint64 {
	return fp.ma.Allocate(ownerID)
}

func (fp *FProxy) Deallocate(nodeID uint64) bool {
	return fp.ma.Deallocate(nodeID)
}

func (fp *FProxy) RegisterOwner(addr string) uint64 {
	return fp.ma.RegisterOwner(addr)
}

func (fp *FProxy) RemoveOwner(ownerID uint64) bool {
	return fp.ma.RemoveOwner(ownerID)
}

func (fp *FProxy) CopyOwnerMap() map[uint64]string {
	return fp.ma.CopyOwnerMap()

}

func (fp *FProxy) AllocateRoot(ownerID uint64) bool {
	return fp.ma.AllocateRoot(ownerID)
}

func (e *FPErr) Error() string {
	return fmt.Sprintf("fproxy error: %v", e.msg)
}
