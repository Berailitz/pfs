package fproxy

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Berailitz/pfs/rnode"

	"github.com/Berailitz/pfs/gclipool"

	pb "github.com/Berailitz/pfs/remotetree"

	"github.com/Berailitz/pfs/utility"
	"google.golang.org/grpc"

	"github.com/Berailitz/pfs/fbackend"
	"github.com/Berailitz/pfs/rclient"
	"github.com/jacobsa/fuse/fuseops"
)

type FProxy struct {
	fb   *fbackend.FBackEnd
	pcli *rclient.RClient
	pool *gclipool.GCliPool
}

type FPErr struct {
	msg string
}

var _ = (error)((*FPErr)(nil))

// Create a file system that stores data and metadata in memory.
//
// The supplied UID/GID pair will own the root rnode.RNode. This file system does no
// permissions checking, and should therefore be mounted with the
// default_permissions option.
func NewFProxy(
	uid uint32,
	gid uint32,
	masterAddr string,
	localAddr string,
	gopts []grpc.DialOption) *FProxy {
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

	fb := fbackend.NewFBackEnd(uid, gid, masterAddr, localAddr, gopts)
	if fb == nil {
		log.Fatalf("new fp nil fb error: uid=%v, gid=%v, masterAddr=%v, localAddr=%v, gopts=%+v",
			uid, gid, masterAddr, localAddr, gopts)
	}
	// Set up the root rnode.RNode.

	return &FProxy{
		fb:   fb,
		pcli: pcli,
		pool: gclipool.NewGCliPool(gopts, localAddr),
	}
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////
func (fp *FProxy) decodeError(perr *pb.Error) error {
	if perr != nil && perr.Status != 0 {
		return fmt.Errorf(perr.Msg)
	}
	return nil
}

func (fp *FProxy) buillNoGCliErr(addr string) error {
	return &FPErr{msg: fmt.Sprintf("no gcli: addr=%v", addr)}
}

func (fp *FProxy) buillNotSupportedErr(operation string) error {
	return &FPErr{msg: fmt.Sprintf("not supported: %v", operation)}
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (fp *FProxy) LoadNode(id uint64) (*rnode.RNode, error) {
	return fp.fb.LoadNodeForRead(id)
}

func (fp *FProxy) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	return nil
}

func (fp *FProxy) LookUpInode(
	ctx context.Context,
	parentID uint64,
	name string) (uint64, fuseops.InodeAttributes, error) {
	if fp.IsLocalNode(ctx, parentID) {
		return fp.fb.LookUpInode(ctx, parentID, name)
	}

	addr := fp.pcli.QueryOwner(parentID)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, fuseops.InodeAttributes{}, fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.LookUpInode(ctx, &pb.LookUpInodeRequest{
		ParentID: parentID,
		Name:     name,
	})
	if err != nil {
		log.Printf("rpc look up inode error: parentID=%v, name=%v, err=%+v", parentID, name, err)
		return 0, fuseops.InodeAttributes{}, err
	}
	return reply.Id, utility.FromPbAttr(*reply.Attr), fp.decodeError(reply.Err)
}

func (fp *FProxy) GetInodeAttributes(
	ctx context.Context,
	id uint64) (fuseops.InodeAttributes, error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.GetInodeAttributes(ctx, id)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return fuseops.InodeAttributes{}, fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.GetInodeAttributes(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		log.Printf("rpc get inode attr error: id=%v, err=%+v", id, err)
		return fuseops.InodeAttributes{}, err
	}
	return utility.FromPbAttr(*reply.Attr), fp.decodeError(reply.Err)
}

func (fp *FProxy) SetInodeAttributes(
	ctx context.Context,
	id uint64,
	param fbackend.SetInodeAttributesParam) (fuseops.InodeAttributes, error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.SetInodeAttributes(ctx, id, param)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return fuseops.InodeAttributes{}, fp.buillNoGCliErr(addr)
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
		return fuseops.InodeAttributes{}, err
	}
	return utility.FromPbAttr(*reply.Attr), fp.decodeError(reply.Err)
}

func (fp *FProxy) MkDir(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (uint64, fuseops.InodeAttributes, error) {
	if fp.IsLocalNode(ctx, parentID) {
		return fp.fb.MkDir(ctx, parentID, name, mode)
	}

	addr := fp.pcli.QueryOwner(parentID)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, fuseops.InodeAttributes{}, fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.MkDir(ctx, &pb.MkDirRequest{
		Id:   parentID,
		Name: name,
		Mode: uint32(mode),
	})
	if err != nil {
		log.Printf("rpc mkdir error: parentID=%v, name=%v, err=%+v", parentID, name, err)
		return 0, fuseops.InodeAttributes{}, err
	}
	return reply.Id, utility.FromPbAttr(*reply.Attr), fp.decodeError(reply.Err)
}

// LOCKS_REQUIRED(fp.mu)
func (fp *FProxy) CreateNode(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (fuseops.ChildInodeEntry, error) {
	if fp.IsLocalNode(ctx, parentID) {
		return fp.fb.CreateNode(ctx, parentID, name, mode)
	}

	addr := fp.pcli.QueryOwner(parentID)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return fuseops.ChildInodeEntry{}, fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.CreateNode(ctx, &pb.CreateNodeRequest{
		Id:   parentID,
		Name: name,
		Mode: uint32(mode),
	})
	if err != nil {
		log.Printf("rpc look up inode error: parentID=%v, name=%v, err=%+v", parentID, name, err)
		return fuseops.ChildInodeEntry{}, err
	}
	return utility.FromPbEntry(*reply.Entry), fp.decodeError(reply.Err)
}

// LOCKS_REQUIRED(fp.mu)
func (fp *FProxy) CreateFile(
	ctx context.Context,
	parentID uint64,
	name string,
	mode os.FileMode) (fuseops.ChildInodeEntry, uint64, error) {
	if fp.IsLocalNode(ctx, parentID) {
		return fp.fb.CreateFile(ctx, parentID, name, mode)
	}

	addr := fp.pcli.QueryOwner(parentID)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return fuseops.ChildInodeEntry{}, 0, fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.CreateFile(ctx, &pb.CreateNodeRequest{
		Id:   parentID,
		Name: name,
		Mode: uint32(mode),
	})
	if err != nil {
		log.Printf("rpc look up inode error: parentID=%v, name=%v, err=%+v", parentID, name, err)
		return fuseops.ChildInodeEntry{}, 0, err
	}
	return utility.FromPbEntry(*reply.Entry), reply.Handle, fp.decodeError(reply.Err)
}

func (fp *FProxy) CreateSymlink(
	ctx context.Context,
	parentID uint64,
	name string,
	target string) (uint64, fuseops.InodeAttributes, error) {
	if fp.IsLocalNode(ctx, parentID) {
		return fp.fb.CreateSymlink(ctx, parentID, name, target)
	}

	addr := fp.pcli.QueryOwner(parentID)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, fuseops.InodeAttributes{}, fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.CreateSymlink(ctx, &pb.CreateSymlinkRequest{
		Id:     parentID,
		Name:   name,
		Target: target,
	})
	if err != nil {
		log.Printf("rpc create symlink error: parentID=%v, name=%v, target=%v, err=%+v",
			parentID, name, target, err)
		return 0, fuseops.InodeAttributes{}, err
	}
	return reply.Id, utility.FromPbAttr(*reply.Attr), fp.decodeError(reply.Err)
}

func (fp *FProxy) CreateLink(
	ctx context.Context,
	parentID uint64,
	name string,
	targetID uint64) (fuseops.InodeAttributes, error) {
	if fp.IsLocalNode(ctx, parentID) {
		return fp.fb.CreateLink(ctx, parentID, name, targetID)
	}

	// TODO: Parent owner start and acquire child
	return fuseops.InodeAttributes{}, fp.buillNotSupportedErr(
		fmt.Sprintf("CreateSymlink: parentID=%v, name=%v, targetID=%v", parentID, name, targetID))
	//addr := fp.pcli.QueryOwner(parentID)
	//gcli := fp.pool.Load(addr)
	//if gcli == nil {
	//	return fuseops.InodeAttributes{}, fp.buillNoGCliErr(addr)
	//}
	//
	//reply, err := gcli.CreateLink(ctx, &pb.CreateLinkRequest{
	//	Id:       parentID,
	//	Name:     name,
	//	TargetID: targetID,
	//})
	//if err != nil {
	//	log.Printf("rpc set inode attr error: id=%v, err=%+v", id, err)
	//	return fuseops.InodeAttributes{}, err
	//}
	//return utility.FromPbAttr(*reply.Attr), fp.decodeError(reply.Err)
}

func (fp *FProxy) Rename(
	ctx context.Context,
	op fuseops.RenameOp) (err error) {
	if fp.IsLocalNode(ctx, uint64(op.NewParent)) && fp.IsLocalNode(ctx, uint64(op.OldParent)) &&
		fp.isChildLocal(ctx, uint64(op.OldParent), op.OldName) {
		return fp.fb.Rename(ctx, op)
	}

	// TODO: NewParent owner start and acquire child, OldParent owner rm OldChild, rm node, NewParent add child
	return fp.buillNotSupportedErr(fmt.Sprintf("rename: op=%#v", op))
}

func (fp *FProxy) RmDir(
	ctx context.Context,
	op fuseops.RmDirOp) (err error) {
	if fp.IsLocalNode(ctx, uint64(op.Parent)) &&
		fp.isChildLocal(ctx, uint64(op.Parent), op.Name) {
		return fp.fb.RmDir(ctx, op)
	}

	// TODO: Parent owner start, Child owner rm node
	return fp.buillNotSupportedErr(fmt.Sprintf("rmdir: op=%#v", op))
}

func (fp *FProxy) Unlink(
	ctx context.Context,
	op fuseops.UnlinkOp) (err error) {
	if fp.IsLocalNode(ctx, uint64(op.Parent)) &&
		fp.isChildLocal(ctx, uint64(op.Parent), op.Name) {
		return fp.fb.Unlink(ctx, op)
	}

	// TODO: Parent owner start and acquire child
	return fp.buillNotSupportedErr(fmt.Sprintf("unlink: op=%#v", op))
}

func (fp *FProxy) OpenDir(
	ctx context.Context,
	id uint64) (handle uint64, err error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.OpenDir(ctx, id)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.OpenDir(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		log.Printf("rpc opendir error: id=%v, err=%+v",
			id, err)
		return 0, err
	}

	err = fp.decodeError(reply.Err)
	h := reply.Num
	if err != nil || h <= 0 {
		log.Printf("rpc opendir error: id=%v, h=%v, perr=%+v",
			id, h, err)
	}
	return h, err
}

func (fp *FProxy) ReadDir(
	ctx context.Context,
	id uint64,
	length uint64,
	offset uint64) (bytesRead uint64, buf []byte, err error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ReadDir(ctx, id, length, offset)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, nil, fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.ReadDir(ctx, &pb.ReadXRequest{
		Id:     id,
		Length: length,
		Offset: offset,
	})
	if err != nil {
		log.Printf("rpc opendir error: id=%v, err=%+v",
			id, err)
		return 0, nil, err
	}
	return 0, nil, fp.decodeError(reply.Err)
}

func (fp *FProxy) OpenFile(ctx context.Context, id uint64) (handle uint64, err error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.OpenFile(ctx, id)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.OpenFile(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		log.Printf("rpc openfile error: id=%v, err=%+v",
			id, err)
		return 0, err
	}

	err = fp.decodeError(reply.Err)
	h := reply.Num
	if err != nil || h <= 0 {
		log.Printf("rpc openfile error: id=%v, h=%v, perr=%+v",
			id, h, err)
	}
	return h, err
}

func (fp *FProxy) ReadFile(
	ctx context.Context,
	id uint64,
	length uint64,
	offset uint64) (bytesRead uint64, buf []byte, err error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ReadFile(ctx, id, length, offset)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, nil, fp.buillNoGCliErr(addr)
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
	return 0, nil, fp.decodeError(reply.Err)
}

func (fp *FProxy) WriteFile(
	ctx context.Context,
	id uint64,
	offset uint64,
	data []byte) (uint64, error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.WriteFile(ctx, id, offset, data)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, fp.buillNoGCliErr(addr)
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
	return 0, fp.decodeError(reply.Err)
}

func (fp *FProxy) ReadSymlink(
	ctx context.Context,
	id uint64) (target string, err error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ReadSymlink(ctx, id)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return "", fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.ReadSymlink(ctx, &pb.UInt64ID{
		Id: id,
	})
	if err != nil {
		log.Printf("rpc read symlink error: id=%v, err=%+v",
			id, err)
		return "", err
	}
	return "", fp.decodeError(reply.Err)
}

func (fp *FProxy) GetXattr(ctx context.Context,
	id uint64,
	name string,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.GetXattr(ctx, id, name, length)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, nil, fp.buillNoGCliErr(addr)
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
	return 0, nil, fp.decodeError(reply.Err)
}

func (fp *FProxy) ListXattr(ctx context.Context,
	id uint64,
	length uint64) (bytesRead uint64, dst []byte, err error) {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.ListXattr(ctx, id, length)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return 0, nil, fp.buillNoGCliErr(addr)
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
	return 0, nil, fp.decodeError(reply.Err)
}

func (fp *FProxy) RemoveXattr(ctx context.Context, id uint64, name string) error {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.RemoveXattr(ctx, id, name)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return fp.buillNoGCliErr(addr)
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
	return fp.decodeError(reply)
}

func (fp *FProxy) SetXattr(ctx context.Context, op fuseops.SetXattrOp) error {
	if fp.IsLocalNode(ctx, uint64(op.Inode)) {
		return fp.fb.SetXattr(ctx, op)
	}

	addr := fp.pcli.QueryOwner(uint64(op.Inode))
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return fp.buillNoGCliErr(addr)
	}

	reply, err := gcli.SetXattr(ctx, &pb.SetXattrRequest{
		Id:    uint64(op.Inode),
		Name:  op.Name,
		Value: op.Value,
		Flag:  op.Flags,
	})
	if err != nil {
		log.Printf("rpc setxattr error: op=%#v, err=%+v", op, err)
		return err
	}
	return fp.decodeError(reply)
}

func (fp *FProxy) Fallocate(ctx context.Context,
	id uint64,
	mode uint32,
	length uint64) error {
	if fp.IsLocalNode(ctx, id) {
		return fp.fb.Fallocate(ctx, id, mode, length)
	}

	addr := fp.pcli.QueryOwner(id)
	gcli := fp.pool.Load(addr)
	if gcli == nil {
		return fp.buillNoGCliErr(addr)
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
	return fp.decodeError(reply)
}

func (fp *FProxy) IsLocalNode(ctx context.Context, id uint64) bool {
	_, err := fp.fb.LoadLocalNodeForRead(id)
	return err == nil
}

func (fp *FProxy) isChildLocal(ctx context.Context, parentId uint64, name string) bool {
	_, _, err := fp.fb.LookUpInode(ctx, parentId, name)
	return err == nil
}

func (e *FPErr) Error() string {
	return fmt.Sprintf("fproxy error: %v", e.msg)
}
