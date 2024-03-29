//go:generate  protoc -I ../remotetree/ ../remotetree/remotetree.proto --go_out=plugins=grpc:../remotetree

package rserver

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/Berailitz/pfs/rnode"

	grpc_logrus "github.com/Berailitz/pfs/logger/grpc"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	"bazil.org/fuse"

	"github.com/Berailitz/pfs/logger"
	"github.com/Berailitz/pfs/utility"

	"github.com/Berailitz/pfs/fbackend"

	"google.golang.org/grpc"

	pb "github.com/Berailitz/pfs/remotetree"
)

const rServerStartTime = time.Second * 2

type RServer struct {
	pb.UnimplementedRemoteTreeServer
	Server *grpc.Server
	fp     *fbackend.FProxy
	ma     *fbackend.RManager
}

func makeServerOptions(ctx context.Context) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc_middleware.WithUnaryServerChain(
			grpc_ctxtags.UnaryServerInterceptor(
				grpc_ctxtags.WithFieldExtractor(
					grpc_ctxtags.CodeGenRequestFieldExtractor)),
			utility.CtxMDServerInterceptor(ctx),
			grpc_logrus.UnaryServerInterceptor(
				logger.Entry(), grpc_logrus.WithMessageProducer(logger.StubMessageProducer))),
	}
}

func (s *RServer) Push(ctx context.Context, req *pb.PushNodeRequest) (*pb.Error, error) {
	err := s.fp.PushNode(ctx, req.Addr, rnode.FromPbNode(ctx, req.Node))
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingReply, error) {
	offset, err := s.fp.Ping(ctx, req.Addr, false, false)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.PingReply{
		Err:       perr,
		Departure: req.Departure,
		Offset:    offset,
	}, nil
}

func (s *RServer) Vote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteReply, error) {
	vote := &fbackend.Vote{
		Voter:      req.Voter,
		ElectionID: req.ElectionID,
		ProposalID: req.ProposalID,
		Nominee:    req.Nominee,
	}
	var perr *pb.Error = &pb.Error{}
	masterAddr, err := s.fp.Vote(ctx, req.Addr, vote)
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.VoteReply{
		Err:        perr,
		MasterAddr: masterAddr,
	}, nil
}

func (s *RServer) Propose(ctx context.Context, req *pb.ProposeRequest) (_ *pb.ProposeReply, err error) {
	var state int64
	proposal := &fbackend.Proposal{
		ID:      req.ProposeID,
		Typ:     req.ProposeType,
		OwnerID: req.OwnerID,
		NodeID:  req.NodeID,
		Strs:    req.Strs,
		Value:   req.Value,
	}
	if s.fp == nil {
		state, err = s.ma.AnswerProposal(ctx, req.Addr, proposal)
	} else {
		state, err = s.fp.AnswerProposal(ctx, req.Addr, proposal)
	}

	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.ProposeReply{
		Err:   perr,
		State: state,
	}, nil
}

func (s *RServer) MakeRegular(ctx context.Context, req *pb.UInt64IDAddr) (*pb.Error, error) {
	ferr := s.fp.MakeRegular(ctx, req.Addr, req.Id)
	return rnode.ToPbErr(ferr), nil
}

func (s *RServer) Gossip(ctx context.Context, req *pb.GossipRequest) (*pb.GossipReply, error) {
	tofMap, err := s.fp.Gossip(ctx, req.Addr)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.GossipReply{
		Err:    perr,
		TofMap: tofMap,
	}, nil
}

func (s *RServer) CopyManager(ctx context.Context, req *pb.EmptyMsg) (*pb.Manager, error) {
	return s.fp.CopyManager(ctx)
}

func (s *RServer) GetOwnerMap(ctx context.Context, _ *pb.EmptyMsg) (*pb.Uint64StrMapMsg, error) {
	ownerMap, err := s.fp.GetOwnerMap(ctx)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.Uint64StrMapMsg{
		Map: ownerMap,
		Err: perr,
	}, nil
}

func (s *RServer) FetchNode(ctx context.Context, req *pb.NodeIsReadRequest) (*pb.FetchNodeReply, error) {
	node, lockID, err := s.fp.LoadNode(ctx, req.Id, req.IsRead)
	if err != nil {
		logger.E(ctx, "fetch node load node error", "id", req.Id, "IsRead", req.IsRead)
		return &pb.FetchNodeReply{
			Node:   &pb.Node{},
			LockID: 0,
			Err:    rnode.ToPbErr(err),
		}, nil
	}
	return &pb.FetchNodeReply{
		Node:   rnode.ToPbNode(node),
		LockID: lockID,
		Err:    rnode.ToPbErr(err),
	}, nil
}

func (s *RServer) RUnlockNode(ctx context.Context, req *pb.RUnlockNodeRequest) (*pb.Error, error) {
	err := s.fp.RUnlockNode(ctx, req.NodeID, req.LockID)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) UnlockNode(ctx context.Context, req *pb.UnlockNodeRequest) (*pb.Error, error) {
	err := s.fp.UnlockNode(ctx, rnode.FromPbNode(ctx, req.Node), req.LockID)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) LookUpInode(ctx context.Context, req *pb.LookUpInodeRequest) (*pb.LookUpInodeReply, error) {
	id, attr, err := s.fp.LookUpInode(ctx, req.ParentID, req.Name)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.LookUpInodeReply{
		Id:   id,
		Attr: rnode.ToPbAttr(attr),
		Err:  perr,
	}, nil
}

func (s *RServer) GetInodeAttributes(ctx context.Context, req *pb.UInt64ID) (*pb.GetInodeAttributesReply, error) {
	attr, err := s.fp.GetInodeAttributes(ctx, req.Id)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.GetInodeAttributesReply{
		Err:  perr,
		Attr: rnode.ToPbAttr(attr),
	}, nil
}

func (s *RServer) SetInodeAttributes(ctx context.Context, req *pb.SetInodeAttributesRequest) (*pb.SetInodeAttributesReply, error) {
	attr, err := s.fp.SetInodeAttributes(ctx, req.Id, fbackend.SetInodeAttributesParam{
		Size:     req.Size,
		Mode:     os.FileMode(req.Mode),
		Mtime:    time.Unix(req.Mtime, 0),
		HasSize:  req.HasSize,
		HasMode:  req.HasMode,
		HasMtime: req.HasMtime,
	})
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.SetInodeAttributesReply{
		Err:  perr,
		Attr: rnode.ToPbAttr(attr),
	}, nil
}

func (s *RServer) MkDir(ctx context.Context, req *pb.MkDirRequest) (*pb.MkDirReply, error) {
	id, err := s.fp.MkDir(ctx, req.Id, req.Name, os.FileMode(req.Mode))
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.MkDirReply{
		Id:  id,
		Err: perr,
	}, nil
}

func (s *RServer) CreateNode(ctx context.Context, req *pb.CreateNodeRequest) (*pb.Uint64Reply, error) {
	id, err := s.fp.CreateNode(ctx, req.Id, req.Name, os.FileMode(req.Mode))
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.Uint64Reply{
		Num: id,
		Err: perr,
	}, nil
}

func (s *RServer) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileReply, error) {
	nid, handle, err := s.fp.CreateFile(ctx, req.Id, req.Name, os.FileMode(req.Mode), req.Flags)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.CreateFileReply{
		Id:     nid,
		Handle: handle,
		Err:    perr,
	}, nil
}

func (s *RServer) AttachChild(ctx context.Context, req *pb.AttachChildRequest) (*pb.Uint64Reply, error) {
	id, err := s.fp.AttachChild(ctx, req.ParentID, req.ChildID, req.Name, fuse.DirentType(req.Dt), req.DoOpen)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.Uint64Reply{
		Num: id,
		Err: perr,
	}, nil
}

func (s *RServer) CreateSymlink(ctx context.Context, req *pb.CreateSymlinkRequest) (*pb.Uint64Reply, error) {
	id, err := s.fp.CreateSymlink(ctx, req.Id, req.Name, req.Target)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.Uint64Reply{
		Num: id,
		Err: perr,
	}, nil
}

func (s *RServer) CreateLink(ctx context.Context, req *pb.CreateLinkRequest) (*pb.Uint64Reply, error) {
	id, err := s.fp.CreateLink(ctx, req.Id, req.Name, req.TargetID)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.Uint64Reply{
		Num: id,
		Err: perr,
	}, nil
}

func (s *RServer) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Error, error) {
	err := s.fp.Rename(ctx, req.OldParent, req.OldName, req.NewParent, req.NewName)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) DetachChild(ctx context.Context, req *pb.UnlinkRequest) (*pb.Error, error) {
	err := s.fp.DetachChild(ctx, req.Parent, req.Name)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Error, error) {
	err := s.fp.Unlink(ctx, req.Parent, req.Name)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) Open(ctx context.Context, req *pb.OpenXRequest) (*pb.Uint64Reply, error) {
	h, err := s.fp.Open(ctx, req.Id, req.Flags)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.Uint64Reply{
		Err: perr,
		Num: h,
	}, nil
}
func (s *RServer) ReadDir(ctx context.Context, req *pb.UInt64ID) (*pb.ReadDirReply, error) {
	dirents, err := s.fp.ReadDirAll(ctx, req.Id)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.ReadDirReply{
		Err:     perr,
		Dirents: rnode.ToPbDirents(dirents),
	}, nil
}

func (s *RServer) ReleaseHandle(ctx context.Context, req *pb.UInt64ID) (*pb.Error, error) {
	err := s.fp.ReleaseHandle(ctx, req.Id)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) ReadFile(ctx context.Context, req *pb.ReadXRequest) (*pb.ReadXReply, error) {
	bytesRead, buf, err := s.fp.ReadFile(ctx, req.Id, req.Length, req.Offset)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.ReadXReply{
		Err:       perr,
		BytesRead: bytesRead,
		Buf:       buf,
	}, nil
}

func (s *RServer) WriteFile(ctx context.Context, req *pb.WriteXRequest) (*pb.Uint64Reply, error) {
	bytesWrite, err := s.fp.WriteFile(ctx, req.Id, req.Offset, req.Data)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.Uint64Reply{
		Err: perr,
		Num: bytesWrite,
	}, nil
}

func (s *RServer) ReadSymlink(ctx context.Context, req *pb.UInt64ID) (*pb.ReadSymlinkReply, error) {
	target, err := s.fp.ReadSymlink(ctx, req.Id)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.ReadSymlinkReply{
		Err:    perr,
		Target: target,
	}, nil
}

func (s *RServer) GetXattr(ctx context.Context, req *pb.GetXattrRequest) (*pb.ReadXReply, error) {
	bytesRead, buf, err := s.fp.GetXattr(ctx, req.Id, req.Name, req.Length)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.ReadXReply{
		Err:       perr,
		BytesRead: bytesRead,
		Buf:       buf,
	}, nil
}

func (s *RServer) ListXattr(ctx context.Context, req *pb.ListXattrRequest) (*pb.ReadXReply, error) {
	bytesRead, buf, err := s.fp.ListXattr(ctx, req.Id, req.Length)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return &pb.ReadXReply{
		Err:       perr,
		BytesRead: bytesRead,
		Buf:       buf,
	}, nil
}

func (s *RServer) RemoveXattr(ctx context.Context, req *pb.RemoveXattrRequest) (*pb.Error, error) {
	err := s.fp.RemoveXattr(ctx, req.Id, req.Name)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) SetXattr(ctx context.Context, req *pb.SetXattrRequest) (*pb.Error, error) {
	err := s.fp.SetXattr(ctx, req.Id, req.Name, req.Flag, req.Value)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) Fallocate(ctx context.Context, req *pb.FallocateRequest) (*pb.Error, error) {
	err := s.fp.Fallocate(ctx, req.Id, req.Mode, req.Length)
	var perr *pb.Error = &pb.Error{}
	if err != nil {
		perr = rnode.ToPbErr(err)
	}
	return perr, nil
}

func (s *RServer) QueryOwner(ctx context.Context, req *pb.UInt64ID) (*pb.AddrReply, error) {
	var addr string
	var ferr error
	if s.fp != nil {
		addr, ferr = s.fp.QueryOwner(ctx, req.Id)
	} else {
		addr, ferr = s.ma.QueryOwner(ctx, req.Id)
	}
	return &pb.AddrReply{Addr: addr, Err: rnode.ToPbErr(ferr)}, nil
}

func (s *RServer) Allocate(ctx context.Context, req *pb.OwnerId) (*pb.UInt64Reply, error) {
	var id uint64
	var ferr error
	if s.fp != nil {
		id, ferr = s.fp.Allocate(ctx, req.Id)
	} else {
		id, ferr = s.ma.Allocate(ctx, req.Id)
	}
	return &pb.UInt64Reply{Id: id, Err: rnode.ToPbErr(ferr)}, nil
}

func (s *RServer) Deallocate(ctx context.Context, req *pb.UInt64ID) (*pb.Error, error) {
	var ferr error
	if s.fp != nil {
		ferr = s.fp.Deallocate(ctx, req.Id)
	} else {
		ferr = s.ma.Deallocate(ctx, req.Id)
	}

	return rnode.ToPbErr(ferr), nil
}

func (s *RServer) RegisterOwner(ctx context.Context, req *pb.Addr) (*pb.UInt64Reply, error) {
	var id uint64
	var ferr error
	if s.fp != nil {
		id, ferr = s.fp.RegisterOwner(ctx, req.Addr)
	} else {
		id, ferr = s.ma.RegisterOwner(ctx, req.Addr)
	}
	return &pb.UInt64Reply{Id: id, Err: rnode.ToPbErr(ferr)}, nil
}

func (s *RServer) RemoveOwner(ctx context.Context, req *pb.OwnerId) (*pb.Error, error) {
	var ferr error
	if s.fp != nil {
		ferr = s.fp.RemoveOwner(ctx, req.Id)
	} else {
		ferr = s.ma.RemoveOwner(ctx, req.Id)
	}

	return rnode.ToPbErr(ferr), nil
}

func (s *RServer) AllocateRoot(ctx context.Context, req *pb.OwnerId) (*pb.Error, error) {
	var err error
	if s.fp != nil {
		err = s.fp.AllocateRoot(ctx, req.Id)

	} else {
		err = s.ma.AllocateRoot(ctx, req.Id)
	}

	return rnode.ToPbErr(err), nil
}

func (s *RServer) RegisterFProxy(ctx context.Context, fp *fbackend.FProxy) {
	if fp == nil {
		logger.Pf(ctx, "nil backend error")
	}
	if s.fp != nil {
		logger.Pf(ctx, "duplicate backend error")
	}
	s.fp = fp
}

// NewRServer do NOT register backend
func NewRServer(ma *fbackend.RManager) *RServer {
	return &RServer{
		ma: ma,
	}
}

// Start blocks and starts the server
func (s *RServer) Start(ctx context.Context, port int) error {
	if s.Server != nil {
		return fmt.Errorf("server already created error")
	}
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		logger.Pf(ctx, "failed to listen: %v", err)
	}
	s.Server = grpc.NewServer(makeServerOptions(ctx)...)
	pb.RegisterRemoteTreeServer(s.Server, s)
	logger.If(ctx, "starting...localhost:%d", port)
	go func() {
		if err := s.Server.Serve(lis); err != nil {
			logger.Ef(ctx, "rserver serve error: err=%+v", err)
		}
	}()
	time.Sleep(rServerStartTime) // wait for the server to start
	return nil
}

func (s *RServer) Stop(ctx context.Context) error {
	if s.Server == nil {
		return fmt.Errorf("server is nil error")
	}
	s.Server.Stop()
	return nil
}
