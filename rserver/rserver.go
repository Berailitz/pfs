//go:generate  protoc -I ../remotetree/ ../remotetree/remotetree.proto --go_out=plugins=grpc:../remotetree

package rserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/Berailitz/pfs/fproxy"

	"github.com/Berailitz/pfs/utility"

	"github.com/jacobsa/fuse/fuseops"

	"github.com/Berailitz/pfs/manager"

	"github.com/Berailitz/pfs/fbackend"

	"google.golang.org/grpc"

	pb "github.com/Berailitz/pfs/remotetree"
)

const rServerStartTime = time.Second * 2

type RServer struct {
	pb.UnimplementedRemoteTreeServer
	Server *grpc.Server
	fp     *fproxy.FProxy
	ma     *manager.RManager
}

func (s *RServer) FetchNode(ctx context.Context, req *pb.UInt64ID) (*pb.Node, error) {
	node, err := s.fp.LoadNode(ctx, req.Id)
	if err != nil {
		return &pb.Node{
			NID: 0,
		}, nil
	}
	return utility.ToPbNode(node), nil
}

func (s *RServer) UnlockNode(ctx context.Context, req *pb.NodeIsReadRequest) (*pb.Error, error) {
	err := s.fp.UnlockNode(ctx, req.Id, req.IsRead)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) LookUpInode(ctx context.Context, req *pb.LookUpInodeRequest) (*pb.LookUpInodeReply, error) {
	id, attr, err := s.fp.LookUpInode(ctx, req.ParentID, req.Name)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.LookUpInodeReply{
		Id:   id,
		Attr: utility.ToPbAttr(attr),
		Err:  perr,
	}, nil
}

func (s *RServer) GetInodeAttributes(ctx context.Context, req *pb.UInt64ID) (*pb.GetInodeAttributesReply, error) {
	attr, err := s.fp.GetInodeAttributes(ctx, req.Id)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.GetInodeAttributesReply{
		Err:  perr,
		Attr: utility.ToPbAttr(attr),
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
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.SetInodeAttributesReply{
		Err:  perr,
		Attr: utility.ToPbAttr(attr),
	}, nil
}

func (s *RServer) MkDir(ctx context.Context, req *pb.MkDirRequest) (*pb.MkDirReply, error) {
	id, attr, err := s.fp.MkDir(ctx, req.Id, req.Name, os.FileMode(req.Mode))
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.MkDirReply{
		Id:   id,
		Err:  perr,
		Attr: utility.ToPbAttr(attr),
	}, nil
}

func (s *RServer) CreateNode(ctx context.Context, req *pb.CreateNodeRequest) (*pb.CreateNodeReply, error) {
	entry, err := s.fp.CreateNode(ctx, req.Id, req.Name, os.FileMode(req.Mode))
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.CreateNodeReply{
		Entry: utility.ToPbEntry(entry),
		Err:   perr,
	}, nil
}

func (s *RServer) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileReply, error) {
	entry, handle, err := s.fp.CreateFile(ctx, req.Id, req.Name, os.FileMode(req.Mode), req.Flags)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.CreateFileReply{
		Entry:  utility.ToPbEntry(entry),
		Handle: handle,
		Err:    perr,
	}, nil
}

func (s *RServer) CreateSymlink(ctx context.Context, req *pb.CreateSymlinkRequest) (*pb.CreateSymlinkReply, error) {
	id, attr, err := s.fp.CreateSymlink(ctx, req.Id, req.Name, req.Target)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.CreateSymlinkReply{
		Attr: utility.ToPbAttr(attr),
		Id:   id,
		Err:  perr,
	}, nil
}

func (s *RServer) CreateLink(ctx context.Context, req *pb.CreateLinkRequest) (*pb.CreateLinkReply, error) {
	attr, err := s.fp.CreateLink(ctx, req.Id, req.Name, req.TargetID)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.CreateLinkReply{
		Attr: utility.ToPbAttr(attr),
		Err:  perr,
	}, nil
}

func (s *RServer) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Error, error) {
	err := s.fp.Rename(ctx, fuseops.RenameOp{
		OldParent: fuseops.InodeID(req.OldParent),
		OldName:   req.OldName,
		NewParent: fuseops.InodeID(req.NewParent),
		NewName:   req.NewName,
	})
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) RmDir(ctx context.Context, req *pb.RmDirRequest) (*pb.Error, error) {
	err := s.fp.RmDir(ctx, fuseops.RmDirOp{
		Parent: fuseops.InodeID(req.Parent),
		Name:   req.Name,
	})
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Error, error) {
	err := s.fp.Unlink(ctx, fuseops.UnlinkOp{
		Parent: fuseops.InodeID(req.Parent),
		Name:   req.Name,
	})
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) OpenDir(ctx context.Context, req *pb.OpenXRequest) (*pb.Uint64Reply, error) {
	h, err := s.fp.OpenDir(ctx, req.Id, req.Flags)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.Uint64Reply{
		Err: perr,
		Num: h,
	}, nil
}
func (s *RServer) ReadDir(ctx context.Context, req *pb.ReadXRequest) (*pb.ReadXReply, error) {
	bytesRead, buf, err := s.fp.ReadDir(ctx, req.Id, req.Length, req.Offset)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.ReadXReply{
		Err:       perr,
		BytesRead: bytesRead,
		Buf:       buf,
	}, nil
}

func (s *RServer) ReleaseHandle(ctx context.Context, req *pb.UInt64ID) (*pb.Error, error) {
	err := s.fp.ReleaseHandle(ctx, req.Id)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) OpenFile(ctx context.Context, req *pb.OpenXRequest) (*pb.Uint64Reply, error) {
	h, err := s.fp.OpenFile(ctx, req.Id, req.Flags)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.Uint64Reply{
		Err: perr,
		Num: h,
	}, nil
}

func (s *RServer) ReadFile(ctx context.Context, req *pb.ReadXRequest) (*pb.ReadXReply, error) {
	bytesRead, buf, err := s.fp.ReadFile(ctx, req.Id, req.Length, req.Offset)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.ReadXReply{
		Err:       perr,
		BytesRead: bytesRead,
		Buf:       buf,
	}, nil
}

func (s *RServer) WriteFile(ctx context.Context, req *pb.WriteXRequest) (*pb.Uint64Reply, error) {
	bytesWrite, err := s.fp.WriteFile(ctx, req.Id, req.Offset, req.Data)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.Uint64Reply{
		Err: perr,
		Num: bytesWrite,
	}, nil
}

func (s *RServer) ReadSymlink(ctx context.Context, req *pb.UInt64ID) (*pb.ReadSymlinkReply, error) {
	target, err := s.fp.ReadSymlink(ctx, req.Id)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.ReadSymlinkReply{
		Err:    perr,
		Target: target,
	}, nil
}

func (s *RServer) GetXattr(ctx context.Context, req *pb.GetXattrRequest) (*pb.ReadXReply, error) {
	bytesRead, buf, err := s.fp.GetXattr(ctx, req.Id, req.Name, req.Length)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.ReadXReply{
		Err:       perr,
		BytesRead: bytesRead,
		Buf:       buf,
	}, nil
}

func (s *RServer) ListXattr(ctx context.Context, req *pb.ListXattrRequest) (*pb.ReadXReply, error) {
	bytesRead, buf, err := s.fp.ListXattr(ctx, req.Id, req.Length)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.ReadXReply{
		Err:       perr,
		BytesRead: bytesRead,
		Buf:       buf,
	}, nil
}

func (s *RServer) RemoveXattr(ctx context.Context, req *pb.RemoveXattrRequest) (*pb.Error, error) {
	err := s.fp.RemoveXattr(ctx, req.Id, req.Name)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) SetXattr(ctx context.Context, req *pb.SetXattrRequest) (*pb.Error, error) {
	err := s.fp.SetXattr(ctx, fuseops.SetXattrOp{
		Inode: fuseops.InodeID(req.Id),
		Name:  req.Name,
		Value: req.Value,
		Flags: req.Flag,
	})
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) Fallocate(ctx context.Context, req *pb.FallocateRequest) (*pb.Error, error) {
	err := s.fp.Fallocate(ctx, req.Id, req.Mode, req.Length)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) QueryOwner(ctx context.Context, req *pb.UInt64ID) (*pb.Addr, error) {
	return &pb.Addr{Addr: s.ma.QueryOwner(req.Id)}, nil
}

func (s *RServer) QueryAddr(ctx context.Context, req *pb.UInt64ID) (*pb.Addr, error) {
	return &pb.Addr{Addr: s.ma.QueryAddr(req.Id)}, nil
}

func (s *RServer) Allocate(ctx context.Context, req *pb.OwnerId) (*pb.UInt64ID, error) {
	return &pb.UInt64ID{Id: s.ma.Allocate(req.Id)}, nil
}

func (s *RServer) Deallocate(ctx context.Context, req *pb.UInt64ID) (*pb.IsOK, error) {
	return &pb.IsOK{Ok: s.ma.Deallocate(req.Id)}, nil
}

func (s *RServer) RegisterOwner(ctx context.Context, req *pb.Addr) (*pb.OwnerId, error) {
	return &pb.OwnerId{Id: s.ma.RegisterOwner(req.Addr)}, nil
}

func (s *RServer) RemoveOwner(ctx context.Context, req *pb.OwnerId) (*pb.IsOK, error) {
	return &pb.IsOK{Ok: s.ma.RemoveOwner(req.Id)}, nil
}

func (s *RServer) AllocateRoot(ctx context.Context, req *pb.OwnerId) (*pb.IsOK, error) {
	return &pb.IsOK{Ok: s.ma.AllocateRoot(req.Id)}, nil
}

func (s *RServer) RegisterFProxy(fp *fproxy.FProxy) {
	if fp == nil {
		log.Fatalf("nil backend error")
	}
	if s.fp != nil {
		log.Fatalf("duplicate backend error")
	}
	s.fp = fp
}

// NewRServer do NOT register backend
func NewRServer() *RServer {
	return &RServer{ma: manager.NewRManager()}
}

// Start blocks and starts the server
func (s *RServer) Start(port int) error {
	if s.Server != nil {
		return fmt.Errorf("server already created error")
	}
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	s.Server = grpc.NewServer(opts...)
	pb.RegisterRemoteTreeServer(s.Server, s)
	log.Printf("starting...localhost:%d", port)
	go func() {
		if err := s.Server.Serve(lis); err != nil {
			log.Printf("rserver serve error: err=%+v", err)
		}
	}()
	time.Sleep(rServerStartTime) // wait for the server to start
	return nil
}

func (s *RServer) Stop() error {
	if s.Server == nil {
		return fmt.Errorf("server is nil error")
	}
	s.Server.Stop()
	return nil
}
