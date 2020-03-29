//go:generate  protoc -I ../remotetree/ ../remotetree/remotetree.proto --go_out=plugins=grpc:../remotetree

package rserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/Berailitz/pfs/utility"

	"github.com/jacobsa/fuse/fuseops"

	"github.com/Berailitz/pfs/manager"

	"github.com/Berailitz/pfs/fbackend"

	"google.golang.org/grpc"

	pb "github.com/Berailitz/pfs/remotetree"
)

type RServer struct {
	pb.UnimplementedRemoteTreeServer
	Server *grpc.Server
	fb     *fbackend.FBackEnd
	ma     *manager.RManager
}

func (s *RServer) LookUpInode(ctx context.Context, req *pb.LookUpInodeRequest) (*pb.LookUpInodeReply, error) {
	id, attr, err := s.fb.LookUpInode(ctx, req.ParentID, req.Name)
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

func (s *RServer) GetInodeAttributes(ctx context.Context, req *pb.NodeId) (*pb.GetInodeAttributesReply, error) {
	attr, err := s.fb.GetInodeAttributes(ctx, req.Id)
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
	attr, err := s.fb.SetInodeAttributes(ctx, req.Id, fbackend.SetInodeAttributesParam{
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
	id, attr, err := s.fb.MkDir(ctx, req.Id, req.Name, os.FileMode(req.Mode))
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
	entry, err := s.fb.CreateNode(ctx, req.Id, req.Name, os.FileMode(req.Mode))
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

func (s *RServer) CreateSymlink(ctx context.Context, req *pb.CreateSymlinkRequest) (*pb.CreateSymlinkReply, error) {
	id, attr, err := s.fb.CreateSymlink(ctx, req.Id, req.Name, req.Target)
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
	attr, err := s.fb.CreateLink(ctx, req.Id, req.Name, req.TargetID)
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
	err := s.fb.Rename(ctx, fuseops.RenameOp{
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
	err := s.fb.RmDir(ctx, fuseops.RmDirOp{
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
	err := s.fb.Unlink(ctx, fuseops.UnlinkOp{
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

func (s *RServer) OpenDir(ctx context.Context, req *pb.NodeId) (*pb.Error, error) {
	err := s.fb.OpenDir(ctx, req.Id)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}
func (s *RServer) ReadDir(ctx context.Context, req *pb.ReadXRequest) (*pb.ReadXReply, error) {
	bytesRead, buf, err := s.fb.ReadDir(ctx, req.Id, req.Length, req.Offset)
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

func (s *RServer) OpenFile(ctx context.Context, req *pb.NodeId) (*pb.Error, error) {
	err := s.fb.OpenFile(ctx, req.Id)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) ReadFile(ctx context.Context, req *pb.ReadXRequest) (*pb.ReadXReply, error) {
	bytesRead, buf, err := s.fb.ReadFile(ctx, req.Id, req.Length, req.Offset)
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

func (s *RServer) WriteFile(ctx context.Context, req *pb.WriteXRequest) (*pb.WriteXReply, error) {
	bytesWrite, err := s.fb.WriteFile(ctx, req.Id, req.Offset, req.Data)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return &pb.WriteXReply{
		Err:        perr,
		BytesWrite: bytesWrite,
	}, nil
}

func (s *RServer) ReadSymlink(ctx context.Context, req *pb.NodeId) (*pb.ReadSymlinkReply, error) {
	target, err := s.fb.ReadSymlink(ctx, req.Id)
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
	bytesRead, buf, err := s.fb.GetXattr(ctx, req.Id, req.Name, req.Length)
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
	bytesRead, buf, err := s.fb.ListXattr(ctx, req.Id, req.Length)
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
	err := s.fb.RemoveXattr(ctx, req.Id, req.Name)
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
	err := s.fb.SetXattr(ctx, fuseops.SetXattrOp{
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
	err := s.fb.Fallocate(ctx, req.Id, req.Mode, req.Length)
	var perr *pb.Error = nil
	if err != nil {
		perr = &pb.Error{
			Status: 1,
			Msg:    err.Error(),
		}
	}
	return perr, nil
}

func (s *RServer) QueryOwner(ctx context.Context, req *pb.NodeId) (*pb.Addr, error) {
	return &pb.Addr{Addr: s.ma.QueryOwner(req.Id)}, nil
}

func (s *RServer) Allocate(ctx context.Context, req *pb.OwnerId) (*pb.NodeId, error) {
	return &pb.NodeId{Id: s.ma.Allocate(req.Id)}, nil
}

func (s *RServer) Deallocate(ctx context.Context, req *pb.NodeId) (*pb.IsOK, error) {
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

func (s *RServer) RegisterFBackEnd(fb *fbackend.FBackEnd) {
	if fb == nil {
		log.Fatalf("nil backend error")
	}
	if s.fb != nil {
		log.Fatalf("duplicate backend error")
	}
	s.fb = fb
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
	err = s.Server.Serve(lis)
	if err != nil {
		return err
	}
	return nil
}

func (s *RServer) Stop() error {
	if s.Server == nil {
		return fmt.Errorf("server is nil error")
	}
	s.Server.Stop()
	return nil
}
