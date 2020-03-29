//go:generate  protoc -I ../remotetree/ ../remotetree/remotetree.proto --go_out=plugins=grpc:../remotetree

package rserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/Berailitz/pfs/manager"

	"github.com/Berailitz/pfs/fbackend"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc"

	pb "github.com/Berailitz/pfs/remotetree"
)

type RServer struct {
	pb.UnimplementedRemoteTreeServer
	Server *grpc.Server
	FB     *fbackend.FBackEnd
	ma     *manager.RManager
}

func (s *RServer) Create(ctx context.Context, req *pb.CreateRequest) (*pb.CreateReply, error) {
	s.FB.Lock()
	defer s.FB.Unlock()

	entry, err := s.FB.CreateNode(ctx, req.Parent, req.Name, os.FileMode(req.Dt))
	if err != nil {
		return &pb.CreateReply{
			Err: &pb.Error{
				Status: 1,
				Msg:    err.Error(),
			},
		}, nil
	}
	return &pb.CreateReply{
		Child: uint64(entry.Child),
		Err:   nil,
	}, nil
}

func (s *RServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Delete not implemented")
}
func (s *RServer) WriteFile(ctx context.Context, req *pb.WriteFileRequest) (*pb.WriteFileReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteFile not implemented")
}
func (s *RServer) WriteAttr(ctx context.Context, req *pb.WriteAttrRequest) (*pb.WriteAttrReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteAttr not implemented")
}
func (s *RServer) ReadDir(ctx context.Context, req *pb.NodeId) (*pb.ReadDirReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadDir not implemented")
}
func (s *RServer) ReadFile(ctx context.Context, req *pb.NodeId) (*pb.ReadFileReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadFile not implemented")
}
func (s *RServer) ReadAttr(ctx context.Context, req *pb.NodeId) (*pb.ReadAttrReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadAttr not implemented")
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
	if s.FB != nil {
		log.Fatalf("duplicate backend error")
	}
	s.FB = fb
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
