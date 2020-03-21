//go:generate  protoc -I ../remotetree/ ../remotetree/remotetree.proto --go_out=plugins=grpc:../remotetree

package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/Berailitz/pfs/tree"

	"google.golang.org/grpc"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"

	pb "github.com/Berailitz/pfs/remotetree"
)

var (
	tls      = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile = flag.String("cert_file", "", "The TLS cert file")
	keyFile  = flag.String("key_file", "", "The TLS key file")
	port     = flag.Int("port", 10000, "The server port")
)

type byteStreamServer struct {
	pb.UnimplementedRemoteTreeServer
	tree tree.Treer
}

// GetFeature returns the feature at the given point.
func (s *byteStreamServer) Read(req *pb.ReadRequest, srv pb.RemoteTree_ReadServer) error {
	path := pb.Path(*req.Path)
	log.Printf("read %v", path)
	b := make([]byte, req.Length)
	node, err := s.tree.Lookup(req.Path.Names)
	if err != nil {
		log.Printf("failed read: %v", err)
		return err
	}
	fileNode, ok := node.(tree.FileNoder)
	if !ok {
		log.Printf("failed read: %v", err)
		return tree.ErrPathInvalid{}
	}
	n, err := fileNode.ReadAt(b, req.Start)
	if err != nil {
		log.Printf("failed read: %v", err)
		return err
	}
	log.Printf("read %v bytes", n)
	chunk := pb.Chunk(pb.Chunk{
		Start: 0,
		Data:  b,
	})
	return srv.Send(&chunk)
}

func newServer() *byteStreamServer {
	s := &byteStreamServer{
		tree: tree.NewTestTree(),
	}
	return s
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if *tls {
		if *certFile == "" {
			*certFile = testdata.Path("server1.pem")
		}
		if *keyFile == "" {
			*keyFile = testdata.Path("server1.key")
		}
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterRemoteTreeServer(grpcServer, newServer())
	log.Printf("starting...localhost:%d", *port)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
