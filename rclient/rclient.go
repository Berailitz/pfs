//go:generate  protoc -I ../remotetree/ ../remotetree/remotetree.proto --go_out=plugins=grpc:../remotetree

package rclient

import (
	"context"
	"log"

	"github.com/Berailitz/pfs/manager"
	"github.com/Berailitz/pfs/remotetree"
	pb "github.com/Berailitz/pfs/remotetree"
	"google.golang.org/grpc"
)

type Client interface {
	manager.Manager
}

type RClient struct {
	ID      uint64
	Remote  string
	Local   string
	GClient remotetree.RemoteTreeClient
}

var _ = (Client)((*RClient)(nil))

func (c *RClient) QueryOwner(nodeID uint64) string {
	ctx := context.Background()
	addr, err := c.GClient.QueryOwner(ctx, &remotetree.NodeId{
		Id: nodeID,
	})
	if err != nil {
		log.Printf("query owner error: nodeID=%v, err=%+v", nodeID, err)
		return ""
	}
	return addr.Addr
}

func (c *RClient) Allocate(ownerID uint64) uint64 {
	ctx := context.Background()
	nodeID, err := c.GClient.Allocate(ctx, &remotetree.OwnerId{
		Id: ownerID,
	})
	if err != nil {
		log.Printf("allocate error: ownerID=%v, err=%+v", ownerID, err)
		return 0
	}
	return nodeID.Id
}

func (c *RClient) Deallocate(nodeID uint64) bool {
	ctx := context.Background()
	out, err := c.GClient.Deallocate(ctx, &remotetree.NodeId{
		Id: nodeID,
	})
	if err != nil {
		log.Printf("deallocate error: nodeID=%v, err=%+v", nodeID, err)
		return false
	}
	return out.Ok
}

// RegisterOwner return 0 if err
func (c *RClient) RegisterOwner(addr string) uint64 {
	ctx := context.Background()
	out, err := c.GClient.RegisterOwner(ctx, &remotetree.Addr{
		Addr: addr,
	})
	if err != nil {
		log.Printf("register owner error: addr=%v, err=%+v", addr, err)
		return 0
	}
	return out.Id
}

func (c *RClient) RemoveOwner(ownerID uint64) bool {
	ctx := context.Background()
	out, err := c.GClient.RemoveOwner(ctx, &remotetree.OwnerId{
		Id: ownerID,
	})
	if err != nil {
		log.Printf("remove owner error: ownerID=%v, err=%+v", ownerID, err)
		return false
	}
	return out.Ok
}

func NewRClient(id uint64, remote, local string, opts []grpc.DialOption) *RClient {
	// TODO: add tls support
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(remote, opts...)
	if err != nil {
		return nil
	}
	return &RClient{
		ID:      id,
		Remote:  remote,
		Local:   local,
		GClient: pb.NewRemoteTreeClient(conn),
	}
}
