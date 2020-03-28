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

// RegisterSelf is called only at initialization
func (c *RClient) RegisterSelf(addr string) bool {
	if c.ID > 0 {
		log.Printf("duplicate register error: addr=%v", addr)
		return false
	}

	localID := c.RegisterOwner(addr)
	if localID > 0 {
		c.ID = localID
		log.Printf("register success: addr=%v, localID=%v", addr, localID)
		return true
	}

	log.Printf("register error: addr=%v", addr)
	return false
}

func NewRClient(master, local string, opts []grpc.DialOption) *RClient {
	// TODO: add tls support
	log.Printf("new rcli: master=%v, local=%v, opts=%+v", master, local, opts)
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(master, opts...)
	if err != nil {
		log.Printf("new rcli fial error: master=%v, local=%v, opts=%+v, err=%+V", master, local, opts, err)
		return nil
	}
	rcli := &RClient{
		GClient: pb.NewRemoteTreeClient(conn),
	}
	if !rcli.RegisterSelf(local) {
		log.Printf("new rcli register self error: master=%v, local=%v", master, local)
		return nil
	}
	log.Printf("new rcli success: master=%v, local=%v", master, local)
	return rcli
}
