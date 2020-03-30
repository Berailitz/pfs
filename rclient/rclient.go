//go:generate  protoc -I ../pb/ ../pb/pb.proto --go_out=plugins=grpc:../pb

package rclient

import (
	"context"
	"log"

	pb "github.com/Berailitz/pfs/remotetree"
)

type RClient struct {
	id   uint64
	gcli pb.RemoteTreeClient
}

func (c *RClient) mustHaveID() {
	if c.id <= 0 {
		log.Fatalf("rcli has no id.")
	}
}

// QueryOwner fetch owner'addr of a node
func (c *RClient) QueryOwner(nodeID uint64) string {
	log.Printf("query owner: nodeID=%v", nodeID)
	ctx := context.Background()
	addr, err := c.gcli.QueryOwner(ctx, &pb.NodeId{
		Id: nodeID,
	})
	if err != nil {
		log.Printf("query owner error: nodeID=%v, err=%+v", nodeID, err)
		return ""
	}
	log.Printf("query owner success: nodeID=%v", nodeID)
	return addr.Addr
}

func (c *RClient) Allocate() uint64 {
	ctx := context.Background()
	c.mustHaveID()
	log.Printf("allocate node")
	nodeID, err := c.gcli.Allocate(ctx, &pb.OwnerId{
		Id: c.id,
	})
	if err != nil {
		log.Printf("allocate error: ownerID=%v, err=%+v", c.id, err)
		return 0
	}
	log.Printf("allocate node success: nodeID=%v", nodeID.Id)
	return nodeID.Id
}

func (c *RClient) Deallocate(nodeID uint64) bool {
	log.Printf("deallocate: nodeID=%v", nodeID)
	ctx := context.Background()
	out, err := c.gcli.Deallocate(ctx, &pb.NodeId{
		Id: nodeID,
	})
	if err != nil {
		log.Printf("deallocate error: nodeID=%v, err=%+v", nodeID, err)
		return false
	}
	log.Printf("deallocate success: nodeID=%v", nodeID)
	return out.Ok
}

// RegisterOwner return 0 if err
func (c *RClient) RegisterOwner(addr string) uint64 {
	log.Printf("register owner: addr=%v", addr)
	ctx := context.Background()
	out, err := c.gcli.RegisterOwner(ctx, &pb.Addr{
		Addr: addr,
	})
	if err != nil {
		log.Printf("register owner error: addr=%v, err=%+v", addr, err)
		return 0
	}
	log.Printf("register owner success: addr=%v", addr)
	return out.Id
}

func (c *RClient) RemoveOwner(ownerID uint64) bool {
	log.Printf("remove owner: ownerID=%v", ownerID)
	ctx := context.Background()
	out, err := c.gcli.RemoveOwner(ctx, &pb.OwnerId{
		Id: ownerID,
	})
	if err != nil {
		log.Printf("remove owner error: ownerID=%v, err=%+v", ownerID, err)
		return false
	}
	log.Printf("remove owner success: ownerID=%v", ownerID)
	return out.Ok
}

func (c *RClient) AllocateRoot() bool {
	ctx := context.Background()
	c.mustHaveID()
	log.Printf("allocate root: ownerID=%v", c.id)
	out, err := c.gcli.AllocateRoot(ctx, &pb.OwnerId{
		Id: c.id,
	})
	if err != nil {
		log.Printf("allocate root error: ownerID=%v, err=%+v", c.id, err)
		return false
	}
	log.Printf("allocate root success: ownerID=%v", c.id)
	return out.Ok
}

// RegisterSelf is called only at initialization
func (c *RClient) RegisterSelf(addr string) uint64 {
	if c.id > 0 {
		log.Printf("duplicate register error: addr=%v", addr)
		return 0
	}

	localID := c.RegisterOwner(addr)
	if localID > 0 {
		c.id = localID
		log.Printf("register success: addr=%v, localID=%v", addr, localID)
		return localID
	}

	log.Printf("register error: addr=%v", addr)
	return 0
}

func (c *RClient) AssignID(id uint64) {
	log.Printf("rcli assigned id: id=%v", id)
	if c.id > 0 {
		log.Printf("rcli get re-assigned id: id=%v", id)
	}
	c.id = id
}

func NewRClient(gcli pb.RemoteTreeClient) *RClient {
	return &RClient{
		gcli: gcli,
	}
}
