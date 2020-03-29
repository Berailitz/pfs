//go:generate  protoc -I ../remotetree/ ../remotetree/remotetree.proto --go_out=plugins=grpc:../remotetree

package rclient

import (
	"context"
	"log"

	"github.com/Berailitz/pfs/remotetree"
	pb "github.com/Berailitz/pfs/remotetree"
	"google.golang.org/grpc"
)

type RClient struct {
	id   uint64
	gcli remotetree.RemoteTreeClient
}

type RCliCfg struct {
	Master string
	Local  string
	GOpts  []grpc.DialOption
}

// QueryOwner fetch owner'addr of a node
func (c *RClient) QueryOwner(nodeID uint64) string {
	ctx := context.Background()
	addr, err := c.gcli.QueryOwner(ctx, &remotetree.NodeId{
		Id: uint64(nodeID),
	})
	if err != nil {
		log.Printf("query owner error: nodeID=%v, err=%+v", nodeID, err)
		return ""
	}
	return addr.Addr
}

func (c *RClient) Allocate() uint64 {
	ctx := context.Background()
	log.Printf("allocate node")
	nodeID, err := c.gcli.Allocate(ctx, &remotetree.OwnerId{
		Id: c.id,
	})
	if err != nil {
		log.Printf("allocate error: ownerID=%v, err=%+v", c.id, err)
		return 0
	}
	log.Printf("allocate node success: nodeID=%v", nodeID.Id)
	return uint64(nodeID.Id)
}

func (c *RClient) Deallocate(nodeID uint64) bool {
	ctx := context.Background()
	out, err := c.gcli.Deallocate(ctx, &remotetree.NodeId{
		Id: uint64(nodeID),
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
	out, err := c.gcli.RegisterOwner(ctx, &remotetree.Addr{
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
	out, err := c.gcli.RemoveOwner(ctx, &remotetree.OwnerId{
		Id: ownerID,
	})
	if err != nil {
		log.Printf("remove owner error: ownerID=%v, err=%+v", ownerID, err)
		return false
	}
	return out.Ok
}

func (c *RClient) AllocateRoot() bool {
	ctx := context.Background()
	log.Printf("allocate root: ownerID=%v", c.id)
	out, err := c.gcli.AllocateRoot(ctx, &remotetree.OwnerId{
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

func NewRClient(cfg RCliCfg) *RClient {
	// TODO: add tls support
	log.Printf("new rcli: master=%v, local=%v, opts=%#v", cfg.Master, cfg.Local, cfg.GOpts)
	cfg.GOpts = append(cfg.GOpts, grpc.WithInsecure())
	conn, err := grpc.Dial(cfg.Master, cfg.GOpts...)
	if err != nil {
		log.Fatalf("new rcli fial error: master=%v, local=%v, opts=%+v, err=%+V",
			cfg.Master, cfg.Local, cfg.GOpts, err)
		return nil
	}
	rcli := &RClient{
		gcli: pb.NewRemoteTreeClient(conn),
	}
	log.Printf("new rcli success: master=%v, local=%v", cfg.Master, cfg.Local)
	return rcli
}
