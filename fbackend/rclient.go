//go:generate  protoc -I ../pb/ ../pb/pb.proto --go_out=plugins=grpc:../pb

package fbackend

import (
	"context"
	"fmt"
	"log"

	"github.com/Berailitz/pfs/utility"
	"google.golang.org/grpc"

	pb "github.com/Berailitz/pfs/remotetree"
)

type RClient struct {
	id uint64
	ma *RManager

	cachedMasterAddr string
	cachedGCli       pb.RemoteTreeClient
	gopts            []grpc.DialOption
}

type RClientErr struct {
	msg string
}

func (c *RClient) buildGCli(ctx context.Context) (_ pb.RemoteTreeClient, err error) {
	masterAddr := c.ma.MasterAddr()
	if c.cachedMasterAddr != masterAddr {
		if c.cachedGCli, err = utility.BuildGCli(masterAddr, c.gopts); err != nil {
			return nil, err
		}
		c.cachedMasterAddr = masterAddr
	}
	return c.cachedGCli, nil
}

func (c *RClient) mustHaveID(ctx context.Context) {
	if c.id <= 0 {
		log.Fatalf("rcli has no id.")
	}
}

// QueryOwner fetch owner'addr of a node
func (c *RClient) QueryOwner(ctx context.Context, nodeID uint64) string {
	log.Printf("query owner: nodeID=%v", nodeID)
	gcli, err := c.buildGCli(ctx)
	if err != nil {
		return ""
	}

	addr, err := gcli.QueryOwner(ctx, &pb.UInt64ID{
		Id: nodeID,
	})
	if err != nil {
		log.Printf("query owner error: nodeID=%v, err=%+v", nodeID, err)
		return ""
	}
	log.Printf("query owner success: nodeID=%v, addr=%v", nodeID, addr.Addr)
	return addr.Addr
}

func (c *RClient) QueryAddr(ctx context.Context, ownerID uint64) string {
	log.Printf("query addr: ownerID=%v", ownerID)
	gcli, err := c.buildGCli(ctx)
	if err != nil {
		return ""
	}

	addr, err := gcli.QueryAddr(ctx, &pb.UInt64ID{
		Id: ownerID,
	})
	if err != nil {
		log.Printf("query addr error: ownerID=%v, err=%+v", ownerID, err)
		return ""
	}
	log.Printf("query addr success: ownerID=%v", ownerID)
	return addr.Addr
}

func (c *RClient) Allocate(ctx context.Context) uint64 {
	c.mustHaveID(ctx)
	log.Printf("allocate node")
	gcli, err := c.buildGCli(ctx)
	if err != nil {
		return 0
	}

	nodeID, err := gcli.Allocate(ctx, &pb.OwnerId{
		Id: c.id,
	})
	if err != nil {
		log.Printf("allocate error: ownerID=%v, err=%+v", c.id, err)
		return 0
	}
	log.Printf("allocate node success: nodeID=%v", nodeID.Id)
	return nodeID.Id
}

func (c *RClient) Deallocate(ctx context.Context, nodeID uint64) error {
	log.Printf("deallocate: nodeID=%v", nodeID)
	gcli, err := c.buildGCli(ctx)
	if err != nil {
		return err
	}

	out, err := gcli.Deallocate(ctx, &pb.UInt64ID{
		Id: nodeID,
	})
	if err != nil {
		log.Printf("deallocate error: nodeID=%v, err=%+v", nodeID, err)
		return err
	}
	if !out.Ok {
		err := &RClientErr{fmt.Sprintf("deallocate no node error: nodeID=%v, err=%+v", nodeID, err)}
		log.Println(err.Error())
		return err
	}
	log.Printf("deallocate success: nodeID=%v", nodeID)
	return nil
}

// RegisterOwner return 0 if err
func (c *RClient) RegisterOwner(ctx context.Context, addr string) uint64 {
	log.Printf("register owner: addr=%v", addr)
	gcli, err := c.buildGCli(ctx)
	if err != nil {
		return 0
	}

	out, err := gcli.RegisterOwner(ctx, &pb.Addr{
		Addr: addr,
	})
	if err != nil {
		log.Printf("register owner error: addr=%v, err=%+v", addr, err)
		return 0
	}
	log.Printf("register owner success: addr=%v", addr)
	return out.Id
}

func (c *RClient) RemoveOwner(ctx context.Context, ownerID uint64) bool {
	log.Printf("remove owner: ownerID=%v", ownerID)
	gcli, err := c.buildGCli(ctx)
	if err != nil {
		return false
	}

	out, err := gcli.RemoveOwner(ctx, &pb.OwnerId{
		Id: ownerID,
	})
	if err != nil {
		log.Printf("remove owner error: ownerID=%v, err=%+v", ownerID, err)
		return false
	}
	log.Printf("remove owner success: ownerID=%v", ownerID)
	return out.Ok
}

func (c *RClient) AllocateRoot(ctx context.Context) bool {
	gcli, err := c.buildGCli(ctx)
	if err != nil {
		return false
	}

	c.mustHaveID(ctx)
	log.Printf("allocate root: ownerID=%v", c.id)
	out, err := gcli.AllocateRoot(ctx, &pb.OwnerId{
		Id: c.id,
	})
	if err != nil {
		log.Printf("allocate root error: ownerID=%v, err=%+v", c.id, err)
		return false
	}
	log.Printf("allocate root finished: ownerID=%v, out=%+v", c.id, out)
	return out.Ok
}

// RegisterSelf is called only at initialization
func (c *RClient) RegisterSelf(ctx context.Context, addr string) uint64 {
	if c.id > 0 {
		log.Printf("duplicate register error: addr=%v", addr)
		return 0
	}

	localID := c.RegisterOwner(ctx, addr)
	if localID > 0 {
		c.id = localID
		log.Printf("register success: addr=%v, localID=%v", addr, localID)
		return localID
	}

	log.Printf("register error: addr=%v", addr)
	return 0
}

func (c *RClient) AssignID(ctx context.Context, id uint64) {
	log.Printf("rcli assigned id: id=%v", id)
	if c.id > 0 {
		log.Printf("rcli get re-assigned id: id=%v", id)
	}
	c.id = id
}

func (c *RClient) ID() uint64 {
	return c.id
}

func NewRClient(ma *RManager, gopts []grpc.DialOption) *RClient {
	return &RClient{
		ma:    ma,
		gopts: gopts,
	}
}

func (e *RClientErr) Error() string {
	return fmt.Sprintf("rcli error: %v", e.msg)
}
