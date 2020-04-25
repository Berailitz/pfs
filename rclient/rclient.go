//go:generate  protoc -I ../pb/ ../pb/pb.proto --go_out=plugins=grpc:../pb

package rclient

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/Berailitz/pfs/remotetree"
)

type RClient struct {
	id   uint64
	gcli pb.RemoteTreeClient
}

type RClientErr struct {
	msg string
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
	addr, err := c.gcli.QueryOwner(ctx, &pb.UInt64ID{
		Id: nodeID,
	})
	if err != nil {
		log.Printf("query owner error: nodeID=%v, err=%+v", nodeID, err)
		return ""
	}
	log.Printf("query owner success: nodeID=%v, addr=%v", nodeID, addr.Addr)
	return addr.Addr
}

func (c *RClient) QueryAddr(ownerID uint64) string {
	log.Printf("query addr: ownerID=%v", ownerID)
	ctx := context.Background()
	addr, err := c.gcli.QueryAddr(ctx, &pb.UInt64ID{
		Id: ownerID,
	})
	if err != nil {
		log.Printf("query addr error: ownerID=%v, err=%+v", ownerID, err)
		return ""
	}
	log.Printf("query addr success: ownerID=%v", ownerID)
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

func (c *RClient) Deallocate(nodeID uint64) error {
	log.Printf("deallocate: nodeID=%v", nodeID)
	ctx := context.Background()
	out, err := c.gcli.Deallocate(ctx, &pb.UInt64ID{
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
	log.Printf("allocate root finished: ownerID=%v, out=%+v", c.id, out)
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

func (c *RClient) Ping(dst uint64) int64 {
	log.Printf("start ping: dst=%v", dst)
	ctx := context.Background()
	departure := time.Now().UnixNano()
	_, err := c.gcli.Ping(ctx, &pb.PingMsg{
		Src:       c.id,
		Dst:       dst,
		Departure: departure,
	})
	arrival := time.Now().UnixNano()
	if err != nil {
		log.Printf("ping error: src=%v, dst=%v, departure=%v, arrival=%v, err=%+v",
			c.id, dst, departure, arrival, err)
		return -1
	}
	tof := arrival - departure
	log.Printf("ping: src=%v, dst=%v, departure=%v, tof=%v",
		c.id, dst, departure, tof)
	return tof
}

func (c *RClient) GetID() uint64 {
	return c.id
}

func NewRClient(gcli pb.RemoteTreeClient) *RClient {
	return &RClient{
		gcli: gcli,
	}
}

func (e *RClientErr) Error() string {
	return fmt.Sprintf("rcli error: %v", e.msg)
}
