package gclipool

import (
	"log"
	"sync"

	"google.golang.org/grpc"

	pb "github.com/Berailitz/pfs/remotetree"
	"github.com/Berailitz/pfs/utility"
)

type GCliPool struct {
	cmap  sync.Map // [addr]remotetree.RemoteTreeClient
	gopts []grpc.DialOption
}

func (p *GCliPool) Load(addr string) pb.RemoteTreeClient {
	if out, ok := p.cmap.Load(addr); ok {
		if gcli, ok := out.(pb.RemoteTreeClient); ok {
			return gcli
		}
	}
	gcli, err := utility.BuildGCli(addr, p.gopts)
	if err != nil {
		log.Fatalf("build gcli fial error: addr=%v, opts=%#v, err=%+v",
			addr, p.gopts, err)
		return nil
	}
	p.cmap.Store(addr, gcli)
	return gcli
}

func NewGCliPool(gopts []grpc.DialOption) *GCliPool {
	return &GCliPool{
		gopts: gopts,
	}
}
