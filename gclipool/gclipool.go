package gclipool

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"

	pb "github.com/Berailitz/pfs/remotetree"
	"github.com/Berailitz/pfs/utility"
)

type GCliPool struct {
	cmap  sync.Map // [addr]remotetree.RemoteTreeClient
	gopts []grpc.DialOption
	local string
}

type GCliPoolErr struct {
	msg string
}

var _ = (error)((*GCliPoolErr)(nil))

func (p *GCliPool) Load(addr string) (pb.RemoteTreeClient, error) {
	if addr == "" {
		err := &GCliPoolErr{fmt.Sprintf("gclipool load cli empty addr error: addr=%v", addr)}
		return nil, err
	}

	if addr == p.local {
		err := &GCliPoolErr{fmt.Sprintf("gclipool load local cli error: addr=%v", addr)}
		return nil, err
	}

	if out, ok := p.cmap.Load(addr); ok {
		if gcli, ok := out.(pb.RemoteTreeClient); ok {
			return gcli, nil
		}
	}

	gcli, err := utility.BuildGCli(addr, p.gopts)
	if err != nil {
		err := &GCliPoolErr{fmt.Sprintf("build gcli fial error: addr=%v, opts=%#v, err=%+v",
			addr, p.gopts, err)}
		return nil, err
	}
	p.cmap.Store(addr, gcli)
	return gcli, nil
}

func NewGCliPool(gopts []grpc.DialOption, local string) *GCliPool {
	return &GCliPool{
		gopts: gopts,
		local: local,
	}
}

func (e *GCliPoolErr) Error() string {
	return fmt.Sprintf("gclipool error: %v", e.msg)
}
