package fbackend

import (
	"context"
	"fmt"
	"sync"

	"github.com/Berailitz/pfs/logger"

	pb "github.com/Berailitz/pfs/remotetree"
	"github.com/Berailitz/pfs/utility"
)

type GCliPool struct {
	cmap  sync.Map // [addr]remotetree.RemoteTreeClient
	ma    *RManager
	local string
}

type GCliPoolErr struct {
	msg string
}

var _ = (error)((*GCliPoolErr)(nil))

func (p *GCliPool) Load(ctx context.Context, addr string) (_ pb.RemoteTreeClient, err error) {
	hopAddr, err := p.ma.Route(addr)
	if err != nil {
		logger.E(ctx, "load gcli error", "addr", addr, "err", err)
		return nil, err
	}
	return p.LoadWithoutRoute(ctx, hopAddr.next)
}

func (p *GCliPool) LoadWithoutRoute(ctx context.Context, addr string) (pb.RemoteTreeClient, error) {
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

	gcli, err := utility.BuildGCli(ctx, addr)
	if err != nil {
		err := &GCliPoolErr{fmt.Sprintf("build gcli fial error: addr=%v, opts=%#v, err=%+v",
			addr, err)}
		return nil, err
	}
	p.cmap.Store(addr, gcli)
	return gcli, nil
}

func NewGCliPool(local string, ma *RManager) *GCliPool {
	return &GCliPool{
		local: local,
		ma:    ma,
	}
}

func (e *GCliPoolErr) Error() string {
	return fmt.Sprintf("gclipool error: %v", e.msg)
}
