package utility

import (
	"context"
	"fmt"

	"github.com/Berailitz/pfs/logger"
	pb "github.com/Berailitz/pfs/remotetree"
	"google.golang.org/grpc"
)

type RemoteErr struct {
	msg string
}

var _ = (error)((*RemoteErr)(nil))

func BuildGCli(ctx context.Context, addr string, gopts []grpc.DialOption) (pb.RemoteTreeClient, error) {
	logger.If(ctx, "build gcli: addr=%v", addr)
	conn, err := grpc.Dial(addr, gopts...)
	if err != nil {
		logger.If(ctx, "new rcli fial error: addr=%v, opts=%#v, err=%+V",
			addr, gopts, err)
		return nil, err
	}
	return pb.NewRemoteTreeClient(conn), nil
}

func (e *RemoteErr) Error() string {
	return fmt.Sprintf("remote err: %v", e.msg)
}
