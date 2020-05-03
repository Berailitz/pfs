package utility

import (
	"context"
	"fmt"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	grpc_logrus "github.com/Berailitz/pfs/logger/grpc"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"

	"github.com/Berailitz/pfs/logger"
	pb "github.com/Berailitz/pfs/remotetree"
	"google.golang.org/grpc"
)

const (
	rpcTimeout    = 3 * time.Second
	rpcMaxRetries = 1
)

var (
	gcliOptions = []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(
				RequestIDClientInterceptor(),
				grpc_logrus.UnaryClientInterceptor(
					logger.Entry(), grpc_logrus.WithMessageProducer(
						logger.StubMessageProducer)),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithPerRetryTimeout(rpcTimeout),
					grpc_retry.WithMax(rpcMaxRetries)),
			)),
	}
)

type RemoteErr struct {
	msg string
}

var _ = (error)((*RemoteErr)(nil))

func BuildGCli(ctx context.Context, addr string) (pb.RemoteTreeClient, error) {
	logger.If(ctx, "build gcli: addr=%v", addr)
	conn, err := grpc.Dial(addr, gcliOptions...)
	if err != nil {
		logger.If(ctx, "new rcli fial error: addr=%v, opts=%#v, err=%+V",
			addr, gcliOptions, err)
		return nil, err
	}
	return pb.NewRemoteTreeClient(conn), nil
}

func (e *RemoteErr) Error() string {
	return fmt.Sprintf("remote err: %v", e.msg)
}
