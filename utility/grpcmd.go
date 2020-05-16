package utility

import (
	"context"

	"github.com/Berailitz/pfs/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func CtxMDClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string, req, resp interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) (err error) {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.Pairs()
		}

		for _, contextKey := range logger.ContextLogKeys {
			if strValue, ok := ctx.Value(contextKey).(string); ok && strValue != "" {
				md[contextKey.(string)] = []string{strValue}
			}
		}

		return invoker(metadata.NewOutgoingContext(ctx, md), method, req, resp, cc, opts...)
	}
}

func CtxMDServerInterceptor(serverCtx context.Context) grpc.UnaryServerInterceptor {
	return func(
		requestCtx context.Context,
		req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(requestCtx)
		if !ok {
			md = metadata.Pairs()
		}

		for _, contextKey := range logger.ContextLogKeys {
			if clientValues := md[contextKey.(string)]; len(clientValues) >= 1 {
				requestCtx = context.WithValue(requestCtx, contextKey, clientValues[0])
			}
			if serverValue := serverCtx.Value(contextKey); serverValue != nil {
				requestCtx = context.WithValue(requestCtx, contextKey, serverValue)
			}
		}

		return handler(requestCtx, req)
	}
}
