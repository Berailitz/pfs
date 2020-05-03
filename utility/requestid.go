package utility

import (
	"context"

	"github.com/Berailitz/pfs/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func RequestIDClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string, req, resp interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) (err error) {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.Pairs()
		}

		value := ctx.Value(logger.ContextRequestIDKey)
		if requestID, ok := value.(string); ok && requestID != "" {
			md[logger.ContextRequestIDKey.(string)] = []string{requestID}
		}
		return invoker(metadata.NewOutgoingContext(ctx, md), method, req, resp, cc, opts...)
	}
}

func RequestIDServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.Pairs()
		}
		// Set request ID for context.
		requestIDs := md[logger.ContextRequestIDKey.(string)]
		if len(requestIDs) >= 1 {
			ctx = context.WithValue(ctx, logger.ContextRequestIDKey, requestIDs[0])
			return handler(ctx, req)
		}

		// Generate request ID and set context if not exists.
		ctx = context.WithValue(ctx, logger.ContextRequestIDKey, logger.EmptyRequestID)
		return handler(ctx, req)
	}
}
