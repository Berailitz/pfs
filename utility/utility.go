package utility

import (
	"log"

	pb "github.com/Berailitz/pfs/remotetree"
	"google.golang.org/grpc"
)

func BuildGCli(addr string, gopts []grpc.DialOption) (pb.RemoteTreeClient, error) {
	log.Printf("build gcli: addr=%v", addr)
	conn, err := grpc.Dial(addr, gopts...)
	if err != nil {
		log.Printf("new rcli fial error: addr=%v, opts=%#v, err=%+V",
			addr, gopts, err)
		return nil, err
	}
	return pb.NewRemoteTreeClient(conn), nil
}
