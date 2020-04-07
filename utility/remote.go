package utility

import (
	"fmt"
	"log"

	pb "github.com/Berailitz/pfs/remotetree"
	"google.golang.org/grpc"
)

type RemoteErr struct {
	msg string
}

var _ = (error)((*RemoteErr)(nil))

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

func (e *RemoteErr) Error() string {
	return fmt.Sprintf("remote err: %v", e.msg)
}
