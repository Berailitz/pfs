package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "github.com/Berailitz/pfs/remotetree"

	"github.com/Berailitz/pfs/utility"
	"google.golang.org/grpc"
)

var gopts = []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	masterAddr := flag.String("masterAddr", "127.0.0.1:10000", "masterAddr")
	dst := flag.String("dst", "127.0.0.1:10000", "dst")
	src := flag.String("src", "127.0.0.1:10000", "src")
	flag.Parse()

	ctx := context.Background()
	gcli, err := utility.BuildGCli(*masterAddr, gopts)
	if err != nil {
		log.Fatalf("new rcli fial error: master=%v, opts=%+v, err=%+V",
			masterAddr, gopts, err)
		return
	}

	departure := time.Now().UnixNano()
	reply, err := gcli.Ping(ctx, &pb.PingRequest{
		Addr:      *dst,
		Departure: departure,
		Src:       *src,
	})
	if err != nil {
		log.Fatalf("fp ping error: addr=%v, err=%+v", *dst, err)
	}
	if err := utility.FromPbErr(reply.Err); err != nil {
		log.Fatalf("fp ping reply error: addr=%v, err=%+v", *dst, err)
	}
	tof := time.Now().UnixNano() - departure + reply.Offset
	fmt.Printf("tof=%v, reply=%+v\n", tof, reply)
}
