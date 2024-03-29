package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/Berailitz/pfs/logger"
	pb "github.com/Berailitz/pfs/remotetree"
	"github.com/Berailitz/pfs/rnode"

	"github.com/Berailitz/pfs/utility"
)

func main() {
	masterAddr := flag.String("masterAddr", "127.0.0.1:10000", "masterAddr")
	dst := flag.String("dst", "127.0.0.1:10000", "dst")
	src := flag.String("src", "127.0.0.1:10010", "src")
	flag.Parse()

	ctx := context.Background()
	gcli, err := utility.BuildGCli(ctx, *masterAddr)
	if err != nil {
		logger.Pf(ctx, "new rcli fial error: master=%v, err=%+V",
			masterAddr, err)
		return
	}

	departure := time.Now().UnixNano()
	reply, err := gcli.Ping(ctx, &pb.PingRequest{
		Addr:      *dst,
		Departure: departure,
		Src:       *src,
	})
	if err != nil {
		logger.Pf(ctx, "fp ping error: addr=%v, err=%+v", *dst, err)
	}
	if err := rnode.FromPbErr(reply.Err); err != nil {
		logger.Pf(ctx, "fp ping reply error: addr=%v, err=%+v", *dst, err)
	}
	tof := time.Now().UnixNano() - departure + reply.Offset
	fmt.Printf("tof=%v, reply=%+v\n", tof, reply)
}
