package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/Berailitz/pfs/rclient"
	"github.com/Berailitz/pfs/utility"
	"google.golang.org/grpc"
)

var gopts = []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	masterAddr := flag.String("masterAddr", "127.0.0.1:10000", "masterAddr")
	dst := flag.Uint64("dst", 1, "masterAddr")
	flag.Parse()

	gcli, err := utility.BuildGCli(*masterAddr, gopts)
	if err != nil {
		log.Fatalf("new rcli fial error: master=%v, opts=%+v, err=%+V",
			masterAddr, gopts, err)
		return
	}

	rc := rclient.NewRClient(gcli)
	tof := rc.Ping(*dst)
	fmt.Printf("tof=%v\n", tof)
}
