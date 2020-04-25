package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/Berailitz/pfs/rclient"
	"github.com/Berailitz/pfs/utility"
	"google.golang.org/grpc"
)

var gopts = []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	masterAddr := flag.String("masterAddr", "127.0.0.1:10000", "masterAddr")
	dst := flag.String("dst", "127.0.0.1:10000", "dst")
	flag.Parse()

	gcli, err := utility.BuildGCli(*masterAddr, gopts)
	if err != nil {
		log.Fatalf("new rcli fial error: master=%v, opts=%+v, err=%+V",
			masterAddr, gopts, err)
		return
	}

	rc := rclient.NewRClient(gcli)
	departure := time.Now().UnixNano()
	offset, err := rc.Ping(*dst)
	tof := time.Now().UnixNano() - departure + offset
	fmt.Printf("tof=%v, offset=%v\n", tof, offset)
}
