package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strconv"
	"time"

	"github.com/Berailitz/pfs/lfs"

	"github.com/Berailitz/pfs/fbackend"

	"google.golang.org/grpc"

	"github.com/Berailitz/pfs/rserver"

	"github.com/jacobsa/fuse"
)

const (
	dir              = "x"
	rServerStartTime = time.Second * 3
)

func currentUid() uint32 {
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	uid, err := strconv.ParseUint(user.Uid, 10, 32)
	if err != nil {
		panic(err)
	}

	return uint32(uid)
}

func currentGid() uint32 {
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	gid, err := strconv.ParseUint(user.Gid, 10, 32)
	if err != nil {
		panic(err)
	}

	return uint32(gid)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	debug := flag.Bool("debug", false, "print debugging messages.")
	port := flag.Int("port", 10000, "The server port")
	host := flag.String("host", "127.0.0.1", "The server host")
	master := flag.String("master", "127.0.0.1:10000", "The master server addr")
	flag.Parse()
	ctx := context.Background()

	localAddr := fmt.Sprintf("%s:%d", *host, *port)
	gopts := []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}

	rsvr := rserver.NewRServer()
	go func() {
		rsvr.Start(*port)
	}()
	time.Sleep(rServerStartTime) // wait for the server to start

	fb := fbackend.NewFBackEnd(currentUid(), currentGid(),
		*master, localAddr, gopts)
	rsvr.RegisterFBackEnd(fb)

	cfg := fuse.MountConfig{}
	if *debug {
		cfg.DebugLogger = log.New(os.Stderr, "fuse: ", 0)
	}
	if cfg.OpContext == nil {
		cfg.OpContext = ctx
	}

	fsvr := lfs.NewLFSServer(lfs.NewLFS(currentUid(), currentGid(), fb))
	// Mount the file system.
	mfs, err := fuse.Mount(dir, fsvr, &cfg)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}
	mfs.Join(ctx)
	rsvr.Stop()
}
