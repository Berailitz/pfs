package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strconv"

	"github.com/Berailitz/pfs/fproxy"

	"github.com/Berailitz/pfs/lfs"

	"google.golang.org/grpc"

	"github.com/Berailitz/pfs/rserver"

	"github.com/jacobsa/fuse"
)

const (
	dir = "x"
)

var (
	gitCommit string
	buildTime string
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
	log.Printf("buildTime=%v, gitCommit=%v\n", buildTime, gitCommit)
	debug := flag.Bool("debug", false, "print debugging messages.")
	port := flag.Int("port", 10000, "The server port")
	host := flag.String("host", "127.0.0.1", "The server host")
	master := flag.String("master", "127.0.0.1:10000", "The master server addr")
	flag.Parse()
	ctx := context.Background()

	localAddr := fmt.Sprintf("%s:%d", *host, *port)
	gopts := []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}

	log.Printf("start rs")
	rsvr := rserver.NewRServer()
	if err := rsvr.Start(*port); err != nil {
		log.Fatalf("start rs error: err=%+v", err)
	}
	defer func() {
		log.Printf("stop rs")
		if err := rsvr.Stop(); err != nil {
			log.Printf("stop rs error: err=%+v", err)
		}
	}()

	fp := fproxy.NewFProxy(currentUid(), currentGid(), *master, localAddr, gopts)
	rsvr.RegisterFProxy(fp)

	cfg := fuse.MountConfig{}
	if *debug {
		cfg.DebugLogger = log.New(os.Stderr, "fuse: ", 0)
	}
	if cfg.OpContext == nil {
		cfg.OpContext = ctx
	}

	log.Printf("mount fs")
	isGracefulStop := false
	fsvr := lfs.NewLFSServer(lfs.NewLFS(currentUid(), currentGid(), fp))
	// Mount the file system.
	mfs, err := fuse.Mount(dir, fsvr, &cfg)
	if err != nil {
		log.Fatalf("mount fs error: dir=%v, err=%+v", dir, err)
	}
	defer func(_isGracefulStop *bool) {
		if *_isGracefulStop {
			log.Printf("no need to unmount fs")
			return
		}
		log.Printf("unmount fs")
		if err := fuse.Unmount(dir); err != nil {
			log.Fatalf("unmount fs error: dir=%v, err=%+v", dir, err)
		}
	}(&isGracefulStop)
	if err := mfs.Join(ctx); err != nil {
		log.Fatalf("mfs join error: err=%+v", err)
	} else {
		isGracefulStop = true
	}
}
