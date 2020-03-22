package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/user"
	"strconv"

	"github.com/Berailitz/pfs/fs"

	"github.com/jacobsa/fuse"
)

const dir = "x"

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
	debug := flag.Bool("debug", false, "print debugging messages.")
	flag.Parse()
	ctx := context.Background()
	cfg := fuse.MountConfig{}
	if *debug {
		cfg.DebugLogger = log.New(os.Stderr, "fuse: ", 0)
	}
	server := fs.NewMemFS(currentUid(), currentGid())
	if cfg.OpContext == nil {
		cfg.OpContext = ctx
	}

	// Set up a temporary directory.
	var err error

	// Mount the file system.
	mfs, err := fuse.Mount(dir, server, &cfg)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}
	mfs.Join(ctx)
}
