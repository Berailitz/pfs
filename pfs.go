package pfs

import (
	"fmt"
	"log"
	"sync"

	"github.com/Berailitz/pfs/utility"

	"github.com/Berailitz/pfs/fproxy"

	"github.com/Berailitz/pfs/lfs"

	"google.golang.org/grpc"

	"github.com/Berailitz/pfs/rserver"
)

type PFSParam struct {
	debug      bool
	port       int
	host       string
	master     string
	dir        string
	fsName     string
	fsType     string
	volumeName string
}

type TestParam struct {
	testCmd string
	testLog string
}

func StartPFS(param PFSParam) {
	if param.dir == "" {
		log.Fatalf("no dir specified, exit")
	}

	log.Printf("debug=%v", param.debug)

	localAddr := fmt.Sprintf("%s:%d", param.host, param.port)
	gopts := []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}

	log.Printf("start rs: port=%v", param.port)
	rsvr := rserver.NewRServer()
	if err := rsvr.Start(param.port); err != nil {
		log.Fatalf("start rs error: err=%+v", err)
	}
	defer func() {
		log.Printf("stop rs")
		if err := rsvr.Stop(); err != nil {
			log.Printf("stop rs error: err=%+v", err)
		}
	}()

	log.Printf("create fp: master=%v, localAddr=%v, gopts=%+v", param.master, localAddr, gopts)
	fp := fproxy.NewFProxy(currentUid(), currentGid(), *master, localAddr, gopts)
	rsvr.RegisterFProxy(fp)

	lfsvr := lfs.NewLFS(fp)
	log.Printf("mount fs: dir=%v, fsName=%v, fsType=%v, volumeName=%v",
		*dir, *fsName, *fsType, *volumeName)
	if err := lfsvr.Mount(*dir, *fsName, *fsType, *volumeName); err != nil {
		log.Fatalf("lfs mount error: dir=%v, fsName=%v, fsType=%v, volumeName=%v, err=%+v",
			*dir, *fsName, *fsType, *volumeName, err)
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("serve fs")
		isGracefulStop := false
		if err := lfsvr.Serve(); err != nil {
			log.Fatalf("lfs serve error: err=%+v", err)
		} else {
			isGracefulStop = true
		}

		defer func(_isGracefulStop *bool) {
			if *_isGracefulStop {
				log.Printf("lfs graceful stopped, no need to umount")
				return
			}

			log.Printf("lfs serve error, unmount it")
			if err := lfsvr.Umount(); err != nil {
				log.Printf("lfs unmount fs error: err=%+v", err)
			}
		}(&isGracefulStop)
	}()

	if *testCmd != "" {
		wg.Add(1)
		go utility.StartCMD(&wg, *testCmd, *testLog)
	}

	wg.Wait()
}
