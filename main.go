package main

import (
	"flag"
	"fmt"
	"log"
	"os/user"
	"strconv"
	"sync"

	"github.com/Berailitz/pfs/utility"

	"github.com/Berailitz/pfs/fproxy"

	"github.com/Berailitz/pfs/lfs"

	"google.golang.org/grpc"

	"github.com/Berailitz/pfs/rserver"
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
	dir := flag.String("dir", "", "Dir to mount.")
	testCmd := flag.String("testCmd", "", "Script to test.")
	testLog := flag.String("testLog", "testlog.txt", "Log file to write test output.")
	fsName := flag.String("fsName", "pfs", "Name of the filesystem.")
	fsType := flag.String("fsType", "pfs", "Type of the filesystem.")
	volumeName := flag.String("volumeName", "PVolume", "Name of the volume to mount.")
	flag.Parse()

	if *dir == "" {
		log.Fatalf("no dir specified, exit")
	}

	log.Printf("debug=%v", *debug)

	localAddr := fmt.Sprintf("%s:%d", *host, *port)
	gopts := []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}

	log.Printf("start rs: port=%v", *port)
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

	log.Printf("create fp: master=%v, localAddr=%v, gopts=%+v", *master, localAddr, gopts)
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
