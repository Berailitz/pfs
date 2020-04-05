package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"sync"

	"github.com/Berailitz/pfs/fproxy"

	"github.com/Berailitz/pfs/lfs"

	"google.golang.org/grpc"

	"github.com/Berailitz/pfs/rserver"

	"github.com/jacobsa/fuse"
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

func startTest(wg *sync.WaitGroup, testCmd string, testLog string) {
	log.Printf("run test cmd: cmd=%v, testLog=%v", testCmd, testLog)
	defer func() {
		log.Printf("test cmd finished success: cmd=%v, testLog=%v", testCmd, testLog)
		wg.Done()
	}()

	f, err := os.OpenFile(testLog, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("test open log error: testLog=%v", testLog)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("test close log error: testLog=%v, err=%+v", testLog, err)
		}
	}()
	mwriter := io.MultiWriter(os.Stdout, f)

	cmd := exec.Command("bash", "-c", testCmd)
	cmd.Stdout = mwriter
	cmd.Stderr = mwriter

	if err := cmd.Start(); err != nil {
		log.Fatalf("start test script error: err=%+v", err)
		return
	}
	log.Printf("test cmd start success: cmd=%v, testLog=%v", testCmd, testLog)

	if err := cmd.Wait(); err != nil {
		log.Printf("test wait cmd error: cmd=%v, testLog=%v, err=%+v", testCmd, testLog, err)
		return
	}
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
	flag.Parse()
	ctx := context.Background()

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

	cfg := fuse.MountConfig{}
	if *debug {
		cfg.DebugLogger = log.New(os.Stderr, "fuse: ", 0)
	}
	if cfg.OpContext == nil {
		cfg.OpContext = ctx
	}

	log.Printf("mount fs: dir=%v", *dir)
	isGracefulStop := false
	fsvr := lfs.NewLFSServer(lfs.NewLFS(currentUid(), currentGid(), fp))
	// Mount the file system.
	mfs, err := fuse.Mount(*dir, fsvr, &cfg)
	if err != nil {
		log.Fatalf("mount fs error: dir=%v, err=%+v", dir, err)
	}
	defer func(_isGracefulStop *bool) {
		if *_isGracefulStop {
			log.Printf("no need to unmount fs: dir=%v", *dir)
			return
		}
		log.Printf("unmount fs: dir=%v", *dir)
		if err := fuse.Unmount(*dir); err != nil {
			log.Fatalf("unmount fs error: dir=%v, err=%+v", dir, err)
		}
	}(&isGracefulStop)

	wg := sync.WaitGroup{}
	if *testCmd != "" {
		wg.Add(1)
		go startTest(&wg, *testCmd, *testLog)
	}

	wg.Add(1)
	go func() {
		log.Printf("join mfs")
		defer func() {
			log.Printf("mfs exited")
			wg.Done()
		}()
		if err := mfs.Join(ctx); err != nil {
			log.Fatalf("mfs join error: err=%+v", err)
		} else {
			isGracefulStop = true
		}
	}()

	wg.Wait()
}
