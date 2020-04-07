package pfs

import (
	"fmt"
	"log"

	"github.com/Berailitz/pfs/fproxy"
	"github.com/Berailitz/pfs/lfs"
	"github.com/Berailitz/pfs/utility"

	"google.golang.org/grpc"

	"github.com/Berailitz/pfs/rserver"
)

var gopts = []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}

type PFSParam struct {
	Debug      bool   `yaml:"debug,omitempty"`
	Port       int    `yaml:"port,omitempty"`
	Host       string `yaml:"host,omitempty"`
	Master     string `yaml:"master,omitempty"`
	Dir        string `yaml:"dir,omitempty"`
	FsName     string `yaml:"fsName,omitempty"`
	FsType     string `yaml:"fsType,omitempty"`
	VolumeName string `yaml:"volumeName,omitempty"`
}

type PFS struct {
	param PFSParam
	rsvr  *rserver.RServer
	lfsvr *lfs.LFS
}

func NewPFS(param PFSParam) *PFS {
	if param.Dir == "" {
		log.Fatalf("no dir specified, exit")
		return nil
	}

	return &PFS{
		param: param,
	}
}

func (p *PFS) Mount() error {
	log.Printf("debug=%v", p.param.Debug)

	localAddr := fmt.Sprintf("%s:%d", p.param.Host, p.param.Port)

	log.Printf("start rs: port=%v", p.param.Port)
	p.rsvr = rserver.NewRServer()
	if err := p.rsvr.Start(p.param.Port); err != nil {
		log.Fatalf("start rs error: err=%+v", err)
		return err
	}

	log.Printf("create fp: master=%v, localAddr=%v, gopts=%+v", p.param.Master, localAddr, gopts)
	fp := fproxy.NewFProxy(utility.GetUID(), utility.GetGID(), p.param.Master, localAddr, gopts)
	p.rsvr.RegisterFProxy(fp)

	p.lfsvr = lfs.NewLFS(fp)
	log.Printf("mount fs: dir=%v, fsName=%v, fsType=%v, volumeName=%v",
		p.param.Dir, p.param.FsName, p.param.FsType, p.param.VolumeName)
	if err := p.lfsvr.Mount(p.param.Dir, p.param.FsName, p.param.FsType, p.param.VolumeName); err != nil {
		log.Fatalf("lfs mount error: dir=%v, fsName=%v, fsType=%v, volumeName=%v, err=%+v",
			p.param.Dir, p.param.FsName, p.param.FsType, p.param.VolumeName, err)
		return err
	}

	return nil
}

func (p *PFS) Run() (err error) {
	defer func() {
		if serr := p.stopRS(); serr != nil && err == nil {
			err = serr
		}
	}()

	defer func() {
		if err == nil {
			log.Printf("lfs graceful stopped, no need to umount")
			return
		}

		log.Printf("lfs serve error, unmount it: err=%+v", err)
		if uerr := p.Umount(); uerr != nil {
			log.Printf("lfs umount error, keep serve error: uerr=%+v", uerr)
		}
	}()

	log.Printf("serve fs")
	if err = p.lfsvr.Serve(); err != nil {
		log.Printf("lfs serve error: err=%+v", err)
		return err
	}
	return nil
}

func (p *PFS) stopRS() error {
	log.Printf("stop rs: host=%v, port=%v", p.param.Host, p.param.Port)
	if err := p.rsvr.Stop(); err != nil {
		log.Printf("stop rs error: serr=%+v", err)
		return err
	}
	return nil
}

func (p *PFS) Umount() error {
	log.Printf("umount pfs: dir=%v", p.param.Dir)
	if err := p.lfsvr.Umount(); err != nil {
		log.Printf("lfs unmount fs error, keep serve error: err=%+v", err)
		return err
	}
	return nil
}

func (p *PFS) Stop() error {
	log.Printf("stop pfs")
	err := p.Umount()

	if serr := p.stopRS(); serr != nil && err == nil {
		err = serr
	}
	return err
}
