package pfs

import (
	"context"
	"fmt"

	"github.com/Berailitz/pfs/fbackend"
	"github.com/Berailitz/pfs/lfs"
	"github.com/Berailitz/pfs/logger"
	"github.com/Berailitz/pfs/utility"

	"github.com/Berailitz/pfs/rserver"
)

type PFSParam struct {
	Debug            bool   `yaml:"debug,omitempty"`
	Port             int    `yaml:"port,omitempty"`
	Host             string `yaml:"host,omitempty"`
	Master           string `yaml:"master,omitempty"`
	Dir              string `yaml:"dir,omitempty"`
	FsName           string `yaml:"fsName,omitempty"`
	FsType           string `yaml:"fsType,omitempty"`
	VolumeName       string `yaml:"volumeName,omitempty"`
	StaticTofCfgFile string `yaml:"staticTofCfgFile,omitempty"`
	BackupSize       int    `json:"backupSize,omitempty"`
}

type PFS struct {
	param PFSParam
	rsvr  *rserver.RServer
	lfsvr *lfs.LFS
	ma    *fbackend.RManager
}

func NewPFS(ctx context.Context, param PFSParam) *PFS {
	if param.Dir == "" {
		logger.Pf(ctx, "no dir specified, exit")
		return nil
	}

	return &PFS{
		param: param,
	}
}

func (p *PFS) Mount(ctx context.Context) error {
	logger.If(ctx, "debug=%v", p.param.Debug)

	localAddr := fmt.Sprintf("%s:%d", p.param.Host, p.param.Port)

	p.ma = fbackend.NewRManager(ctx, localAddr, p.param.Master, p.param.StaticTofCfgFile, p.param.BackupSize)
	p.ma.SetMaster(ctx, p.param.Master)

	logger.If(ctx, "start rs: port=%v", p.param.Port)
	p.rsvr = rserver.NewRServer(p.ma)

	p.ma.Start(ctx)

	logger.If(ctx, "create fp: master=%v, localAddr=%v", p.param.Master, localAddr)
	fp := fbackend.NewFProxy(ctx, utility.GetUID(ctx), utility.GetGID(ctx), localAddr, p.ma)
	p.ma.SetFP(fp)
	p.rsvr.RegisterFProxy(ctx, fp)

	if err := p.rsvr.Start(ctx, p.param.Port); err != nil {
		logger.Pf(ctx, "start rs error: err=%+v", err)
		return err
	}

	p.lfsvr = lfs.NewLFS(ctx, fp)
	logger.If(ctx, "mount fs: dir=%v, fsName=%v, fsType=%v, volumeName=%v",
		p.param.Dir, p.param.FsName, p.param.FsType, p.param.VolumeName)
	if err := p.lfsvr.Mount(p.param.Dir, p.param.FsName, p.param.FsType, p.param.VolumeName); err != nil {
		logger.Pf(ctx, "lfs mount error: dir=%v, fsName=%v, fsType=%v, volumeName=%v, err=%+v",
			p.param.Dir, p.param.FsName, p.param.FsType, p.param.VolumeName, err)
		return err
	}

	return nil
}

func (p *PFS) Run(ctx context.Context) (err error) {
	defer func() {
		if serr := p.stopRS(ctx); serr != nil && err == nil {
			err = serr
		}
	}()

	defer func() {
		if err == nil {
			logger.If(ctx, "lfs graceful stopped, no need to umount")
			return
		}

		logger.Ef(ctx, "lfs serve error, unmount it: err=%+v", err)
		if uerr := p.Umount(ctx); uerr != nil {
			logger.Ef(ctx, "lfs umount error, keep serve error: uerr=%+v", uerr)
		}
	}()

	logger.If(ctx, "serve fs")
	if err = p.lfsvr.Serve(); err != nil {
		logger.Ef(ctx, "lfs serve error: err=%+v", err)
		return err
	}
	return nil
}

func (p *PFS) stopRS(ctx context.Context) error {
	logger.If(ctx, "stop rs: host=%v, port=%v", p.param.Host, p.param.Port)
	if err := p.rsvr.Stop(ctx); err != nil {
		logger.Ef(ctx, "stop rs error: serr=%+v", err)
		return err
	}
	return nil
}

func (p *PFS) Umount(ctx context.Context) error {
	logger.If(ctx, "umount pfs: dir=%v", p.param.Dir)
	if err := p.lfsvr.Umount(); err != nil {
		logger.Ef(ctx, "lfs unmount fs error, keep serve error: err=%+v", err)
		return err
	}
	return nil
}

func (p *PFS) Stop(ctx context.Context) error {
	logger.If(ctx, "stop pfs")
	err := p.Umount(ctx)

	p.ma.Stop(ctx)
	if serr := p.stopRS(ctx); serr != nil && err == nil {
		err = serr
	}
	return err
}
