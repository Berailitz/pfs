package main

import (
	"context"
	"flag"
	"io/ioutil"
	"sync"

	"gopkg.in/yaml.v2"

	"github.com/Berailitz/pfs"
	"github.com/Berailitz/pfs/logger"
	"github.com/Berailitz/pfs/utility"
)

var (
	gitCommit string
	buildTime string
)

type TestConfig struct {
	PFS []pfs.PFSParam     `yaml:"pfs,omitempty"`
	CMD []utility.CMDParam `yaml:"cmd,omitempty"`
}

func (c *TestConfig) Load(ctx context.Context, path string) error {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Ef(ctx, "cfg load read file error: path=%v, err=%+v", path, err)
		return err
	}
	if err := yaml.Unmarshal(bytes, c); err != nil {
		logger.Ef(ctx, "cfg load unmarshal error: path=%v, err=%+v", path, err)
		return err
	}
	return nil
}

func main() {
	ctx := context.Background()
	logger.If(ctx, "buildTime=%v, gitCommit=%v", buildTime, gitCommit)
	cfg := flag.String("cfg", "", "Path to confg YAML.")
	dry := flag.Bool("dry", false, "Dry run or not.")
	ncmd := flag.Bool("ncmd", false, "Ignore cmd or not.")
	flag.Parse()

	if *cfg == "" {
		flag.Usage()
		logger.Pf(ctx, "no cfg specified, exit")
	}

	var tc TestConfig
	if err := tc.Load(ctx, *cfg); err != nil {
		logger.Pf(ctx, "load cfg error: cfg=%v, err=%+v", *cfg, err)
	}

	logger.If(ctx, "sample start: file=%v", cfg)
	logger.If(ctx, "tc=%#v", tc)
	if *dry {
		return
	}

	wg := &sync.WaitGroup{}

	for i, pc := range tc.PFS {
		pfsCtx := context.WithValue(ctx, logger.ContextLocalPortKey, pc.Port)
		logger.If(pfsCtx, "run pfs instance (%v/%v)", i+1, len(tc.PFS))
		p := pfs.NewPFS(pfsCtx, pc)
		if p == nil {
			logger.Pf(pfsCtx, "create pfs error")
			return
		}

		if err := p.Mount(pfsCtx); err != nil {
			logger.Pf(pfsCtx, "mount pfs error: i=%d, err=%+v", i, err)
		}

		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			if err := p.Run(pfsCtx); err != nil {
				logger.Ef(pfsCtx, "run pfs error: i=%d, err=%+v", j, err)
			}
		}(i)
	}

	if !*ncmd {
		for i, cc := range tc.CMD {
			cmdCtx := context.WithValue(ctx, logger.ContextCMDKey, cc.CMD)
			logger.If(cmdCtx, "run pfs instance (%v/%v)", i+1, len(tc.CMD))
			wg.Add(1)
			if err := utility.RunCMD(cmdCtx, wg, cc); err != nil {
				logger.E(cmdCtx, "run cmd error", "i", i, "err", err)
			}
		}
	}

	wg.Wait()
	logger.I(ctx, "sample finished", "cfg", *cfg)
}
