package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"sync"

	"gopkg.in/yaml.v2"

	"github.com/Berailitz/pfs"
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

func (c *TestConfig) Load(path string) error {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("cfg load read file error: path=%v, err=%+v", path, err)
		return err
	}
	if err := yaml.Unmarshal(bytes, c); err != nil {
		log.Printf("cfg load unmarshal error: path=%v, err=%+v", path, err)
		return err
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("buildTime=%v, gitCommit=%v\n", buildTime, gitCommit)
	cfg := flag.String("cfg", "", "Path to confg YAML.")
	dry := flag.Bool("dry", false, "Dry run or not.")
	ncmd := flag.Bool("ncmd", false, "Ignore cmd or not.")
	flag.Parse()

	ctx := context.Background()

	if *cfg == "" {
		flag.Usage()
		log.Fatalf("no cfg specified, exit")
	}

	var tc TestConfig
	if err := tc.Load(*cfg); err != nil {
		log.Fatalf("load cfg error: cfg=%v, err=%+v", *cfg, err)
	}

	log.Printf("sample start: file=%v", cfg)
	log.Printf("tc=%#v", tc)
	if *dry {
		return
	}

	wg := &sync.WaitGroup{}

	for i, pc := range tc.PFS {
		log.Printf("run pfs instance (%v/%v)", i+1, len(tc.PFS))
		p := pfs.NewPFS(pc)
		if p == nil {
			log.Fatalf("create pfs error")
			return
		}

		if err := p.Mount(ctx); err != nil {
			log.Fatalf("mount pfs error: i=%d, err=%+v", i, err)
		}

		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			if err := p.Run(ctx); err != nil {
				log.Printf("run pfs error: i=%d, err=%+v", j, err)
			}
		}(i)
	}

	if !*ncmd {
		for i, cc := range tc.CMD {
			log.Printf("run pfs instance (%v/%v)", i+1, len(tc.CMD))
			wg.Add(1)
			if err := utility.RunCMD(wg, cc); err != nil {
				log.Printf("run cmd error: i=%d, err=%+v", i, err)
			}
		}
	}

	wg.Wait()
	log.Printf("sample finished: file=%v", cfg)
}
