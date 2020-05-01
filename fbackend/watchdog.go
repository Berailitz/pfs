package fbackend

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	updateInterval = 10 * time.Second
	tofUpdateRatio = 0.8
)

type RouteRule struct {
	next string
	tof  int64
}

type WatchDog struct {
	ma *RManager

	tofMap       sync.Map         // map[string]int64
	tofMapRead   map[string]int64 // map[string]int64
	muTofMapRead sync.RWMutex

	routeMap sync.Map // map[string]*RouteRule
	fp       *FProxy
	toStop   chan interface{}
	stopped  chan interface{}

	staticTofCfgFile string

	nominee string
}

type WatchDogErr struct {
	msg string
}

var _ = (error)((*WatchDogErr)(nil))

func (d *WatchDog) SetFP(fp *FProxy) {
	d.fp = fp
}

func (d *WatchDog) Route(addr string) string {
	// TODO: find best route
	if out, ok := d.routeMap.Load(addr); ok {
		if route, ok := out.(*RouteRule); ok {
			return route.next
		}
	}
	return addr
}

func (d *WatchDog) Tof(addr string) (int64, bool) {
	if distance, ok := d.tofMap.Load(addr); ok {
		return distance.(int64), true
	}

	return 0, false
}

func (d *WatchDog) Nominee(ctx context.Context) string {
	return d.nominee
}

func (d *WatchDog) CopyTofMap(ctx context.Context) (copied map[string]int64) {
	d.muTofMapRead.RLock()
	defer d.muTofMapRead.RUnlock()
	copied = make(map[string]int64, len(d.tofMapRead))
	for k, v := range d.tofMapRead {
		copied[k] = v
	}
	return copied
}

func (d *WatchDog) saveTof(ctx context.Context, addr string, tof int64) {
	d.tofMap.Store(addr, tof)
	smoothTof := tof
	if oldTof, ok := d.Tof(addr); ok {
		smoothTof = int64(float64(oldTof)*(1-tofUpdateRatio) + float64(tof)*tofUpdateRatio)
	}

	if _, ok := d.routeMap.Load(addr); !ok {
		d.routeMap.Store(addr, &RouteRule{
			next: addr,
			tof:  smoothTof,
		})
	}

	d.muTofMapRead.Lock()
	defer d.muTofMapRead.Unlock()
	d.tofMapRead[addr] = tof
}

func (d *WatchDog) checkTransit(ctx context.Context, transit string, remoteTofMap map[string]int64) {
	transitTof, ok := d.Tof(transit)
	if !ok {
		log.Printf("checkTransit no tof error: transit=%v", transit)
		return
	}

	for dst, remoteTof := range remoteTofMap {
		if out, ok := d.routeMap.Load(dst); ok {
			if rule, ok := out.(*RouteRule); ok {
				totalTof := transitTof + remoteTof
				if rule.tof > totalTof {
					d.routeMap.Store(dst, &RouteRule{
						next: transit,
						tof:  totalTof,
					})
					log.Printf("save new rule: dst=%v, transit=%v, transitTof=%v, totalTof=%v, oldRule=%+v",
						dst, transit, transitTof, totalTof, *rule)
				}
				continue
			}
			log.Printf("non-rule error: dst=%v, out=%+v", dst, out)
		}
	}
}

func (d *WatchDog) loadStaticTof(ctx context.Context) (staticTofMap map[string]int64) {
	staticTofMap = make(map[string]int64)
	if len(d.staticTofCfgFile) > 0 {
		bytes, err := ioutil.ReadFile(d.staticTofCfgFile)
		if err != nil {
			log.Printf("load static tof read file error: path=%v, err=%+v", d.staticTofCfgFile, err)
			return
		}
		if err := yaml.Unmarshal(bytes, staticTofMap); err != nil {
			log.Printf("load static tof unmarshal error: path=%v, err=%+v", d.staticTofCfgFile, err)
			return
		}
	}
	return
}

func (d *WatchDog) UpdateMap(ctx context.Context) {
	log.Printf("updating tof map")
	owners, err := d.fp.GetOwnerMap(ctx)
	if err != nil {
		log.Printf("get owners error: err=%+v", err)
	}

	staticTofMap := d.loadStaticTof(ctx)
	nomineeMap := make(map[string]int64, len(owners))

	for _, addr := range owners {
		tof, ok := staticTofMap[addr]
		if ok {
			log.Printf("use static tof: addr=%v, tof=%v", addr, tof)
		} else {
			tof, err := d.fp.Measure(ctx, addr, true, true)
			if err != nil {
				log.Printf("ping error: ownerID=%v, addr=%v, err=%+v",
					addr, addr, err)
				continue
			}
			log.Printf("ping success: ownerID=%v, addr=%v, tof=%v",
				addr, addr, tof)
		}
		d.saveTof(ctx, addr, tof)

		remoteTofMap, nominee, err := d.fp.Gossip(ctx, addr)
		if err != nil {
			log.Printf("gossip error: ownerID=%v, addr=%v, err=%+v",
				addr, addr, err)
			continue
		}
		log.Printf("gossip success: addr=%v, remoteTofMap=%+v", addr, remoteTofMap)
		d.checkTransit(ctx, addr, remoteTofMap)

		if nominee != "" {
			if counter, ok := nomineeMap[nominee]; ok {
				nomineeMap[nominee] = counter + 1
			} else {
				nomineeMap[nominee] = 1
			}
		}
	}

	for nominee, poll := range nomineeMap {
		if poll<<1 > int64(len(owners)) {
			d.ma.SetMaster(nominee)
		}
	}
}

func (d *WatchDog) Run(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("watch dog recovered: r=%+v", r)
			if err == nil {
				if rerr, ok := r.(error); ok {
					err = rerr
				} else {
					err = &WatchDogErr{
						fmt.Sprintf("run error: r=%+v", r),
					}
				}
			}
		}
		close(d.stopped)
	}()

	for {
		select {
		case <-d.toStop:
			log.Printf("watch dog is quitting")
			return nil
		case <-time.After(updateInterval):
			d.UpdateMap(ctx)
		}
	}
}

func (d *WatchDog) Stop(ctx context.Context) {
	log.Printf("wd stopping")
	close(d.toStop)
	<-d.stopped
	log.Printf("wd stopped")
}

func NewWatchDog(ma *RManager, staticTofCfgFile string) *WatchDog {
	return &WatchDog{
		ma:               ma,
		tofMapRead:       make(map[string]int64),
		toStop:           make(chan interface{}),
		stopped:          make(chan interface{}),
		staticTofCfgFile: staticTofCfgFile,
	}
}

func (e *WatchDogErr) Error() string {
	return fmt.Sprintf("watch dog error: %v", e.msg)
}
