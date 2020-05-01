package fbackend

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	updateInterval = 10 * time.Second
)

type RouteRule struct {
	next string
	tof  int64
}

type WatchDog struct {
	tofMap       sync.Map         // map[string]int64
	tofMapRead   map[string]int64 // map[string]int64
	muTofMapRead sync.RWMutex

	routeMap sync.Map // map[string]*RouteRule
	fp       *FProxy
	toStop   chan interface{}
	stopped  chan interface{}
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

	if _, ok := d.routeMap.Load(addr); !ok {
		d.routeMap.Store(addr, &RouteRule{
			next: addr,
			tof:  tof,
		})
	}

	d.muTofMapRead.Lock()
	defer d.muTofMapRead.Unlock()
	d.tofMapRead[addr] = tof
}

func (d *WatchDog) checkTransit(ctx context.Context, transit string, transitTof int64, remoteTofMap map[string]int64) {
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

func (d *WatchDog) UpdateMap(ctx context.Context) {
	log.Printf("updating tof map")
	owners, err := d.fp.GetOwnerMap(ctx)
	if err != nil {
		log.Printf("get owners error: err=%+v", err)
	}

	for _, addr := range owners {
		tof, err := d.fp.Measure(ctx, addr, true, true)
		if err != nil {
			log.Printf("ping error: ownerID=%v, addr=%v, err=%+v",
				addr, addr, err)
			continue
		}
		log.Printf("ping success: ownerID=%v, addr=%v, tof=%v",
			addr, addr, tof)
		d.saveTof(ctx, addr, tof)

		remoteTofMap, err := d.fp.Gossip(ctx, addr)
		if err != nil {
			log.Printf("gossip error: ownerID=%v, addr=%v, err=%+v",
				addr, addr, err)
			continue
		}
		log.Printf("gossip success: addr=%v, remoteTofMap=%+v", addr, remoteTofMap)
		d.checkTransit(ctx, addr, tof, remoteTofMap)
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

func NewWatchDog() *WatchDog {
	return &WatchDog{
		tofMapRead: make(map[string]int64),
		toStop:     make(chan interface{}),
		stopped:    make(chan interface{}),
	}
}

func (e *WatchDogErr) Error() string {
	return fmt.Sprintf("watch dog error: %v", e.msg)
}
