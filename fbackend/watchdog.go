package fbackend

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type WatchDog struct {
	tofMap sync.Map // map[string]int64
	fp     *FProxy
	c      chan interface{}
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
	return addr
}

func (d *WatchDog) Tof(addr string) (int64, bool) {
	if distance, ok := d.tofMap.Load(addr); ok {
		return distance.(int64), true
	}

	return 0, false
}

func (d *WatchDog) UpdateTof(ctx context.Context) {
	log.Printf("updating tof map")
	owners, err := d.fp.GetOwnerMap(ctx)
	if err != nil {
		log.Printf("get owners error: err=%+v", err)
	}

	for _, addr := range owners {
		tof, err := d.fp.ProxyMeasure(ctx, addr, true)
		if err != nil {
			log.Printf("ping error: ownerID=%v, addr=%v, err=%+v",
				addr, addr, err)
			continue
		}
		log.Printf("ping success: ownerID=%v, addr=%v, tof=%v",
			addr, addr, tof)
		d.tofMap.Store(addr, tof)
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
	}()

	select {
	case <-d.c:
		log.Printf("watch dog is quitting")
		return nil
	default:
		for {
			d.UpdateTof(ctx)
			time.Sleep(time.Second * 10)
		}
	}
}

func (d *WatchDog) Stop(ctx context.Context) {
	close(d.c)
}

func NewWatchDog() *WatchDog {
	return &WatchDog{
		c: make(chan interface{}),
	}
}

func (e *WatchDogErr) Error() string {
	return fmt.Sprintf("watch dog error: %v", e.msg)
}
