package fbackend

import (
	"fmt"
	"sync"
)

type WatchDog struct {
	tofMap sync.Map // map[string]int64
	fp     *FProxy
}

type WatchDogErr struct {
	msg string
}

var _ = (error)((*WatchDogErr)(nil))

func (d *WatchDog) SetFP(fp *FProxy) {
	d.fp = fp
}

func (d *WatchDog) Tof(addr string) (int64, bool) {
	if distance, ok := d.tofMap.Load(addr); ok {
		return distance.(int64), true
	}

	return 0, false
}

func NewWatchDog() *WatchDog {
	return &WatchDog{}
}

func (e *WatchDogErr) Error() string {
	return fmt.Sprintf("watch dog error: %v", e.msg)
}
