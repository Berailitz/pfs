package fbackend

import (
	"context"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/Berailitz/pfs/logger"
	"github.com/Berailitz/pfs/utility"

	"gopkg.in/yaml.v2"
)

const (
	wdRunnableName         = "watchdog"
	wdRunnableLoopInterval = 10 * time.Second
	tofUpdateRatio         = 0.8
)

const (
	LookingState   = 0
	LeadingState   = 1
	FollowingState = 2
)

type RouteRule struct {
	next string
	tof  int64
}

type WatchDog struct {
	utility.Runnable

	ma        *RManager
	localAddr string

	realTofMap   sync.Map         // map[string]int64
	tofMapRead   map[string]int64 // map[string]int64
	muTofMapRead sync.RWMutex

	routeMap       sync.Map // map[string]*RouteRule
	routeMapRead   map[string]*RouteRule
	muRouteMapRead sync.RWMutex

	fp *FProxy

	staticTofCfgFile string

	nominee string

	backupsOwnerMaps []*sync.Map // map[uint64]string

	state int64
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

func (d *WatchDog) LogicTof(addr string) (int64, bool) {
	if out, ok := d.routeMap.Load(addr); ok {
		return out.(*RouteRule).tof, true
	}
	return 0, false
}

func (d *WatchDog) realTof(addr string) (int64, bool) {
	if distance, ok := d.realTofMap.Load(addr); ok {
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

func (d *WatchDog) smoothTof(ctx context.Context, addr string, rawTof int64) (smoothTof int64) {
	smoothTof = rawTof
	if oldTof, ok := d.realTof(addr); ok {
		smoothTof = int64(float64(oldTof)*(1-tofUpdateRatio) + float64(rawTof)*tofUpdateRatio)
	}
	return smoothTof
}

func (d *WatchDog) saveRealTof(ctx context.Context, addr string, tof int64) {
	d.realTofMap.Store(addr, tof)
	func() {
		d.muTofMapRead.Lock()
		defer d.muTofMapRead.Unlock()
		d.tofMapRead[addr] = tof
	}()

	if _, ok := d.routeMap.Load(addr); !ok {
		d.saveRoute(ctx, addr, addr, tof)
	}
}

func (d *WatchDog) saveRoute(ctx context.Context, addr string, next string, tof int64) {
	rule := &RouteRule{
		next: addr,
		tof:  tof,
	}
	d.routeMap.Store(addr, rule)
	d.muRouteMapRead.Lock()
	defer d.muRouteMapRead.Unlock()
	d.routeMapRead[addr] = rule
}

func (d *WatchDog) checkTransit(ctx context.Context, transitAddr string, transitTof int64, remoteTofMap map[string]int64) {
	for dst, remoteTof := range remoteTofMap {
		if out, ok := d.routeMap.Load(dst); ok {
			if rule, ok := out.(*RouteRule); ok {
				totalTof := transitTof + remoteTof
				if rule.tof > totalTof {
					d.saveRoute(ctx, dst, transitAddr, totalTof)
					logger.If(ctx, "save new rule: dst=%v, transit=%v, transitTof=%v, totalTof=%v, oldRule=%+v",
						dst, transitAddr, transitTof, totalTof, *rule)
				}
				continue
			}
			logger.Ef(ctx, "non-rule error: dst=%v, out=%+v", dst, out)
		}
	}
}

func (d *WatchDog) loadStaticTof(ctx context.Context) (staticTofMap map[string]int64) {
	staticTofMap = make(map[string]int64)
	if len(d.staticTofCfgFile) > 0 {
		bytes, err := ioutil.ReadFile(d.staticTofCfgFile)
		if err != nil {
			logger.Ef(ctx, "load static tof read file error: path=%v, err=%+v", d.staticTofCfgFile, err)
			return
		}
		if err := yaml.Unmarshal(bytes, staticTofMap); err != nil {
			logger.Ef(ctx, "load static tof unmarshal error: path=%v, err=%+v", d.staticTofCfgFile, err)
			return
		}
	}
	return
}

func (d *WatchDog) randomOtherOwner(ctx context.Context) (addr string) {
	d.muTofMapRead.RLock()
	defer d.muTofMapRead.RUnlock()

	for addr, _ := range d.tofMapRead {
		if addr != d.localAddr {
			return addr
		}
	}
	return ""
}

func (d *WatchDog) getBackupAddrs(ctx context.Context, nodeID uint64) (addrs []string) {
	for i, backupOwnerMap := range d.backupsOwnerMaps {
		if addr := d.randomOtherOwner(ctx); addr != "" && !utility.InStringSlice(addr, addrs) {
			if out, loaded := backupOwnerMap.LoadOrStore(nodeID, addr); loaded {
				addr = out.(string)
			}
			logger.If(ctx, "get backup addrs: i=%v, addr=%v", i, addr)
			addrs = append(addrs, addr)
		} else {
			break
		}
	}
	return addrs
}

func (d *WatchDog) AnswerGossip(ctx context.Context, addr string) (tofMap map[string]int64, nominee string, err error) {
	return d.CopyTofMap(ctx), d.nominee, nil
}

func (d *WatchDog) runLoop(ctx context.Context) (err error) {
	logger.I(ctx, "updating tof map")
	owners, err := d.fp.GetOwnerMap(ctx)
	if err != nil {
		logger.E(ctx, "get owners error", "err", err)
	}

	staticTofMap := d.loadStaticTof(ctx)
	nomineeMap := make(map[string]int64, len(owners))

	for _, addr := range owners {
		rawTof, ok := staticTofMap[addr]
		if ok {
			logger.I(ctx, "use static tof", "addr", addr, "rawTof", rawTof)
		} else {
			rawTof, err = d.fp.Measure(ctx, addr)
			if err != nil {
				logger.E(ctx, "ping error", "addr", addr, "rawTof", rawTof, "err", err)
				continue
			}
			logger.I(ctx, "ping success", "addr", addr, "rawTof", rawTof)
		}
		smoothTof := d.smoothTof(ctx, addr, rawTof)
		d.saveRealTof(ctx, addr, smoothTof)

		if addr == d.localAddr {
			continue
		}
		remoteTofMap, nominee, err := d.fp.Gossip(ctx, addr)
		if err != nil {
			logger.E(ctx, "gossip error", "addr", addr, "err", err)
			continue
		}
		logger.I(ctx, "gossip success", "addr", addr, "remoteTofMap", remoteTofMap)
		d.checkTransit(ctx, addr, smoothTof, remoteTofMap)

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
	return nil
}

func NewWatchDog(ctx context.Context, ma *RManager, localAddr string, staticTofCfgFile string, backupSize int) *WatchDog {
	wd := &WatchDog{
		ma:               ma,
		localAddr:        localAddr,
		tofMapRead:       make(map[string]int64),
		routeMapRead:     make(map[string]*RouteRule),
		staticTofCfgFile: staticTofCfgFile,
		backupsOwnerMaps: make([]*sync.Map, backupSize),
	}
	switch ma.MasterAddr() {
	case localAddr:
		wd.state = LeadingState
	case "":
		wd.state = LookingState
	default:
		wd.state = FollowingState
	}
	wd.InitRunnable(ctx, wdRunnableName, wdRunnableLoopInterval, wd.runLoop, nil)
	return wd
}

func (e *WatchDogErr) Error() string {
	return fmt.Sprintf("watch dog error: %v", e.msg)
}
