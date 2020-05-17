package utility

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Berailitz/pfs/logger"
)

const (
	NotStartYetState    = 0
	RunningState        = 1
	OutsideStoppedState = 2
)

type Runnable struct {
	Name         string
	LoopInterval time.Duration
	ToStop       chan interface{}
	Stopped      chan interface{}
	run          func(ctx context.Context) error
	runLoop      func(ctx context.Context) error
	interval     int
	state        int64
}

func (r *Runnable) runFunc(ctx context.Context) (err error) {
	for {
		r.interval++
		ctx = context.WithValue(ctx, logger.ContextRunnableIntervalIKey, strconv.Itoa(r.interval))
		select {
		case <-r.ToStop:
			logger.W(ctx, "runnable is quitting", "name", r.Name)
			return nil
		case <-time.After(r.LoopInterval):
			if err = r.runLoop(ctx); err != nil {
				logger.E(ctx, "runFunc error", "name", r.Name, "err", err)
				return err
			}
		}
	}
}

func (r *Runnable) Start(ctx context.Context) {
	atomic.StoreInt64(&r.state, RunningState)
	go func() {
		ctx = context.WithValue(ctx, logger.ContextRunnableNameIKey, r.Name)
		defer func() {
			RecoverWithStack(ctx, nil)
			if atomic.LoadInt64(&r.state) != OutsideStoppedState {
				close(r.ToStop)
			}
			close(r.Stopped)
		}()

		logger.If(ctx, "runnable start: name=%v", r.Name)
		_ = r.run(ctx)
	}()
}

func (r *Runnable) Stop(ctx context.Context) {
	oldState := atomic.LoadInt64(&r.state)
	if oldState != RunningState {
		logger.E(ctx, "not running error", "name", r.Name, "state", oldState)
		return
	}

	atomic.StoreInt64(&r.state, OutsideStoppedState)
	logger.W(ctx, "runnable stopping", "name", r.Name)
	close(r.ToStop)
	<-r.Stopped
	logger.W(ctx, "runnable stopped", "name", r.Name)
}

func (r *Runnable) InitRunnable(
	ctx context.Context,
	name string,
	loopInterval time.Duration,
	runLoop func(ctx context.Context) error,
	runFunc func(ctx context.Context) error) {
	r.Name = name
	r.LoopInterval = loopInterval
	r.ToStop = make(chan interface{})
	r.Stopped = make(chan interface{})
	r.runLoop = runLoop
	r.run = runFunc
	if r.run == nil {
		r.run = r.runFunc
	}
}
