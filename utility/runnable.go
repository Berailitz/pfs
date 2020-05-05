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
	SelfStoppedState    = 2
	OutsideStoppedState = 3
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
			logger.I(ctx, "runnable is quitting", "name", r.Name)
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
			if atomic.LoadInt64(&r.state) == SelfStoppedState {
				close(r.ToStop)
			}
			close(r.Stopped)
		}()

		logger.If(ctx, "runnable start: name=%v", r.Name)
		_ = r.run(ctx)
		atomic.StoreInt64(&r.state, SelfStoppedState)
	}()
}

func (r *Runnable) Stop(ctx context.Context) {
	oldState := atomic.LoadInt64(&r.state)
	if oldState == RunningState {
		logger.E(ctx, "not running error", "name", r.Name, "state", oldState)
		return
	}

	atomic.StoreInt64(&r.state, OutsideStoppedState)
	logger.If(ctx, "runnable stopping: name=%v", r.Name)
	close(r.ToStop)
	<-r.Stopped
	logger.If(ctx, "runnable stopped: name=%v", r.Name)
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
