package utility

import (
	"context"
	"strconv"
	"time"

	"github.com/Berailitz/pfs/logger"
)

type Runnable struct {
	Name         string
	LoopInterval time.Duration
	ToStop       chan interface{}
	Stopped      chan interface{}
	run          func(ctx context.Context) error
	runLoop      func(ctx context.Context) error
	interval     int
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
	go func() {
		ctx = context.WithValue(ctx, logger.ContextRunnableNameIKey, r.Name)
		defer func() {
			RecoverWithStack(ctx, nil)
			close(r.Stopped)
		}()

		logger.If(ctx, "runnable start: name=%v", r.Name)
		_ = r.run(ctx)
	}()
}

func (r *Runnable) Stop(ctx context.Context) {
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
