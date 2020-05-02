package utility

import (
	"context"
	"log"
	"time"
)

type Runnable struct {
	Name         string
	LoopInterval time.Duration
	ToStop       chan interface{}
	Stopped      chan interface{}
	run          func(ctx context.Context) error
}

func (r *Runnable) RunLoop(ctx context.Context) (err error) {
	return nil
}

func (r *Runnable) runFunc(ctx context.Context) (err error) {
	for {
		select {
		case <-r.ToStop:
			log.Printf("runnable is quitting: name=%v", r.Name)
			return nil
		case <-time.After(r.LoopInterval):
			if err = r.RunLoop(ctx); err != nil {
				return err
			}
		}
	}
}

func (r *Runnable) Start(ctx context.Context) {
	go func() {
		defer func() {
			RecoverWithStack(nil)
			close(r.Stopped)
		}()

		log.Printf("runnable start: name=%v", r.Name)
		_ = r.run(ctx)
	}()
}

func (r *Runnable) Stop(ctx context.Context) {
	log.Printf("runnable stopping: name=%v", r.Name)
	close(r.ToStop)
	<-r.Stopped
	log.Printf("runnable stopped: name=%v", r.Name)
}

func (r *Runnable) InitRunnable(ctx context.Context, name string, loopInterval time.Duration, runFunc func(ctx context.Context) error) {
	r.Name = name
	r.LoopInterval = loopInterval
	r.ToStop = make(chan interface{})
	r.Stopped = make(chan interface{})
	r.run = runFunc
	if r.run == nil {
		r.run = r.runFunc
	}
}
