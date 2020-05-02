package utility

import (
	"context"
	"log"
	"time"
)

type Runnable struct {
	name         string
	loopInterval time.Duration
	toStop       chan interface{}
	stopped      chan interface{}
	Run          func(ctx context.Context) error
}

func (r *Runnable) RunLoop(ctx context.Context) (err error) {
	return nil
}

func (r *Runnable) run(ctx context.Context) (err error) {
	for {
		select {
		case <-r.toStop:
			log.Printf("runnable is quitting: name=%v", r.name)
			return nil
		case <-time.After(r.loopInterval):
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
			close(r.stopped)
		}()

		log.Printf("runnable start: name=%v", r.name)
		_ = r.Run(ctx)
	}()
}

func (r *Runnable) Stop(ctx context.Context) {
	log.Printf("runnable stopping: name=%v", r.name)
	close(r.toStop)
	<-r.stopped
	log.Printf("runnable stopped: name%v", r.name)
}

func (r *Runnable) InitRunnable(ctx context.Context, name string, loopInterval time.Duration, runFunc func(ctx context.Context) error) {
	r.name = name
	r.loopInterval = loopInterval
	r.toStop = make(chan interface{})
	r.stopped = make(chan interface{})
	r.Run = runFunc
	if r.Run == nil {
		r.Run = r.run
	}
}
