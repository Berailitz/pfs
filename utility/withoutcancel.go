package utility

import (
	"context"
	"time"
)

type contextWithoutCancel struct {
	ctx context.Context
}

var _ = (context.Context)((*contextWithoutCancel)(nil))

func (c contextWithoutCancel) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c contextWithoutCancel) Done() <-chan struct{} {
	return nil
}

func (c contextWithoutCancel) Err() error {
	return nil
}

func (c contextWithoutCancel) Value(key interface{}) interface{} {
	return c.ctx.Value(key)
}

// WithoutCancel returns a context that is never canceled.
func WithoutCancel(ctx context.Context) context.Context {
	return contextWithoutCancel{ctx: ctx}
}
