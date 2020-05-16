package utility_test

import (
	"context"
	"testing"
	"time"

	"github.com/Berailitz/pfs/utility"
)

func TestNewMutexWithTimeout(t *testing.T) {
	ctx := context.Background()
	lock := utility.NewMutexWithTimeout(ctx, time.Second*1)
	lockID := lock.RLock(ctx)
	time.Sleep(time.Second * 2)
	lock.RUnlock(ctx, lockID)
}
