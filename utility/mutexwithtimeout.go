package utility

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/Berailitz/pfs/logger"
)

const (
	DefaultTimeout = time.Second * 30
)

type MutexWithTimeout struct {
	mutex   sync.RWMutex
	timeout time.Duration
	locks   NXMap
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (m *MutexWithTimeout) storeID(ctx context.Context) (lockID int64) {
	for ok := false; !ok; {
		lockID = rand.Int63()
		ok = m.locks.StoreNX(lockID, time.Now().Unix())
	}
	return lockID
}

func (m *MutexWithTimeout) Lock(ctx context.Context) int64 {
	m.mutex.Lock()
	lockID := m.storeID(ctx)
	logger.I(ctx, "lock", "lockID", lockID)
	m.pendingUnlock(ctx, lockID, m.mutex.Unlock)
	return lockID
}

func (m *MutexWithTimeout) Unlock(ctx context.Context, lockID int64) bool {
	return m.doUnlock(ctx, lockID, false, m.mutex.Unlock)
}

func (m *MutexWithTimeout) RLock(ctx context.Context) int64 {
	m.mutex.RLock()
	lockID := m.storeID(ctx)
	m.pendingUnlock(ctx, lockID, m.mutex.RUnlock)
	return lockID
}

func (m *MutexWithTimeout) RUnlock(ctx context.Context, lockID int64) bool {
	return m.doUnlock(ctx, lockID, false, m.mutex.RUnlock)
}

func (m *MutexWithTimeout) pendingUnlock(ctx context.Context, lockID int64, unlockFunc func()) {
	time.AfterFunc(m.timeout, func() {
		m.doUnlock(ctx, lockID, true, unlockFunc)
	})
}

func (m *MutexWithTimeout) doUnlock(ctx context.Context, lockID int64, isTimeout bool, unlockFunc func()) bool {
	_, ok := m.locks.PopX(lockID)
	if ok {
		unlockFunc()
		if isTimeout {
			logger.W(ctx, "lock timeout", "lockID", lockID)
		} else {
			logger.I(ctx, "unlock success", "lockID", lockID)
		}
		return true
	} else {
		if !isTimeout {
			logger.W(ctx, "duplicate unlock", "lockID", lockID)
		}
		return false
	}
}

func NewMutexWithTimeout(ctx context.Context, timeout time.Duration) *MutexWithTimeout {
	if timeout/time.Millisecond == 0 {
		timeout = DefaultTimeout
	}
	return &MutexWithTimeout{
		timeout: timeout,
	}
}
