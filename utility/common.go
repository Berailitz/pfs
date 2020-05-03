package utility

import (
	"context"
	"fmt"
	"os/user"
	"runtime/debug"
	"strconv"

	"github.com/Berailitz/pfs/logger"
)

func GetUID(ctx context.Context) uint32 {
	u, err := user.Current()
	if err != nil {
		logger.Pf(ctx, "getuid get user error: err=%+v", err)
	}

	uid, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		logger.Pf(ctx, "getuid parse uid error: uid=%v, err=%+v", uid, err)
	}

	return uint32(uid)
}

func GetGID(ctx context.Context) uint32 {
	u, err := user.Current()
	if err != nil {
		logger.Pf(ctx, "getgid get user error: err=%+v", err)
	}

	gid, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		logger.Pf(ctx, "getgid parse gid error: gid=%v, err=%+v", gid, err)
	}

	return uint32(gid)
}

func RecoverWithStack(ctx context.Context, err *error) {
	if recovered := recover(); recovered != nil {
		fallbackErr := fmt.Errorf("stacktrace from panic: \n%v\n", string(debug.Stack()))
		logger.E(ctx, "stacktrace from panic", "stack", string(debug.Stack()), "err", err, "recovered", recovered)
		if err != nil {
			if *err == nil {
				if rerr, ok := recovered.(error); ok {
					*err = rerr
				} else {
					*err = fallbackErr
				}
			}
		}
	}
}

func InStringSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
