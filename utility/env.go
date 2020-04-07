package utility

import (
	"log"
	"os/user"
	"strconv"
)

func GetUID() uint32 {
	u, err := user.Current()
	if err != nil {
		log.Fatalf("getuid get user error: err=%+v", err)
	}

	uid, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		log.Fatalf("getuid parse uid error: uid=%v, err=%+v", uid, err)
	}

	return uint32(uid)
}

func GetGID() uint32 {
	u, err := user.Current()
	if err != nil {
		log.Fatalf("getgid get user error: err=%+v", err)
	}

	gid, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		log.Fatalf("getgid parse gid error: gid=%v, err=%+v", gid, err)
	}

	return uint32(gid)
}
