package utility

import (
	"log"
	"os"
	"time"

	"github.com/jacobsa/fuse/fuseops"

	pb "github.com/Berailitz/pfs/remotetree"
	"google.golang.org/grpc"
)

func BuildGCli(addr string, gopts []grpc.DialOption) (pb.RemoteTreeClient, error) {
	log.Printf("build gcli: addr=%v", addr)
	conn, err := grpc.Dial(addr, gopts...)
	if err != nil {
		log.Printf("new rcli fial error: addr=%v, opts=%#v, err=%+V",
			addr, gopts, err)
		return nil, err
	}
	return pb.NewRemoteTreeClient(conn), nil
}

func ToPbAttr(attr fuseops.InodeAttributes) *pb.InodeAttributes {
	return &pb.InodeAttributes{
		Size:   attr.Size,
		Nlink:  attr.Nlink,
		Mode:   uint32(attr.Mode),
		Atime:  attr.Atime.Unix(),
		Mtime:  attr.Mtime.Unix(),
		Ctime:  attr.Ctime.Unix(),
		Crtime: attr.Crtime.Unix(),
		Uid:    attr.Uid,
		Gid:    attr.Gid,
	}
}

func FromPbAttr(attr pb.InodeAttributes) fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Size:   attr.Size,
		Nlink:  attr.Nlink,
		Mode:   os.FileMode(attr.Mode),
		Atime:  time.Unix(attr.Atime, 0),
		Mtime:  time.Unix(attr.Mtime, 0),
		Ctime:  time.Unix(attr.Ctime, 0),
		Crtime: time.Unix(attr.Crtime, 0),
		Uid:    attr.Uid,
		Gid:    attr.Gid,
	}
}

func ToPbEntry(entry fuseops.ChildInodeEntry) *pb.ChildInodeEntry {
	return &pb.ChildInodeEntry{
		Child:                uint64(entry.Child),
		Generation:           uint64(entry.Generation),
		Attributes:           ToPbAttr(entry.Attributes),
		AttributesExpiration: entry.AttributesExpiration.Unix(),
		EntryExpiration:      entry.EntryExpiration.Unix(),
	}
}

func FromPbEntry(entry pb.ChildInodeEntry) fuseops.ChildInodeEntry {
	return fuseops.ChildInodeEntry{
		Child:                fuseops.InodeID(entry.Child),
		Generation:           fuseops.GenerationNumber(entry.Generation),
		Attributes:           FromPbAttr(*entry.Attributes),
		AttributesExpiration: time.Unix(entry.AttributesExpiration, 0),
		EntryExpiration:      time.Unix(entry.EntryExpiration, 0),
	}
}
