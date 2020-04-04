package utility

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Berailitz/pfs/rnode"

	"github.com/jacobsa/fuse/fuseutil"

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

func ToPbDirent(dirent fuseutil.Dirent) *pb.Dirent {
	return &pb.Dirent{
		Offset: uint64(dirent.Offset),
		Inode:  uint64(dirent.Inode),
		Name:   dirent.Name,
		Type:   uint32(dirent.Type),
	}
}

func FromPbDirent(dirent pb.Dirent) fuseutil.Dirent {
	return fuseutil.Dirent{
		Offset: fuseops.DirOffset(dirent.Offset),
		Inode:  fuseops.InodeID(dirent.Inode),
		Name:   dirent.Name,
		Type:   fuseutil.DirentType(dirent.Type),
	}
}

func ToPbDirents(dirents []fuseutil.Dirent) []*pb.Dirent {
	r := make([]*pb.Dirent, len(dirents))
	for i, d := range dirents {
		r[i] = ToPbDirent(d)
	}
	return r
}

func FromPbDirents(dirents []*pb.Dirent) []fuseutil.Dirent {
	r := make([]fuseutil.Dirent, len(dirents))
	for i, d := range dirents {
		r[i] = FromPbDirent(*d)
	}
	return r
}

func ToPbNode(node *rnode.RNode) *pb.Node {
	return &pb.Node{
		NID:       node.NID,
		NAttr:     ToPbAttr(node.NAttr),
		NTarget:   node.NTarget,
		NXattrs:   node.NXattrs,
		NEntries:  ToPbDirents(node.NEntries),
		NContents: node.NContents,
		CanLock:   node.NCanLock,
	}
}

func FromPbNode(node *pb.Node) *rnode.RNode {
	return &rnode.RNode{
		NID: node.NID,
		RNodeAttr: rnode.RNodeAttr{
			NAttr:   FromPbAttr(*node.NAttr),
			NTarget: node.NTarget,
			NXattrs: node.NXattrs,
		},
		NEntries:  FromPbDirents(node.NEntries),
		NContents: node.NContents,
		NLock:     &sync.RWMutex{},
		NCanLock:  node.CanLock,
	}
}

func DecodeError(perr *pb.Error) error {
	if perr != nil && perr.Status != 0 {
		return fmt.Errorf(perr.Msg)
	}
	return nil
}
