package utility

import (
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"github.com/Berailitz/pfs/rnode"

	pb "github.com/Berailitz/pfs/remotetree"
)

const MagicPbErrStatus uint64 = 0x435b681a4d5623c4

func ToPbAttr(attr fuse.Attr) *pb.InodeAttributes {
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

func FromPbAttr(attr pb.InodeAttributes) fuse.Attr {
	return fuse.Attr{
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

func ToPbDirent(dirent fuse.Dirent) *pb.Dirent {
	return &pb.Dirent{
		Offset: 0,
		Inode:  dirent.Inode,
		Name:   dirent.Name,
		Type:   uint32(dirent.Type),
	}
}

func FromPbDirent(dirent pb.Dirent) fuse.Dirent {
	return fuse.Dirent{
		Inode: dirent.Inode,
		Type:  fuse.DirentType(dirent.Type),
		Name:  dirent.Name,
	}
}

func ToPbDirents(dirents []fuse.Dirent) []*pb.Dirent {
	r := make([]*pb.Dirent, len(dirents))
	for i, d := range dirents {
		r[i] = ToPbDirent(d)
	}
	return r
}

func FromPbDirents(dirents []*pb.Dirent) []fuse.Dirent {
	r := make([]fuse.Dirent, len(dirents))
	for i, d := range dirents {
		r[i] = FromPbDirent(*d)
	}
	return r
}

func ToPbNode(node *rnode.RNode) *pb.Node {
	return &pb.Node{
		NID:       node.NID,
		NAttr:     ToPbAttr(node.Attrs()),
		NTarget:   node.NTarget,
		NXattrs:   node.NXattrs,
		NEntries:  ToPbDirents(node.NEntries),
		NContents: node.NContents,
		CanLock:   node.NCanLock,
		Version:   node.Version(),
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
		NCanLock:  node.CanLock,
		NVersion:  node.Version,
	}
}

func ToPbErr(err error) *pb.Error {
	if err != nil {
		if serr, ok := err.(syscall.Errno); ok {
			return &pb.Error{
				Status: uint64(serr),
			}
		} else {
			return &pb.Error{
				Status: MagicPbErrStatus,
				Msg:    err.Error(),
			}
		}
	}
	return &pb.Error{}
}

func FromPbErr(perr *pb.Error) error {
	if perr != nil && perr.Status != 0 {
		if perr.Msg == "" {
			return syscall.Errno(perr.Status)
		} else {
			return &RemoteErr{perr.Msg}
		}
	}
	return nil
}
