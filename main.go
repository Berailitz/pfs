// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const mntDir = "x"

type MineNode struct {
	// Must embed an Inode for the struct to work as a node.
	fs.Inode

	// When file systems are mutable, all access must use
	// synchronization.
	mu sync.Mutex
	// num is the integer represented in this file/directory
	num   int
	data  []byte
	mtime time.Time
}

func numberToMode(n int) uint32 {
	// prime numbers are files
	if n&1 != 0 {
		return fuse.S_IFREG
	}
	// composite numbers are directories
	return fuse.S_IFDIR
}

// Implement GetAttr to provide size and mtime
var _ = (fs.NodeGetattrer)((*MineNode)(nil))

func (n *MineNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.getattr(out)
	return 0
}

func (n *MineNode) getattr(out *fuse.AttrOut) {
	out.Size = uint64(len(n.data))
	out.SetTimes(nil, &n.mtime, nil)
}

func (n *MineNode) resize(sz uint64) {
	log.Printf("REsize %d to %d.", n.num, sz)
	if sz > uint64(cap(n.data)) {
		newData := make([]byte, sz)
		copy(newData, n.data)
		n.data = newData
	} else {
		n.data = n.data[:sz]
	}
	n.mtime = time.Now()
}

// Implement Setattr to support truncation
var _ = (fs.NodeSetattrer)((*MineNode)(nil))

func (n *MineNode) Setattr(_ context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	if sz, ok := in.GetSize(); ok {
		n.resize(sz)
	}
	n.getattr(out)
	return 0
}

// Implement handleless read.
var _ = (fs.NodeReader)((*MineNode)(nil))

func (n *MineNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.Printf("Want to read %d.", n.num)
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("Read %d from %d.", n.num, off)
	end := off + int64(len(dest))
	if end > int64(len(n.data)) {
		end = int64(len(n.data))
	}

	// We could copy to the `dest` buffer, but since we have a
	// []byte already, return that.
	return fuse.ReadResultData(n.data[off:end]), 0
}

// Implement handleless write.
var _ = (fs.NodeWriter)((*MineNode)(nil))

func (n *MineNode) Write(ctx context.Context, fh fs.FileHandle, buf []byte, off int64) (uint32, syscall.Errno) {
	log.Printf("Want to write %d.", n.num)
	n.mu.Lock()
	defer n.mu.Unlock()

	sz := int64(len(buf))
	log.Printf("Write %dB to %d from %d.", sz, n.num, off)
	if off+sz > int64(len(n.data)) {
		n.resize(uint64(off + sz))
	}
	copy(n.data[off:], buf)
	n.mtime = time.Now()
	return uint32(sz), 0
}

// Ensure we are implementing the NodeReaddirer interface
var _ = (fs.NodeReaddirer)((*MineNode)(nil))

// Readdir is part of the NodeReaddirer interface
func (n *MineNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("Read dir %d", n.num)
	r := make([]fuse.DirEntry, 0, n.num)
	for i := 2; i < n.num; i++ {
		d := fuse.DirEntry{
			Name: strconv.Itoa(i),
			Ino:  uint64(i),
			Mode: numberToMode(i),
		}
		r = append(r, d)
	}
	return fs.NewListDirStream(r), 0
}

// Ensure we are implementing the NodeLookuper interface
var _ = (fs.NodeLookuper)((*MineNode)(nil))

// Lookup is part of the NodeLookuper interface
func (n *MineNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("Lookup for %s->%s\n", strconv.Itoa(n.num), name)
	i, err := strconv.Atoi(name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	if i >= n.num || i <= 1 {
		return nil, syscall.ENOENT
	}

	operations := &MineNode{num: i, data: []byte(strconv.Itoa(i) + "\n")}
	log.Printf("Create node %d", i)

	// The NewInode call wraps the `operations` object into an Inode.
	child := n.NewInode(ctx, operations, fs.StableAttr{
		Mode: numberToMode(i),
		// The child inode is identified by its Inode number.
		// If multiple concurrent lookups try to find the same
		// inode, they are deduplicated on this key.
		Ino: uint64(i),
	})

	// In case of concurrent lookup requests, it can happen that operations !=
	// child.Operations().
	return child, 0
}

var _ = (fs.NodeOpener)((*MineNode)(nil))

func (n *MineNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Printf("Open %d with %v", n.num, flags)

	// Return FOPEN_DIRECT_IO so content is not cached.
	return nil, fuse.FOPEN_DIRECT_IO, 0
}

// ExampleDynamic is a whimsical example of a dynamically discovered
// file system.
func main() {
	log.Println("Start.")
	debug := flag.Bool("debug", true, "print debug data")
	// This is where we'll mount the FS
	if _, err := os.Stat(mntDir); os.IsNotExist(err) {
		err := os.Mkdir(mntDir, 0755)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer func() {
		err := os.Remove(mntDir)
		if err != nil {
			log.Fatal(err)
		}
	}()
	root := &MineNode{num: 10}
	server, err := fs.Mount(mntDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			// Set to true to see how the file system works.
			Debug: *debug,
		},
	})
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Mounted on %s", mntDir)
	log.Printf("Unmount by calling 'fusermount -u %s'", mntDir)

	// Wait until unmount before exiting
	server.Wait()
}
