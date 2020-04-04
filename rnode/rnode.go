// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rnode

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

const (
	CanLockTrue        = 0
	CanLockTransfering = 1
)

var (
	RNodeCanNotLockErr = &RNodeErr{"can not lock"}
)

// Common attributes for files and directories.
//
// External synchronization is required.

type RNodeAttr struct {
	// The current attributes of this RNode.
	//
	// INVARIANT: attrs.Mode &^ (os.ModePerm|os.ModeDir|os.ModeSymlink) == 0
	// INVARIANT: !(IsDir() && IsSymlink())
	// INVARIANT: attrs.Size == len(contents)
	NAttr fuseops.InodeAttributes

	// For symlinks, the target of the symlink.
	//
	// INVARIANT: If !IsSymlink(), len(target) == 0
	NTarget string

	// extended attributes and values
	NXattrs map[string][]byte
}

type RNode struct {
	NID uint64
	/////////////////////////
	// Mutable state
	/////////////////////////
	RNodeAttr

	// For directories, entries describing the children of the directory. Unused
	// entries are of type DT_Unknown.
	//
	// This array can never be shortened, nor can its elements be moved, because
	// we use its indices for Dirent.Offset, which is exposed to the user who
	// might be calling readdir in a loop while concurrently modifying the
	// directory. Unused entries can, however, be reused.
	//
	// INVARIANT: If !IsDir(), len(entries) == 0
	// INVARIANT: For each i, entries[i].Offset == i+1
	// INVARIANT: Contains no duplicate names in used entries.
	NEntries []fuseutil.Dirent

	// For files, the current contents of the file.
	//
	// INVARIANT: If !IsFile(), len(contents) == 0
	NContents []byte

	lock    sync.RWMutex
	canLock int32 // 0 for can lock
}

type RNodeErr struct {
	msg string
}

var _ = (error)((*RNodeErr)(nil))

func (rn *RNode) ID() uint64 {
	return rn.NID
}

func (rna *RNodeAttr) Attrs() fuseops.InodeAttributes {
	return rna.NAttr
}

func (rna *RNodeAttr) SetAttrs(attrs fuseops.InodeAttributes) {
	rna.NAttr = attrs
}

func (rn *RNode) Entries() []fuseutil.Dirent {
	return rn.NEntries
}

func (rn *RNode) SetEntries(entries []fuseutil.Dirent) {
	rn.NEntries = entries
}

func (rn *RNode) Contents() []byte {
	return rn.NContents
}

func (rn *RNode) SetContents(contents []byte) {
	rn.NContents = contents
}

func (rna *RNodeAttr) Target() string {
	return rna.NTarget
}

func (rna *RNodeAttr) SetTarget(target string) {
	rna.NTarget = target
}

func (rna *RNodeAttr) Xattrs() map[string][]byte {
	return rna.NXattrs
}

func (rna *RNodeAttr) SetXattrs(xattrs map[string][]byte) {
	rna.NXattrs = xattrs
}

func (rn *RNode) GetRNodeAttr() RNodeAttr {
	return rn.RNodeAttr
}

func (rn *RNode) SetRNodeAttr(attr RNodeAttr) {
	rn.RNodeAttr = attr
}

func (rn *RNode) GetRNodeAttrBytes() *bytes.Buffer {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(rn.RNodeAttr); err != nil {
		log.Printf("encode attr error: err=%+v", err)
		return nil
	}
	return &buf
}

func (rn *RNode) CanLock() bool {
	return atomic.LoadInt32(&rn.canLock) == CanLockTrue
}

func (rn *RNode) RLock() error {
	log.Printf("rlock node: id=%v", rn.ID())
	if rn.CanLock() {
		rn.lock.RLock()
		log.Printf("rlock node success: id=%v", rn.ID())
		return nil
	}
	log.Printf("rlock node cannot lock: id=%v", rn.ID())
	return RNodeCanNotLockErr
}

func (rn *RNode) RUnlock() {
	log.Printf("runlock node: id=%v", rn.ID())
	rn.lock.RUnlock()
}

func (rn *RNode) Lock() error {
	log.Printf("lock node: id=%v", rn.ID())
	if rn.CanLock() {
		rn.lock.Lock()
		log.Printf("lock node success: id=%v", rn.ID())
		return nil
	}
	log.Printf("lock node cannot lock error: id=%v", rn.ID())
	return RNodeCanNotLockErr
}

func (rn *RNode) Unlock() {
	log.Printf("unlock node: id=%v", rn.ID())
	rn.lock.Unlock()
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// Create a new RNode with the supplied attributes, which need not contain
// time-related information (the RNode object will take care of that).
func NewRNode(attrs fuseops.InodeAttributes, id uint64) *RNode {
	// Update time info.
	now := time.Now()
	attrs.Mtime = now
	attrs.Crtime = now

	// Create the object.
	return &RNode{
		NID: id,
		RNodeAttr: RNodeAttr{
			NAttr:   attrs,
			NXattrs: make(map[string][]byte),
		},
		canLock: CanLockTrue,
	}
}

// TODO: check at times
func (rn *RNode) CheckInvariants() {
	// INVARIANT: attrs.Mode &^ (os.ModePerm|os.ModeDir|os.ModeSymlink) == 0
	if !(rn.Attrs().Mode&^(os.ModePerm|os.ModeDir|os.ModeSymlink) == 0) {
		panic(fmt.Sprintf("Unexpected mode: %v", rn.Attrs().Mode))
	}

	// INVARIANT: !(IsDir() && IsSymlink())
	if rn.IsDir() && rn.IsSymlink() {
		panic(fmt.Sprintf("Unexpected mode: %v", rn.Attrs().Mode))
	}

	// INVARIANT: attrs.Size == len(contents)
	if rn.Attrs().Size != uint64(len(rn.Contents())) {
		panic(fmt.Sprintf(
			"Size mismatch: %d vs. %d",
			rn.Attrs().Size,
			len(rn.Contents())))
	}

	// INVARIANT: If !IsDir(), len(entries) == 0
	if !rn.IsDir() && len(rn.Entries()) != 0 {
		panic(fmt.Sprintf("Unexpected entries length: %d", len(rn.Entries())))
	}

	// INVARIANT: For each i, entries[i].Offset == i+1
	for i, e := range rn.Entries() {
		if !(e.Offset == fuseops.DirOffset(i+1)) {
			panic(fmt.Sprintf("Unexpected offset for index %d: %d", i, e.Offset))
		}
	}

	// INVARIANT: Contains no duplicate names in used entries.
	childNames := make(map[string]struct{})
	for _, e := range rn.Entries() {
		if e.Type != fuseutil.DT_Unknown {
			if _, ok := childNames[e.Name]; ok {
				panic(fmt.Sprintf("Duplicate name: %s", e.Name))
			}

			childNames[e.Name] = struct{}{}
		}
	}

	// INVARIANT: If !IsFile(), len(contents) == 0
	if !rn.IsFile() && len(rn.Contents()) != 0 {
		panic(fmt.Sprintf("Unexpected length: %d", len(rn.Contents())))
	}

	// INVARIANT: If !IsSymlink(), len(target) == 0
	if !rn.IsSymlink() && len(rn.Target()) != 0 {
		panic(fmt.Sprintf("Unexpected target length: %d", len(rn.Target())))
	}

	return
}

func (rn *RNode) IsDir() bool {
	return rn.Attrs().Mode&os.ModeDir != 0
}

func (rn *RNode) IsSymlink() bool {
	return rn.Attrs().Mode&os.ModeSymlink != 0
}

func (rn *RNode) IsFile() bool {
	return !(rn.IsDir() || rn.IsSymlink())
}

// Return the index of the child within rn.Entries(), if it exists.
//
// REQUIRES: rn.IsDir()
func (rn *RNode) findChild(name string) (i int, ok bool) {
	if !rn.IsDir() {
		panic("findChild called on non-directory.")
	}

	var e fuseutil.Dirent
	for i, e = range rn.Entries() {
		if e.Name == name {
			return i, true
		}
	}

	return 0, false
}

////////////////////////////////////////////////////////////////////////
// Public methods
////////////////////////////////////////////////////////////////////////

// Return the number of children of the directory.
//
// REQUIRES: rn.IsDir()
func (rn *RNode) Len() int {
	var n int
	for _, e := range rn.Entries() {
		if e.Type != fuseutil.DT_Unknown {
			n++
		}
	}

	return n
}

// Find an entry for the given child name and return its RNode ID.
//
// REQUIRES: rn.IsDir()
func (rn *RNode) LookUpChild(name string) (
	// TODO: lock remote children
	id uint64,
	typ fuseutil.DirentType,
	ok bool) {
	index, ok := rn.findChild(name)
	if ok {
		id = uint64(rn.Entries()[index].Inode)
		typ = rn.Entries()[index].Type
	}

	return id, typ, ok
}

// Add an entry for a child.
//
// REQUIRES: rn.IsDir()
// REQUIRES: dt != fuseutil.DT_Unknown
func (rn *RNode) AddChild(
	id uint64,
	name string,
	dt fuseutil.DirentType) {
	var index int

	// Update the modification time.
	attrs := rn.Attrs()
	attrs.Mtime = time.Now()
	rn.SetAttrs(attrs)

	// No matter where we place the entry, make sure it has the correct Offset
	// field.
	defer func() {
		entries := rn.Entries()
		entries[index].Offset = fuseops.DirOffset(index + 1)
		rn.SetEntries(entries)
	}()

	// Set up the entry.
	e := fuseutil.Dirent{
		Inode: fuseops.InodeID(id),
		Name:  name,
		Type:  dt,
	}

	// Look for a gap in which we can insert it.
	for index = range rn.Entries() {
		if rn.Entries()[index].Type == fuseutil.DT_Unknown {
			entries := rn.Entries()
			entries[index] = e
			rn.SetEntries(entries)
			return
		}
	}

	// Append it to the end.
	index = len(rn.Entries())
	rn.SetEntries(append(rn.Entries(), e))
}

// Remove an entry for a child.
//
// REQUIRES: rn.IsDir()
// REQUIRES: An entry for the given name exists.
func (rn *RNode) RemoveChild(name string) {
	// Update the modification time.
	attrs := rn.Attrs()
	attrs.Mtime = time.Now()
	rn.SetAttrs(attrs)

	// Find the entry.
	i, ok := rn.findChild(name)
	if !ok {
		panic(fmt.Sprintf("Unknown child: %s", name))
	}

	// Mark it as unused.
	entries := rn.Entries()
	entries[i] = fuseutil.Dirent{
		Type:   fuseutil.DT_Unknown,
		Offset: fuseops.DirOffset(i + 1),
	}
	rn.SetEntries(entries)
}

// Serve a ReadDir request.
//
// REQUIRES: rn.IsDir()
func (rn *RNode) ReadDir(p []byte, offset int) int {
	if !rn.IsDir() {
		panic("ReadDir called on non-directory.")
	}

	var n int
	for i := offset; i < len(rn.Entries()); i++ {
		e := rn.Entries()[i]

		// Skip unused entries.
		if e.Type == fuseutil.DT_Unknown {
			continue
		}

		tmp := fuseutil.WriteDirent(p[n:], rn.Entries()[i])
		if tmp == 0 {
			break
		}

		n += tmp
	}

	return n
}

// Read from the file's contents. See documentation for ioutil.ReaderAt.
//
// REQUIRES: rn.IsFile()
func (rn *RNode) ReadAt(p []byte, off int64) (int, error) {
	if !rn.IsFile() {
		panic("ReadAt called on non-file.")
	}

	// Ensure the offset is in range.
	if off > int64(len(rn.Contents())) {
		return 0, io.EOF
	}

	// Read what we can.
	n := copy(p, rn.Contents()[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// Write to the file's contents. See documentation for ioutil.WriterAt.
//
// REQUIRES: rn.IsFile()
func (rn *RNode) WriteAt(p []byte, off int64) (int, error) {
	if !rn.IsFile() {
		panic("WriteAt called on non-file.")
	}

	// Update the modification time.
	attrs := rn.Attrs()
	attrs.Mtime = time.Now()
	rn.SetAttrs(attrs)

	// Ensure that the contents slice is long enough.
	newLen := int(off) + len(p)
	if len(rn.Contents()) < newLen {
		padding := make([]byte, newLen-len(rn.Contents()))
		rn.SetContents(append(rn.Contents(), padding...))
		attrs := rn.Attrs()
		attrs.Size = uint64(newLen)
		rn.SetAttrs(attrs)
	}

	// Copy in the data.
	n := copy(rn.Contents()[off:], p)

	// Sanity check.
	if n != len(p) {
		panic(fmt.Sprintf("Unexpected short copy: %v", n))
	}

	return n, nil
}

// Update attributes from non-nil parameters.
func (rn *RNode) SetAttributes(
	size *uint64,
	mode *os.FileMode,
	mtime *time.Time) {
	// Update the modification time.
	attrs := rn.Attrs()
	attrs.Mtime = time.Now()
	rn.SetAttrs(attrs)

	// Truncate?
	if size != nil {
		intSize := int(*size)

		// Update contents.
		if intSize <= len(rn.Contents()) {
			rn.SetContents(rn.Contents()[:intSize])
		} else {
			padding := make([]byte, intSize-len(rn.Contents()))
			rn.SetContents(append(rn.Contents(), padding...))
		}

		// Update attributes.
		attrs := rn.Attrs()
		attrs.Size = *size
		rn.SetAttrs(attrs)
	}

	attrs = rn.Attrs()
	// Change mode?
	if mode != nil {
		attrs.Mode = *mode
	}

	// Change mtime?
	if mtime != nil {
		attrs.Mtime = *mtime
	}
	rn.SetAttrs(attrs)
}

func (rn *RNode) Fallocate(mode uint32, offset uint64, length uint64) error {
	if mode != 0 {
		return fuse.ENOSYS
	}
	newSize := int(offset + length)
	if newSize > len(rn.Contents()) {
		padding := make([]byte, newSize-len(rn.Contents()))
		rn.SetContents(append(rn.Contents(), padding...))
		attrs := rn.Attrs()
		attrs.Size = offset + length
		rn.SetAttrs(attrs)
	}
	return nil
}

func (e *RNodeErr) Error() string {
	return fmt.Sprintf("rnode error: err=%v", e.msg)
}
