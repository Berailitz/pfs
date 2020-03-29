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
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
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
}

func (in *RNode) ID() uint64 {
	return in.NID
}

func (in *RNodeAttr) Attrs() fuseops.InodeAttributes {
	return in.NAttr
}

func (in *RNodeAttr) SetAttrs(attrs fuseops.InodeAttributes) {
	in.NAttr = attrs
}

func (in *RNode) Entries() []fuseutil.Dirent {
	return in.NEntries
}

func (in *RNode) SetEntries(entries []fuseutil.Dirent) {
	in.NEntries = entries
}

func (in *RNode) Contents() []byte {
	return in.NContents
}

func (in *RNode) SetContents(contents []byte) {
	in.NContents = contents
}

func (in *RNodeAttr) Target() string {
	return in.NTarget
}

func (in *RNodeAttr) SetTarget(target string) {
	in.NTarget = target
}

func (in *RNodeAttr) Xattrs() map[string][]byte {
	return in.NXattrs
}

func (in *RNodeAttr) SetXattrs(xattrs map[string][]byte) {
	in.NXattrs = xattrs
}

func (in *RNode) GetRNodeAttr() RNodeAttr {
	return in.RNodeAttr
}

func (in *RNode) SetRNodeAttr(attr RNodeAttr) {
	in.RNodeAttr = attr
}

func (in *RNode) GetRNodeAttrBytes() *bytes.Buffer {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(in.RNodeAttr); err != nil {
		log.Printf("encode attr error: err=%+v", err)
		return nil
	}
	return &buf
}

func (in *RNode) SetRNodeAttrBytes(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&in.RNodeAttr); err != nil {
		log.Printf("decode attr error: err=%+v", err)
		return err
	}
	return nil
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
	}
}

func (in *RNode) CheckInvariants() {
	// INVARIANT: attrs.Mode &^ (os.ModePerm|os.ModeDir|os.ModeSymlink) == 0
	if !(in.Attrs().Mode&^(os.ModePerm|os.ModeDir|os.ModeSymlink) == 0) {
		panic(fmt.Sprintf("Unexpected mode: %v", in.Attrs().Mode))
	}

	// INVARIANT: !(IsDir() && IsSymlink())
	if in.IsDir() && in.IsSymlink() {
		panic(fmt.Sprintf("Unexpected mode: %v", in.Attrs().Mode))
	}

	// INVARIANT: attrs.Size == len(contents)
	if in.Attrs().Size != uint64(len(in.Contents())) {
		panic(fmt.Sprintf(
			"Size mismatch: %d vs. %d",
			in.Attrs().Size,
			len(in.Contents())))
	}

	// INVARIANT: If !IsDir(), len(entries) == 0
	if !in.IsDir() && len(in.Entries()) != 0 {
		panic(fmt.Sprintf("Unexpected entries length: %d", len(in.Entries())))
	}

	// INVARIANT: For each i, entries[i].Offset == i+1
	for i, e := range in.Entries() {
		if !(e.Offset == fuseops.DirOffset(i+1)) {
			panic(fmt.Sprintf("Unexpected offset for index %d: %d", i, e.Offset))
		}
	}

	// INVARIANT: Contains no duplicate names in used entries.
	childNames := make(map[string]struct{})
	for _, e := range in.Entries() {
		if e.Type != fuseutil.DT_Unknown {
			if _, ok := childNames[e.Name]; ok {
				panic(fmt.Sprintf("Duplicate name: %s", e.Name))
			}

			childNames[e.Name] = struct{}{}
		}
	}

	// INVARIANT: If !IsFile(), len(contents) == 0
	if !in.IsFile() && len(in.Contents()) != 0 {
		panic(fmt.Sprintf("Unexpected length: %d", len(in.Contents())))
	}

	// INVARIANT: If !IsSymlink(), len(target) == 0
	if !in.IsSymlink() && len(in.Target()) != 0 {
		panic(fmt.Sprintf("Unexpected target length: %d", len(in.Target())))
	}

	return
}

func (in *RNode) IsDir() bool {
	return in.Attrs().Mode&os.ModeDir != 0
}

func (in *RNode) IsSymlink() bool {
	return in.Attrs().Mode&os.ModeSymlink != 0
}

func (in *RNode) IsFile() bool {
	return !(in.IsDir() || in.IsSymlink())
}

// Return the index of the child within in.Entries(), if it exists.
//
// REQUIRES: in.IsDir()
func (in *RNode) findChild(name string) (i int, ok bool) {
	if !in.IsDir() {
		panic("findChild called on non-directory.")
	}

	var e fuseutil.Dirent
	for i, e = range in.Entries() {
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
// REQUIRES: in.IsDir()
func (in *RNode) Len() int {
	var n int
	for _, e := range in.Entries() {
		if e.Type != fuseutil.DT_Unknown {
			n++
		}
	}

	return n
}

// Find an entry for the given child name and return its RNode ID.
//
// REQUIRES: in.IsDir()
func (in *RNode) LookUpChild(name string) (
	// TODO: lock remote children
	id uint64,
	typ fuseutil.DirentType,
	ok bool) {
	index, ok := in.findChild(name)
	if ok {
		id = uint64(in.Entries()[index].Inode)
		typ = in.Entries()[index].Type
	}

	return id, typ, ok
}

// Add an entry for a child.
//
// REQUIRES: in.IsDir()
// REQUIRES: dt != fuseutil.DT_Unknown
func (in *RNode) AddChild(
	// TODO: add remote child
	id uint64,
	name string,
	dt fuseutil.DirentType) {
	var index int

	// Update the modification time.
	attrs := in.Attrs()
	attrs.Mtime = time.Now()
	in.SetAttrs(attrs)

	// No matter where we place the entry, make sure it has the correct Offset
	// field.
	defer func() {
		entries := in.Entries()
		entries[index].Offset = fuseops.DirOffset(index + 1)
		in.SetEntries(entries)
	}()

	// Set up the entry.
	e := fuseutil.Dirent{
		Inode: fuseops.InodeID(id),
		Name:  name,
		Type:  dt,
	}

	// Look for a gap in which we can insert it.
	for index = range in.Entries() {
		if in.Entries()[index].Type == fuseutil.DT_Unknown {
			entries := in.Entries()
			entries[index] = e
			in.SetEntries(entries)
			return
		}
	}

	// Append it to the end.
	index = len(in.Entries())
	in.SetEntries(append(in.Entries(), e))
}

// Remove an entry for a child.
//
// REQUIRES: in.IsDir()
// REQUIRES: An entry for the given name exists.
func (in *RNode) RemoveChild(name string) {
	// TODO: remove remote child
	// Update the modification time.
	attrs := in.Attrs()
	attrs.Mtime = time.Now()
	in.SetAttrs(attrs)

	// Find the entry.
	i, ok := in.findChild(name)
	if !ok {
		panic(fmt.Sprintf("Unknown child: %s", name))
	}

	// Mark it as unused.
	entries := in.Entries()
	entries[i] = fuseutil.Dirent{
		Type:   fuseutil.DT_Unknown,
		Offset: fuseops.DirOffset(i + 1),
	}
	in.SetEntries(entries)
}

// Serve a ReadDir request.
//
// REQUIRES: in.IsDir()
func (in *RNode) ReadDir(p []byte, offset int) int {
	// TODO: fetch remote dir
	if !in.IsDir() {
		panic("ReadDir called on non-directory.")
	}

	var n int
	for i := offset; i < len(in.Entries()); i++ {
		e := in.Entries()[i]

		// Skip unused entries.
		if e.Type == fuseutil.DT_Unknown {
			continue
		}

		tmp := fuseutil.WriteDirent(p[n:], in.Entries()[i])
		if tmp == 0 {
			break
		}

		n += tmp
	}

	return n
}

// Read from the file's contents. See documentation for ioutil.ReaderAt.
//
// REQUIRES: in.IsFile()
func (in *RNode) ReadAt(p []byte, off int64) (int, error) {
	// TODO: read remote content
	if !in.IsFile() {
		panic("ReadAt called on non-file.")
	}

	// Ensure the offset is in range.
	if off > int64(len(in.Contents())) {
		return 0, io.EOF
	}

	// Read what we can.
	n := copy(p, in.Contents()[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// Write to the file's contents. See documentation for ioutil.WriterAt.
//
// REQUIRES: in.IsFile()
func (in *RNode) WriteAt(p []byte, off int64) (int, error) {
	// TODO: write remote content
	if !in.IsFile() {
		panic("WriteAt called on non-file.")
	}

	// Update the modification time.
	attrs := in.Attrs()
	attrs.Mtime = time.Now()
	in.SetAttrs(attrs)

	// Ensure that the contents slice is long enough.
	newLen := int(off) + len(p)
	if len(in.Contents()) < newLen {
		padding := make([]byte, newLen-len(in.Contents()))
		in.SetContents(append(in.Contents(), padding...))
		attrs := in.Attrs()
		attrs.Size = uint64(newLen)
		in.SetAttrs(attrs)
	}

	// Copy in the data.
	n := copy(in.Contents()[off:], p)

	// Sanity check.
	if n != len(p) {
		panic(fmt.Sprintf("Unexpected short copy: %v", n))
	}

	return n, nil
}

// Update attributes from non-nil parameters.
func (in *RNode) SetAttributes(
	size *uint64,
	mode *os.FileMode,
	mtime *time.Time) {
	// Update the modification time.
	attrs := in.Attrs()
	attrs.Mtime = time.Now()
	in.SetAttrs(attrs)

	// Truncate?
	if size != nil {
		intSize := int(*size)

		// Update contents.
		if intSize <= len(in.Contents()) {
			in.SetContents(in.Contents()[:intSize])
		} else {
			padding := make([]byte, intSize-len(in.Contents()))
			in.SetContents(append(in.Contents(), padding...))
		}

		// Update attributes.
		attrs := in.Attrs()
		attrs.Size = *size
		in.SetAttrs(attrs)
	}

	attrs = in.Attrs()
	// Change mode?
	if mode != nil {
		attrs.Mode = *mode
	}

	// Change mtime?
	if mtime != nil {
		attrs.Mtime = *mtime
	}
	in.SetAttrs(attrs)
}

func (in *RNode) Fallocate(mode uint32, offset uint64, length uint64) error {
	if mode != 0 {
		return fuse.ENOSYS
	}
	newSize := int(offset + length)
	if newSize > len(in.Contents()) {
		padding := make([]byte, newSize-len(in.Contents()))
		in.SetContents(append(in.Contents(), padding...))
		attrs := in.Attrs()
		attrs.Size = offset + length
		in.SetAttrs(attrs)
	}
	return nil
}
