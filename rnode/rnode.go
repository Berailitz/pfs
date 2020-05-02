package rnode

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"bazil.org/fuse"
	"github.com/Berailitz/pfs/logger"
)

const (
	CanLockTrue        = 0
	CanLockTransfering = 1
)

var (
	RNodeCanNotLockErr = &RNodeErr{"can not lock"}
)

type RNodeAttr struct {
	// The current attributes of this RNode.
	//
	// INVARIANT: attrs.Mode &^ (os.ModePerm|os.ModeDir|os.ModeSymlink) == 0
	// INVARIANT: !(IsDir() && IsSymlink())
	// INVARIANT: attrs.Size == len(contents)
	NAttr  fuse.Attr
	NMTime int64
	NCTime int64

	// For symlinks, the target of the symlink.
	//
	// INVARIANT: If !IsSymlink(), len(target) == 0
	NTarget string

	// extended attributes and values
	NXattrs map[string][]byte
}

type RNode struct {
	NID uint64

	RNodeAttr

	NEntries []fuse.Dirent

	NContents []byte

	NLock    *sync.RWMutex
	NCanLock int32 // 0 for can lock

	NAddr string

	NVersion int64
}

type RNodeErr struct {
	msg string
}

var _ = (error)((*RNodeErr)(nil))

func (rn *RNode) ID() uint64 {
	return rn.NID
}

func (rna *RNodeAttr) Attrs() fuse.Attr {
	attr := rna.NAttr
	attr.Ctime = time.Unix(rna.NCTime, 0)
	attr.Mtime = time.Unix(rna.NMTime, 0)
	return attr
}

func (rn *RNode) Entries() []fuse.Dirent {
	return rn.NEntries
}

func (rn *RNode) SetEntries(entries []fuse.Dirent) {
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

func (rn *RNode) GetRNodeAttrBytes(ctx context.Context) *bytes.Buffer {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(rn.RNodeAttr); err != nil {
		logger.Ef(ctx, "encode attr error: err=%+v", err)
		return nil
	}
	return &buf
}

func (rn *RNode) CanLock() bool {
	return atomic.LoadInt32(&rn.NCanLock) == CanLockTrue
}

func (rn *RNode) RLock(ctx context.Context) error {
	logger.If(ctx, "rlock node: id=%v", rn.ID())
	if rn.CanLock() {
		rn.NLock.RLock()
		logger.If(ctx, "rlock node success: id=%v", rn.ID())
		return nil
	}
	logger.If(ctx, "rlock node cannot lock: id=%v", rn.ID())
	return RNodeCanNotLockErr
}

func (rn *RNode) RUnlock(ctx context.Context) {
	logger.If(ctx, "runlock node: id=%v", rn.ID())
	rn.NLock.RUnlock()
}

func (rn *RNode) Lock(ctx context.Context) error {
	logger.If(ctx, "lock node: id=%v", rn.ID())
	if rn.CanLock() {
		rn.NLock.Lock()
		logger.If(ctx, "lock node success: id=%v", rn.ID())
		return nil
	}
	logger.Ef(ctx, "lock node cannot lock error: id=%v", rn.ID())
	return RNodeCanNotLockErr
}

func (rn *RNode) Unlock(ctx context.Context) {
	logger.If(ctx, "unlock node: id=%v", rn.ID())
	rn.NLock.Unlock()
}

func (rn *RNode) Addr() string {
	return rn.NAddr
}

func (rn *RNode) SetAddr(addr string) {
	rn.NAddr = addr
}

// Create a new RNode with the supplied attributes, which need not contain
// time-related information (the RNode object will take care of that).
func NewRNode(attrs fuse.Attr, id uint64) *RNode {
	// Update time info.
	attrs.Inode = id
	// no need to set following 2 lines for NMTime and NCTime takes cares for them
	// attrs.Mtime = now
	// attrs.Crtime = now

	// Create the object.
	return &RNode{
		NID: id,
		RNodeAttr: RNodeAttr{
			NAttr:   attrs,
			NMTime:  attrs.Mtime.Unix(),
			NCTime:  attrs.Ctime.Unix(),
			NXattrs: make(map[string][]byte),
		},
		NCanLock: CanLockTrue,
		NLock:    &sync.RWMutex{},
		NVersion: 0,
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

	//// INVARIANT: For each i, entries[i].Offset == i+1
	//for i, e := range rn.Entries() {
	//	if !(e.Offset == fuseops.DirOffset(i+1)) {
	//		panic(fmt.Sprintf("Unexpected offset for index %d: %d", i, e.Offset))
	//	}
	//}

	// INVARIANT: Contains no duplicate names in used entries.
	childNames := make(map[string]struct{})
	for _, e := range rn.Entries() {
		if e.Type != fuse.DT_Unknown {
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

	var e fuse.Dirent
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
		if e.Type != fuse.DT_Unknown {
			n++
		}
	}

	return n
}

// Find an entry for the given child name and return its RNode ID.
//
// REQUIRES: rn.IsDir()
func (rn *RNode) LookUpChild(name string) (
	id uint64,
	typ fuse.DirentType,
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
// REQUIRES: dt != fuse.DT_Unknown
func (rn *RNode) AddChild(
	id uint64,
	name string,
	dt fuse.DirentType) {
	//var index int

	// Update the modification time.
	rn.SetMtime(time.Now())

	// No matter where we place the entry, make sure it has the correct Offset
	// field.
	//defer func() {
	//	entries := rn.Entries()
	//	//entries[index].Offset = fuseops.DirOffset(index + 1)
	//	rn.SetEntries(entries)
	//}()

	// Set up the entry.
	e := fuse.Dirent{
		Inode: id,
		Name:  name,
		Type:  dt,
	}

	// Look for a gap in which we can insert it.
	//for index = range rn.Entries() {
	//	if rn.Entries()[index].Type == fuse.DT_Unknown {
	//		entries := rn.Entries()
	//		entries[index] = e
	//		rn.SetEntries(entries)
	//		return
	//	}
	//}

	// Append it to the end.
	//index = len(rn.Entries())
	rn.SetEntries(append(rn.Entries(), e))
}

// Remove an entry for a child.
//
// REQUIRES: rn.IsDir()
// REQUIRES: An entry for the given name exists.
func (rn *RNode) RemoveChild(name string) {
	// Update the modification time.
	rn.SetMtime(time.Now())

	// Find the entry.
	i, ok := rn.findChild(name)
	if !ok {
		panic(fmt.Sprintf("Unknown child: %s", name))
	}

	// Mark it as unused.
	entries := rn.Entries()
	entries[len(entries)-1], entries[i] = entries[i], entries[len(entries)-1]
	rn.SetEntries(entries[:len(entries)-1])
}

// Serve a ReadDir request.
//
// REQUIRES: rn.IsDir()
func (rn *RNode) ReadDir() []fuse.Dirent {
	if !rn.IsDir() {
		panic("ReadDir called on non-directory.")
	}

	return rn.NEntries
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
	rn.SetMtime(time.Now())

	// Ensure that the contents slice is long enough.
	newLen := int(off) + len(p)
	if len(rn.Contents()) < newLen {
		padding := make([]byte, newLen-len(rn.Contents()))
		rn.SetContents(append(rn.Contents(), padding...))
		rn.SetSize(uint64(newLen))
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
	rn.SetMtime(time.Now())

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
		rn.SetSize(*size)
	}

	// Change mode?
	if mode != nil {
		rn.SetMode(*mode)
	}

	// Change mtime?
	if mtime != nil {
		rn.SetMtime(*mtime)
	}
}

func (rn *RNode) Fallocate(mode uint32, offset uint64, length uint64) error {
	if mode != 0 {
		return fuse.ENOSYS
	}
	newSize := int(offset + length)
	if newSize > len(rn.Contents()) {
		padding := make([]byte, newSize-len(rn.Contents()))
		rn.SetContents(append(rn.Contents(), padding...))
		rn.SetSize(offset + length)
	}
	return nil
}

func (rn *RNode) IncrNlink() {
	atomic.AddUint32(&rn.NAttr.Nlink, 1)
}

func (rn *RNode) DecrNlink() {
	atomic.AddUint32(&rn.NAttr.Nlink, ^uint32(0))
}

func (rn *RNode) IsLost() bool {
	return atomic.LoadUint32(&rn.NAttr.Nlink) == 0
}

func (rn *RNode) SetSize(s uint64) {
	atomic.StoreUint64(&rn.NAttr.Size, s)
}

func (rn *RNode) SetMode(m os.FileMode) {
	atomic.StoreUint32((*uint32)(&rn.NAttr.Mode), uint32(m))
}

func (rn *RNode) SetMtime(t time.Time) {
	rn.NMTime = t.Unix()
}

func (rn *RNode) SetCtime(t time.Time) {
	rn.NCTime = t.Unix()
}

func (rn *RNode) Version() int64 {
	return atomic.LoadInt64(&rn.NVersion)
}

func (rn *RNode) IncrVersion() {
	atomic.AddInt64(&rn.NVersion, 1)
}

func (rn *RNode) SetVersion(version int64) {
	atomic.StoreInt64(&rn.NVersion, version)
}

func (e *RNodeErr) Error() string {
	return fmt.Sprintf("rnode error: err=%v", e.msg)
}
