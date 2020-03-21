package tree

import (
	"fmt"
	"io"
	"log"
)

type Path []string

type Noder interface {
	//IsFile() bool
	Name() string
}

// FileNoder should be opened before read or write, ends with close
type FileNoder interface {
	io.ReaderAt
	io.WriterAt
	//io.Closer
	//Open(readOnly bool) bool
}

// DirNode returns nil if err
type DirNoder interface {
	Create(name string, isFile bool) Noder
	Remove(name string) Noder // Node is not destroyed
	//Move(src string, dst string, p bool) bool // mkdir -p
	Lookup(path Path) (Noder, error) // might be multi-level
}

type Node struct {
	name string
	//isFile bool
}

var _ Noder = Node{}

type FileNode struct {
	Node
	data []byte
}

var _ = (FileNoder)((*FileNode)(nil))

type DirNode struct {
	Node
	children map[string]Noder
}

var _ = (DirNoder)((*DirNode)(nil))

type ErrPathInvalid struct {
	path Path
}

var _ error = ErrPathInvalid{}

//func (n Node) IsFile() bool {
//	return n.isFile
//}

func (n Node) Name() string {
	return n.name
}

func (f *FileNode) ReadAt(b []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	return copy(b, f.data[off:]), nil
}

func (f *FileNode) resize(sz uint64) {
	log.Printf("Resize %v to %d.", f.name, sz)
	if sz > uint64(cap(f.data)) {
		newData := make([]byte, sz)
		copy(newData, f.data)
		f.data = newData
	} else {
		f.data = f.data[:sz]
	}
}

func (f *FileNode) WriteAt(b []byte, off int64) (int, error) {
	sz := int64(len(b))
	if off+sz > int64(len(f.data)) {
		f.resize(uint64(off + sz))
	}
	copy(f.data[off:], b)
	return copy(b, f.data[off:]), nil
}

func (d *DirNode) Create(name string, isFile bool) (node Noder) {
	_, ok := d.children[name]
	if ok {
		return
	}
	if isFile {
		node = NewFileNode(name)
	} else {
		node = NewDirNode(name)
	}
	d.children[name] = node
	return
}

func (d *DirNode) Remove(name string) (node Noder) {
	node, ok := d.children[name]
	if ok {
		delete(d.children, name)
	}
	return
}

func (d *DirNode) Lookup(path Path) (Noder, error) {
	if len(path) == 0 {
		return d, nil
	}
	if child, ok := d.children[path[0]]; ok {
		if len(path) == 1 {
			return child, nil
		} else if childDir, ok := child.(DirNoder); ok {
			return childDir.Lookup(path[1:])
		}
	}
	return nil, ErrPathInvalid{path: path}
}

func (e ErrPathInvalid) Error() string {
	return fmt.Sprintf("invalid path: path=%v", e.path)
}

func NewFileNode(name string) *FileNode {
	return &FileNode{
		Node: Node{
			name: name,
		},
		data: make([]byte, 0),
	}
}

func NewDirNode(name string) *DirNode {
	return &DirNode{
		Node: Node{
			name: name,
		},
		children: make(map[string]Noder),
	}
}
