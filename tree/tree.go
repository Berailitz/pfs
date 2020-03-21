package tree

import (
	"log"
	"strings"
)

type Treer interface {
	DirNoder
	//TreeLookup(names string) (Noder, error) // might be multi-level
}

type Tree struct {
	DirNode
}

func NewTree(name string) Tree {
	return Tree{
		DirNode: *NewDirNode(name),
	}
}

var _ = (Treer)((*Tree)(nil))

func NewTestTree() *Tree {
	tree := NewTree("testTree")
	tree.Create("a", false)
	tree.Create("b", true)
	b, err := tree.Lookup([]string{"b"})
	if err != nil {
		log.Fatalf("NewTestTree error: err=%+v", err)
	}
	bf, ok := b.(FileNoder)
	if !ok {
		log.Fatalf("b is not file")
	}
	_, err = bf.WriteAt([]byte("File b is here."), 0)
	if err != nil {
		log.Fatalf("write b error: err=%+v", err)
	}
	return &tree
}

func StrToPath(names string) Path {
	if names == "/" {
		return make(Path, 0)
	}
	r := strings.Split(names, "/")
	if len(r) < 2 {
		return nil
	}
	path := r[1:]
	for _, name := range path {
		if name == "" {
			return nil
		}
	}
	return path
}
