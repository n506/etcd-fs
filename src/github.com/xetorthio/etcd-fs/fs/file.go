package etcdfs

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

type etcdFile struct {
	etcdClient *etcd.Client
	path       string
	root       string
	verbose    bool
}

func NewEtcdFile(client *etcd.Client, path string, root string, verbose bool) nodefs.File {
	file := new(etcdFile)
	file.etcdClient = client
	file.path = path
	file.root = root
	file.verbose = verbose

	if file.verbose {
		log.Printf("F| NewEtcdFile: %v\n", path)
	}

	return file
}

func (f *etcdFile) logfuse(s string, i fuse.Status) fuse.Status {
	if f.verbose {
		log.Printf("%s: %v", s, i)
	}
	return i
}

func (f *etcdFile) SetInode(n *nodefs.Inode) {
	if f.verbose {
		log.Printf("F| SetInode: %v\n", n)
	}
}

func (f *etcdFile) InnerFile() nodefs.File {
	if f.verbose {
		log.Println("InnerFile")
	}
	return nil
}

func (f *etcdFile) String() string {
	return "etcdFile"
}

func (f *etcdFile) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	if f.verbose {
		log.Printf("F| Read: %s, %v\n", f.path, off)
	}
	res, err := f.etcdClient.Get(f.root+"/"+f.path, false, false)

	if err != nil {
		log.Println("Error:", err)
		return nil, f.logfuse("F| Read", fuse.ENOENT)
	}

	end := int(off) + int(len(buf))
	if end > len(res.Node.Value) {
		end = len(res.Node.Value)
	}

	data := []byte(res.Node.Value)
	return fuse.ReadResultData(data[off:end]), f.logfuse("F| Read ("+fmt.Sprintf("%v, %v", off, end)+")", fuse.OK)
}

func (f *etcdFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	if f.verbose {
		log.Printf("F| Write: %s, %v\n", f.path, off)
	}
	res, err := f.etcdClient.Get(f.root+"/"+f.path, false, false)

	if err != nil {
		log.Println("Error:", err)
		return 0, f.logfuse("F| Write", fuse.ENOENT)
	}

	originalValue := []byte(res.Node.Value)
	leftChunk := originalValue[:off]
	end := int(off) + int(len(data))

	var rightChunk []byte
	if end > len(res.Node.Value) {
		rightChunk = []byte{}
	} else {
		rightChunk = data[int(off)+int(len(data)):]
	}

	newValue := bytes.NewBuffer(leftChunk)
	newValue.Grow(len(data) + len(rightChunk))
	newValue.Write(data)
	newValue.Write(rightChunk)
	_, err = f.etcdClient.Set(f.root+"/"+f.path, newValue.String(), 0)

	if err != nil {
		log.Println("Error:", err)
		return 0, f.logfuse("F| Write", fuse.EROFS)
	}

	return uint32(len(data)), f.logfuse("F| Write ("+fmt.Sprintf("%v", len(data))+")", fuse.OK)
}

func (f *etcdFile) Flush() fuse.Status {
	if f.verbose {
		log.Printf("F| Flush: %s\n", f.path)
	}
	return f.logfuse("F| Flush", fuse.OK)
}

func (f *etcdFile) Release() {
	if f.verbose {
		log.Printf("F| Release: %s\n", f.path)
	}
}

func (f *etcdFile) GetAttr(out *fuse.Attr) fuse.Status {
	if f.verbose {
		log.Printf("F| GetAttr: %s\n", f.path)
	}
	res, err := f.etcdClient.Get(f.root+"/"+f.path, false, false)

	if err != nil {
		log.Println("Error:", err)
		return f.logfuse("F| GetAttr", fuse.ENOENT)
	}

	out.Mode = fuse.S_IFREG | 0666
	out.Size = uint64(len(res.Node.Value))
	return f.logfuse("F| GetAttr ("+fmt.Sprintf("%v, %v", out.Mode, out.Size)+")", fuse.OK)
}

func (f *etcdFile) Fsync(flags int) fuse.Status {
	if f.verbose {
		log.Printf("F| Fsync: %s\n", f.path)
	}
	return f.logfuse("F| Fsync", fuse.OK)
}

func (f *etcdFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	if f.verbose {
		log.Printf("F| Utimens: %s\n", f.path)
	}
	return f.logfuse("F| Utimens", fuse.OK)
}

func (f *etcdFile) Truncate(size uint64) fuse.Status {
	if f.verbose {
		log.Printf("F| Truncate: %s, %v\n", f.path, size)
	}

	res, err := f.etcdClient.Get(f.root+"/"+f.path, false, false)
	if err != nil {
		log.Println(err)
		return f.logfuse("F| Truncate", fuse.ENOENT)
	}

	newValue := ""

	if size != 0 {
		originalValue := []byte(res.Node.Value)
		s := size
		if size > uint64(len(res.Node.Value)) {
			s = uint64(len(res.Node.Value))
		}
		n := bytes.NewBuffer(originalValue[:s])
		newValue = n.String()
	}

	if _, err := f.etcdClient.Set(f.root+"/"+f.path, newValue, 0); err != nil {
		log.Println(err)
		return f.logfuse("F| Truncate", fuse.EROFS)
	}
	return f.logfuse("F| Truncate", fuse.OK)
}

func (f *etcdFile) Chown(uid uint32, gid uint32) fuse.Status {
	if f.verbose {
		log.Printf("F| Chown: %s, %v, %v\n", f.path, uid, gid)
	}

	return f.logfuse("F| Chown", fuse.OK)
}

func (f *etcdFile) Chmod(perms uint32) fuse.Status {
	if f.verbose {
		log.Printf("F| Chmod: %s, %v\n", f.path, perms)
	}

	return f.logfuse("F| Chmod", fuse.OK)
}

func (f *etcdFile) Allocate(off uint64, size uint64, mode uint32) fuse.Status {
	if f.verbose {
		log.Printf("F| Allocate: %s, %v, %v, %v\n", f.path, off, size, mode)
	}

	return f.logfuse("F| Allocate", fuse.OK)
}
