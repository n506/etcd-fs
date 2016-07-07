package etcdfs

import (
        "bytes"
	"log"
	"strings"
	"sync"
        "time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"github.com/coreos/go-etcd/etcd"
)

type EtcdFs struct {
	pathfs.FileSystem
	EtcdEndpoint []string
        Verbose bool

	connlock   sync.RWMutex
	connection *etcd.Client
}

func (me *EtcdFs) NewEtcdClient() *etcd.Client {
	me.connlock.Lock()
	defer me.connlock.Unlock()

	if me.connection == nil {
		me.connection = etcd.NewClient(me.EtcdEndpoint)
	}
	return me.connection
}

func (me *EtcdFs) String() string {
        return "etcd-fs"
}

func (me *EtcdFs) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	if name == "" {
		return fuse.OK
	}

	_, err := me.NewEtcdClient().Delete(name, false)

	if err != nil {
		log.Println(err)
		return fuse.ENOENT
	}

	return fuse.OK
}

func (me *EtcdFs) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	if name == "" {
		return fuse.OK
	}

	_, err := me.NewEtcdClient().RawDelete(name, true, true)

	if err != nil {
		log.Println(err)
		return fuse.ENOENT
	}

	return fuse.OK
}

func (me *EtcdFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	_, err := me.NewEtcdClient().Set(name, "", 0)

	if err != nil {
		log.Println("Create Error:", err)
		return nil, fuse.ENOENT
	}

	return NewEtcdFile(me.NewEtcdClient(), name), fuse.OK
}

func (me *EtcdFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	if name == "" {
		return fuse.OK
	}

	_, err := me.NewEtcdClient().CreateDir(name, 0)

	if err != nil {
		log.Println(err)
		return fuse.ENOENT
	}

	return fuse.OK
}

func (me *EtcdFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0666,
		}, fuse.OK
	}

	res, err := me.NewEtcdClient().Get(name, false, false)

	if err != nil {
		return nil, fuse.ENOENT
	}

	var attr fuse.Attr

	if res.Node.Dir {
		attr = fuse.Attr{
			Mode: fuse.S_IFDIR | 0666,
		}
	} else {
		attr = fuse.Attr{
			Mode: fuse.S_IFREG | 0666, Size: uint64(len(res.Node.Value)),
		}
	}

	return &attr, fuse.OK
}

func (me *EtcdFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	res, err := me.NewEtcdClient().Get(name, false, false)

	if err != nil {
		log.Println("OpenDir Error:", err)
		return nil, fuse.ENOENT
	}

	entries := []fuse.DirEntry{}

	for _, e := range res.Node.Nodes {
		chunks := strings.Split(e.Key, "/")
		file := chunks[len(chunks)-1]
		if e.Dir {
			entries = append(entries, fuse.DirEntry{Name: file, Mode: fuse.S_IFDIR})
		} else {
			entries = append(entries, fuse.DirEntry{Name: file, Mode: fuse.S_IFREG})
		}
	}

	return entries, fuse.OK
}

func (me *EtcdFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	_, err := me.NewEtcdClient().Get(name, false, false)

	if err != nil {
		log.Println("Open Error:", err)
		return nil, fuse.ENOENT
	}

	return NewEtcdFile(me.NewEtcdClient(), name), fuse.OK
}

func (me *EtcdFs) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
        etcdClient := me.NewEtcdClient()
        res, err := etcdClient.Get(oldName, false, false)
        if err != nil {
            log.Println(err)
            return fuse.ENOENT
        }
        originalValue := []byte(res.Node.Value)
        newValue := bytes.NewBuffer(originalValue)
        if _, err :=etcdClient.Set(newName, newValue.String(), 0); err != nil {
            log.Println(err)
            return fuse.ENOENT
        }
        if _, err := etcdClient.Delete(oldName, false); err != nil {
            log.Println(err)
            etcdClient.Delete(newName, false)
            return fuse.ENOENT
        }

        return fuse.OK
}

func (me *EtcdFs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
        etcdClient := me.NewEtcdClient()
        _, err := etcdClient.Get(name, false, false)
        if err != nil {
            log.Println(err)
            return fuse.ENOENT
        }
        return fuse.OK
}

func (me *EtcdFs) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
        return fuse.ENOSYS
}

func (me *EtcdFs) Symlink(name string, linkName string, context *fuse.Context) (fuse.Status) {
        return fuse.ENOSYS
}

func (me *EtcdFs) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
        return "", fuse.ENOSYS
}

func (me *EtcdFs) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
        return fuse.ENOSYS
}

func (me *EtcdFs) Utimens(name string, atime *time.Time, mtime *time.Time, context *fuse.Context) (code fuse.Status) {
        return fuse.OK
}

func (me *EtcdFs) Chmod(name string, perms uint32, context *fuse.Context) (code fuse.Status) {
        return fuse.OK
}

func (me *EtcdFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
        return fuse.OK
}

func (me *EtcdFs) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
        etcdClient := me.NewEtcdClient()

        _, err := etcdClient.Get(name, false, false)

        if err != nil {
            log.Println(err)
            return fuse.EIO
        }
        if _, err := etcdClient.Set(name, "", 0); err != nil {
            log.Println(err)
            return fuse.EIO
        }
        return fuse.OK
}

func (me *EtcdFs) GetXAttr(name string, attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
        r := []byte{}
        return r, fuse.ENOSYS
}

func (me *EtcdFs) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
        return fuse.ENOSYS
}

func (me *EtcdFs) SetXAttr(name string, attr string,data []byte, flags int, context *fuse.Context) fuse.Status {
        return fuse.ENOSYS
}

func (me *EtcdFs) ListXAttr(name string, context *fuse.Context) (attrs []string, code fuse.Status) {
        r := []string{}
        return r, fuse.ENOSYS
}
