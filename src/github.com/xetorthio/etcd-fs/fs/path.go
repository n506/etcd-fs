package etcdfs

import (
    "bytes"
    "log"
    "strings"
    "sync"
    "time"
    "fmt"

    "github.com/hanwen/go-fuse/fuse"
    "github.com/hanwen/go-fuse/fuse/nodefs"
    "github.com/hanwen/go-fuse/fuse/pathfs"

    "github.com/coreos/go-etcd/etcd"
)

type EtcdFs struct {
    pathfs.FileSystem
    EtcdEndpoint      []string
    Verbose           bool
    Root              string
    Cons              string
    connlock          sync.Mutex
    connection        *etcd.Client
    lock              sync.RWMutex
}

func (me *EtcdFs) logfuse(s string, i fuse.Status) fuse.Status {
    if me.Verbose {log.Printf("%s: %v", s, i)}
    return i
}

func (me *EtcdFs) NewEtcdClient() *etcd.Client {
    me.connlock.Lock()
    defer me.connlock.Unlock()

    if me.connection == nil {
        if me.Verbose {log.Println("Make new ETCD connection")}
        me.connection = etcd.NewClient(me.EtcdEndpoint)
        me.connection.SetConsistency(me.Cons)
    }
    return me.connection
}

func (me *EtcdFs) String() string {
    return "etcdfs"
}

func (me *EtcdFs) Unlink(name string, context *fuse.Context) fuse.Status {
    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Unlink: %v\n", name)}

    if name == "" {
        return me.logfuse("Unlink", fuse.OK)
    }

    _, err := me.NewEtcdClient().Delete(me.Root + "/" + name, false)

    if err != nil {
        log.Println(err)
        return me.logfuse("Unlink", fuse.ENOENT)
    }

    return me.logfuse("Unlink", fuse.OK)
}

func (me *EtcdFs) Rmdir(name string, context *fuse.Context) fuse.Status {
    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Rmdir: %v\n", name)}

    if name == "" {
        return me.logfuse("Rmdir", fuse.EROFS)
    }

    _, err := me.NewEtcdClient().RawDelete(me.Root + "/" + name, true, true)

    if err != nil {
        log.Println(err)
        return me.logfuse("Rmdir", fuse.ENOENT)
    }

    return me.logfuse("Rmdir", fuse.OK)
}

func (me *EtcdFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Create: %v, %v, %v\n", name, flags, mode)}
    if name == "" {
        return nil, me.logfuse("Create", fuse.EROFS)
    }

    _, err := me.NewEtcdClient().Set(me.Root + "/" + name, "", 0)

    if err != nil {
        log.Println("Create Error:", err)
        return nil, me.logfuse("Create", fuse.ENOENT)
    }
    f := NewEtcdFile(me.NewEtcdClient(), name, me.Root, me.Verbose)
    return f, me.logfuse("Create (" + fmt.Sprintf("%v", f) + ")", fuse.OK)
}

func (me *EtcdFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Mkdir: %v, %v\n", name, mode)}
    if name == "" {
        return me.logfuse("MkDir", fuse.EROFS)
    }

    _, err := me.NewEtcdClient().CreateDir(me.Root + "/" + name, 0)

    if err != nil {
        log.Println(err)
        return me.logfuse("Mkdir", fuse.ENOENT)
    }

    return me.logfuse("Mkdir", fuse.OK)
}

func (me *EtcdFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
    me.lock.RLock()
    defer me.lock.RUnlock()
    if me.Verbose {log.Printf("GetAttr: %v\n", name)}
    if name == "" {
        a := fuse.Attr{Mode: fuse.S_IFDIR | 0777, }
        return &a, me.logfuse("GetAttr (" + fmt.Sprintf("%v, %v", a.Mode, a.Size) + ")", fuse.OK)
    }

    res, err := me.NewEtcdClient().Get(me.Root + "/" + name, false, false)

    if err != nil {
        return nil, me.logfuse("GetAttr", fuse.ENOENT)
    }

    var attr fuse.Attr

    if res.Node.Dir {
        attr = fuse.Attr{
            Mode: fuse.S_IFDIR | 0777,
        }
    } else {
        attr = fuse.Attr{
            Mode: fuse.S_IFREG | 0666, Size: uint64(len(res.Node.Value)),
        }
    }

    return &attr, me.logfuse("GetAttr (" + fmt.Sprintf("%v, %v", attr.Mode, attr.Size) + ")", fuse.OK)
}

func (me *EtcdFs) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
    me.lock.RLock()
    defer me.lock.RUnlock()
    if me.Verbose {log.Printf("OpenDir: %v\n", name)}

    res, err := me.NewEtcdClient().Get(me.Root + "/" + name, false, false)

    if err != nil {
        log.Println("OpenDir Error:", err)
        return nil, me.logfuse("OpenDir", fuse.ENOENT)
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

    return entries, me.logfuse("OpenDir (" + fmt.Sprintf("%v", entries) + ")", fuse.OK)
}

func (me *EtcdFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
    me.lock.RLock()
    defer me.lock.RUnlock()
    if me.Verbose {log.Printf("Open: %v, %v\n", name, flags)}

    _, err := me.NewEtcdClient().Get(me.Root + "/" + name, false, false)

    if err != nil {
        log.Println("Open Error:", err)
        return nil, me.logfuse("Open", fuse.ENOENT)
    }

    f := NewEtcdFile(me.NewEtcdClient(), name, me.Root, me.Verbose)
    return f, me.logfuse("Open (" + fmt.Sprintf("%v", f) + ")", fuse.OK)
}

func (me *EtcdFs) Rename(oldName string, newName string, context *fuse.Context) fuse.Status {
    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Rename: %v -> %v\n", oldName, newName)}
    if oldName=="" || newName=="" {
        return me.logfuse("Rename", fuse.EROFS)
    }

    etcdClient := me.NewEtcdClient()
    res, err := etcdClient.Get(me.Root + "/" + oldName, false, false)
    if err != nil {
        log.Println(err)
        return me.logfuse("Rename", fuse.ENOENT)
    }
    originalValue := []byte(res.Node.Value)
    newValue := bytes.NewBuffer(originalValue)
    if _, err :=etcdClient.Set(me.Root + "/" + newName, newValue.String(), 0); err != nil {
        log.Println(err)
        return me.logfuse("Rename", fuse.ENOENT)
    }
    if _, err := etcdClient.Delete(me.Root + "/" + oldName, false); err != nil {
        log.Println(err)
        etcdClient.Delete(me.Root + "/" + newName, false)
        return me.logfuse("Rename", fuse.ENOENT)
    }
        return me.logfuse("Rename", fuse.OK)
}

func (me *EtcdFs) Access(name string, mode uint32, context *fuse.Context) fuse.Status {
    me.lock.RLock()
    defer me.lock.RUnlock()
    if me.Verbose {log.Printf("Access: %v, %v\n", name, mode)}
    if name == "" {
        return me.logfuse("Access", fuse.OK)
    }

    etcdClient := me.NewEtcdClient()
    _, err := etcdClient.Get(me.Root + "/" + name, false, false)
    if err != nil {
        log.Println(err)
        return me.logfuse("Access", fuse.ENOENT)
    }
    return me.logfuse("Access", fuse.OK)
}

func (me *EtcdFs) Link(oldName string, newName string, context *fuse.Context) fuse.Status {
    if me.Verbose {log.Printf("Link: %v, %v\n", oldName, newName)}
    return me.logfuse("Link", fuse.ENOSYS)
}

func (me *EtcdFs) Symlink(name string, linkName string, context *fuse.Context) fuse.Status {
    if me.Verbose {log.Printf("Symlink: %v\n", name)}
    return me.logfuse("Link", fuse.ENOSYS)
}

func (me *EtcdFs) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
    if me.Verbose {log.Printf("Readlink: %v\n", name)}
    return "", me.logfuse("ReadLink", fuse.ENOSYS)
}

func (me *EtcdFs) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
    if me.Verbose {log.Printf("Mknod: %v, %v, %v\n", name, mode, dev)}
    return me.logfuse("Mknod", fuse.ENOSYS)
}

func (me *EtcdFs) Utimens(name string, atime *time.Time, mtime *time.Time, context *fuse.Context) fuse.Status {
    if me.Verbose {log.Printf("Utimens: %v, %v, %v\n", name, atime, mtime)}
    return me.logfuse("Utimens", fuse.OK)
}

func (me *EtcdFs) Chmod(name string, perms uint32, context *fuse.Context) fuse.Status {
    if me.Verbose {log.Printf("Chmod: %v, %v\n", name, perms)}
    return me.logfuse("Chmod", fuse.OK)
}

func (me *EtcdFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
    if me.Verbose {log.Printf("Chown: %v, %v, %v\n", name, uid, gid)}
    return me.logfuse("Chown", fuse.OK)
}

func (me *EtcdFs) Truncate(name string, size uint64, context *fuse.Context) fuse.Status {
    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Truncate: %v, %v\n", name, size)}
    if name == "" {
        return me.logfuse("Truncate", fuse.EROFS)
    }

    etcdClient := me.NewEtcdClient()

    res, err := etcdClient.Get(me.Root + "/" + name, false, false)
    if err != nil {
        log.Println(err)
        return me.logfuse("Truncate", fuse.ENOENT)
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

    if _, err := etcdClient.Set(me.Root + "/" + name, newValue, 0); err != nil {
        log.Println(err)
        return me.logfuse("Truncate", fuse.EROFS)
    }
    return me.logfuse("Truncate", fuse.OK)
}

func (me *EtcdFs) GetXAttr(name string, attribute string, context *fuse.Context) ([]byte, fuse.Status) {
    if me.Verbose {log.Printf("GetXAttr: %v, %v\n", name, attribute)}
    return nil, me.logfuse("GetXAttr", fuse.ENOSYS)
}

func (me *EtcdFs) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
    if me.Verbose {log.Printf("RemoveXAttr: %v, %v\n", name, attr)}
    return me.logfuse("RemoveXAttr", fuse.ENOSYS)
}

func (me *EtcdFs) SetXAttr(name string, attr string,data []byte, flags int, context *fuse.Context) fuse.Status {
    if me.Verbose {log.Printf("SetXAttr: %v, %v\n", name, attr)}
    return me.logfuse("SetXAttr", fuse.ENOSYS)
}

func (me *EtcdFs) ListXAttr(name string, context *fuse.Context) (attrs []string, code fuse.Status) {
    if me.Verbose {log.Printf("ListXAttr: %v\n", name)}
    return nil, me.logfuse("ListXAttr", fuse.ENOSYS)
}
