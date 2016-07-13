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
    EtcdEndpoint      []string
    Verbose           bool
    connlock          sync.Mutex
    connection        *etcd.Client
    lock              sync.RWMutex
}

var Status map[fuse.Status]string

func init() {
    Status = make(map[fuse.Status]string)
    Status[fuse.OK]      = "OK"
    Status[fuse.EACCES]  = "EACCES"
    Status[fuse.EBUSY]   = "EBUSY"
    Status[fuse.EINVAL]  = "EINVAL"
    Status[fuse.EIO]     = "EIO"
    Status[fuse.ENOENT]  = "ENOENT"
    Status[fuse.ENOSYS]  = "ENOSYS"
    Status[fuse.ENODATA] = "ENODATA"
    Status[fuse.ENOTDIR] = "ENOTDIR"
    Status[fuse.EPERM]   = "EPERM"
    Status[fuse.ERANGE]  = "ERANGE"
    Status[fuse.EXDEV]   = "EXDEV"
    Status[fuse.EBADF]   = "EBADF"
    Status[fuse.ENODEV]  = "ENODEV"
    Status[fuse.EROFS]   = "EROFS"
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
    }
    return me.connection
}

func (me *EtcdFs) String() string {
    return "etcd-fs"
}

func (me *EtcdFs) Unlink(name string, context *fuse.Context) (code fuse.Status) {
    if name == "" {
        return me.logfuse("Unlink", fuse.ENOENT)
    }

    me.lock.Lock()
    defer me.lock.Unlock()

    if me.Verbose {log.Printf("Unlink: %v\n", name)}

    if name == "" {
        return me.logfuse("Unlink", fuse.OK)
    }

    _, err := me.NewEtcdClient().Delete(name, false)

    if err != nil {
        log.Println(err)
        return me.logfuse("Unlink", fuse.ENOENT)
    }

    return me.logfuse("Unlink", fuse.OK)
}

func (me *EtcdFs) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
    if name == "" {
        return me.logfuse("Rmdir", fuse.ENOENT)
    }

    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Rmdir: %v\n", name)}

    if name == "" {
        return me.logfuse("Rmdir", fuse.OK)
    }

    _, err := me.NewEtcdClient().RawDelete(name, true, true)

    if err != nil {
        log.Println(err)
        return me.logfuse("Rmdir", fuse.ENOENT)
    }

    return me.logfuse("Rmdir", fuse.OK)
}

func (me *EtcdFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
    if name == "" {
        return me.logfuse("Create", fuse.ENOENT)
    }

    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Create: %v, %v, %v\n", name, flags, mode)}

    _, err := me.NewEtcdClient().Set(name, "", 0)

    if err != nil {
        log.Println("Create Error:", err)
        return nil, me.logfuse("Create", fuse.ENOENT)
    }

    return NewEtcdFile(me.NewEtcdClient(), name, me.Verbose), me.logfuse("Create", fuse.OK)
}

func (me *EtcdFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
    if name == "" {
        return me.logfuse("MkDir", fuse.ENOENT)
    }

    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Mkdir: %v, %v\n", name, mode)}

    if name == "" {
        return me.logfuse("Mkdir", fuse.OK)
    }

    _, err := me.NewEtcdClient().CreateDir(name, 0)

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
        return &fuse.Attr{
            Mode: fuse.S_IFDIR | 0666,
        }, fuse.OK
    }

    res, err := me.NewEtcdClient().Get(name, false, false)

    if err != nil {
        return nil, me.logfuse("GetAttr", fuse.ENOENT)
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

    return &attr, me.logfuse("GetAttr", fuse.OK)
}

func (me *EtcdFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
    if name == "" {
        return me.logfuse("OpenDir", fuse.ENOENT)
    }

    me.lock.RLock()
    defer me.lock.RUnlock()
    if me.Verbose {log.Printf("OpenDir: %v\n", name)}

    res, err := me.NewEtcdClient().Get(name, false, false)

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

    return entries, me.logfuse("OpenDir", fuse.OK)
}

func (me *EtcdFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
    if name == "" {
        return me.logfuse("Open", fuse.ENOENT)
    }

    me.lock.RLock()
    defer me.lock.RUnlock()
    if me.Verbose {log.Printf("Open: %v, %v\n", name, flags)}

    _, err := me.NewEtcdClient().Get(name, false, false)

    if err != nil {
        log.Println("Open Error:", err)
        return nil, me.logfuse("Open", fuse.ENOENT)
    }

    return NewEtcdFile(me.NewEtcdClient(), name, me.Verbose), me.logfuse("Open", fuse.OK)
}

func (me *EtcdFs) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
    if oldName=="" || newName=="" {
        return me.logfuse("Rename", fuse.ENOENT)
    }
    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Rename: %v -> %v\n", oldName, newName)}

    etcdClient := me.NewEtcdClient()
    res, err := etcdClient.Get(oldName, false, false)
    if err != nil {
        log.Println(err)
        return me.logfuse("Rename", fuse.ENOENT)
    }
    originalValue := []byte(res.Node.Value)
    newValue := bytes.NewBuffer(originalValue)
    if _, err :=etcdClient.Set(newName, newValue.String(), 0); err != nil {
        log.Println(err)
        return me.logfuse("Rename", fuse.ENOENT)
    }
    if _, err := etcdClient.Delete(oldName, false); err != nil {
        log.Println(err)
        etcdClient.Delete(newName, false)
        return me.logfuse("Rename", fuse.ENOENT)
    }
        return me.logfuse("Rename", fuse.OK)
}

func (me *EtcdFs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
    if name == "" {
        return me.logfuse("Access", fuse.ENOENT)
    }

    me.lock.RLock()
    defer me.lock.RUnlock()
    if me.Verbose {log.Printf("Access: %v, %v\n", name, mode)}

    etcdClient := me.NewEtcdClient()
    _, err := etcdClient.Get(name, false, false)
    if err != nil {
        log.Println(err)
        return me.logfuse("Access", fuse.ENOENT)
    }
    return me.logfuse("Access", fuse.OK)
}

func (me *EtcdFs) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
    if me.Verbose {log.Printf("Link: %v, %v\n", oldName, newName)}
    return me.logfuse("Link", fuse.ENOSYS)
}

func (me *EtcdFs) Symlink(name string, linkName string, context *fuse.Context) (fuse.Status) {
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

func (me *EtcdFs) Utimens(name string, atime *time.Time, mtime *time.Time, context *fuse.Context) (code fuse.Status) {
    if me.Verbose {log.Printf("Utimens: %v, %v, %v\n", name, atime, mtime)}
    return me.logfuse("Utimens", fuse.OK)
}

func (me *EtcdFs) Chmod(name string, perms uint32, context *fuse.Context) (code fuse.Status) {
    if me.Verbose {log.Printf("Chmod: %v, %v\n", name, perms)}
    return me.logfuse("Chmod", fuse.OK)
}

func (me *EtcdFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
    if me.Verbose {log.Printf("Chown: %v, %v, %v\n", name, uid, gid)}
    return me.logfuse("Chown", fuse.OK)
}

func (me *EtcdFs) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
    if name == "" {
        return me.logfuse("Truncate", fuse.ENOENT)
    }
    me.lock.Lock()
    defer me.lock.Unlock()
    if me.Verbose {log.Printf("Truncate: %v, %v\n", name, size)}
    etcdClient := me.NewEtcdClient()

    _, err := etcdClient.Get(name, false, false)
    if err != nil {
        log.Println(err)
        return me.logfuse("Truncate", fuse.EIO)
    }
    if _, err := etcdClient.Set(name, "", 0); err != nil {
        log.Println(err)
        return me.logfuse("Truncate", fuse.EIO)
    }
    return me.logfuse("Truncate", fuse.OK)
}

func (me *EtcdFs) GetXAttr(name string, attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
    if me.Verbose {log.Printf("GetXAttr: %v, %v\n", name, attribute)}
    r := []byte{}
    return r, me.logfuse("GetXAttr", fuse.ENOSYS)
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
    r := []string{}
    return r, me.logfuse("ListXAttr", fuse.ENOSYS)
}
