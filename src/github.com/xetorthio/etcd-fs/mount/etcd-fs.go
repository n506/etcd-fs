package main

import (
	"flag"
	"fmt"
	"log"
	"os"
        "strings"

	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	etcdfs "github.com/xetorthio/etcd-fs/fs"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage:\n  etcd-fs MOUNTPOINT ETCDENDPOINTS\n")
		os.Exit(1)
	}
	flag.Parse()
	if len(flag.Args()) < 2 {
		flag.Usage()
	}
        endpoints := strings.FieldsFunc(flag.Arg(1), func(c rune) bool { return c == ',' })
        log.Printf("ETCD endpoints: %v\n", endpoints)
        log.Printf("Muntpoint: %v\n", flag.Arg(0))
	etcdFs := etcdfs.EtcdFs{
		FileSystem:   pathfs.NewDefaultFileSystem(),
		EtcdEndpoint: endpoints,
	}
	nfs := pathfs.NewPathNodeFs(&etcdFs, nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}
