package main

import (
	"flag"
	"fmt"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/xetorthio/etcd-fs/fs"
	"log"
	"os"
        "github.com/coreos/etcd/client"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage:\n  etcd-fs MOUNTPOINT ETCDENDPOINT\n")
	}
	flag.Parse()
	if len(flag.Args()) < 2 {
		flag.Usage()
	}
	etcdFs := etcdfs.EtcdFs{
		FileSystem:   pathfs.NewDefaultFileSystem(),
		EtcdEndpointCfg: client.Config{Endpoints: []flag.Arg(1), Transport: client.DefaultTransport, HeaderTimeoutPerRequest: time.Second, }
	}
	nfs := pathfs.NewPathNodeFs(&etcdFs, nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}
