package main

import (
	"flag"
	"log"
	"os"
        "strings"

	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	etcdfs "github.com/xetorthio/etcd-fs/fs"
)

type ConfigT struct {
    verbose bool
    end string
    mount string
}

var Config ConfigT

func init() {
    flag.StringVar(&Config.mount, "mount", "", "mountpoint for fs, REQUIRED")
    flag.StringVar(&Config.end, "endpoint", "http://localhost:2739", "ETCD endpoints, comma separated")
    flag.BoolVar(&Config.verbose, "verbose", false, "verbose debug output")
}

func main() {
        flag.Parse()

        if Config.mount == "" {
            flag.Usage()
            os.Exit(1)
        }

        defer log.Println("etcd-fs stopped")

        endpoints := strings.Split(Config.end, ",")
        log.Println("etcd-fs started")
        log.Printf("ETCD endpoints: %v\n", endpoints)
        log.Printf("Mountpoint: %v\n", Config.mount)
        log.Printf("Verbose output: %v\n", Config.verbose)

	etcdFs := etcdfs.EtcdFs{
		FileSystem:   pathfs.NewDefaultFileSystem(),
		EtcdEndpoint: endpoints,
                Verbose: Config.verbose,
	}
	nfs := pathfs.NewPathNodeFs(&etcdFs, nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	if err != nil {
		log.Printf("Mount fail: %v", err)
	} else {
	    server.Serve()
        }
}
