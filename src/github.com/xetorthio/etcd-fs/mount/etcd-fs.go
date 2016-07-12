package main

import (
    "log"
    "os"
    "strings"
    "bytes"
    "fmt"

    "github.com/hanwen/go-fuse/fuse/nodefs"
    "github.com/hanwen/go-fuse/fuse/pathfs"
    etcdfs "github.com/xetorthio/etcd-fs/fs"
    "github.com/jessevdk/go-flags"
)

type ConfigT struct {
    Verbose bool `short:"v" long:"verbose" description:"verbose debug output" env:"ETCD_FS_VERBOSE"`
    End string `short:"e" long:"endpoint" description:"ETCD endpoints, comma separated" default:"http://localhost:2379" env:"ETCD_FS_ENDPOINT"`
    Mount string `short:"m" long:"mount" description:"mountpoint for fs, must be created.  REQUIRED" required:"true" env:"ETCD_FS_MOUNT"`
    Help bool `short:"h" long:"help" description:"Show this help message"`
}

var Config ConfigT

func main() {

    parser := flags.NewParser(&Config, flags.PrintErrors | flags.PassDoubleDash)
    if _, err := parser.Parse(); err != nil {
        var b bytes.Buffer
        parser.WriteHelp(&b)
        fmt.Fprintf(os.Stderr, "%s", &b)
        os.Exit(1)
    }

    if Config.Help == true {
        var b bytes.Buffer
        parser.WriteHelp(&b)
        fmt.Fprintf(os.Stderr, "%s", &b)
        os.Exit(1)
    }

    if Config.Mount == "" {
        fmt.Fprint(os.Stderr, "Mountpoint is empty\n")
        var b bytes.Buffer
        parser.WriteHelp(&b)
        fmt.Fprintf(os.Stderr, "%s", &b)
        os.Exit(1)
    }

    if _, err := os.Stat(Config.Mount); os.IsNotExist(err) {
        fmt.Fprint(os.Stderr, "Mountpoint does not exists\n")
        var b bytes.Buffer
        parser.WriteHelp(&b)
        fmt.Fprintf(os.Stderr, "%s", &b)
        os.Exit(1)
    }

    endpoints := strings.Split(Config.End, ",")

    if len(endpoints) == 0  || endpoints[0] == "" {
        fmt.Fprint(os.Stderr, "Endpoints list is empty\n")
        var b bytes.Buffer
        parser.WriteHelp(&b)
        fmt.Fprintf(os.Stderr, "%s", &b)
        os.Exit(1)
    }

    log.Printf("ETCD endpoints: %v\n", endpoints)
    log.Printf("Mountpoint: %v\n", Config.Mount)
    log.Printf("Verbose output: %v\n", Config.Verbose)


    etcdFs := etcdfs.EtcdFs{
        FileSystem:   pathfs.NewDefaultFileSystem(),
        EtcdEndpoint: endpoints,
        Verbose: Config.Verbose,
    }
    nfs := pathfs.NewPathNodeFs(&etcdFs, nil)
    server, _, err := nodefs.MountRoot(Config.Mount, nfs.Root(), nil)
    if err != nil {
        log.Fatalf("Mount fail: %v", err)
    } else {
        log.Println("etcd-fs started")
        defer log.Println("etcd-fs stopped")
        server.Serve()
    }
}
