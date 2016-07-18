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
    Debug bool `short:"d" long:"debug" description:"more debug output" env:"ETCD_FS_DEBUG"`
    End string `short:"e" long:"endpoint" description:"ETCD endpoints, comma separated" default:"http://localhost:2379" env:"ETCD_FS_ENDPOINT"`
    Mount string `short:"m" long:"mount" description:"mountpoint for fs, must be created.  REQUIRED" required:"true" env:"ETCD_FS_MOUNT"`
    Root string `short:"r" long:"root" description:"etcd root node" env:"ETCD_FS_ROOT" default:"/"`
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
    log.Printf("ETCD root: %v\n", Config.Root)

    vtype := "Normal"
    if !Config.Debug {
        if Config.Verbose {
            vtype = "Verbose"
        }
    } else {
        vtype = "Debug"
        Config.Verbose = true
    }
    log.Printf("Verbose output: %v\n", vtype)

    if Config.Root == "/" {
        Config.Root = ""
    }

    etcdFs := etcdfs.EtcdFs{
        FileSystem:   pathfs.NewDefaultFileSystem(),
        EtcdEndpoint: endpoints,
        Verbose: Config.Verbose,
        Root: Config.Root,
    }

    if Config.Root != "" {
        if cli := etcdFs.NewEtcdClient(); cli == nil {
            log.Fatalf("etcd connection failed")
            os.Exit(1)
        } else {
            if _, err := cli.UpdateDir(Config.Root, 0); err!=nil {
                if _, err = cli.CreateDir(Config.Root, 0); err!=nil {
                    log.Fatalf("Failed to create etcd root node (%v), %v", Config.Root, err)
                    os.Exit(1)
                } else {
                    log.Printf("ETCD root node created (%v)\n", Config.Root)
                }
            }
        }
    }

    var optsp *pathfs.PathNodeFsOptions = nil;
    var optsn *nodefs.Options = nodefs.NewOptions();

    if Config.Debug {
        optsp = &pathfs.PathNodeFsOptions{Debug:true}
        optsn.Debug = true
    }
    nfs := pathfs.NewPathNodeFs(&etcdFs, optsp)
    server, _, err := nodefs.MountRoot(Config.Mount, nfs.Root(), optsn)

    if err != nil {
        log.Fatalf("Mount fail: %v", err)
    } else {
        log.Println("etcd-fs started")
        defer log.Println("etcd-fs stopped")
        server.Serve()
    }
}
