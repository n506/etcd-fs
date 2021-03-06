package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/coreos/go-etcd/etcd"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/jessevdk/go-flags"
	"github.com/xetorthio/etcd-fs/fs"
)

type ConfigT struct {
	Log   string `short:"l" long:"loglevel" description:"log level (info, verbose, debug)" default:"info" env:"ETCD_FS_LOGLEVEL"`
	End   string `short:"e" long:"endpoint" description:"ETCD endpoints, comma separated" default:"http://localhost:2379" env:"ETCD_FS_ENDPOINT"`
	Mount string `short:"m" long:"mount" description:"mountpoint for fs, must be created.  REQUIRED" required:"true" env:"ETCD_FS_MOUNT"`
	Root  string `short:"r" long:"root" description:"etcd root node" env:"ETCD_FS_ROOT" default:"/"`
	Whole bool   `short:"w" long:"whole" description:"use whole cluster, otherwise bind etcd-fs only to listed endpoints" env:"ETCD_FS_BIND"`
	Cons  string `short:"c" long:"consistency" description:"Sets connection consistency, allow GET requests to any node (strong, weak)" default:"strong" env:"ETCD_FS_CONSISTENCY"`
	Help  bool   `short:"h" long:"help" description:"Show this help message"`
}

var Config ConfigT

func MountRoot(mountpoint string, root nodefs.Node, opts *nodefs.Options, end []string) (*fuse.Server, *nodefs.FileSystemConnector, error) {
	conn := nodefs.NewFileSystemConnector(root, opts)

	mountOpts := fuse.MountOptions{FsName: fmt.Sprintf("%v", end), Name: "etcdfs"}
	if opts != nil && opts.Debug {
		mountOpts.Debug = opts.Debug
	}
	s, err := fuse.NewServer(conn.RawFS(), mountpoint, &mountOpts)
	if err != nil {
		return nil, nil, err
	}
	return s, conn, nil
}

func main() {

	parser := flags.NewParser(&Config, flags.PrintErrors|flags.PassDoubleDash)
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

	if len(endpoints) == 0 || endpoints[0] == "" {
		fmt.Fprint(os.Stderr, "Endpoints list is empty\n")
		var b bytes.Buffer
		parser.WriteHelp(&b)
		fmt.Fprintf(os.Stderr, "%s", &b)
		os.Exit(1)
	}

	var vtype string

	switch Config.Log {
	case "info", "verbose", "debug":
		vtype = Config.Log
	default:
		fmt.Fprint(os.Stderr, "Log level mus be one of: info, verbose, debug\n")
		var b bytes.Buffer
		parser.WriteHelp(&b)
		fmt.Fprintf(os.Stderr, "%s", &b)
		os.Exit(1)
	}

	var cons string
	switch Config.Cons {
	case "weak":
		cons = etcd.WEAK_CONSISTENCY
	case "strong":
		cons = etcd.STRONG_CONSISTENCY
	default:
		fmt.Fprint(os.Stderr, "Consistency must be one of: strong, weak\n")
		var b bytes.Buffer
		parser.WriteHelp(&b)
		fmt.Fprintf(os.Stderr, "%s", &b)
		os.Exit(1)
	}

	log.Printf("ETCD endpoints: %v\n", endpoints)
	log.Printf("Mountpoint: %v\n", Config.Mount)
	log.Printf("ETCD root: %v\n", Config.Root)
	log.Printf("Whole cluster: %v\n", Config.Whole)
	log.Printf("Consistency: %v\n", Config.Cons)
	log.Printf("Log level: %v\n", vtype)

	verbose := Config.Log != "info"
	debug := Config.Log == "debug"

	if Config.Root == "/" {
		Config.Root = ""
	}

	etcdFs := etcdfs.EtcdFs{
		FileSystem:   pathfs.NewDefaultFileSystem(),
		EtcdEndpoint: endpoints,
		Verbose:      verbose,
		Root:         Config.Root,
		Cons:         cons,
		Whole:        Config.Whole,
	}

	if Config.Root != "" {
		if cli := etcdFs.NewEtcdClient(); cli == nil {
			log.Fatalf("etcd connection failed")
			os.Exit(1)
		} else {
			if _, err := cli.UpdateDir(Config.Root, 0); err != nil {
				if _, err = cli.CreateDir(Config.Root, 0); err != nil {
					log.Fatalf("Failed to create etcd root node (%v), %v", Config.Root, err)
					os.Exit(1)
				} else {
					log.Printf("ETCD root node created (%v)\n", Config.Root)
				}
			}
		}
	}

	var optsp *pathfs.PathNodeFsOptions = nil
	var optsn *nodefs.Options = nodefs.NewOptions()

	if debug {
		optsp = &pathfs.PathNodeFsOptions{Debug: true}
		optsn.Debug = true
	}
	nfs := pathfs.NewPathNodeFs(&etcdFs, optsp)
	server, _, err := MountRoot(Config.Mount, nfs.Root(), optsn, endpoints)

	if err != nil {
		log.Fatalf("Mount fail: %v", err)
	} else {
		log.Println("etcd-fs started")
		defer log.Println("etcd-fs stopped")

		csignal := make(chan os.Signal, 1)
		cfinal := make(chan bool, 1)
		signal.Notify(csignal, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			s := <-csignal
			log.Printf("Received %v", s)
			cfinal <- true
		}()

		go func() {
			server.Serve()
			cfinal <- true
		}()

		<-cfinal
		server.Unmount()
	}
}
