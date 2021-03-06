etcd-fs
=======

Use etcd as a filesystem

*THis IS STILL WORK IN PROGRESS*

Why using etcd as a fileystem?
==============================

Because filesystem API is super stable, widely known and supported, and very simple.

Also because it seems like this can be useful for lots of companies out there that have already deployed apps that read some configuration file from local filesystem and would love to load these configuration files to something like etcd and ensure a consistent view of across a cluster of nodes.

How does it work?
=================

Etcd-fs uses [go-fuse](https://github.com/hanwen/go-fuse) and [go-etcd](https://github.com/coreos/go-etcd), two nice modules to create fuse filesystem in go and use etcd from go.

Every file maps to a key in etcd. Every directory maps to a directory in etcd.
The content of every file maps to the value of the key in etcd.

When you mount the filesystem, it will mount the root directory of etcd.

How do I install it?
====================

Clone the project and build it.

```bash
make build
```

Or using official docker golang container
```bash
docker run --rm -it -v $(pwd):/go/src -w /go/src golang:1.6-wheezy make build
```

This generates an executable file ```etcd-fs```. You can mount etcd as a filesystem by running ```etcd-fs MOUNT_PATH ETCD_ENDPOINT```. For example:

```bash
./etcd-fs /tmp/foobar http://localhost:4001
```

Then you can access ```/tmp/foobar``` and use etcd as a filesystem.

To unmount it:

```bash
fusermount -u /tmp/foobar
```

What is supported?
==================

Basic filesystem operations like:
+ Reading/Writing files
+ Creating/Deleting files
+ Creating/Deleting directories

What is missing?
================

+ Lots of optimizations
+ Lots of error handling
+ Specifiying more options to etcd connection
+ Mount filesystem to some node in etcd, not necessarily the root
+ Use watch to get updates from etcd and maybe change file modification time (????)
