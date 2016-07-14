export GOPATH=/tmp:$(shell pwd)
test:
	-docker run -d -p 8001:8001 -p 4001:4001 coreos/etcd -name etcd-node1
	go get github.com/coreos/go-etcd/etcd
	go get github.com/hanwen/go-fuse/fuse
	go get github.com/jessevdk/go-flags
	go test -v etcdfs

install:
	sudo apt-get install -qq fuse
	sudo modprobe fuse

build:
	go get github.com/coreos/go-etcd/etcd
	go get github.com/hanwen/go-fuse/fuse
	go get github.com/jessevdk/go-flags
	go build src/github.com/xetorthio/etcd-fs/mount/etcd-fs.go
