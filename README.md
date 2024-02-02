# rafty

`Rafty` is yet another golang library that manage to replicate log state machine.
Details about `Raft protocol` can be found [here](https://raft.github.io/raft.pdf)

Reading this documentation is recommanded before going any further.

Check out these websites will also be useful:
- https://raft.github.io/
- https://thesecretlivesofdata.com/raft/ 

## Why another library?

There are many libraries out there implementing the `search of an understandable consensus algorithm`.
Unfortunately, I mostly found them difficult to understand as there is not so much clear documentation about how to use them.
As examples, we have production ready repositories like:
- https://github.com/hashicorp/raft
- https://github.com/etcd-io/raft

So let's try to redo the wheel with more explanations.

## Supported features

Here is a list of the supported features of `rafty`:
- [x] Leader election

## References

- https://github.com/logcabin/logcabin
- https://github.com/funktor/distributed-hash-table/tree/raft
- https://www.cncf.io/blog/2019/11/04/building-a-large-scale-distributed-storage-system-based-on-raft/
- https://www.sofastack.tech/en/projects/sofa-jraft/consistency-raft-jraft/
- https://github.com/baidu/braft/blob/master/docs/cn/raft_protocol.md
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum
- https://towardsdatascience.com/raft-algorithm-explained-2-30db4790cdef
- https://www.alibabacloud.com/blog/raft-engineering-practices-and-the-cluster-membership-change_597742
- https://github.com/lni/dragonboat
- https://github.com/lni/dragonboat-example
- https://github.com/PlatformLab/epaxos/tree/master


2024-02-02T06:19:32+01:00 | INFO | ../../../../requests.go:288 > heartbeat received from leader abe35d4f-787e-4262-9894-f6475ed81028 for term 5
^Cpanic: runtime error: invalid memory address or nil pointer dereference
	panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x10 pc=0x7b0a77]

goroutine 35 [running]:
google.golang.org/grpc.(*ClientConn).Close.func1()
	/home/ypougeol/go/pkg/mod/google.golang.org/grpc@v1.59.0/clientconn.go:1252 +0x17
panic({0x879600?, 0xcdf820?})
	/usr/local/go/src/runtime/panic.go:914 +0x21f
google.golang.org/grpc.(*ClientConn).Close(0x0)
	/home/ypougeol/go/pkg/mod/google.golang.org/grpc@v1.59.0/clientconn.go:1256 +0x60
github.com/Lord-Y/rafty.(*Rafty).disconnectToPeers(...)
	/home/ypougeol/projects/cloud/rafty_all/rafty_chan/rafty.go:387
github.com/Lord-Y/rafty.(*Server).Stop(0xc000002180)
	/home/ypougeol/projects/cloud/rafty_all/rafty_chan/server.go:125 +0xcc
github.com/Lord-Y/rafty.(*Server).Start.func1()
	/home/ypougeol/projects/cloud/rafty_all/rafty_chan/server.go:101 +0x75
created by github.com/Lord-Y/rafty.(*Server).Start in goroutine 1
	/home/ypougeol/projects/cloud/rafty_all/rafty_chan/server.go:96 +0x396
exit status 2