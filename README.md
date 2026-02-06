# rafty

`Rafty` is a Golang library that replicates a log-backed state machine.
Details about `Raft protocol` can be found [here](https://raft.github.io/raft.pdf)

Reading this documentation is recommended before going any further.

These resources are also useful:
- https://raft.github.io/
- https://thesecretlivesofdata.com/raft/ 

## Why another library?

There are many libraries out there implementing `In Search of an Understandable Consensus Algorithm`.
Unfortunately, I mostly found them difficult to understand as there is not so much clear documentation about how to use them.
As examples, we have production ready repositories like:
- https://github.com/hashicorp/raft
- https://github.com/etcd-io/raft

So let's rebuild the wheel with clearer explanations.

## Supported features

Here is a list of the supported features of `rafty`:
- [x] PreVote election
- [x] Leader election
- [x] Leader lease
- [x] Leadership transfer
- [x] Logs
  - [x] Log replication
  - [x] Submit write commands
  - [x] Submit read commands
    - [x] Submit read commands linearizable (read index)
    - [x] Submit read commands leader lease
  - [x] Forward read/write commands to leader
  - [x] Log compaction
- [x] Log storage
  - [x] etcd bbolt for long term storage
  - [x] log cache backed by etcd bbolt long term storage
  - [x] in memory storage (for development)
- [x] Membership changes
  - [x] Add member
  - [x] promote member
  - [x] demote member
  - [x] remove member (with shutdown if proper flag set)
  - [x] forceRemove member
  - [x] leaveOnTerminate
- [x] Single server cluster
- [x] Bootstrap cluster
- [x] Snapshot
  - [x] Take snapshot (with FSM interface)
  - [x] Restore snapshot (with FSM interface)
  - [x] InstallSnapshot RPC call
- [x] Prometheus metrics

## Real world app

See [real world app](https://github.com/Lord-Y/rafty/tree/main/examples/grpc/realworldapp) as an implementation example.

## Lifecycle

- `Start()` starts the node and blocks until process shutdown signals (`SIGTERM`/`os.Interrupt`) are received.
- `Serve(ctx)` starts the node and blocks until `ctx` is canceled. Use this when your application owns signal handling.

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
- https://www.cs.princeton.edu/courses/archive/fall18/cos418/docs/p7-raft.pdf
- https://github.com/dgurkaynak/stalk-demo-raft-consensus/?tab=readme-ov-file
- https://github.com/ongardie/raft.tla
