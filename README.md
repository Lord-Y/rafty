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
- [x] PreVote election
- [x] Leader election
- [x] Leader lease
- [x] Leadership transfer
- [] Logs
  - [x] Log replication
  - [x] Submit write commands
  - [x] Forward commands to leader
  - [] Submit read commands
  - [] Log compaction
- [x] Membership changes
  - [x] Add member
  - [x] promote member
  - [x] demote member
  - [x] remove member (with shutdown if proper flag set)
  - [x] forceRemove member
  - [x] leaveOnTerminate
- [x] Single server cluster
- [x] Bootstrap cluster

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