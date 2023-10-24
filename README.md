# rafty

`Rafty` is yet another golang library that manage to replicate log state machine.
Details about `Raft protocol` can be found [here](https://raft.github.io/raft.pdf)

## Why another library?

There are many libraries out there implementing the `search of an understandable consensus algorithm`.
Unfortunately, I mostly found them difficult to understand as there is not so much clear documentation about how to use them.
As examples, we have production ready repositories like:
- https://github.com/hashicorp/raft
- https://github.com/etcd-io/raft

So let's try to redo the wheel with more explanations.
