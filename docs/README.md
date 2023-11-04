# Documentation

`Rafty` is a library for maintaining a replicated state machine following the Raft consensus protocol in `distributed systems`.

## What is `raft`?

Raft is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance.

The difference is that it's decomposed into relatively independent subproblems and, it cleanly addresses all major piecies needed for distributed systems.

## What is a consencus?

Consencus is a funcdamental problem in faul-tolerant distributed systems. It involves multiple servers agreeing on values. Once they reach a decision, that decision is final.

A consencus can be made when a majority of servers are available. With `raft protocol`, if we have 5 servers, we can tolerate 2 nodes failure and the system will still working. If more, the system will stop working as it will try to elect a new leader but failing in this process.

According to the `raft documentation` available [here](https://raft.github.io/raft.pdf):

_Consensus typically arises in the context of replicated state machines, a general approach to building fault-tolerant systems. Each server has a state machine and a log. The state machine is the component that we want to make fault-tolerant, such as a hash table. It will appear to clients that they are interacting with a single, reliable state machine, even if a minority of the servers in the cluster fail. Each state machine takes as input commands from its log. In our hash table example, the log would include commands like set x to 3. A consensus algorithm is used to agree on the commands in the servers' logs. The consensus algorithm must ensure that if any state machine applies set x to 3 as the nth command, no other state machine will ever apply a different nth command. As a result, each state machine processes the same series of commands and thus produces the same series of results and arrives at the same series of states_.

## Definition

A `Down` node is a node that has been unreachable for a long period of time.

A `ReadOnly` node is a node that does not pariticipate into the `voting campain`. It's a passive node that issue not requests on his own be simply respond answer from the leader. This node can never become a `follower`

A `Follower` node is a node that participate into the `voting campain`. It's a passive node that issue not requests on his own be simply respond answer from the leader. This node can become a `candidate` if all requirements are available.

A `Candidate` node is a node that participate into the `voting campain`. It that can become a `Leader`.

A `Leader` is a node that was previously a `Candidate`. It received the majority of the votes including itself and get elected as the `Leader`. It will then handle all client requests. Writes requests can only be done on the leader.

## Voting campain

When a node is starting, it start as a `Follower` except if it's a `ReadOnly` node.

If there is no `Leader`, the `Follower` will become a `Candidate` node.
The `Candidate` will also vote for himself and requests votes from others nodes with an election timeout.
The other nodes will reply with their votes based on `current Term`. The `term` increase other the time when new elections.

If a server `current term` is smaller than other's `current term`, it will update is `term` to the larger value known.

If a `Candidate` or a `Leader` discover that his `current term` is out to date, it will step down as a `Follower`.
If a server receive a request with a stale term, it will reject the request.

`Raft` servers communicate using `remote procedure calls` know as `RPCs`.