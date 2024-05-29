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

`Raft consensus` is splitted in multiple modules:
- leader election
- membership changes
- log replication
- install snapshot

## Definition

A `Down` node is a node that has been unreachable for a long period of time.

A `ReadOnly` node is a node that does not pariticipate into the `voting campain`. It's a passive node that issue not requests on his own be simply respond answer from the leader. This node can never become a `follower`

A `Follower` node is a node that participate into the `voting campain`. It's a passive node that issue not requests on his own be simply respond answer from the leader. This node can become a `candidate` if all requirements are available.

A `Candidate` node is a node that participate into the `voting campain`. It that can become a `Leader`.

A `Leader` is a node that was previously a `Candidate`. It received the majority of the votes including itself and get elected as the `Leader`. It will then handle all client requests. Writes requests can only be done on the leader. There can only be one `Leader`. To conserve his leadership, it will also send heartbeats to `followers` and `readOnly` nodes otherwise, a new voting campain will be initiated.

## Voting campain

When a node is starting, it starts as a `Follower` except if it's a `ReadOnly` node.

If there is no `Leader`, the `Follower` will become a `Candidate` node.
During the campain, it will vote for himself and request votes from others nodes with an election timeout.
The other nodes will reply with their votes based on `current Term`. The `term` increase over time with new election campain.

If a server `current term` is smaller than other's `current term`, it will update is `term` to the larger value known and step down as a `Follower`.

If a `Candidate` or a `Leader` discover that his `current term` is out to date, it will step down as a `Follower`.
If a server receive a request with a stale `term`, it will reject the request.

When a server has reached the vote majority of votes based on the quorum of servers, it will be elected as new leader.

## Prevote

During the voting campain, by design a prevote is done in order to make sure that a node with lower attributes (current term, current termcommit index etc) does not become a `Leader`.

### Multiple leaders

If you have a 3 or 5 nodes constituing a cluster for example, you can have 2 leaders at the same time and the same `term`. In order to fix that problem, the `raft protocol` introduced a random election timeout on each nodes participating into the campain.
If the `term` of one `Leader` is lower than the other `Leader`, the first one will step down as a `Follower`.

### Split vote

A `raft cluster` must have a minimum of 3 nodes in which you can allow only one fail node. If the leader is unreachable, a new campain will be initiatied by the 2 other `followers`. The problem is at some point a `split vote` can happen. The random election timeout will fix that problem.

## Election timeout

One thing you need to know is the election timeout keep running whether or not there is a `Leader`.
It permits the follower node to start a new election campain in case there is no `Leader`.

## Heartbeats

When a new `Leader` is elected, it must send heartbeats to all `Follower` a the same frequency in order to prevent new election campain. After every heartbeats, a `Follower` will reset his own election timeout.

### Remote Procedure Calls

`Raft` servers communicate using `remote procedure calls` know as `RPCs` which are:
- RequestVote
- AppendEntries
- InstallSnapshot

A `RequestVote` will be sent by a candidate to other nodes and receive a `ReplyVote`.

`AppendEntries` is sent by the `Leader` in order to replicate new logs with a heartbeat.

`InstallSnapshot` will be sent by the leader to `followers` that are to far behind the `Leader`.
