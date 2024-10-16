# Documentation

`Rafty` is a library for maintaining a replicated state machine following the Raft consensus protocol in `distributed systems`.

## What is `raft`?

Raft is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance.

The difference is that it's decomposed into relatively independent subproblems and, it cleanly addresses all major pieces needed for distributed systems.

## What is a consencus?

Consencus is a fundamental problem in fault-tolerant distributed systems. It involves multiple servers agreeing on values. Once they reach a decision, that decision is final.

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

A `Candidate` node is a node that participate into the `voting campain`. This node can become a `Leader`.

A `Leader` is a node that was previously a `Candidate`. It received the majority of the votes including itself and get elected as the `Leader`. It will then handle all client requests. Writes requests can only be done on the leader. There can only be one `Leader`. To conserve his leadership, it will also send heartbeats/appendEntries to `followers` and `readOnly` nodes otherwise, a new voting campain will be initiated.

## Voting campain

When a node is starting, it starts as a `Follower` except if it's a `ReadOnly` node.

If there is no `Leader`, the `Follower` will become a `Candidate` node.
During the campain, the `Candidate` will:
- reset its election timeout
- increase its `currentTerm`
- vote for himself
- send request votes from others nodes.
The other nodes will grant their votes based on `currentTerm`, `lastLogTerm` and `lastLogIndex` named `RequestVote`. The `term` increase over time with new election campain.

If a server `currenTerm` is smaller than other's `currentTerm`, it will update is `term` to the larger value known and step down as a `Follower`.

If a `Candidate` or a `Leader` discover that his `currenTerm` is out to date, it will step down as a `Follower`.
If a server receive a request with a stale `term`, it will reject the request.

The `Follower` will also grant the vote to the most updated node based on `lastLogTerm` and `lastLogIndex`.

When a server has reached the majority of votes based on the quorum of servers, it will be elected as new leader.

## PreVote

During the voting campain, by design a `preVote` is done in order to make sure that nodes with must update `currentTerm` will participate in the election campain before on of them become a `Leader`.

### Multiple leaders

If you have a 3 or 5 nodes constituing a cluster for example, you can have 2 leaders at the same time and the same sets of `RequestVote`. In order to fix that problem, the `raft protocol` introduced a random election timeout on each nodes participating into the campain.
If the `term` of one `Leader` is lower than the other `Leader`, the first one will step down as a `Follower`.

### Split vote

A `raft cluster` must have a minimum of 3 nodes in which you can allow only one fail node. If the leader is unreachable, a new campain will be initiatied by the 2 other `followers`. The problem is at some point a `split vote` can happen. The random election timeout will fix that problem.

## Election timeout

One thing you need to know is the election timeout keep running whether or not there is a `Leader`.
It allow the follower node to start a new election campain in case there is no `Leader`.
This election timeout will randomly be between `150` and `300` milliseconds.

## Heartbeats

When a new `Leader` is elected, it must send heartbeats to all `Follower` before election timeout in order to prevent new election campain. After every heartbeats, a `Follower` will reset his own election timeout.
The `Leader` will send `heartbeats` with empty logs.
Those `heartbeats` are done with by `appendEntries`.

### Remote Procedure Calls

`Raft` servers communicate using `remote procedure calls` know as `RPCs` which are:
- PreRequestVote (added for our purpose)
- RequestVote
- AppendEntries
- InstallSnapshot

A `RequestVote` will be sent by a candidate to other nodes and receive a `ReplyVote`.

`AppendEntries` is sent by the `Leader` in order to replicate new logs. Empty logs will served as heartbeats.

`InstallSnapshot` will be sent by the leader to `followers` that are to far behind the `Leader`.

### AppendEntries

`Leader` send `AppendEntries` with empty logs as hearbeats.
It will also send `AppendEntries` to append `Follower` logs.

When the `Leader` is elected, it will first start to reset its volatile state to:
- `nextIndex[]` set each peer id as to `lastLogIndex + 1`
- `matchIndex[]` set each peer id to 0


When sending `AppendEntries`, the `Leader` will give as parameters:
- his `currentTerm`
- its `id`
- his `prevLogIndex` to `nextIndex[peer.id] -1`
- his `prevLogTerm` to his previous log term like `log[prevLogIndex].Term`
- `entries[]` are logs to send; empty in case of heartbeats
- his `leaderCommit` which is his `commitIndex`

The `Follower` will reply `false` if:
- its own `currentTerm` is greater thant leader `term`
- its logs does not contain an entry at `prevLogIndex` whose `term` matches `prevLogTerm`
- an existing entry conflicts with a new one (same index but different terms) and delete the existing entry and all that follow it

The `Follower` will reply `true` if:
- append any new entries not already in the log
- if `leaderCommit` > `commitIndex`, set `commitIndex = min(leaderCommit, index of the last new entry)`

The `Follower` will reset his election timeout when replying `true`.

