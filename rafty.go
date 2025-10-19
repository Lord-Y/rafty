package rafty

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Lord-Y/rafty/logger"
	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	channelzservice "google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

/*

How our raft cluster implementation works? It diverge slighly from raft paper by adding new safegards.
When a 3 nodes cluster start with 0 logs, all nodes will contact each other to know who is the leader
and if a node is part of the cluster, they will exchange theirs ids. Without getting all ids,
no further steps will be done like election campaign.
This also means that if nth nodes out of 3 is not up and running, no further process will be done.
On the opposite side, if all 3 nodes were running and one of them shutted down, when restarting, it will do business as usual it already has all ids.

ADDING A NODE
If the 4th node contact the cluster, it will:
- ask to all members who is the leader and will keep asking when not found by any actual members of the cluster
- the contacted node will provide the leader informations AND will also tell it to ask for membership
  as the new node is NOT part of the cluster
- user land will perform an AddMember call to be part of the cluster
- the leader will start replicate its logs in nth rounds with the informations provided when asked for membership
- if the leader determined the the new node logs are up to date enough, a log entry config will be appended to
  the logs and replicated to followers. In this log entry config, the flag WaitToBePromoted will be set to true.
	That means the current cluster will never start election campaign with this node if the actual
	leader crashes. If the actual leader crashes, the new node will restart the membership process from the beginning.
	Once the leader knows that the new log entry config has been committed, it will ONLY THEN promote the node.
	Of course if the node is TOO SLOW or something got wrong, it won't be promoted AND will restart membership process again.
	To promote the new node, another log entry config will created BUT with WaitToBePromoted flag set to false and
	sent for replication. The leader will also add the new node configuration into the replication process in order to send
	append entries like any other followers.
	Again if the leader crashes during the promotion process, no worries, the new node will have to restart membership process.
	A node with WaitToBePromoted set to true can be safely removed or force removed

DEMOTING A NODE
Demoting a node MUST be used when a it needs to be stopped for maintenance for example.
A log config entry will be created AND Decommissioning flag will be set to true.
The cluster will never start election campaign with this node.
The leader will keep sending append entries to it but will not be counted into the quorum.
The leader will check if the node can be demoted WITHOUT breaking the cluster. If it will, an error will be returned.
A demoted node can be promoted back.
A demoted node can be then safely removed from the cluster, see REMOVE section.
Somehow if both WaitToBePromoted AND Decommissioning are set to true the leader WON'T send any append entries to it.

PROMOTING A NODE
Promoting a node can be used to promote back a demoted node.
It is also used when a new node is added into the cluster, see ADDING A NODE section
Somehow if both WaitToBePromoted AND Decommissioning are set to true, promoting the node will
only will set Decommissioning to false.
The node will then retry the membership process to be promoted by the leader

REMOVE A NODE
To remove a node it MUST first be demoted otherwise and error will be returned.
If demoded, it will be removed from the replication process and a log entry config will be appended
without this node and replicated to the cluster.
The devops can then safely destroy the node.
A node with WaitToBePromoted set to true can be safely removed or force removed.

FORCE REMOVE A NODE
This must be done with cautious because:
- NO demoting node is involved
- NO check is done
The leader symply remove it from the replication process and a log entry config without this node.
A node with WaitToBePromoted set to true can be safely removed or force removed.

LEAVE ON TERMINATE A NODE
Before the node is going down and when LeaveOnTerminate flag is set, it will
send a leaveOnTerminate command to tell the leader that it won't be part of the cluster
anymore. This flag is usually used by read replicas.
No checks will be done when this flag is set so it must be set with cautious.

*/

// NewRafty instantiate rafty with default configuration
// with server address and its id
func NewRafty(address net.TCPAddr, id string, options Options, logStore LogStore, clusterStore ClusterStore, fsm StateMachine, snapshot SnapshotStore) (*Rafty, error) {
	r := &Rafty{
		rpcPreVoteRequestChan:                  make(chan RPCRequest),
		rpcVoteRequestChan:                     make(chan RPCRequest),
		rpcAppendEntriesRequestChan:            make(chan RPCRequest),
		rpcAppendEntriesReplicationRequestChan: make(chan RPCRequest),
		rpcAskNodeIDChan:                       make(chan RPCResponse),
		rpcClientGetLeaderChan:                 make(chan RPCResponse),
		rpcBootstrapClusterRequestChan:         make(chan RPCRequest),
		rpcInstallSnapshotRequestChan:          make(chan RPCRequest),
	}
	r.Address = address
	r.id = id

	if options.Logger == nil {
		var zlogger zerolog.Logger
		if options.logSource == "" {
			zlogger = logger.NewLogger().With().Str("logProvider", "rafty_test").Logger()
		} else {
			zlogger = logger.NewLogger().With().Str("logProvider", "rafty_test").Str("logSource", options.logSource).Logger()
		}
		options.Logger = &zlogger
	}

	if options.TimeMultiplier == 0 {
		options.TimeMultiplier = 1
	}
	if options.TimeMultiplier > 10 {
		options.TimeMultiplier = 10
	}

	if options.MinimumClusterSize == 0 {
		options.MinimumClusterSize = 3
	}

	if options.MaxAppendEntries == 0 {
		options.MaxAppendEntries = maxAppendEntries
	}

	if options.MaxAppendEntries > maxAppendEntries {
		options.MaxAppendEntries = maxAppendEntries
	}

	if options.ElectionTimeout < electionTimeout {
		options.ElectionTimeout = electionTimeout
	}

	if options.HeartbeatTimeout < heartbeatTimeout {
		options.HeartbeatTimeout = heartbeatTimeout
	}

	if options.ElectionTimeout < options.HeartbeatTimeout {
		options.ElectionTimeout, options.HeartbeatTimeout = options.HeartbeatTimeout, options.ElectionTimeout
	}

	if options.ForceStopTimeout == 0 {
		options.ForceStopTimeout = 60 * time.Second
	}

	if options.SnapshotInterval == 0 || options.SnapshotInterval <= 10*time.Second {
		options.SnapshotInterval = time.Hour
	}

	if options.SnapshotThreshold == 0 {
		options.SnapshotThreshold = 64
	}

	if options.DataDir == "" {
		return nil, ErrDataDirRequired
	}

	r.options = options
	r.Logger = options.Logger

	r.connectionManager = connectionManager{
		id:                           id,
		address:                      address.String(),
		logger:                       options.Logger,
		connections:                  make(map[string]*grpc.ClientConn),
		clients:                      make(map[string]raftypb.RaftyClient),
		leadershipTransferInProgress: &r.leadershipTransferInProgress,
	}

	if r.options.IsSingleServerCluster {
		r.leadershipTransferDisabled.Store(true)
	}
	r.timer = time.NewTicker(r.randomElectionTimeout())
	r.quitCtx, r.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	r.logStore = logStore
	r.clusterStore = clusterStore
	r.fsm = fsm
	if snapshot == nil {
		r.snapshot = NewSnapshot(r.options.DataDir, 3)
	} else {
		r.snapshot = snapshot
	}

	metadata, err := clusterStore.GetMetadata()
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}
	if err = r.restore(metadata); err != nil {
		return nil, err
	}

	// must stay after restoring metadata
	if err = r.parsePeers(); err != nil {
		return nil, fmt.Errorf("fail to parse peer ip/port %w", err)
	}

	return r, nil
}

// Start permits to start the node with the provided configuration
func (r *Rafty) Start() error {
	var err error

	if r.id == "" {
		r.id = uuid.NewString()
		r.connectionManager.id = r.id
		if err := r.clusterStore.StoreMetadata(r.buildMetadata()); err != nil {
			return fmt.Errorf("fail to persist metadata %w", err)
		}
	}

	r.metrics = newMetrics(r.options.MetricsNamespacePrefix, r.id, r.options.IsVoter)
	r.metrics.setNodeStateGauge(Down)

	r.mu.Lock()
	if r.listener, err = net.Listen(r.Address.Network(), r.Address.String()); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("fail to listen gRPC server %w", err)
	}

	r.grpcServer = grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	rpcManager := rpcManager{
		rafty: r,
	}
	r.mu.Unlock()

	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(r.grpcServer, healthcheck)
	raftypb.RegisterRaftyServer(r.grpcServer, &rpcManager)
	reflection.Register(r.grpcServer)
	channelzservice.RegisterChannelzServiceToServer(r.grpcServer)

	r.wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		defer r.wg.Done()
		errChan <- r.grpcServer.Serve(r.listener)
	}()

	select {
	case err := <-errChan:
		return fmt.Errorf("fail to start gRPC server %w", err)
	default:
	}

	r.isRunning.Store(true)
	if r.getState() == Down {
		r.switchState(Follower, stepUp, true, r.currentTerm.Load())
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("isVoter", fmt.Sprintf("%t", r.options.IsVoter)).
			Msgf("Node successfully started")
	}

	go r.start()
	<-r.quitCtx.Done()
	r.Stop()
	return nil
}

// start run sub requirements from Start() func
func (r *Rafty) start() {
	r.wg.Add(1)
	defer r.wg.Done()

	go r.commonLoop()
	if !r.checkNodeIDs() {
		r.sendAskNodeIDRequest()
		r.startClusterWithMinimumSize()
	}

	go r.snapshotLoop()
	r.stateLoop()
}

// Stop permits to stop the gRPC server by using signal
func (r *Rafty) Stop() {
	// if statement is need otherwise some tests panics
	if r.isRunning.Load() {
		r.stop()
	}
}

// stop permits to stop the gRPC server and Rafty with the provided configuration
func (r *Rafty) stop() {
	if r.options.LeaveOnTerminate && r.waitForLeader() {
		_ = r.LeaveOnTerminateMember(5, r.Address.String(), r.id, r.options.IsVoter)
	}
	r.isRunning.Store(false)
	r.stopCtx()
	// this is just a safe guard when invoking Stop function directly
	r.switchState(Down, stepDown, true, r.currentTerm.Load())
	r.release()

	timer := time.AfterFunc(r.options.ForceStopTimeout, func() {
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Node couldn't stop gracefully in time, doing force stop")
		r.grpcServer.Stop()
	})
	defer timer.Stop()
	r.grpcServer.GracefulStop()
	r.wg.Wait()
	if err := r.logStore.Close(); err != nil {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Fail to close store")
	}
	r.Logger.Info().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Node successfully stopped with term %d", r.currentTerm.Load())
}

// checkNodeIDs check if we gather all node ids.
// If it is, we will check if there is a leader
// otherwise we will wait to fetch them all.
// If we are restarting the node, we normally already have all nodes ids
// so we won't go further
func (r *Rafty) checkNodeIDs() bool {
	peers, count := r.getPeers()

	for _, peer := range peers {
		if peer.ID != "" {
			count--
			if r.clusterSizeCounter.Load()+1 < r.options.MinimumClusterSize && !r.minimumClusterSizeReach.Load() && peer.IsVoter {
				r.clusterSizeCounter.Add(1)
			}
		}
	}

	if count == 0 {
		r.minimumClusterSizeReach.Store(true)
		return true
	}
	return false
}

// startClusterWithMinimumSize allows us to reach minimum cluster size
// before doing anything else
func (r *Rafty) startClusterWithMinimumSize() {
	timer := time.NewTicker(5 * time.Second)
	defer timer.Stop()
	for r.getState() != Down && !r.minimumClusterSizeReach.Load() {
		select {
		case <-r.quitCtx.Done():
			return

		case <-timer.C:
			if r.options.MinimumClusterSize == r.clusterSizeCounter.Load()+1 {
				r.minimumClusterSizeReach.Store(true)
				r.Logger.Info().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Msgf("Minimum cluster size has been reached: %d out of %d", r.clusterSizeCounter.Load()+1, r.options.MinimumClusterSize)
				return
			}
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("clusterSize", fmt.Sprintf("%d", r.clusterSizeCounter.Load()+1)).
				Msgf("Cluster size not reached")
			go r.sendAskNodeIDRequest()
		}
	}
}

// buildMetadata will build metadata to be then stored
// in long term storage
func (r *Rafty) buildMetadata() []byte {
	r.wg.Add(1)
	defer r.wg.Done()

	peers, _ := r.getPeers()
	data := metadata{
		Id:                     r.id,
		CurrentTerm:            r.currentTerm.Load(),
		VotedFor:               r.votedFor,
		LastApplied:            r.lastApplied.Load(),
		LastAppliedConfigIndex: r.lastAppliedConfigIndex.Load(),
		LastAppliedConfigTerm:  r.lastAppliedConfigTerm.Load(),
		Configuration: Configuration{
			ServerMembers: peers,
		},
		LastIncludedIndex: r.lastIncludedIndex.Load(),
		LastIncludedTerm:  r.lastIncludedTerm.Load(),
	}

	result, _ := json.Marshal(data)
	return result
}

// restore allows us to restore last applied configuration
// and many other requirements
func (r *Rafty) restore(result []byte) error {
	if len(result) > 0 {
		var data metadata
		if err := json.Unmarshal(result, &data); err != nil {
			return err
		}

		r.id = data.Id
		r.currentTerm.Store(data.CurrentTerm)
		r.votedFor = data.VotedFor
		r.lastApplied.Store(data.LastApplied)
		r.lastAppliedConfigIndex.Store(data.LastAppliedConfigIndex)
		r.lastAppliedConfigTerm.Store(data.LastAppliedConfigTerm)
		r.lastIncludedIndex.Store(data.LastIncludedIndex)
		r.lastIncludedTerm.Store(data.LastIncludedTerm)
		if r.options.BootstrapCluster && !r.isBootstrapped.Load() && data.LastAppliedConfigIndex > 0 {
			r.isBootstrapped.Store(true)
		}
		if len(data.Configuration.ServerMembers) > 0 {
			r.configuration.ServerMembers = data.Configuration.ServerMembers
		}

		snapshots := r.snapshot.List()
		if len(snapshots) > 0 {
			metadata := snapshots[0]
			_, file, err := r.snapshot.PrepareSnapshotReader(metadata.SnapshotName)
			if err != nil {
				return err
			}
			defer func() {
				_ = file.Close()
			}()
			if err := r.fsm.Restore(file); err != nil {
				return err
			}
		}
		return nil
	}

	for _, server := range r.options.InitialPeers {
		r.configuration.ServerMembers = append(r.configuration.ServerMembers, Peer{Address: server.Address})
	}
	return nil
}

// storeLogs will store logs to the long term storage.
// If we cannot store them, the current node will step down
// as follower
func (r *Rafty) storeLogs(entries []*LogEntry) {
	currentTerm := r.currentTerm.Load()
	lastLogIndex := r.lastLogIndex.Load()

	for _, entry := range entries {
		lastLogIndex += 1
		entry.Index = lastLogIndex
		entry.Term = currentTerm
	}

	if err := r.logStore.StoreLogs(entries); err != nil {
		r.switchState(Follower, stepDown, true, currentTerm)
	}

	r.lastLogIndex.Store(lastLogIndex)
	r.lastLogTerm.Store(currentTerm)
}

// getPreviousLogIndexAndTerm will return the previous index and term
// of last committed log
func (r *Rafty) getPreviousLogIndexAndTerm() (uint64, uint64) {
	if r.lastLogIndex.Load() == 0 {
		return 0, 0
	}

	// if equal to last snapshot
	if r.nextIndex.Load()-1 == r.lastIncludedIndex.Load() {
		return r.lastIncludedIndex.Load(), r.lastIncludedTerm.Load()
	}

	result, err := r.logStore.GetLogByIndex(r.lastLogIndex.Load())
	if err != nil {
		return 0, 0
	}

	return result.Index, result.Term
}
