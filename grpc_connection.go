package rafty

// disconnectToPeers permits to disconnect to all grpc servers
// from which this client is connected to
func (r *Rafty) disconnectToPeers() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, peer := range r.Peers {
		if peer.client != nil {
			// we won't check errors on the targetted server it maybe already down
			_ = peer.client.Close()
			peer.client = nil
		}
	}
}
