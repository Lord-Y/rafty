package rafty

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// NewServer instantiate default configuration of the gRPC server and rafty to later start or stop it
func NewServer(address net.TCPAddr) *Rafty {
	rafty := NewRafty()
	rafty.Address = address
	return rafty
}

// Start permits to start the gRPC server with the provided configuration
// ready parameter is the channel that will be used by server to accept client requests
func (r *Rafty) Start() error {
	r.mu.Lock()
	var err error
	r.listener, err = net.Listen(r.Address.Network(), r.Address.String())
	if err != nil {
		r.mu.Unlock()
		return errors.Wrap(err, "Fail to listen gRPC server")
	}

	r.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	)
	rm := rpcManager{
		rafty: r,
	}
	r.mu.Unlock()

	raftypb.RegisterRaftyServer(r.grpcServer, &rm)
	reflection.Register(r.grpcServer)

	r.Logger.Info().Msgf("gRPC server at %s is starting", r.Address.String())

	var stop context.CancelFunc
	r.mu.Lock()
	r.quitCtx, stop = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	r.mu.Unlock()
	defer stop()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		err := r.grpcServer.Serve(r.listener)
		if err != nil {
			r.Logger.Fatal().Err(err).Msg("Fail to start grpc server")
		}
	}()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.start()
	}()

	r.Logger.Info().Msgf("gRPC server at %s has successfully started", r.Address.String())

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		// stop go routine when os signal is receive or ctrl+c
		<-r.quitCtx.Done()
		r.Stop()
	}()
	r.wg.Wait()
	return nil
}

// Stop permits to stop the gRPC server and Rafty with the provided configuration
func (r *Rafty) Stop() {
	// this is just a safe guard when invoking Stop function directly
	r.switchState(Down, true, r.getCurrentTerm())
	stopped := make(chan struct{})
	go func() {
		r.grpcServer.GracefulStop()
		close(stopped)
	}()
	t := time.NewTimer(30 * time.Second)
	<-t.C
	t.Stop()
	r.disconnectToPeers()
	r.closeAllFilesDescriptor()
}
