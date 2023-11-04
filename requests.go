package rafty

import (
	"context"
	"log"

	"github.com/Lord-Y/rafty/grpcrequests"
)

func (g *ProtobufSVC) SayHello(ctx context.Context, in *grpcrequests.HelloRequest) (*grpcrequests.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &grpcrequests.HelloReply{Message: "Hello " + in.GetName()}, nil
}
