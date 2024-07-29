package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Lord-Y/rafty"
	"github.com/Lord-Y/rafty/grpcrequests"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", rafty.GRPCAddress, rafty.GRPCPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Fail to connect to grpc server %s", err.Error())
	}
	defer conn.Close()
	client := grpcrequests.NewGreeterClient(conn)

	// ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second, errors.New("Fail to connect after 1 second"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	name := "rafty"
	response, err := client.SayHello(ctx, &grpcrequests.HelloRequest{Name: name})
	if err != nil {
		log.Fatalf("Fail to greet grpc server %s", err.Error())
	}
	log.Println("Greeting", response.GetMessage())
}
