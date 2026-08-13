package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"ride-sharing/services/trip-service/internal/infrastructure/events"
	"ride-sharing/services/trip-service/internal/infrastructure/grpc"
	"ride-sharing/services/trip-service/internal/infrastructure/repository"
	"ride-sharing/services/trip-service/internal/service"
	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	"syscall"

	grpc_server "google.golang.org/grpc"
)

var (
	grpcAddr = env.GetString("GRPC_ADDR", ":9093")
	ampqUri  = env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")
)

func main() {

	// Shutdown for K8S syscalls
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		cancel()
	}()

	// Initalize layers and server
	inmemRepo := repository.NewInmemRepository()
	svc := service.NewTripService(inmemRepo)
	// RabbitMQ
	log.Println("Starting RabbitMQ connection")
	rabbitMQ, err := messaging.NewRabbitMQ(ampqUri)
	if err != nil {
		log.Fatal(err)
	}
	defer rabbitMQ.Close()

	publisher := events.NewTripEventPublisher(rabbitMQ)
	consumer := events.NewDriverConsumer(rabbitMQ, svc)
	go consumer.Listen()

	// gRPC server
	server := grpc_server.NewServer()
	grpc.NewGRPCHandler(server, svc, publisher)

	// Run gRPC server
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Starting TRIP Service on port %s", grpcAddr)
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Printf("Error initializing TRIP Service: %s", err)
			cancel()
		}
	}()

	// Graceful shutdown
	<-ctx.Done()
	log.Println("Shutting down TRIP service")
	server.GracefulStop()
}
