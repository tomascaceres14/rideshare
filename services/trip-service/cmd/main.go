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
	"ride-sharing/shared/tracing"
	"syscall"

	grpc_server "google.golang.org/grpc"
)

var (
	grpcAddr       = env.GetString("GRPC_ADDR", ":9093")
	ampqUri        = env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")
	enviroment     = env.GetString("ENVIROMENT", "development")
	jaegerEndpoint = env.GetString("JAEGER_ENDPOINT", "http://jaeger:14268/api/traces")
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

	// Initialize tracing
	tracerCfg := tracing.Config{
		ServiceName:    "trip-service",
		Enviroment:     enviroment,
		JaegerEndpoint: jaegerEndpoint,
	}

	traceShutdown, err := tracing.InitTracer(tracerCfg)
	if err != nil {
		log.Fatalf("Error initializing tracing: %v", err)
	}

	defer traceShutdown(ctx)

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
	server := grpc_server.NewServer(tracing.WithTracingInterceptors()...)
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
