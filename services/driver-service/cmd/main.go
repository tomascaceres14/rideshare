package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"ride-sharing/services/driver-service/internal/infrastructure/events"
	"ride-sharing/services/driver-service/internal/infrastructure/grpc"
	"ride-sharing/services/driver-service/internal/infrastructure/repository"
	"ride-sharing/services/driver-service/internal/service"
	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	"ride-sharing/shared/tracing"
	"syscall"

	grpc_server "google.golang.org/grpc"
)

var (
	grpcAddr       = env.GetString("GRPC_ADDR", ":9092")
	amqpUri        = env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")
	enviroment     = env.GetString("ENVIROMENT", "development")
	jaegerEndpoint = env.GetString("JAEGER_ENDPOINT", "http://jaeger:14268/api/traces")
)

func main() {

	// Shutdown for K8S syscalls
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize tracing
	tracerCfg := tracing.Config{
		ServiceName:    "driver-service",
		Enviroment:     enviroment,
		JaegerEndpoint: jaegerEndpoint,
	}

	traceShutdown, err := tracing.InitTracer(tracerCfg)
	if err != nil {
		log.Fatalf("Error initializing tracing: %v", err)
	}

	defer traceShutdown(ctx)

	server := grpc_server.NewServer(tracing.WithTracingInterceptors()...)
	repo := repository.NewInmemRepository()
	svc := service.NewDriverService(repo)
	grpc.NewGRPCHandler(server, svc)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		cancel()
	}()

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatal(err)
	}

	// RabbitMQ
	log.Println("Starting RabbitMQ connection")
	rabbitMQ, err := messaging.NewRabbitMQ(amqpUri)
	if err != nil {
		log.Fatal(err)
	}

	defer rabbitMQ.Close()

	consumer := events.NewTripEventConsumer(rabbitMQ, svc)
	go func() {
		if err := consumer.Listen(); err != nil {
			log.Fatalf("Error listening message: %s", err)
		}
	}()

	// Run server
	log.Printf("Starting DRIVER Service on port %s", grpcAddr)
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Printf("Error initializing DRIVER Service: %s", err)
			cancel()
		}
	}()

	// Graceful shutdown
	<-ctx.Done()
	log.Println("Shutting down DRIVER service")
	server.GracefulStop()
}
