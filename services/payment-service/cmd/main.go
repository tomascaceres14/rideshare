package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"ride-sharing/services/payment-service/internal/infrastructure/events"
	stripeProcessor "ride-sharing/services/payment-service/internal/infrastructure/stripe"
	"ride-sharing/services/payment-service/internal/service"
	"ride-sharing/services/payment-service/pkg/types"
	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	"ride-sharing/shared/tracing"
)

var (
	GrpcAddr       = env.GetString("GRPC_ADDR", ":9004")
	enviroment     = env.GetString("ENVIROMENT", "development")
	jaegerEndpoint = env.GetString("JAEGER_ENDPOINT", "http://jaeger:14268/api/traces")
)

func main() {
	rabbitMqURI := env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")

	// Setup graceful shutdown
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
		ServiceName:    "payment-service",
		Enviroment:     enviroment,
		JaegerEndpoint: jaegerEndpoint,
	}

	traceShutdown, err := tracing.InitTracer(tracerCfg)
	if err != nil {
		log.Fatalf("Error initializing tracing: %v", err)
	}

	defer traceShutdown(ctx)

	appURL := env.GetString("APP_URL", "http://localhost:3000")

	// Stripe config
	stripeCfg := &types.PaymentConfig{
		StripeSecretKey: env.GetString("STRIPE_SECRET_KEY", ""),
		SuccessURL:      env.GetString("STRIPE_SUCCESS_URL", appURL+"?payment=success"),
		CancelURL:       env.GetString("STRIPE_CANCEL_URL", appURL+"?payment=cancel"),
	}

	if stripeCfg.StripeSecretKey == "" {
		log.Fatalf("STRIPE_SECRET_KEY is not set")
		return
	}

	// RabbitMQ connection
	rabbitmq, err := messaging.NewRabbitMQ(rabbitMqURI)
	if err != nil {
		log.Fatal(err)
	}
	defer rabbitmq.Close()

	paymentProcessor := stripeProcessor.NewStripeClient(stripeCfg)
	svc := service.NewPaymentService(paymentProcessor)
	consumer := events.NewTripEventConsumer(rabbitmq, svc)

	go func() {
		if err := consumer.Listen(); err != nil {
			log.Printf("Error listening consumer: %v", err)
		}
	}()

	log.Println("Starting RabbitMQ connection")

	// Wait for shutdown signal
	<-ctx.Done()
	log.Println("Shutting down payment service...")
}
