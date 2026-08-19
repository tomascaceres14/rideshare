package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	"ride-sharing/shared/tracing"
)

var (
	httpAddr       = env.GetString("HTTP_ADDR", ":8081")
		enviroment     = env.GetString("ENVIROMENT", "development")
		jaegerEndpoint = env.GetString("JAEGER_ENDPOINT", "http://jaeger:14268/api/traces")
)

func main() {
	log.Println("Starting API Gateway")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize tracing
	tracerCfg := tracing.Config{
		ServiceName:    "api-gateway",
		Enviroment:     enviroment,
		JaegerEndpoint: jaegerEndpoint,
	}

	traceShutdown, err := tracing.InitTracer(tracerCfg)
	if err != nil {
		log.Fatalf("Error initializing tracing: %v", err)
	}

	defer traceShutdown(ctx)

	mux := http.NewServeMux()

	rb, err := messaging.NewRabbitMQ(amqpUri)
	if err != nil {
		log.Printf("Error creating RabbitMQ instance: %v", err)
		return
	}

	mux.Handle("POST /trip/preview", tracing.WrapHandlerFunc(enableCORS(handleTripReview), "/trip/preview"))
	mux.Handle("POST /trip/start", tracing.WrapHandlerFunc(enableCORS(handleTripStart), "/trip/start"))
	mux.Handle("POST /webhook/stripe", tracing.WrapHandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleStripeWebhook(w, r, rb)
	}, "/webhook/stripe"))
	mux.Handle("/ws/drivers", tracing.WrapHandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleDriverWebSocket(w, r, rb)
	}, "/ws/drivers"))
	mux.Handle("/ws/riders", tracing.WrapHandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleRiderWebSocket(w, r, rb)
	}, "/ws/riders"))

	sv := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	svStartCh := make(chan error, 1)
	go func() {
		log.Printf("API Gateway listening on port %s", httpAddr)
		svStartCh <- sv.ListenAndServe()
	}()

	// Graceful shutdown. Wait for OS, K8s signal or error and shutdown after max 10s
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-svStartCh:
		log.Printf("Error starting the server: %s", err)

	case sig := <-shutdown:
		log.Printf("Server shutting down with signal: %s", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		if err := sv.Shutdown(ctx); err != nil {
			log.Printf("Could not shutdown gracefully: %s", err)
			sv.Close()
		}
	}
}
