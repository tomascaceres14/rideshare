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
)

var (
	httpAddr = env.GetString("HTTP_ADDR", ":8081")
)

func main() {
	log.Println("Starting API Gateway")

	mux := http.NewServeMux()

	rb, err := messaging.NewRabbitMQ(amqpUri)
	if err != nil {
		log.Printf("Error creating RabbitMQ instance: %v", err)
		return
	}

	mux.HandleFunc("POST /trip/preview", enableCORS(handleTripReview))
	mux.HandleFunc("POST /trip/start", enableCORS(handleTripStart))
	mux.HandleFunc("/ws/drivers", func(w http.ResponseWriter, r *http.Request) {
		handleDriverWebSocket(w, r, rb)
	})
	mux.HandleFunc("/ws/riders", func(w http.ResponseWriter, r *http.Request) {
		handleRiderWebSocket(w, r, rb)
	})

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
