package main

import (
	"encoding/json"
	"log"
	"net/http"
	"ride-sharing/services/api-gateway/grpc_clients"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	pb "ride-sharing/shared/proto/driver"

	"github.com/gorilla/websocket"
)

var (
	connManager = messaging.NewConnectionManager()
	amqpUri     = env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")
)

func handleRiderWebSocket(w http.ResponseWriter, r *http.Request, rb *messaging.RabbitMQ) {
	conn, err := connManager.Upgrade(w, r)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %s\n", err)
		return
	}

	defer conn.Close()

	userID := r.URL.Query().Get("userID")
	if userID == "" {
		log.Printf("No user ID provided\n")
		return
	}

	connManager.Add(userID, conn)
	defer connManager.Remove(userID)

	// Listen incoming messages
	queues := []string{
		messaging.NotifyRiderNoDriversFoundQueue,
		messaging.NotifyDriverAssignQueue,
		messaging.NotifyPaymentSessionCreatedQueue,
	}

	for _, q := range queues {
		consumer := messaging.NewQueueConsumer(rb, connManager, q)
		if err := consumer.Start(); err != nil {
			log.Printf("Error starting consumer for queue: %s. Error: %v", q, err)
		}
	}

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading WS message: %s", err)
			break
		}

		log.Printf("Message received: %s", msg)
	}
}

func handleDriverWebSocket(w http.ResponseWriter, r *http.Request, rb *messaging.RabbitMQ) {
	conn, err := connManager.Upgrade(w, r)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %s\n", err)
		return
	}
	defer conn.Close()

	driverID := r.URL.Query().Get("userID")
	if driverID == "" {
		log.Printf("No driver ID provided\n")
		return
	}

	pkgSlug := r.URL.Query().Get("packageSlug")
	if pkgSlug == "" {
		log.Printf("No package slug provided\n")
		return
	}

	connManager.Add(driverID, conn)

	driverService, err := grpc_clients.NewDriverServiceClient()
	if err != nil {
		log.Printf("Error connecting to driver service: %s", err)
		conn.WriteMessage(websocket.TextMessage, []byte(err.Error()))
		return
	}

	// Unregister driver when connection gets closed
	defer func() {
		connManager.Remove(driverID)
		driverService.Client.UnregisterDriver(r.Context(), &pb.RegisterDriverRequest{
			DriverID:    driverID,
			PackageSlug: pkgSlug,
		})
		driverService.Close()
		log.Printf("Driver unregistered: %s", driverID)
	}()

	req := &pb.RegisterDriverRequest{
		DriverID:    driverID,
		PackageSlug: pkgSlug,
	}

	registerDriverResponse, err := driverService.Client.RegisterDriver(r.Context(), req)
	if err != nil {
		log.Printf("Error registering driver: %s", err)
		return
	}

	msg := contracts.WSMessage{
		Type: contracts.DriverCmdRegister,
		Data: registerDriverResponse.Driver,
	}

	if err := connManager.SendMessage(driverID, msg); err != nil {
		log.Printf("Error reading message: %s", err)
		return
	}

	// Listen incoming messages
	queues := []string{
		messaging.DriverCmdTripRequestQueue,
	}

	for _, q := range queues {
		consumer := messaging.NewQueueConsumer(rb, connManager, q)
		if err := consumer.Start(); err != nil {
			log.Printf("Error starting consumer for queue: %s. Error: %v", q, err)
		}
	}

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading WS message: %s", err)
			continue
		}

		type DriverMessage struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}

		var driverMessage DriverMessage
		if err := json.Unmarshal(msg, &driverMessage); err != nil {
			log.Printf("Error unmarshaling message: %s", err)
			continue
		}

		// Handle different message types.
		switch driverMessage.Type {
		case contracts.DriverCmdLocation:
			// handle driver location update
			continue
		case contracts.DriverCmdTripAccept, contracts.DriverCmdTripDecline:
			// Forward message to RabbitMQ
			if err := rb.PublishMessage(r.Context(), driverMessage.Type, contracts.AmqpMessage{
				OwnerID: driverID,
				Data:    driverMessage.Data,
			}); err != nil {
				log.Printf("Error publishing message to RabbtiMQ: %v", err)
			}
		default:
			log.Printf("Unknown message type: %v", driverMessage.Type)
		}
	}
}
