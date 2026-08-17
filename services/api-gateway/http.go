package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"ride-sharing/services/api-gateway/grpc_clients"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/env"
	json_utils "ride-sharing/shared/json"
	"ride-sharing/shared/messaging"

	"github.com/stripe/stripe-go/v86"
	"github.com/stripe/stripe-go/v86/webhook"
)

func handleTripReview(w http.ResponseWriter, r *http.Request) {

	var reqBody previewTripRequest
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	defer r.Body.Close()

	if reqBody.UserID == "" {
		http.Error(w, "userID is required.", http.StatusBadRequest)
		return
	}

	log.Println("Body accepted:", reqBody)

	// Call trip service.
	tripService, err := grpc_clients.NewTripServiceClient()
	if err != nil {
		log.Fatal(err)
	}

	defer tripService.Close()

	tripPreview, err := tripService.Client.PreviewTrip(r.Context(), reqBody.ToProto())
	if err != nil {
		log.Printf("Failed to preview trip: %s", err)
		http.Error(w, "Failed to preview trip", 500)
		return
	}

	res := contracts.APIResponse{Data: tripPreview}
	json_utils.WriteJSON(w, http.StatusCreated, res)
}

func handleTripStart(w http.ResponseWriter, r *http.Request) {
	var reqBody startTripRequest
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	defer r.Body.Close()

	if reqBody.UserID == "" {
		http.Error(w, "userID is required.", http.StatusBadRequest)
		return
	}

	if reqBody.RideFareID == "" {
		http.Error(w, "rideFareID is required.", http.StatusBadRequest)
		return
	}

	tripService, err := grpc_clients.NewTripServiceClient()
	if err != nil {
		log.Fatal(err)
	}
	defer tripService.Close()

	createdTrip, err := tripService.Client.CreateTrip(r.Context(), reqBody.ToProto())
	if err != nil {
		log.Printf("Failed to create trip: %s", err)
		http.Error(w, "Failed to create trip", 500)
		return
	}

	res := contracts.APIResponse{Data: createdTrip}
	json_utils.WriteJSON(w, http.StatusCreated, res)
}

func handleStripeWebhook(w http.ResponseWriter, r *http.Request, rb *messaging.RabbitMQ) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	webhookKey := env.GetString("STRIPE_WEBHOOK_KEY", "")
	if webhookKey == "" {
		log.Printf("Webhook key not found")
		return
	}

	event, err := webhook.ConstructEventWithOptions(
		body,
		r.Header.Get("Stripe-Signature"),
		webhookKey,
		webhook.ConstructEventOptions{
			IgnoreAPIVersionMismatch: true,
		},
	)
	if err != nil {
		log.Printf("Error verifying webhook signature: %v", err)
		http.Error(w, "Invalid signature", http.StatusBadRequest)
	}

	log.Printf("Received Stripe event: %v", event)

	switch event.Type {
	case "checkout.session.completed":
		var session stripe.CheckoutSession

		err := json.Unmarshal(event.Data.Raw, &session)
		if err != nil {
			log.Printf("Error parsing webhook JSON: %v", err)
			http.Error(w, "Invalid payload", http.StatusBadRequest)
			return
		}

		payload := messaging.PaymentStatusUpdateData{
			TripID:   session.Metadata["trip_id"],
			UserID:   session.Metadata["user_id"],
			DriverID: session.Metadata["driver_id"],
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Error marshalling payload: %v", err)
			http.Error(w, "Failed to marshal payload", http.StatusInternalServerError)
			return
		}

		message := contracts.AmqpMessage{
			OwnerID: session.Metadata["user_id"],
			Data:    payloadBytes,
		}

		if err := rb.PublishMessage(
			r.Context(),
			contracts.PaymentEventSuccess,
			message,
		); err != nil {
			log.Printf("Error publishing payment event: %v", err)
			http.Error(w, "Failed to publish payment event", http.StatusInternalServerError)
			return
		}
	}
}
