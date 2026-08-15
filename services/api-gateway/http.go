package main

import (
	"encoding/json"
	"log"
	"net/http"
	"ride-sharing/services/api-gateway/grpc_clients"
	"ride-sharing/shared/contracts"
	json_utils "ride-sharing/shared/json"
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

func handleStripeResponse(w http.ResponseWriter, r *http.Request) {}
