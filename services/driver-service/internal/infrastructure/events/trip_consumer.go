package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"ride-sharing/services/driver-service/internal/domain"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/messaging"
	pb "ride-sharing/shared/proto/trip"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TripEventConsumer struct {
	rabbitMQ *messaging.RabbitMQ
	svc      domain.DriverService
}

func NewTripEventConsumer(rmq *messaging.RabbitMQ, svc domain.DriverService) *TripEventConsumer {
	return &TripEventConsumer{
		rabbitMQ: rmq,
		svc:      svc,
	}
}

func (tc *TripEventConsumer) Listen() error {
	tc.rabbitMQ.ConsumeMessages(messaging.FindAvailableDriversQueue, func(ctx context.Context, msg amqp.Delivery) error {
		var tripEvent contracts.AmqpMessage
		if err := json.Unmarshal(msg.Body, &tripEvent); err != nil {
			fmt.Printf("Error unmarshaling message: %s", err)
			return err
		}

		var payload *pb.Trip
		if err := json.Unmarshal(tripEvent.Data, &payload); err != nil {
			log.Printf("Error unmarshaling payload: %s", err)
			return err
		}

		log.Printf("Driver received trip message")
		switch msg.RoutingKey {
		case contracts.TripEventCreated, contracts.TripEventDriverNotInterested:
			return tc.handleFindAndNotifyDrivers(ctx, payload)
		}

		log.Printf("Unknown trip event: %+v", payload)
		return nil
	})
	return nil
}

func (tc *TripEventConsumer) handleFindAndNotifyDrivers(ctx context.Context, trip *pb.Trip) error {
	suitables := tc.svc.FindAvailableDrivers(ctx, trip.SelectedFare.PackageSlug)
	if len(suitables) == 0 {
		// Notify user that not driver has been found
		err := tc.rabbitMQ.PublishMessage(ctx, contracts.TripEventNoDriversFound, contracts.AmqpMessage{
			OwnerID: trip.UserID,
		})
		if err != nil {
			log.Printf("Failed to publish message to exchange: %v", err)
		}
		return fmt.Errorf("No drivers found.")
	}

	selectedDriver := rand.IntN(len(suitables))
	selectedDriverID := suitables[selectedDriver]

	response, err := json.Marshal(trip)
	if err != nil {
		return err
	}

	// Notify user of potential trip
	log.Printf("Driver found. ID: %s", selectedDriverID)
	err = tc.rabbitMQ.PublishMessage(ctx, contracts.DriverCmdTripRequest, contracts.AmqpMessage{
		OwnerID: selectedDriverID,
		Data:    response,
	})
	if err != nil {
		log.Printf("Failed to publish message to exchange: %v", err)
	}

	return nil
}
