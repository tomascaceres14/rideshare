package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"ride-sharing/services/trip-service/internal/domain"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/messaging"

	amqp "github.com/rabbitmq/amqp091-go"
)

type DriverConsumer struct {
	rabbitMQ *messaging.RabbitMQ
	svc      domain.TripService
}

func NewDriverConsumer(rmq *messaging.RabbitMQ, svc domain.TripService) *DriverConsumer {
	return &DriverConsumer{
		rabbitMQ: rmq,
		svc:      svc,
	}
}

func (dc *DriverConsumer) Listen() error {
	dc.rabbitMQ.ConsumeMessages(messaging.DriverTripResponseQueue, func(ctx context.Context, msg amqp.Delivery) error {
		var message contracts.AmqpMessage
		if err := json.Unmarshal(msg.Body, &message); err != nil {
			fmt.Printf("Error unmarshaling message: %s", err)
			return err
		}

		var payload messaging.DriverTripResponseData
		if err := json.Unmarshal(message.Data, &payload); err != nil {
			log.Printf("Error unmarshaling payload: %s", err)
			return err
		}

		log.Printf("Trip received driver message: %+v", payload)
		switch msg.RoutingKey {
		case contracts.DriverCmdTripAccept:
			return dc.handleTripAccepted(ctx, &payload)
		case contracts.DriverCmdTripDecline:
			log.Println("declined trip")
			return nil
		}

		log.Printf("Unknown driver event: %+v", msg.RoutingKey)
		return nil
	})
	return nil
}

func (dc *DriverConsumer) handleTripAccepted(ctx context.Context, driverResponse *messaging.DriverTripResponseData) error {
	// 1. Check if trip exists in db
	trip, err := dc.svc.GetTripByID(ctx, driverResponse.TripID)
	if err != nil {
		fmt.Printf("Error fetching trip: %s\n", err)
		return err
	}

	if trip == nil {
		fmt.Printf("Trip is nil: %s", driverResponse.TripID)
		return nil
	}

	// 2. Update trip
	trip.Status = "accepted"
	trip.Driver = driverResponse.Driver

	if trip, err = dc.svc.UpdateTrip(ctx, trip); err != nil {
		log.Printf("Error updating trip: %v", err)
		return err
	}

	// 3. Driver assigned. Publish event to rabbitmq
	newTrip, err := json.Marshal(trip)
	if err != nil {
		return err
	}
	if err := dc.rabbitMQ.PublishMessage(ctx, contracts.TripEventDriverAssigned, contracts.AmqpMessage{
		OwnerID: driverResponse.Driver.Id,
		Data:    newTrip,
	}); err != nil {
		log.Printf("Error sending message: %s. Error: %v", contracts.TripEventDriverAssigned, err)
		return err
	}

	// TODO: Notify payment service to start payment link

	return nil
}
