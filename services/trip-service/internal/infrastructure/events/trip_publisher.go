package events

import (
	"context"
	"encoding/json"
	"fmt"
	"ride-sharing/services/trip-service/internal/domain"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/messaging"
)

type TripEventPublisher struct {
	rabbitMQ *messaging.RabbitMQ
}

func NewTripEventPublisher(rmq *messaging.RabbitMQ) *TripEventPublisher {
	return &TripEventPublisher{
		rabbitMQ: rmq,
	}
}

func (tp *TripEventPublisher) PublishTripCreated(ctx context.Context, trip *domain.TripModel) error {

	jsonTrip, err := json.Marshal(trip.ToProto())
	if err != nil {
		return fmt.Errorf("Error marshaling trip response: %s", err)
	}

	msg := contracts.AmqpMessage{
		Data: jsonTrip,
	}

	return tp.rabbitMQ.PublishMessage(ctx, contracts.TripEventCreated, msg)
}
