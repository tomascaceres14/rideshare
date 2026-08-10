package events

import (
	"context"
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

func (tp *TripEventPublisher) PublishTripCreated(ctx context.Context) error {
	return tp.rabbitMQ.PublishMessage(ctx, "hello", "Hola")
}
