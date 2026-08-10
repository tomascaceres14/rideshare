package events

import (
	"context"
	"log"
	"ride-sharing/shared/messaging"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TripEventConsumer struct {
	rabbitMQ *messaging.RabbitMQ
}

func NewTripEventPublisher(rmq *messaging.RabbitMQ) *TripEventConsumer {
	return &TripEventConsumer{
		rabbitMQ: rmq,
	}
}

func (tc *TripEventConsumer) Listen() error {
	tc.rabbitMQ.ConsumeMessages("hello", func(ctx context.Context, d amqp.Delivery) error {
		log.Printf("Driver received message: %v", d.Body)
		return nil
	})
	return nil
}
