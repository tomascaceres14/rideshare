package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	conn *amqp.Connection
	Chan *amqp.Channel
}

func NewRabbitMQ(uri string) (*RabbitMQ, error) {
	if uri == "" {
		return nil, errors.New("No URI provided")
	}
	// RabbitMQ Connection
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, fmt.Errorf("Error connecting to RabbitMQ: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("Failed to create channel: %s", err)
	}

	rabbit := &RabbitMQ{
		conn: conn,
		Chan: ch,
	}

	if err := rabbit.setupExchangesAndQueues(); err != nil {
		rabbit.Close()
		return nil, fmt.Errorf("Failed setting up exchanges and queues: %s", err)
	}

	return rabbit, nil
}

func (r *RabbitMQ) Close() {
	if r.conn != nil {
		r.Close()
	}

	if r.Chan != nil {
		r.Chan.Close()
	}
}

func (r *RabbitMQ) setupExchangesAndQueues() error {
	_, err := r.Chan.QueueDeclare(
		"hello", // name
		true,    // durability
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		return fmt.Errorf("Error declaring queue: %s", err)
	}

	return nil
}

func (r *RabbitMQ) PublishMessage(ctx context.Context, routingKey string, message string) error {
	return r.Chan.PublishWithContext(ctx,
		"",         // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(message),
			DeliveryMode: amqp.Persistent,
		})
}

type MessageHandler func(context.Context, amqp.Delivery) error

func (r *RabbitMQ) ConsumeMessages(qName string, handler MessageHandler) error {
	msgs, err := r.Chan.Consume(
		qName, // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)

	if err != nil {
		return err
	}

	ctx := context.Background()

	go func() {
		for msg := range msgs {
			log.Printf("Message received: %s", msg.Body)
			if err := handler(ctx, msg); err != nil {
				log.Fatalf("Failed to handle message: %v", err)
			}
		}
	}()
	return nil
}

var forever chan struct{}
