package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"ride-sharing/shared/contracts"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TripExchange = "trip"
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
	if r.Chan != nil {
		r.Chan.Close()
	}

	if r.conn != nil {
		r.conn.Close()
	}
}

func (r *RabbitMQ) setupExchangesAndQueues() error {

	err := r.Chan.ExchangeDeclare(
		TripExchange,       // name
		amqp.ExchangeTopic, // type
		false,              // durability
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return fmt.Errorf("Error declaring Exchange: %s. Error: %s", TripExchange, err)
	}

	err = r.declareAndBindQueue(
		FindAvailableDriversQueue,
		TripExchange,
		[]string{
			contracts.TripEventCreated,
			contracts.TripEventDriverNotInterested,
		},
	)
	if err != nil {
		return err
	}

	err = r.declareAndBindQueue(
		DriverCmdTripRequestQueue,
		TripExchange,
		[]string{
			contracts.DriverCmdTripRequest,
		},
	)
	if err != nil {
		return err
	}

	err = r.declareAndBindQueue(
		DriverTripResponseQueue,
		TripExchange,
		[]string{
			contracts.DriverCmdTripAccept,
			contracts.DriverCmdTripDecline,
		},
	)
	if err != nil {
		return err
	}

	err = r.declareAndBindQueue(
		NotifyRiderNoDriversFoundQueue,
		TripExchange,
		[]string{
			contracts.TripEventNoDriversFound,
		},
	)
	if err != nil {
		return err
	}

	err = r.declareAndBindQueue(
		NotifyDriverAssignQueue,
		TripExchange,
		[]string{
			contracts.TripEventDriverAssigned,
		},
	)
	if err != nil {
		return err
	}

	err = r.declareAndBindQueue(
		PaymentTripResponseQueue,
		TripExchange,
		[]string{
			contracts.PaymentCmdCreateSession,
		},
	)
	if err != nil {
		return err
	}

	err = r.declareAndBindQueue(
		PaymentTripResponseQueue,
		TripExchange,
		[]string{
			contracts.PaymentCmdCreateSession,
		},
	)
	if err != nil {
		return err
	}

	err = r.declareAndBindQueue(
		NotifyPaymentSessionCreatedQueue,
		TripExchange,
		[]string{
			contracts.PaymentEventSessionCreated,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQ) declareAndBindQueue(queueName string, exchange string, routingKeys []string) error {
	q, err := r.Chan.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		return fmt.Errorf("Error declaring Queue: %s. Error: %s", q.Name, err)
	}

	for _, v := range routingKeys {
		err = r.Chan.QueueBind(
			q.Name,   // queue name
			v,        // routing key
			exchange, // exchange
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("Error declaring QueueBind: %s. Error: %s", q.Name, err)
		}
	}

	return nil
}

func (r *RabbitMQ) PublishMessage(ctx context.Context, routingKey string, msg contracts.AmqpMessage) error {
	log.Printf("Publishing msg with routing key: %s", routingKey)

	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("Error marshaling message: %s", err)
	}

	return r.Chan.PublishWithContext(ctx,
		TripExchange, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         jsonMsg,
			DeliveryMode: amqp.Persistent,
		})
}

type MessageHandler func(context.Context, amqp.Delivery) error

func (r *RabbitMQ) ConsumeMessages(qName string, handler MessageHandler) error {

	err := r.Chan.Qos(
		1,     // prefetchCount: Limit to 1 unacknowledged message per consumer
		0,     // prefetchSize: No specific limit on message size
		false, // global Apply prefetchCount to each consumer individually
	)
	if err != nil {
		return fmt.Errorf("Failed to set Qos: %s", err)
	}

	msgs, err := r.Chan.Consume(
		qName, // queue
		"",    // consumer
		false, // auto-ack
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
			if err := handler(ctx, msg); err != nil {
				log.Printf("ERROR: Failed to handle message: %v.", err)
				if nackErr := msg.Nack(false, false); nackErr != nil {
					log.Printf("ERROR: Failed to nack msg: %s", nackErr)
				}
				continue
			}

			if ackErr := msg.Ack(false); ackErr != nil {
				log.Printf("ERROR: Failed to ack msg: %s", ackErr)
			}
		}
	}()
	return nil
}
