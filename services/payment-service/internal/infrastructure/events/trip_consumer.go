package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"ride-sharing/services/payment-service/internal/domain"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/messaging"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TripEventConsumer struct {
	rabbitMQ *messaging.RabbitMQ
	svc      domain.Service
}

func NewTripEventConsumer(rmq *messaging.RabbitMQ, svc domain.Service) *TripEventConsumer {
	return &TripEventConsumer{
		rabbitMQ: rmq,
		svc:      svc,
	}
}

func (tc *TripEventConsumer) Listen() error {
	tc.rabbitMQ.ConsumeMessages(messaging.PaymentTripResponseQueue, func(ctx context.Context, msg amqp.Delivery) error {
		var tripEvent contracts.AmqpMessage
		if err := json.Unmarshal(msg.Body, &tripEvent); err != nil {
			fmt.Printf("Error unmarshaling message: %s", err)
			return err
		}

		var payload messaging.PaymentTripResponseData
		if err := json.Unmarshal(tripEvent.Data, &payload); err != nil {
			log.Printf("Error unmarshaling payload: %s", err)
			return err
		}

		switch msg.RoutingKey {
		case contracts.PaymentCmdCreateSession:
			if err := tc.handleTripAccepted(ctx, payload); err != nil {
				log.Printf("Error handling trip accepted: %v", err)
				return err
			}
		}

		log.Printf("Unknown trip event: %s", msg.RoutingKey)
		return nil
	})
	return nil
}

func (tc *TripEventConsumer) handleTripAccepted(ctx context.Context, response messaging.PaymentTripResponseData) error {
	intent, err := tc.svc.CreatePaymentSession(ctx, response.TripID, response.UserID, response.DriverID, int64(response.Amount), response.Currency)
	if err != nil {
		return err
	}

	log.Printf("Payment session created: %s", intent.StripeSessionID)

	// Publish payment session created event
	message, err := json.Marshal(messaging.PaymentEventSessionCreatedData{
		TripID:    response.TripID,
		SessionID: intent.StripeSessionID,
		Amount:    float64(intent.Amount) / 100.0, // cents to dollars
		Currency:  intent.Currency,
	})
	if err != nil {
		log.Printf("Failed to marshal payment session payload: %v", err)
		return err
	}

	if err := tc.rabbitMQ.PublishMessage(ctx, contracts.PaymentEventSessionCreated, contracts.AmqpMessage{
		Data:    message,
		OwnerID: response.UserID,
	}); err != nil {
		log.Printf("Error sending message: %s. Error: %v", contracts.PaymentEventSessionCreated, err)
		return err
	}

	return nil
}
