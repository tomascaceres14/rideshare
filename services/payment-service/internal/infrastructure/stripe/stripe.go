package stripeProcessor

import (
	"context"
	"ride-sharing/services/payment-service/internal/domain"
	"ride-sharing/services/payment-service/pkg/types"

	"github.com/stripe/stripe-go/v86"
)

type StripeClient struct {
	config *types.PaymentConfig
}

func NewStripeClient(cfg *types.PaymentConfig) domain.PaymentProcessor {

	stripe.Key = cfg.StripeSecretKey
	return &StripeClient{
		config: cfg,
	}
}

func (c *StripeClient) CreatePaymentSession(ctx context.Context, amount int64, currency string, metadata map[string]string) (string, error) {
	sc := stripe.NewClient(c.config.StripeSecretKey)
	params := &stripe.CheckoutSessionCreateParams{
		SuccessURL: stripe.String(c.config.SuccessURL),
		CancelURL:  stripe.String(c.config.CancelURL),
		LineItems: []*stripe.CheckoutSessionCreateLineItemParams{
			{
				PriceData: &stripe.CheckoutSessionCreateLineItemPriceDataParams{
					Currency: stripe.String(currency),
					ProductData: &stripe.CheckoutSessionCreateLineItemPriceDataProductDataParams{
						Name: stripe.String("Ride Payment"),
					},
					UnitAmount: stripe.Int64(amount),
				},
				Quantity: stripe.Int64(1),
			},
		},
		Mode:     stripe.String(stripe.CheckoutSessionModePayment),
		Metadata: metadata,
	}
	result, err := sc.V1CheckoutSessions.Create(context.TODO(), params)
	if err != nil {
		return "", err
	}

	return result.ID, nil
}
