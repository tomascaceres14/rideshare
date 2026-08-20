package domain

import (
	pb "ride-sharing/shared/proto/trip"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type RideFareModel struct {
	ID                primitive.ObjectID `bson:"_id,omitempty"`
	UserID            string
	PackageSlug       string // ex: van, luxury, sedan, etc.
	TotalPriceInCents float64
	Route             *OsrmAPIResponse
}

type PricingConfig struct {
	PricePerDistanceUnit, PricePerMinute float64
}

func DefaultPricing() *PricingConfig {
	return &PricingConfig{
		PricePerDistanceUnit: 1.5,
		PricePerMinute:       0.25,
	}
}

func (r *RideFareModel) ToProto() *pb.RideFare {
	return &pb.RideFare{
		Id:                r.ID.Hex(),
		UserID:            r.UserID,
		PackageSlug:       r.PackageSlug,
		TotalPriceInCents: r.TotalPriceInCents,
	}
}

func ToRideFaresProto(list []*RideFareModel) []*pb.RideFare {
	l := make([]*pb.RideFare, len(list))

	for i, v := range list {
		l[i] = v.ToProto()
	}

	return l
}
