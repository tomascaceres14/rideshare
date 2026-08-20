package domain

import (
	"context"
	pb "ride-sharing/shared/proto/trip"
	"ride-sharing/shared/types"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type TripModel struct {
	ID       primitive.ObjectID `json:"id" bson:"_id,omitempty":`
	UserID   string             `json:"userID"`
	Status   string             `json:"status"`
	RideFare *RideFareModel     `json:"ride_fare"`
	Driver   *pb.Driver
}

type OsrmAPIResponse struct {
	Routes []struct {
		Distance float64 `json:"distance"`
		Duration float64 `json:"duration"`
		Geometry struct {
			Coordinates [][]float64 `json:"coordinates"`
		} `json:"geometry"`
	} `json:"routes"`
}

type TripRepository interface {
	CreateTrip(ctx context.Context, trip *TripModel) (*TripModel, error)
	SaveRideFare(ctx context.Context, fare *RideFareModel) (*RideFareModel, error)
	GetRideFareByID(ctx context.Context, id string) (*RideFareModel, error)
	GetTripByID(ctx context.Context, tripID string) (*TripModel, error)
	UpdateTrip(ctx context.Context, tripID, status string, trip *pb.Driver) error
}

type TripService interface {
	CreateTrip(ctx context.Context, fare *RideFareModel) (*TripModel, error)
	GetRoute(ctx context.Context, pickup, destination *types.Coordinate) (*OsrmAPIResponse, error)
	SaveTripFares(ctx context.Context, fares []*RideFareModel, route *OsrmAPIResponse, userID string) ([]*RideFareModel, error)
	EstimatePackagesPriceWithRoute(route *OsrmAPIResponse) []*RideFareModel
	GetAndValidateFare(ctx context.Context, fareID, userID string) (*RideFareModel, error)
	GetTripByID(ctx context.Context, tripID string) (*TripModel, error)
	UpdateTrip(ctx context.Context, tripID, status string, driver *pb.Driver) error
}

func (o *OsrmAPIResponse) ToProto() *pb.Route {
	route := o.Routes[0]
	geometry := route.Geometry.Coordinates
	coordinates := make([]*pb.Coordinate, len(geometry))
	for i, coord := range geometry {
		coordinates[i] = &pb.Coordinate{
			Latitude:  coord[0],
			Longitude: coord[1],
		}
	}

	return &pb.Route{
		Geometry: []*pb.Geometry{
			{
				Coordinates: coordinates,
			},
		},
		Distance: route.Distance,
		Duration: route.Duration,
	}
}

func NewTripModel(status string, fare *RideFareModel) *TripModel {
	return &TripModel{
		ID:       primitive.NewObjectID(),
		UserID:   fare.UserID,
		Status:   status,
		RideFare: fare,
		Driver:   &pb.Driver{},
	}
}

func (t *TripModel) ToProto() *pb.Trip {
	return &pb.Trip{
		Id:           t.ID.Hex(),
		UserID:       t.UserID,
		Status:       t.Status,
		SelectedFare: t.RideFare.ToProto(),
		Route:        t.RideFare.Route.ToProto(),
		Driver:       t.Driver,
	}
}
