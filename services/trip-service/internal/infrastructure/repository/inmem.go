package repository

import (
	"context"
	"fmt"
	"ride-sharing/services/trip-service/internal/domain"
)

type InmemRepository struct {
	trips     map[string]*domain.TripModel
	rideFares map[string]*domain.RideFareModel
}

func NewInmemRepository() *InmemRepository {
	return &InmemRepository{
		trips:     make(map[string]*domain.TripModel),
		rideFares: make(map[string]*domain.RideFareModel),
	}
}

func (r *InmemRepository) CreateTrip(ctx context.Context, trip *domain.TripModel) (*domain.TripModel, error) {
	r.trips[trip.ID.Hex()] = trip
	return trip, nil
}

func (r *InmemRepository) SaveRideFare(ctx context.Context, fare *domain.RideFareModel) (*domain.RideFareModel, error) {
	r.rideFares[fare.ID.Hex()] = fare
	return fare, nil
}

func (r *InmemRepository) GetRideFareByID(ctx context.Context, id string) (*domain.RideFareModel, error) {
	v, ok := r.rideFares[id]
	if !ok {
		return nil, fmt.Errorf("RideFare with id: %s not found", id)
	}

	return v, nil
}

func (r *InmemRepository) GetTripByID(ctx context.Context, tripID string) (*domain.TripModel, error) {
	trip, ok := r.trips[tripID]
	if !ok {
		return nil, fmt.Errorf("Trip not found.")
	}

	return trip, nil
}
func (r *InmemRepository) UpdateTrip(ctx context.Context, trip *domain.TripModel) (*domain.TripModel, error) {
	r.trips[trip.ID.Hex()] = trip
	return trip, nil
}
