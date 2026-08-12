package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"ride-sharing/services/trip-service/internal/domain"
	"ride-sharing/services/trip-service/internal/infrastructure/repository"
	"ride-sharing/shared/types"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	BASE_OSRM_URL = "http://router.project-osrm.org"
)

type TripService struct {
	repo domain.TripRepository
}

func NewTripService(repo *repository.InmemRepository) *TripService {
	return &TripService{
		repo: repo,
	}
}

func (s *TripService) CreateTrip(ctx context.Context, fare *domain.RideFareModel) (*domain.TripModel, error) {
	trip := domain.NewTripModel("ok", fare)
	tripDB, err := s.repo.CreateTrip(ctx, trip)
	if err != nil {
		return nil, err
	}
	return tripDB, nil
}

func (s *TripService) GetRoute(ctx context.Context, pickup, destination *types.Coordinate) (*domain.OsrmAPIResponse, error) {
	url := fmt.Sprintf("%s/route/v1/driving/%f,%f;%f,%f?overview=full&geometries=geojson", BASE_OSRM_URL,
		pickup.Longitude, pickup.Latitude,
		destination.Longitude, destination.Latitude)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error fetching OSRM API: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading OSRM API response: %v", err)
	}

	var routeResponse *domain.OsrmAPIResponse
	if err := json.Unmarshal(body, &routeResponse); err != nil {
		return nil, fmt.Errorf("error unparsing OSRM API response: %v", err)
	}

	return routeResponse, nil
}

func (s *TripService) SaveTripFares(ctx context.Context, rideFares []*domain.RideFareModel, route *domain.OsrmAPIResponse, userID string) ([]*domain.RideFareModel, error) {
	fares := make([]*domain.RideFareModel, len(rideFares))

	for i, v := range rideFares {
		id := primitive.NewObjectID()

		fare := &domain.RideFareModel{
			ID:                id,
			UserID:            userID,
			PackageSlug:       v.PackageSlug,
			TotalPriceInCents: v.TotalPriceInCents,
			Route:             route,
		}

		fare, err := s.repo.SaveRideFare(ctx, fare)
		if err != nil {
			return nil, fmt.Errorf("Failed to save fare to db: %s", err)
		}

		fares[i] = fare
	}

	return fares, nil
}

func (s *TripService) EstimatePackagesPriceWithRoute(route *domain.OsrmAPIResponse) []*domain.RideFareModel {
	baseFares := getBaseFares()
	estimatedFares := make([]*domain.RideFareModel, len(baseFares))

	for i, v := range baseFares {
		estimatedFares[i] = estimateFareRoute(v, route)
	}

	return estimatedFares
}

func (s *TripService) GetAndValidateFare(ctx context.Context, fareID, userID string) (*domain.RideFareModel, error) {

	rideFare, err := s.repo.GetRideFareByID(ctx, fareID)
	if err != nil {
		return nil, err
	}

	if rideFare == nil {
		return nil, fmt.Errorf("Ride fare does not exists.")
	}

	if rideFare.UserID != userID {
		return nil, fmt.Errorf("Ride fare does not belong to user id: %s. RideFare.UserID=%s", userID, rideFare.UserID)
	}

	return rideFare, nil
}

func estimateFareRoute(f *domain.RideFareModel, route *domain.OsrmAPIResponse) *domain.RideFareModel {
	pricing := domain.DefaultPricing()
	distance := route.Routes[0].Distance * pricing.PricePerDistanceUnit
	time := route.Routes[0].Duration * pricing.PricePerMinute
	price := distance + time + f.TotalPriceInCents
	return &domain.RideFareModel{
		TotalPriceInCents: price,
		PackageSlug:       f.PackageSlug,
	}
}

func getBaseFares() []*domain.RideFareModel {
	return []*domain.RideFareModel{
		{
			PackageSlug:       "suv",
			TotalPriceInCents: 200,
		},
		{
			PackageSlug:       "sedan",
			TotalPriceInCents: 350,
		},
		{
			PackageSlug:       "van",
			TotalPriceInCents: 400,
		},
		{
			PackageSlug:       "luxury",
			TotalPriceInCents: 1000,
		},
	}
}
