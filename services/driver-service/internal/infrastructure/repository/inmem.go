package repository

import (
	"context"
	"errors"
	"maps"
	"ride-sharing/services/driver-service/internal/domain"
	"slices"
)

type InmemRepository struct {
	drivers map[string]*domain.Driver
}

func NewInmemRepository() *InmemRepository {
	return &InmemRepository{
		drivers: make(map[string]*domain.Driver),
	}
}

func (r *InmemRepository) SaveDriver(ctx context.Context, driver *domain.Driver) (*domain.Driver, error) {
	r.drivers[driver.Id] = driver
	return driver, nil
}

func (r *InmemRepository) DeleteDriver(ctx context.Context, id string) {
	delete(r.drivers, id)
}

func (r *InmemRepository) GetAllDrivers(ctx context.Context) ([]*domain.Driver, error) {
	var drivers []*domain.Driver
	drivers = slices.Collect(maps.Values(r.drivers))
	if drivers == nil {
		return nil, errors.New("No drivers found")
	}
	return drivers, nil
}
