package domain

import (
	"context"
	pb "ride-sharing/shared/proto/driver"
	"ride-sharing/shared/types"
)

type Driver struct {
	Id, Name, ProfilePicture, CarPlate, GeoHash, PackageSlug string
	Location                                                 types.Coordinate
}

func (d *Driver) ToProto() *pb.Driver {
	return &pb.Driver{
		Id:             d.Id,
		Name:           d.Name,
		ProfilePicture: d.ProfilePicture,
		CarPlate:       d.CarPlate,
		Geohash:        d.GeoHash,
		PackageSlug:    d.PackageSlug,
		Location: &pb.Location{
			Latitude:  d.Location.Latitude,
			Longitude: d.Location.Longitude,
		},
	}
}

type DriverRepository interface {
	SaveDriver(ctx context.Context, driver *Driver) (*Driver, error)
	DeleteDriver(ctx context.Context, id string)
	GetAllDrivers(ctx context.Context) ([]*Driver, error)
}

type DriverService interface {
	RegisterDriver(ctx context.Context, id, packageSlug string) (*Driver, error)
	UnregisterDriver(ctx context.Context, id string)
	FindAvailableDrivers(ctx context.Context, pkgType string) []string
}
