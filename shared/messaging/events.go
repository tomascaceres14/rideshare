package messaging

import pb "ride-sharing/shared/proto/trip"

const (
	FindAvailableDriversQueue      = "find_available_drivers"
	DriverCmdTripRequestQueue      = "driver_cmd_trip_request"
	DriverTripResponseQueue        = "driver_trip_response"
	NotifyRiderNoDriversFoundQueue = "notify_rider_no_drivers_found"
	NotifyDriverAssignQueue        = "notify_driver_assign_queue"
)

type DriverTripResponseData struct {
	Driver  *pb.Driver `json:"driver"`
	TripID  string     `json:"tripID"`
	RiderID string     `json:"riderID"`
}
