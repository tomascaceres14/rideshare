package messaging

import pb "ride-sharing/shared/proto/trip"

const (
	FindAvailableDriversQueue        = "find_available_drivers"
	DriverCmdTripRequestQueue        = "driver_cmd_trip_request"
	DriverTripResponseQueue          = "driver_trip_response"
	NotifyRiderNoDriversFoundQueue   = "notify_rider_no_drivers_found"
	NotifyDriverAssignQueue          = "notify_driver_assign"
	PaymentTripResponseQueue         = "payment_trip_response"
	NotifyPaymentSessionCreatedQueue = "notify_payment_session_created"
	NotifyPaymentSuccessQueue        = "payment_success"
)

type DriverTripResponseData struct {
	Driver  *pb.Driver `json:"driver"`
	TripID  string     `json:"tripID"`
	RiderID string     `json:"riderID"`
}

type PaymentEventSessionCreatedData struct {
	TripID    string  `json:"tripID"`
	SessionID string  `json:"sessionID"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

type PaymentTripResponseData struct {
	Amount   float64 `json:"amount"`
	TripID   string  `json:"tripID"`
	UserID   string  `json:"userID"`
	DriverID string  `json:"driverID"`
	Currency string  `json:"currency"`
}

type PaymentStatusUpdateData struct {
	TripID   string `json:"tripID"`
	UserID   string `json:"userID"`
	DriverID string `json:"driverID"`
}
