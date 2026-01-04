package email

type EntityEmailOtpResponse struct {
	Message string `json:"message"`
	Details struct {
		ServiceExternal          string `json:"service_external"`
		ResponseServicesExternal string `json:"responses_ervices_external"`
	} `json:"message "`
}
