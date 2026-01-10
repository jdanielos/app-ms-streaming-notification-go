package email

type EntityEmailOtpResponse struct {
	Response string `json:"message"`
	Details  struct {
		ServiceExternal          string `json:"service_external"`
		ResponseServicesExternal string `json:"responses_ervices_external"`
	} `json:"response"`
}
