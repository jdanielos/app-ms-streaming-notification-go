package command

type AuthenticatedUserCommand struct {
	Email                string `json:"email"`
	Code                 string `json:"code"`
	XFingerPrint         string `json:"xfingerprint"`
	Subject              string `json:"subject"`
	TimeCodeVerification string `json:"time_code_verification"`
	Metadata             map[string]string
}
