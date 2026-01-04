package command

type SendOtpCommandRequest struct {
	Email                string `json:"email"`
	Code                 string `json:"code"`
	XFingerprint         string `json:"xfingerprint"`
	Subject              string `json:"subject"`
	TimeCodeVerification string `json:"time_code_verification"`
}
