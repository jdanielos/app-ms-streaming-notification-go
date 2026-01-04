package dto

type SendDtoRequest struct {
	Email        string `json:"email"`
	Code         string `json:"code"`
	XFingerprint string `xfingerprint"`
}
