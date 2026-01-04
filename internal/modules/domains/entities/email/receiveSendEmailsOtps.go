package email

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/streamingNotifyHub/internal/modules/domains/entities/command"
)

var regValidationEmail = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)

// EntityLoginRequest es la entidad "limpia" y validada
type EntityEmailOtpRequest struct {
	Email                string `json:"email"`
	Code                 string `json:"code"`
	XFingerprint         string `json:"xfingerprint"`
	Subject              string `json:"subject"`
	TimeCodeVerification string `json:"time_code_verification"`
}

func NewEntityOtp(data command.SendOtpCommandRequest) (*EntityEmailOtpRequest, error) {

	if !regValidationEmail.MatchString(data.Email) {
		return nil, fmt.Errorf(`"Email invalido"`)
	}

	if len(data.Code) != 6 {
		return nil, fmt.Errorf(`"codigo otp no valido"`)
	}

	if isTrash(data.XFingerprint) {
		return nil, fmt.Errorf(`"huella digital del dispositivo inválida"`)
	}

	return &EntityEmailOtpRequest{
		Email:                data.Email,
		Code:                 data.Code,
		XFingerprint:         data.Email,
		Subject:              data.Subject,
		TimeCodeVerification: data.TimeCodeVerification,
	}, nil
}

func isTrash(s string) bool {
	s = strings.ToLower(s)
	if s == "" ||
		strings.Contains(s, "undefined") ||
		strings.Contains(s, "unknown") ||
		strings.Contains(s, "null") ||
		strings.Contains(s, "nan") ||
		strings.Contains(s, "not_found_header") {
		return true
	}
	return false
}
